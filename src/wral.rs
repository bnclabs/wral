//! Package implement WRite-Ahead-Logging.
//!
//! Entries are added to `Wal` journal. Journals automatically rotate
//! and are numbered from ZERO.

use arbitrary::{Arbitrary, Unstructured};
use log::debug;
use mkit::{self, thread};

use std::{
    ffi, fs, mem, ops, path,
    sync::{Arc, RwLock},
    vec,
};

use crate::{entry, journal, journal::Journal, state, writer, Error, Result};

/// Default journal file limit is set at 1GB.
pub const JOURNAL_LIMIT: usize = 1024 * 1024 * 1024;
/// Default channel buffer for writer thread.
pub const SYNC_BUFFER: usize = 1024;

/// Configuration for [Wal] type.
#[derive(Debug, Clone)]
pub struct Config {
    /// Uniquely name Wal instances.
    pub name: String,
    /// Directory in which wral journals are stored.
    pub dir: ffi::OsString,
    /// Define file-size limit for a single journal file, beyond with
    /// journal files are rotated.
    pub journal_limit: usize,
    /// Enable fsync for every flush.
    pub fsync: bool,
}

impl Arbitrary for Config {
    fn arbitrary(u: &mut Unstructured) -> arbitrary::Result<Self> {
        let name: String = u.arbitrary()?;
        let dir = tempfile::tempdir().unwrap().path().into();

        let journal_limit = *u.choose(&[100, 1000, 10_000, 1_000_000])?;
        let fsync: bool = u.arbitrary()?;

        let config = Config {
            name,
            dir,
            journal_limit,
            fsync,
        };
        Ok(config)
    }
}

impl Config {
    pub fn new(name: &str, dir: &ffi::OsStr) -> Config {
        Config {
            name: name.to_string(),
            dir: dir.to_os_string(),
            journal_limit: JOURNAL_LIMIT,
            fsync: true,
        }
    }

    pub fn set_journal_limit(&mut self, journal_limit: usize) -> &mut Self {
        self.journal_limit = journal_limit;
        self
    }

    pub fn set_fsync(&mut self, fsync: bool) -> &mut Self {
        self.fsync = fsync;
        self
    }
}

/// Write ahead logging.
pub struct Wal<S = state::NoState> {
    config: Config,

    tx: thread::Tx<writer::Req, writer::Res>,
    t: Arc<RwLock<mkit::thread::Thread<writer::Req, writer::Res, Result<u64>>>>,
    w: Arc<RwLock<writer::Writer<S>>>,
}

impl<S> Clone for Wal<S> {
    fn clone(&self) -> Wal<S> {
        Wal {
            config: self.config.clone(),

            tx: self.tx.clone(),
            t: Arc::clone(&self.t),
            w: Arc::clone(&self.w),
        }
    }
}

impl<S> Wal<S> {
    /// Create a new Write-Ahead-Log instance, while create a new journal,
    /// older journals matching the `name` shall be purged.
    pub fn create(config: Config, state: S) -> Result<Wal<S>>
    where
        S: state::State,
    {
        // try creating the directory, if it does not exist.
        fs::create_dir_all(&config.dir).ok();

        // purge existing journals for this shard.
        for item in err_at!(IOError, fs::read_dir(&config.dir))? {
            let file_path: path::PathBuf = {
                let file_name = err_at!(IOError, item)?.file_name();
                [config.dir.clone(), file_name.clone()].iter().collect()
            };
            match Journal::<S>::load_cold(&config.name, file_path.as_ref()) {
                Some(journal) => match journal.purge() {
                    Ok(_) => (),
                    Err(err) => {
                        debug!(target: "wral", "failed to purge {:?}, {}", file_path, err)
                    }
                },
                None => continue,
            };
        }

        let num = 0;
        let journal = Journal::start(&config.name, &config.dir, num, state)?;

        debug!(target: "wral", "{:?}/{} created", &config.dir, &config.name);

        let seqno = 1;
        let (w, t, tx) = writer::Writer::start(config.clone(), vec![], journal, seqno);

        let val = Wal {
            config,

            tx,
            t: Arc::new(RwLock::new(t)),
            w,
        };

        Ok(val)
    }

    /// Load an existing journal under `dir`, matching `name`. Files that
    /// don't match the journal file-name structure or journals with
    /// corrupted batch or corrupted state shall be ignored.
    ///
    /// Application state shall be loaded from the last batch of the
    /// last journal.
    pub fn load(config: Config) -> Result<Wal<S>>
    where
        S: state::State,
    {
        let mut journals: Vec<(Journal<S>, u64, S)> = vec![];
        for item in err_at!(IOError, fs::read_dir(&config.dir))? {
            let file_path: path::PathBuf = {
                let file_name = err_at!(IOError, item)?.file_name();
                [config.dir.clone(), file_name.clone()].iter().collect()
            };
            match Journal::load(&config.name, file_path.as_ref()) {
                Some((journal, state)) => {
                    let seqno = journal.to_last_seqno().unwrap();
                    journals.push((journal, seqno, state));
                }
                None => debug!(target: "wral", "failed to load {:?}", file_path),
            };
        }

        journals.sort_by(|(_, a, _), (_, b, _)| a.cmp(b));

        let (mut seqno, num, state) = match journals.last() {
            Some((j, seqno, state)) => (*seqno, j.to_journal_number(), state.clone()),
            None => (0, 0, S::default()),
        };
        seqno += 1;
        let num = num.saturating_add(1);
        let journal = Journal::start(&config.name, &config.dir, num, state)?;

        let n_batches: usize = journals.iter().map(|(j, _, _)| j.len_batches()).sum();
        debug!(
            target: "wral",
            "{:?}/{} loaded with {} journals, {} batches",
            config.dir, config.name, journals.len(), n_batches
        );

        let journals: Vec<Journal<S>> = journals.into_iter().map(|(j, _, _)| j).collect();
        let (w, t, tx) = writer::Writer::start(config.clone(), journals, journal, seqno);

        let val = Wal {
            config,

            tx,
            t: Arc::new(RwLock::new(t)),
            w,
        };

        Ok(val)
    }

    /// Close the [Wal] instance. To purge the instance pass `purge` as true.
    pub fn close(self, purge: bool) -> Result<Option<u64>> {
        match Arc::try_unwrap(self.t) {
            Ok(t) => {
                mem::drop(self.tx);
                (err_at!(IPCFail, t.into_inner())?.join()?)?;

                match Arc::try_unwrap(self.w) {
                    Ok(w) => {
                        let w = err_at!(IPCFail, w.into_inner())?;
                        Ok(Some(if purge { w.purge()? } else { w.close()? }))
                    }
                    Err(_) => Ok(None), // there are active clones
                }
            }
            Err(_) => Ok(None), // there are active clones
        }
    }
}

impl<S> Wal<S> {
    /// Add a operation to WAL, operations are pre-serialized and opaque to
    /// Wal instances. Return the sequence-number for this operation.
    pub fn add_op(&self, op: &[u8]) -> Result<u64> {
        let req = writer::Req::AddEntry { op: op.to_vec() };
        let writer::Res::Seqno(seqno) = self.tx.request(req)?;
        Ok(seqno)
    }
}

impl<S> Wal<S> {
    /// Iterate over all entries in this Wal instance, entries can span
    /// across multiple journal files. Iteration will start from lowest
    /// sequence-number to highest.
    pub fn iter(&self) -> Result<impl Iterator<Item = Result<entry::Entry>>> {
        self.range(..)
    }

    /// Iterate over entries whose sequence number fall within the
    /// specified `range`.
    pub fn range<R>(&self, range: R) -> Result<impl Iterator<Item = Result<entry::Entry>>>
    where
        R: ops::RangeBounds<u64>,
    {
        let journals = match Self::range_bound_to_range_inclusive(range) {
            Some(range) => {
                let rd = err_at!(Fatal, self.w.read())?;
                let mut journals = vec![];
                for jn in rd.journals.iter() {
                    journals.push(journal::RdJournal::from_journal(jn, range.clone())?);
                }
                journals.push(journal::RdJournal::from_journal(&rd.journal, range)?);
                journals
            }
            None => vec![],
        };

        Ok(Iter {
            journal: None,
            journals: journals.into_iter(),
        })
    }

    fn range_bound_to_range_inclusive<R>(range: R) -> Option<ops::RangeInclusive<u64>>
    where
        R: ops::RangeBounds<u64>,
    {
        let start = match range.start_bound() {
            ops::Bound::Excluded(start) if *start < u64::MAX => Some(*start + 1),
            ops::Bound::Excluded(_) => None,
            ops::Bound::Included(start) => Some(*start),
            ops::Bound::Unbounded => Some(0),
        }?;
        let end = match range.end_bound() {
            ops::Bound::Excluded(0) => None,
            ops::Bound::Excluded(end) => Some(*end - 1),
            ops::Bound::Included(end) => Some(*end),
            ops::Bound::Unbounded => Some(u64::MAX),
        }?;
        Some(start..=end)
    }
}

struct Iter {
    journal: Option<journal::RdJournal>,
    journals: vec::IntoIter<journal::RdJournal>,
}

impl Iterator for Iter {
    type Item = Result<entry::Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut journal = match self.journal.take() {
            Some(journal) => journal,
            None => self.journals.next()?,
        };
        loop {
            match journal.next() {
                Some(item) => {
                    self.journal = Some(journal);
                    return Some(item);
                }
                None => match self.journals.next() {
                    Some(j) => {
                        journal = j;
                    }
                    None => {
                        return None;
                    }
                },
            }
        }
    }
}

#[cfg(test)]
#[path = "wral_test.rs"]
mod wral_test;
