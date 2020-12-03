//! Package implement WRite-Ahead-Logging.
//!
//! Entries are added to `Wral` journal. Journals automatically rotate
//! and are numbered from ZERO.

use arbitrary::Arbitrary;
use log::debug;
use mkit::{self, thread};

use std::{
    ffi, fs, ops, path,
    sync::{Arc, RwLock},
    time, vec,
};

use crate::{entry, journal, journal::Journal, state, writer, Error, Result};

/// Default journal file limit is set at 1GB.
pub const JOURNAL_LIMIT: usize = 1 * 1024 * 1024 * 1024;
/// Default batch-size for flush is set at 1MB.
pub const BATCH_SIZE: usize = 1 * 1024 * 1024; // 1MB.
/// Default batch-period for flush is set at 10ms.
pub const BATCH_PERIOD: time::Duration = time::Duration::from_micros(10 * 1000); // 10ms

pub const SYNC_BUFFER: usize = 1024;

/// Configuration for [Wral] type.
#[derive(Debug, Clone, Arbitrary)]
pub struct Config {
    /// Uniquely name Wral instances.
    pub name: String,
    /// Directory in which wral journals are stored.
    pub dir: ffi::OsString,
    /// Define file-size limit for a single journal file, beyond with
    /// journal files are rotated.
    pub journal_limit: usize,
    /// Define flush batch-size, large batches can give better throughput
    /// at the expense of data-loss.
    pub batch_size: usize,
    /// Define flush period, large batch-period can give better throughput
    /// at the expense of data-loss.
    pub batch_period: time::Duration, // in micro-seconds.
    /// Enable fsync for every flush.
    pub fsync: bool,
}

impl Config {
    pub fn new(name: &str, dir: &ffi::OsStr) -> Config {
        Config {
            name: name.to_string(),
            dir: dir.to_os_string(),
            journal_limit: JOURNAL_LIMIT,
            batch_size: BATCH_SIZE,
            batch_period: BATCH_PERIOD,
            fsync: true,
        }
    }

    pub fn set_journal_limit(&mut self, journal_limit: usize) -> &mut Self {
        self.journal_limit = journal_limit;
        self
    }

    pub fn set_batch_size(&mut self, batch_size: usize) -> &mut Self {
        self.batch_size = batch_size;
        self
    }

    pub fn set_batch_period(&mut self, batch_period: time::Duration) -> &mut Self {
        self.batch_period = batch_period;
        self
    }

    pub fn set_fsync(&mut self, fsync: bool) -> &mut Self {
        self.fsync = fsync;
        self
    }
}

/// Write alhead logging.
pub struct Wral<S = state::NoState> {
    config: Config,

    tx: thread::Tx<writer::Req, writer::Res>,
    t: Option<Arc<mkit::thread::Thread<writer::Req, writer::Res, Result<u64>>>>,
    w: Option<Arc<RwLock<writer::Writer<S>>>>,
}

impl<S> Clone for Wral<S> {
    fn clone(&self) -> Wral<S> {
        Wral {
            config: self.config.clone(),

            tx: self.tx.clone(),
            t: self.t.as_ref().map(|t| Arc::clone(t)),
            w: self.w.as_ref().map(|w| Arc::clone(w)),
        }
    }
}

impl<S> Wral<S> {
    /// Create a new Write-Ahead-Log instance, while create a new journal,
    /// older journals matching the `name` shall be purged.
    pub fn create(config: Config, state: S) -> Result<Wral<S>>
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
                    Err(err) => debug!(target: "wral", "failed to purge {:?}, {}", file_path, err),
                },
                None => continue,
            };
        }

        let num = 0;
        let journal = Journal::start(&config.name, &config.dir, num, state)?;

        debug!(target: "wral", "{:?}/{} created", &config.dir, &config.name);

        let seqno = 1;
        let (w, t) = writer::Writer::start(config.clone(), vec![], journal, seqno);

        let val = Wral {
            config,

            tx: t.clone_tx(),
            t: Some(Arc::new(t)),
            w: Some(w),
        };

        Ok(val)
    }

    /// Load an existing journal under `dir`, matching `name`. Files that
    /// don't match the journaling file-name structure or journals with
    /// corrupted batch or corrupted state shall be ignored.
    ///
    /// Application state shall be loaded from the last batch of the
    /// last journal.
    pub fn load(config: Config) -> Result<Wral<S>>
    where
        S: Default + state::State,
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
        let (w, t) = writer::Writer::start(config.clone(), journals, journal, seqno);

        let val = Wral {
            config,

            tx: t.clone_tx(),
            t: Some(Arc::new(t)),
            w: Some(w),
        };

        Ok(val)
    }

    /// Close the [Wral] instance. To purge the instance use [Wral::purge] api.
    pub fn close(&mut self) -> Result<Option<u64>> {
        match Arc::try_unwrap(self.t.take().unwrap()) {
            Ok(t) => {
                (t.close_wait()?)?;
            }
            Err(t) => {
                self.t = Some(t);
            }
        }
        match Arc::try_unwrap(self.w.take().unwrap()) {
            Ok(w) => {
                let w = err_at!(IPCFail, w.into_inner())?;
                Ok(Some(w.close()?))
            }
            Err(w) => {
                self.w = Some(w);
                Ok(None)
            }
        }
    }

    /// Purge this [Wral] instance and all its memory and disk footprints.
    pub fn purge(mut self) -> Result<Option<u64>> {
        match Arc::try_unwrap(self.t.take().unwrap()) {
            Ok(t) => {
                (t.close_wait()?)?;
            }
            Err(t) => {
                self.t = Some(t);
            }
        }
        match Arc::try_unwrap(self.w.take().unwrap()) {
            Ok(w) => {
                let w = err_at!(IPCFail, w.into_inner())?;
                Ok(Some(w.purge()?))
            }
            Err(w) => {
                self.w = Some(w);
                Ok(None)
            }
        }
    }
}

impl<S> Wral<S> {
    /// Add a operation to WAL, operations are pre-serialized and opaque to
    /// Wral instances. Return the sequence-number for this operation.
    pub fn add_op(&self, op: &[u8]) -> Result<u64> {
        let req = writer::Req::AddEntry { op: op.to_vec() };
        let seqno = match self.tx.request(req)? {
            writer::Res::Seqno(seqno) => seqno,
        };
        Ok(seqno)
    }
}

impl<S> Wral<S> {
    /// Iterate over all entries in this Wral instance, entries can span
    /// scross multiple journal files. Iteration will start from lowest
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
                let rd = {
                    let m = self.w.as_ref().unwrap();
                    err_at!(Fatal, m.read())?
                };
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
