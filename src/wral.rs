//! Package implement WRite-Ahead-Logging.
//!
//! Entries are added to `Wral` journal. Journals automatically rotate
//! and are numbered from ZERO.

use log::debug;
use mkit::{
    self,
    cbor::{FromCbor, IntoCbor},
    thread,
};

use std::{
    ffi, fs, path,
    sync::{Arc, Mutex},
    time,
};

use crate::{journal::Journal, state, writer, Error, Result};

const JOURNAL_LIMIT: usize = 1 * 1024 * 1024 * 1024; // 1GB.

const BATCH_SIZE: usize = 1 * 1024 * 1024; // 1MB.

const BATCH_PERIOD: time::Duration = time::Duration::from_micros(10 * 1000); // 10ms

#[derive(Clone)]
pub struct Config {
    pub name: String,
    pub dir: ffi::OsString,
    pub journal_limit: usize,
    pub batch_size: usize,
    pub batch_period: time::Duration, // in micro-seconds.
    pub fsync: bool,
}

impl Config {
    fn new(name: &str, dir: &ffi::OsStr) -> Config {
        Config {
            name: name.to_string(),
            dir: dir.to_os_string(),
            journal_limit: JOURNAL_LIMIT,
            batch_size: BATCH_SIZE,
            batch_period: BATCH_PERIOD,
            fsync: true,
        }
    }

    fn set_journal_limit(&mut self, journal_limit: usize) -> &mut Self {
        self.journal_limit = journal_limit;
        self
    }

    fn set_batch_size(&mut self, batch_size: usize) -> &mut Self {
        self.batch_size = batch_size;
        self
    }

    fn set_fsync(&mut self, fsync: bool) -> &mut Self {
        self.fsync = fsync;
        self
    }
}

/// Write alhead logging.
pub struct Wral<S> {
    config: Config,

    tx: thread::Tx<writer::Req, writer::Res>,
    t: Option<Arc<mkit::Thread<writer::Req, writer::Res, Result<u64>>>>,
    w: Option<Arc<Mutex<writer::Writer<S>>>>,
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
        S: 'static + Send + Clone + IntoCbor + FromCbor + state::State,
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
        let journal = Journal::start_journal(&config.name, &config.dir, num, state)?;

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
        S: 'static + Send + Default + Clone + IntoCbor + FromCbor + state::State,
    {
        let mut journals: Vec<(Journal<S>, u64, S)> = vec![];
        for item in err_at!(IOError, fs::read_dir(&config.dir))? {
            let file_path: path::PathBuf = {
                let file_name = err_at!(IOError, item)?.file_name();
                [config.dir.clone(), file_name.clone()].iter().collect()
            };
            match Journal::load_archive(&config.name, file_path.as_ref()) {
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
        let journal = Journal::start_journal(&config.name, &config.dir, num + 1, state)?;

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

    /// Close the [Wal] instance. To purge the instance use [Wal::purge] api.
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

    /// Purge this [Wal] instance and all its memory and disk footprints.
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
    /// Add a serialized operation to WAL.
    pub fn add_op(&self, op: &[u8]) -> Result<u64> {
        let req = writer::Req::AddEntry { op: op.to_vec() };
        let seqno = match self.tx.request(req)? {
            writer::Res::Seqno(seqno) => seqno,
            _ => unreachable!(),
        };
        Ok(seqno)
    }
}

impl<S> Wral<S> {
    //fn iter_entries(&self) -> impl Iterator<Item = entry::Entry> {
    //    todo!()
    //}
}

//#[cfg(test)]
//#[path = "wal_test.rs"]
//mod wal_test;
