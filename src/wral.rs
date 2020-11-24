//! Package implement WRite-Ahead-Logging.
//!
//! Entries are added to `Wral` journal. Journals automatically rotate
//! and are numbered from ZERO.

use log::debug;
use mkit::{self, cbor::Cbor, thread};

use std::{
    convert::TryFrom,
    ffi, fs, path,
    sync::{Arc, Mutex},
};

use crate::{journal::Journal, writer, Error, Result};

const JOURNAL_LIMIT: usize = 1 * 1024 * 1024 * 1024; // 1GB.

const BATCH_SIZE: usize = 1 * 1024 * 1024; // 1MB.

/// Write alhead logging.
pub struct Wral<S> {
    name: String,
    dir: ffi::OsString,
    journal_limit: usize,
    batch_size: usize,
    fsync: bool,

    tx: thread::Tx<writer::Req, writer::Res>,
    t: Option<Arc<mkit::Thread<writer::Req, writer::Res, Result<()>>>>,
    w: Option<Arc<Mutex<writer::Writer<S>>>>,
}

impl<S> Clone for Wral<S> {
    fn clone(&self) -> Wral<S> {
        Wral {
            name: self.name.clone(),
            dir: self.dir.clone(),
            journal_limit: self.journal_limit,
            batch_size: self.batch_size,
            fsync: self.fsync,

            tx: self.tx.clone(),
            t: self.t.as_ref().map(|t| Arc::clone(t)),
            w: self.w.as_ref().map(|w| Arc::clone(w)),
        }
    }
}

impl<S> Wral<S> {
    /// Create a new Write-Ahead-Log instance, while create a new journal,
    /// older journals matching the `name` shall be purged.
    pub fn create(name: &str, dir: &ffi::OsStr, state: S) -> Result<Wral<S>>
    where
        S: 'static + Send,
    {
        // try creating the directory, if it does not exist.
        fs::create_dir_all(&dir).ok();

        // purge existing journals for this shard.
        for item in err_at!(IOError, fs::read_dir(dir))? {
            let file_path: path::PathBuf = {
                let file_name = err_at!(IOError, item)?.file_name();
                [dir, file_name.as_ref()].iter().collect()
            };
            match Journal::<S>::load_cold(name, file_path.as_ref()) {
                Some(journal) => match journal.purge() {
                    Ok(_) => (),
                    Err(err) => debug!(target: "wral", "failed to purge {:?}, {}", file_path, err),
                },
                None => continue,
            };
        }

        let num = 0;
        let journal = Journal::start_journal(name, dir, num, state)?;

        debug!(target: "wral", "{:?}/{} created", dir, name);

        let seqno = 1;
        let (w, t) = writer::Writer::start(name, dir, vec![], journal, seqno);

        let val = Wral {
            name: name.to_string(),
            dir: dir.to_os_string(),
            journal_limit: JOURNAL_LIMIT,
            batch_size: BATCH_SIZE,
            fsync: true,

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
    pub fn load(name: &str, dir: &ffi::OsStr) -> Result<Wral<S>>
    where
        S: 'static + Send + Default + Clone + TryFrom<Cbor, Error = mkit::Error>,
    {
        let mut journals: Vec<(Journal<S>, u64, S)> = vec![];
        for item in err_at!(IOError, fs::read_dir(&dir))? {
            let file_path: path::PathBuf = {
                let file_name = err_at!(IOError, item)?.file_name();
                [dir, file_name.as_ref()].iter().collect()
            };
            match Journal::load_archive(name, file_path.as_ref()) {
                Some((journal, state)) => {
                    let seqno = journal.to_last_seqno().unwrap();
                    journals.push((journal, seqno, state));
                }
                None => debug!(target: "wral", "failed to load {:?}", file_path),
            };
        }

        journals.sort_by(|(_, a, _), (_, b, _)| a.cmp(b));

        let (seqno, num, state) = match journals.last() {
            Some((j, seqno, state)) => (*seqno, j.to_journal_number(), state.clone()),
            None => (1, 0, S::default()),
        };
        let journal = Journal::start_journal(name, dir, num + 1, state)?;

        let n_batches: usize = journals.iter().map(|(j, _, _)| j.len_batches()).sum();
        debug!(
            target: "wral",
            "{:?}/{} loaded with {} journals, {} batches",
            dir, name, journals.len(), n_batches
        );

        let journals: Vec<Journal<S>> = journals.into_iter().map(|(j, _, _)| j).collect();
        let (w, t) = writer::Writer::start(name, dir, journals, journal, seqno);

        let val = Wral {
            name: name.to_string(),
            dir: dir.to_os_string(),
            journal_limit: JOURNAL_LIMIT,
            batch_size: BATCH_SIZE,
            fsync: true,

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
    pub fn add_op(&self, _op: Vec<u8>) -> Result<u64> {
        todo!()
    }
}

//#[cfg(test)]
//#[path = "wal_test.rs"]
//mod wal_test;
