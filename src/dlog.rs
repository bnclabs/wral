//! Module `dlog` implement entry logging for [rdms].
//!
//! [Dlog] type implement append only logging, and can be used by more complex
//! components like [Wal]. Following are the features in Dlog:
//!
//! * [Dlog] instance is managed as shards and journals. Number of shards for
//!   a single Dlog instance is controlled bia `nshards` parameter.
//! * Each shard can handle concurrent writers and serializes all writes
//!   into the same log file.
//! * Multiple shards can append to separate files concurrently.
//! * Entries are appended into a journal file and automatically rotated
//!   when `journal_limit` is exceeded.
//! * Durability guarantee is controlled via `fsync` parameter.
//!
//! **Shards**:
//!
//! Every shard is managed in a separate thread and each shard serializes
//! the log-operations, batches them if possible, flushes them and return
//! a index-sequence-no for each operation back to the caller. Basic idea
//! behind shard is to match with I/O concurrency available in modern SSDs.
//!
//! **Journals**:
//!
//! A shard of `Dlog` is organized into ascending list of journal files,
//! where each journal file do not exceed the configured size-limit.
//! Journal files are append only and flushed in batches when ever
//! possible. Journal files are purged once `Dlog` is notified about
//! durability guarantee uptill an index-sequence-no.
//!
//! Refer to the [Dlog] documentation for more details.

use log::debug;

use std::{
    cmp, ffi, fmt,
    ops::Bound,
    result,
    sync::{
        atomic::{AtomicU64, Ordering::SeqCst},
        Arc,
    },
    vec,
};

#[doc(hidden)]
pub use crate::dlog_entry::DEntry;
use crate::{
    core::{Result, Serialize},
    dlog_journal::Shard,
};
#[allow(unused_imports)] // for documentation
use crate::{rdms, wal::Wal};

#[allow(unused_imports)]
use crate::rdms::Rdms;

/// Default limit for journal file size.
pub const JOURNAL_LIMIT: usize = 1 * 1024 * 1024 * 1024;

/// Dlog entry logging for [`Rdms`] index.
pub struct Dlog<S, T>
where
    S: Clone + Default + Serialize,
    T: Clone + Default + Serialize,
{
    pub(crate) dir: ffi::OsString,
    pub(crate) name: String,

    pub(crate) seqno: Arc<AtomicU64>,
    pub(crate) shards: Vec<Shard<S, T>>,
}

impl<S, T> fmt::Debug for Dlog<S, T>
where
    S: Clone + Default + Serialize,
    T: Clone + Default + Serialize,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "Dlog<{:?},{}>", self.dir, self.name)
    }
}

impl<S, T> Dlog<S, T>
where
    S: Clone + Default + Serialize,
    T: Clone + Default + Serialize,
{
    /// Create a new [`Dlog`] instance under directory `dir`, using specified
    /// number of shards `nshards`. `name` must be unique if more than
    /// one [`Dlog`] instances are going to be created under the same `dir`.
    pub fn create(
        dir: ffi::OsString,
        name: String,
        nshards: usize,
        journal_limit: usize,
        batch_size: usize,
        fsync: bool,
    ) -> Result<Dlog<S, T>>
    where
        S: DlogState<T>,
    {
        let dlog_seqno = Arc::new(AtomicU64::new(1));

        // purge existing shard/journals for name.
        let mut shards = vec![];
        for shard_id in 0..nshards {
            let seqno = Arc::clone(&dlog_seqno);
            shards.push(Shard::<S, T>::create(
                dir.clone(),
                name.clone(),
                shard_id,
                seqno,
                journal_limit,
                batch_size,
                fsync,
            )?);
        }

        debug!(target: "dlog  ", "create a new dlog {:?}/{}", dir, name);

        // create this Dlog. later shards/journals can be added.
        Ok(Dlog {
            dir,
            name,

            seqno: dlog_seqno,
            shards,
        })
    }

    /// Load an existing [`Dlog`] instance identified by `name` under
    /// directory `dir`.
    pub fn load(
        dir: ffi::OsString,
        name: String,
        nshards: usize,
        journal_limit: usize,
        batch_size: usize,
        fsync: bool,
    ) -> Result<Dlog<S, T>>
    where
        S: DlogState<T>,
    {
        let mut last_seqno = 0;
        let dlog_seqno = Arc::new(AtomicU64::new(last_seqno));

        let mut shards = vec![];
        for shard_id in 0..nshards {
            let seqno = Arc::clone(&dlog_seqno);
            let (li, shard) = Shard::<S, T>::load(
                dir.clone(),
                name.clone(),
                shard_id,
                seqno,
                journal_limit,
                batch_size,
                fsync,
            )?;
            shards.push(shard);
            last_seqno = cmp::max(last_seqno, li + 1);
        }

        dlog_seqno.store(last_seqno, SeqCst);

        debug!(target: "dlog  ", "load existing dlog from {:?}/{}", dir, name);

        Ok(Dlog {
            dir,
            name,

            seqno: dlog_seqno,
            shards,
        })
    }

    pub fn set_deep_freeze(&mut self, before: Bound<u64>) -> Result<()> {
        let shards: Vec<Shard<S, T>> = self.shards.drain(..).collect();
        for shard in shards.into_iter() {
            self.shards.push(shard.into_deep_freeze(before.clone())?)
        }

        Ok(())
    }
}

pub(crate) enum OpRequest<T> {
    Op { op: T },
    PurgeTill { before: Bound<u64> },
}

impl<T> OpRequest<T> {
    pub(crate) fn new_op(op: T) -> OpRequest<T> {
        OpRequest::Op { op }
    }

    pub(crate) fn new_purge_till(before: Bound<u64>) -> OpRequest<T> {
        OpRequest::PurgeTill { before }
    }
}

#[derive(PartialEq)]
pub(crate) enum OpResponse {
    Seqno(u64),
    Purged(Bound<u64>),
}

impl OpResponse {
    pub(crate) fn new_seqno(seqno: u64) -> OpResponse {
        OpResponse::Seqno(seqno)
    }

    pub(crate) fn new_purged(seqno: Bound<u64>) -> OpResponse {
        OpResponse::Purged(seqno)
    }
}

#[doc(hidden)]
pub trait DlogState<T> {
    type Key: Default + Serialize;
    type Val: Default + Serialize;

    fn on_add_entry(&mut self, entry: &DEntry<T>) -> ();

    fn to_type(&self) -> String;
}
