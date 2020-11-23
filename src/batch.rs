use mkit::{self, cbor::Cbor, Cborize};

use std::{
    cmp,
    convert::TryInto,
    fmt::{self, Display},
    fs, ops, result,
};

use crate::{entry::Entry, state::State, util, Error, Result};

pub struct Worker<S> {
    index: Vec<Index>,
    entries: Vec<Entry>,
    state: S,
}

impl<S> Worker<S> {
    pub fn new(state: S) -> Worker<S> {
        Worker {
            index: Vec::default(),
            entries: Vec::default(),
            state,
        }
    }

    pub fn add_entry(&mut self, entry: Entry) -> Result<()>
    where
        S: State,
    {
        self.state.on_add_entry(&entry)?;
        self.entries.push(entry);
        Ok(())
    }

    pub fn to_first_seqno(&self) -> Option<u64> {
        match self.entries.len() {
            0 => self.index.first().map(|index| index.first_seqno),
            _ => self.entries.first().map(Entry::to_seqno),
        }
    }

    pub fn to_last_seqno(&self) -> Option<u64> {
        match self.entries.len() {
            0 => self.index.last().map(|index| index.last_seqno),
            _ => self.entries.last().map(Entry::to_seqno),
        }
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn flush(&mut self, file: &mut fs::File) -> Result<()>
    where
        S: Clone + TryInto<Cbor, Error = mkit::Error>,
    {
        let fpos = err_at!(IOError, file.metadata())?.len();
        let batch = match self.entries.len() {
            0 => return Ok(()),
            _ => Batch {
                first_seqno: self.entries.first().map(Entry::to_seqno).unwrap(),
                last_seqno: self.entries.last().map(Entry::to_seqno).unwrap(),
                state: util::encode_cbor(self.state.clone())?,
                entries: self.entries.drain(..).collect(),
            },
        };

        let first_seqno = batch.first_seqno;
        let last_seqno = batch.last_seqno;
        let length = {
            let data = util::encode_cbor(batch)?;
            util::sync_write(file, &data)?;
            data.len()
        };

        self.index
            .push(Index::new(fpos, length, first_seqno, last_seqno));

        Ok(())
    }

    pub fn unwrap(self) -> (Vec<Index>, Vec<Entry>, S) {
        (self.index, self.entries, self.state)
    }
}

/// Batch of entries on disk or in-memory.
#[derive(Cborize, Clone, Eq, PartialEq)]
pub struct Batch {
    // index-seqno of first entry in this batch.
    first_seqno: u64,
    // index-seqno of last entry in this batch.
    last_seqno: u64,
    // state as serialized bytes, shall be in cbor format.
    state: Vec<u8>,
    // list of entries in this batch.
    entries: Vec<Entry>,
}

impl Display for Batch {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(
            f,
            "batch<seqnos:[{},{}]>",
            self.first_seqno, self.last_seqno
        )
    }
}

impl PartialOrd for Batch {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Batch {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        if self.eq(other) {
            cmp::Ordering::Equal
        } else if self.last_seqno < other.first_seqno {
            cmp::Ordering::Less
        } else if other.last_seqno < self.first_seqno {
            cmp::Ordering::Greater
        } else {
            panic!("overlapping batch {} {}", self, other)
        }
    }
}

impl From<Vec<Entry>> for Batch {
    fn from(entries: Vec<Entry>) -> Batch {
        Batch {
            first_seqno: entries.first().map(Entry::to_seqno).unwrap_or(0),
            last_seqno: entries.last().map(Entry::to_seqno).unwrap_or(0),
            state: Vec::default(),
            entries,
        }
    }
}

impl Batch {
    const ID: u64 = 0x1;

    #[inline]
    pub fn to_seqno_range(&self) -> ops::RangeInclusive<u64> {
        ops::RangeInclusive::new(self.first_seqno, self.last_seqno)
    }

    #[inline]
    pub fn to_state(&self) -> Vec<u8> {
        self.state.to_vec()
    }

    #[inline]
    pub fn as_state(&self) -> &[u8] {
        &self.state
    }

    #[inline]
    pub fn to_first_seqno(&self) -> u64 {
        self.first_seqno
    }

    #[inline]
    pub fn to_last_seqno(&self) -> u64 {
        self.last_seqno
    }

    #[inline]
    pub fn to_entries(&self) -> Vec<Entry> {
        self.entries.to_vec()
    }

    #[inline]
    pub fn as_entries(&self) -> &[Entry] {
        &self.entries
    }
}

/// Index of batches on disk.
#[derive(Cborize, Clone, Eq, PartialEq)]
pub struct Index {
    // offset in file, where the batch starts.
    fpos: u64,
    // length from offset that spans the entire batch.
    length: usize,
    // first seqno in the batch.
    first_seqno: u64,
    // last seqno in the batch.
    last_seqno: u64,
}

impl Index {
    const ID: u64 = 0x1;

    pub fn new(fpos: u64, length: usize, first_seqno: u64, last_seqno: u64) -> Index {
        Index {
            fpos,
            length,
            first_seqno,
            last_seqno,
        }
    }

    #[inline]
    pub fn to_last_seqno(&self) -> u64 {
        self.last_seqno
    }
}
