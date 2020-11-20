use mkit::Cborize;

use std::{
    cmp, ops,
    fmt::{self, Display},
    result,
};

use crate::entry::Entry;

/// Batch of entries on disk or in-memory.
#[derive(Cborize, Clone, Eq, PartialEq)]
pub struct Batch {
    // index-seqno of first entry in this batch.
    start_seqno: u64,
    // index-seqno of last entry in this batch.
    last_seqno: u64,
    // state as serialized bytes, shall in cbor format.
    state: Vec<u8>,
    // list of entries in this batch.
    entries: Vec<Entry>;
}

impl Display for Batch {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "batch<seqnos:[{},{}]>", self.start_seqno, self.last_seqno)
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
            cmp::Order::Equal
        } else if self.last_seqno < other.start_seqno {
            cmp::Order::Less
        } else if other.last_seqno < self.start_seqno {
            cmp::Order::Greater
        } else {
            panic!("overlapping batch {} {}", self, other)
        }
    }
}

impl From<Vec<Entry>> for Batch {
    fn from(entries: Vec<Entry>) -> Batch {
        Batch {
            start_seqno: entries.first().map(Entry::to_seqno).unwrap_or(0),
            last_seqno: entries.last().map(Entry::to_seqno).unwrap_or(0),
            state: Vec::default(),
            entries,
        }
    }
}

impl Batch {
    const ID: u64 = 0x1;

    pub fn set_state(&mut self, state: Vec<u8>) {
        self.state = state
    }

    pub fn to_seqno_range(&self) -> ops::RangeInclusive<u64> {
        RangeInclusive::new(self.start_seqno, self.last_seqno)
    }

    pub fn to_state(&self) -> Vec<u8> {
        self.state.to_vec()
    }

    pub fn as_state(&self) -> &[u8] {
        &self.state
    }

    pub fn to_entries(&self) -> Vec<Entry> {
        self.entries.to_vec()
    }

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

