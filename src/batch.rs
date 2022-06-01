use arbitrary::{Arbitrary, Unstructured};
use mkit::{
    self,
    cbor::{Cbor, FromCbor},
    Cborize,
};

use std::{
    cmp,
    fmt::{self, Display},
    fs,
    io::{self, Read, Seek},
    ops, result, vec,
};

use crate::{entry, state, util, Error, Result};

pub struct Worker<S> {
    index: Vec<Index>,
    entries: Vec<entry::Entry>,
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

    pub fn add_entry(&mut self, entry: entry::Entry) -> Result<()>
    where
        S: state::State,
    {
        self.state.on_add_entry(&entry)?;
        self.entries.push(entry);
        Ok(())
    }

    pub fn flush(&mut self, file: &mut fs::File) -> Result<Option<Index>>
    where
        S: state::State,
    {
        let fpos = err_at!(IOError, file.metadata())?.len();
        let batch = match self.entries.len() {
            0 => return Ok(None),
            _ => Batch {
                first_seqno: self.entries.first().map(entry::Entry::to_seqno).unwrap(),
                last_seqno: self.entries.last().map(entry::Entry::to_seqno).unwrap(),
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

        let index = Index::new(fpos, length, first_seqno, last_seqno);
        self.index.push(index.clone());

        Ok(Some(index))
    }
}

impl<S> Worker<S> {
    pub fn to_last_seqno(&self) -> Option<u64> {
        match self.entries.len() {
            0 => self.index.last().map(|index| index.last_seqno),
            _ => self.entries.last().map(entry::Entry::to_seqno),
        }
    }

    pub fn to_index(&self) -> Vec<Index> {
        self.index.clone()
    }

    pub fn to_entries(&self) -> Vec<entry::Entry> {
        self.entries.clone()
    }

    pub fn len_batches(&self) -> usize {
        self.index.len()
    }

    pub fn to_state(&self) -> S
    where
        S: Clone,
    {
        self.state.clone()
    }

    pub fn unwrap(self) -> (Vec<Index>, Vec<entry::Entry>, S) {
        (self.index, self.entries, self.state)
    }
}

/// Batch of entries on disk or in-memory.
#[derive(Debug, Clone, Eq, PartialEq, Cborize)]
pub struct Batch {
    // index-seqno of first entry in this batch.
    first_seqno: u64,
    // index-seqno of last entry in this batch.
    last_seqno: u64,
    // state as serialized bytes, shall be in cbor format.
    state: Vec<u8>,
    // list of entries in this batch.
    entries: Vec<entry::Entry>,
}

impl arbitrary::Arbitrary for Batch {
    fn arbitrary(u: &mut Unstructured) -> arbitrary::Result<Self> {
        let mut entries: Vec<entry::Entry> = u.arbitrary()?;
        entries.dedup_by(|a, b| a.to_seqno() == b.to_seqno());
        entries.sort();

        let first_seqno: u64 = entries.first().map(|e| e.to_seqno()).unwrap_or(0);
        let last_seqno: u64 = entries.last().map(|e| e.to_seqno()).unwrap_or(0);

        let batch = Batch {
            first_seqno,
            last_seqno,
            state: u.arbitrary()?,
            entries,
        };
        Ok(batch)
    }
}

impl Display for Batch {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "batch<{}..{}]>", self.first_seqno, self.last_seqno)
    }
}

impl PartialOrd for Batch {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Batch {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.first_seqno.cmp(&other.first_seqno)
    }
}

impl Batch {
    const ID: u32 = 0x0;

    pub fn from_index(index: Index, file: &mut fs::File) -> Result<Batch> {
        err_at!(IOError, file.seek(io::SeekFrom::Start(index.fpos)))?;
        let mut buf = vec![0; index.length];
        err_at!(IOError, file.read_exact(&mut buf))?;
        let (value, _) = Cbor::decode(&mut buf.as_slice())?;
        Ok(Batch::from_cbor(value)?)
    }

    #[inline]
    pub fn to_state(&self) -> Vec<u8> {
        self.state.to_vec()
    }

    #[inline]
    pub fn to_first_seqno(&self) -> u64 {
        self.first_seqno
    }

    #[inline]
    pub fn to_last_seqno(&self) -> u64 {
        self.last_seqno
    }

    pub fn into_iter(
        self,
        range: ops::RangeInclusive<u64>,
    ) -> vec::IntoIter<entry::Entry> {
        self.entries
            .into_iter()
            .filter(|e| range.contains(&e.to_seqno()))
            .collect::<Vec<entry::Entry>>()
            .into_iter()
    }
}

/// Index of batches on disk.
#[derive(Debug, Clone, Eq, PartialEq, Arbitrary)]
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
    pub fn new(fpos: u64, length: usize, first_seqno: u64, last_seqno: u64) -> Index {
        Index { fpos, length, first_seqno, last_seqno }
    }

    #[inline]
    pub fn to_first_seqno(&self) -> u64 {
        self.first_seqno
    }

    #[inline]
    pub fn to_last_seqno(&self) -> u64 {
        self.last_seqno
    }
}

#[cfg(test)]
#[path = "batch_test.rs"]
mod batch_test;
