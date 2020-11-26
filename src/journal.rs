use log::{debug, error};
use mkit::{
    self,
    cbor::{Cbor, FromCbor},
};

use std::{
    convert::TryFrom,
    ffi,
    fmt::{self, Display},
    fs, path, result, vec,
};

use crate::{batch, entry, files, state, Error, Result};

pub struct Journal<S> {
    name: String,
    num: usize,
    file_path: ffi::OsString, // dir/{name}-journal-{num}.dat
    inner: InnerJournal<S>,
}

impl<S> Display for Journal<S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "journal-{}-{}", self.name, self.num)
    }
}

enum InnerJournal<S> {
    // Active journal, the latest journal, in the journal-set. A journal
    // set is managed by Shard.
    Working {
        worker: batch::Worker<S>,
        file: fs::File,
    },
    // All journals except lastest journal are archives, which means only
    // the metadata for each batch shall be stored.
    Archive {
        index: Vec<batch::Index>,
        state: S,
    },
    // Cold journals are colder than archives, that is, they are not
    // required by the application, may be as frozen-backup.
    Cold,
}

impl<S> Journal<S> {
    pub fn start_journal(name: &str, dir: &ffi::OsStr, num: usize, state: S) -> Result<Journal<S>> {
        let file_path: path::PathBuf = {
            let name: &str = name.as_ref();
            let file: ffi::OsString = files::make_filename(name.to_string(), num);
            [dir, &file].iter().collect()
        };

        fs::remove_file(&file_path).ok(); // cleanup a single journal file

        let file = {
            let mut opts = fs::OpenOptions::new();
            err_at!(IOError, opts.append(true).create_new(true).open(&file_path))?
        };
        debug!(target: "wral", "start_journal {:?}", file_path);

        Ok(Journal {
            name: name.to_string(),
            num,
            file_path: file_path.into_os_string(),
            inner: InnerJournal::Working {
                worker: batch::Worker::new(state),
                file,
            },
        })
    }

    pub fn load_archive(name: &str, file_path: &ffi::OsStr) -> Option<(Journal<S>, S)>
    where
        S: Clone + FromCbor,
    {
        let os_file = path::Path::new(file_path);
        let (nm, num) = files::unwrap_filename(os_file.file_name()?.to_os_string())?;

        if nm != name {
            return None;
        }

        let mut file = err_at!(IOError, fs::OpenOptions::new().read(true).open(os_file)).ok()?;

        let mut state = vec![];
        let mut index = vec![];
        let mut fpos = 0_usize;
        let len = file.metadata().ok()?.len();

        while u64::try_from(fpos).ok()? < len {
            let (val, n) = Cbor::decode(&mut file).ok()?;
            let batch = batch::Batch::from_cbor(val).ok()?;
            index.push(batch::Index::new(
                u64::try_from(fpos).ok()?,
                n,
                batch.to_first_seqno(),
                batch.to_last_seqno(),
            ));
            state = batch.to_state();
            fpos += n
        }

        if index.len() == 0 {
            return None;
        }

        let state: S = match Cbor::decode(&mut state.as_slice()) {
            Ok((state, _)) => match S::from_cbor(state) {
                Ok(state) => Some(state),
                Err(err) => {
                    error!(target: "wral", "corrupted state-cbor {:?} {}", file_path, err);
                    None
                }
            },
            Err(err) => {
                error!(target: "wral", "corrupted state {:?} {}", file_path, err);
                None
            }
        }?;

        debug!(target: "wral", "load journal {:?}, loaded {} batches", file_path, index.len());

        let journal = Journal {
            name: name.to_string(),
            num,
            file_path: file_path.to_os_string(),
            inner: InnerJournal::Archive {
                index,
                state: state.clone(),
            },
        };

        Some((journal, state))
    }

    pub fn load_cold(name: &str, file_path: &ffi::OsStr) -> Option<Journal<S>> {
        let os_file = path::Path::new(file_path);
        let (nm, num) = files::unwrap_filename(os_file.file_name()?.to_os_string())?;

        if nm != name {
            return None;
        }

        let journal = Journal {
            name: name.to_string(),
            num,
            file_path: file_path.to_os_string(),
            inner: InnerJournal::Cold,
        };
        Some(journal)
    }

    pub fn into_archive(mut self) -> (Self, Vec<entry::Entry>, S)
    where
        S: Clone,
    {
        let (inner, entries, state) = match self.inner {
            InnerJournal::Working { worker, .. } => {
                let (index, entries, state) = worker.unwrap();
                let inner = InnerJournal::Archive {
                    index,
                    state: state.clone(),
                };
                (inner, entries, state)
            }
            _ => unreachable!(),
        };
        self.inner = inner;
        (self, entries, state)
    }

    pub fn purge(self) -> Result<()> {
        debug!(target: "wral", "purging {:?} ...", self.file_path);
        err_at!(IOError, fs::remove_file(&self.file_path))?;
        Ok(())
    }
}

impl<S> Journal<S> {
    pub fn add_entry(&mut self, entry: entry::Entry) -> Result<()>
    where
        S: state::State,
    {
        match &mut self.inner {
            InnerJournal::Working { worker, .. } => worker.add_entry(entry),
            InnerJournal::Archive { .. } => unreachable!(),
            InnerJournal::Cold => unreachable!(),
        }
    }

    pub fn flush(&mut self) -> Result<()>
    where
        S: state::State,
    {
        match &mut self.inner {
            InnerJournal::Working { worker, file } => worker.flush(file),
            InnerJournal::Archive { .. } => unreachable!(),
            InnerJournal::Cold { .. } => unreachable!(),
        }
    }
}

impl<S> Journal<S> {
    pub fn to_journal_number(&self) -> usize {
        self.num
    }

    pub fn len_batches(&self) -> usize {
        match &self.inner {
            InnerJournal::Working { worker, .. } => worker.len_batches(),
            InnerJournal::Archive { index, .. } => index.len(),
            InnerJournal::Cold { .. } => unreachable!(),
        }
    }

    pub fn to_last_seqno(&self) -> Option<u64> {
        match &self.inner {
            InnerJournal::Working { worker, .. } => worker.to_last_seqno(),
            InnerJournal::Archive { index, .. } if index.len() == 0 => None,
            InnerJournal::Archive { index, .. } => index.last().map(batch::Index::to_last_seqno),
            _ => None,
        }
    }

    pub fn file_size(&self) -> Result<usize> {
        let n = match &self.inner {
            InnerJournal::Working { file, .. } => {
                let m = err_at!(IOError, file.metadata())?;
                err_at!(FailConvert, usize::try_from(m.len()))?
            }
            InnerJournal::Archive { .. } => unreachable!(),
            InnerJournal::Cold => unreachable!(),
        };
        Ok(n)
    }

    pub fn to_state(&self) -> S
    where
        S: Clone,
    {
        match &self.inner {
            InnerJournal::Working { worker, .. } => worker.to_state(),
            InnerJournal::Archive { state, .. } => state.clone(),
            InnerJournal::Cold => unreachable!(),
        }
    }
}

pub struct RdJournal {
    batch: vec::IntoIter<entry::Entry>,
    index: vec::IntoIter<batch::Index>,
    entries: vec::IntoIter<entry::Entry>,
    file: fs::File,
}

impl RdJournal {
    pub fn from_journal<S>(journal: &Journal<S>) -> Result<RdJournal> {
        let (index, entries) = match &journal.inner {
            InnerJournal::Working { worker, .. } => (worker.to_index(), worker.to_entries()),
            InnerJournal::Archive { index, .. } => (index.to_vec(), vec![]),
            InnerJournal::Cold => unreachable!(),
        };
        let batch: vec::IntoIter<entry::Entry> = vec![].into_iter();
        let index = index.into_iter();
        let entries = entries.into_iter();

        let file = {
            let mut opts = fs::OpenOptions::new();
            err_at!(IOError, opts.read(true).open(&journal.file_path))?
        };

        Ok(RdJournal {
            batch,
            index,
            entries,
            file,
        })
    }
}

impl Iterator for RdJournal {
    type Item = Result<entry::Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.batch.next() {
            Some(entry) => Some(Ok(entry)),
            None => match self.index.next() {
                Some(index) => match batch::Batch::from_index(index, &mut self.file) {
                    Ok(batch) => {
                        self.batch = batch.into_iter();
                        self.next()
                    }
                    Err(err) => Some(Err(err)),
                },
                None => match self.entries.next() {
                    Some(entry) => Some(Ok(entry)),
                    None => None,
                },
            },
        }
    }
}

//#[cfg(test)]
//#[path = "dlog_journal_test.rs"]
//mod dlog_journal_test;
