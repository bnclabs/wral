use log::debug;
use mkit::cbor::Cbor;

use std::{convert::TryFrom, convert::TryInto, ffi, fs, path};

use crate::{batch, entry, files, Error, Result};

pub(crate) struct Journal<S> {
    file_path: ffi::OsString, // dir/{name}-journal-{num}.dat
    inner: InnerJournal<S>,
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
    },
    // Cold journals are colder than archives, that is, they are not
    // required by the application, may be as frozen-backup.
    Cold,
}

impl<S> Journal<S> {
    pub fn start_journal(
        dir: ffi::OsString,
        name: &str,
        num: usize,
        state: S,
    ) -> Result<Journal<S>> {
        let file_path: path::PathBuf = {
            let name: &str = name.as_ref();
            let file: ffi::OsString = files::make_filename(name.to_string(), num);
            [dir, file].iter().collect()
        };

        fs::remove_file(&file_path).ok(); // cleanup a single journal file

        let file = {
            let mut opts = fs::OpenOptions::new();
            err_at!(IOError, opts.append(true).create_new(true).open(&file_path))?
        };
        debug!(target: "wral", "start_journal {:?}", file_path);

        Ok(Journal {
            file_path: file_path.into_os_string(),
            inner: InnerJournal::Working {
                worker: batch::Worker::new(state),
                file,
            },
        })
    }

    pub fn load_archive(name: String, file_path: ffi::OsString) -> Option<Journal<S>> {
        let os_file = path::Path::new(&file_path);
        let (nm, num) = files::unwrap_filename(os_file.file_name()?.to_os_string())?;

        if nm != name {
            return None;
        }

        let mut file = err_at!(IOError, fs::OpenOptions::new().read(true).open(os_file)).ok()?;

        let mut index = vec![];
        let mut fpos = 0_usize;
        let len = file.metadata().ok()?.len();

        while u64::try_from(fpos).ok()? < len {
            let (val, n) = Cbor::decode(&mut file).ok()?;
            let batch: batch::Batch = val.try_into().ok()?;
            index.push(batch::Index::new(
                u64::try_from(fpos).ok()?,
                n,
                batch.to_first_seqno(),
                batch.to_last_seqno(),
            ));
            fpos += n
        }

        debug!(target: "wral", "load journal {:?}, loaded {} batches", file_path, index.len());

        Some(Journal {
            file_path,
            inner: InnerJournal::Archive { index },
        })
    }

    pub fn into_cold(mut self) -> Self {
        self.inner = match self.inner {
            InnerJournal::Archive { .. } => InnerJournal::Cold,
            _ => unreachable!(),
        };

        debug!(target: "wral", "moving journal {:?} to cold state", self.file_path);

        self
    }

    pub fn purge(self) -> Result<()> {
        debug!(target: "wral", "purging {:?} ...", self.file_path);
        err_at!(IOError, fs::remove_file(&self.file_path))?;
        Ok(())
    }

    pub fn into_archive(mut self) -> (Self, Vec<entry::Entry>, S) {
        let (inner, entries, state) = match self.inner {
            InnerJournal::Working { worker, .. } => {
                let (index, entries, state) = worker.unwrap();
                (InnerJournal::Archive { index }, entries, state)
            }
            _ => unreachable!(),
        };
        self.inner = inner;
        (self, entries, state)
    }
}

impl<S> Journal<S> {
    pub fn to_last_seqno(&self) -> Option<u64> {
        match &self.inner {
            InnerJournal::Working { worker, .. } => worker.to_last_seqno(),
            InnerJournal::Archive { index } if index.len() == 0 => None,
            InnerJournal::Archive { index } => index.last().map(batch::Index::to_last_seqno),
            _ => unreachable!(),
        }
    }
}

//impl<S, T> Journal<S, T>
//where
//    S: Default + Serialize,
//    T: Serialize,
//{
//    // periodically flush journal entries from memory to disk.
//    fn flush1(
//        &mut self,
//        journal_limit: usize,
//        fsync: bool,
//    ) -> Result<Option<(Vec<u8>, Batch<S, T>)>> {
//        let (file_path, fd, batches, active, rotate) = match &mut self.inner {
//            InnerJournal::Active {
//                file_path,
//                fd,
//                batches,
//                active,
//            } => {
//                let limit: u64 = convert_at!(journal_limit)?;
//                let rotate = err_at!(IOError, fd.metadata())?.len() > limit;
//                Ok((file_path, fd, batches, active, rotate))
//            }
//            _ => err_at!(Fatal, msg: format!("unreachable")),
//        }?;
//
//        match rotate {
//            true if active.len()? > 0 => Ok(Some(active.to_refer(0)?)),
//            false if active.len()? > 0 => {
//                let (buffer, batch) = {
//                    let fpos = err_at!(IOError, fd.metadata())?.len();
//                    active.to_refer(fpos)?
//                };
//                batches.push(batch);
//                write_file!(fd, &buffer, file_path.clone(), "wal-flush1")?;
//                if fsync {
//                    err_at!(IOError, fd.sync_all())?;
//                }
//                *active = Batch::default_active();
//                Ok(None)
//            }
//            _ => Ok(None),
//        }
//    }
//
//    fn flush2(&mut self, buffer: &[u8], batch: Batch<S, T>, fsync: bool) -> Result<()> {
//        let (file_path, fd, batches, active) = match &mut self.inner {
//            InnerJournal::Active {
//                file_path,
//                fd,
//                batches,
//                active,
//            } => Ok((file_path, fd, batches, active)),
//            _ => err_at!(Fatal, msg: format!("unreachable")),
//        }?;
//
//        write_file!(fd, &buffer, file_path.clone(), "wal-flush2")?;
//        if fsync {
//            err_at!(IOError, fd.sync_all())?;
//        }
//        batches.push(batch);
//        *active = Batch::default_active();
//
//        Ok(())
//    }

//#[cfg(test)]
//#[path = "dlog_journal_test.rs"]
//mod dlog_journal_test;
