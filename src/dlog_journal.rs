use log::debug;

use std::{
    cmp,
    convert::{TryFrom, TryInto},
    ffi, fmt, fs,
    io::{self, Read, Seek, Write},
    mem,
    ops::Bound,
    path, result,
    sync::{
        atomic::{AtomicU64, Ordering::SeqCst},
        mpsc, Arc,
    },
    thread,
    time::Duration,
    vec,
};

use crate::{
    core::{Result, Serialize},
    dlog::{DlogState, OpRequest, OpResponse},
    dlog_entry::{Batch, DEntry},
    error::Error,
    thread as rt, util,
};

// default block size while loading the Dlog/Journal batches.
const DLOG_BLOCK_SIZE: usize = 10 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct JournalFile(ffi::OsString);

impl JournalFile {
    fn next(self) -> JournalFile {
        let (s, typ, shard_id, num): (String, String, usize, usize) =
            TryFrom::try_from(self).unwrap();
        From::from((s, typ, shard_id, num + 1))
    }
}

impl From<(String, String, usize, usize)> for JournalFile {
    fn from((s, typ, shard_id, num): (String, String, usize, usize)) -> JournalFile {
        let jfile = format!(
            "{}-{}-shard-{:03}-journal-{:03}.dlog",
            s, typ, shard_id, num
        );
        let jfile: &ffi::OsStr = jfile.as_ref();
        JournalFile(jfile.to_os_string())
    }
}

impl From<JournalFile> for ffi::OsString {
    fn from(jf: JournalFile) -> ffi::OsString {
        jf.0
    }
}

impl TryFrom<JournalFile> for (String, String, usize, usize) {
    type Error = Error;

    fn try_from(jfile: JournalFile) -> Result<(String, String, usize, usize)> {
        let get_stem = |jfile: JournalFile| -> Option<String> {
            let fname = path::Path::new(&jfile.0);
            match fname.extension()?.to_str()? {
                "dlog" => Some(fname.file_stem()?.to_str()?.to_string()),
                _ => None,
            }
        };

        let stem = match get_stem(jfile.clone()) {
            Some(stem) => Ok(stem),
            None => err_at!(InvalidInput, msg: format!("not dlog journal")),
        }?;
        let mut parts: Vec<&str> = stem.split('-').collect();

        let (name, parts) = match parts.len() {
            6 => Ok((parts.remove(0).to_string(), parts)),
            n if n > 6 => {
                let name: Vec<&str> = parts.drain(..n - 5).collect();
                Ok((name.join("-").to_string(), parts))
            }
            _ => err_at!(InvalidFile, msg: format!("not dlog journal")),
        }?;

        match &parts[..] {
            [typ, "shard", shard_id, "journal", num] => {
                let shard_id: usize = parse_at!(shard_id, usize)?;
                let num: usize = parse_at!(num, usize)?;
                Ok((name, typ.to_string(), shard_id, num))
            }
            _ => err_at!(InvalidFile, msg: format!("not dlog journal")),
        }
    }
}

impl fmt::Display for JournalFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{:?}", self.0)
    }
}

impl fmt::Debug for JournalFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{:?}", self.0)
    }
}

// shards are monotonically increasing number from 1 to N
pub(crate) struct Shard<S, T> {
    dir: ffi::OsString,
    name: String,
    shard_id: usize,
    journal_limit: usize,
    batch_size: usize,
    fsync: bool,

    dlog_seqno: Arc<AtomicU64>,
    journals: Vec<Journal<S, T>>,
    active: Journal<S, T>,
}

impl<S, T> Shard<S, T>
where
    S: Default + Serialize,
    T: Default + Serialize,
{
    pub(crate) fn create(
        dir: ffi::OsString,
        name: String,
        shard_id: usize,
        seqno: Arc<AtomicU64>,
        journal_limit: usize,
        batch_size: usize,
        fsync: bool,
    ) -> Result<Shard<S, T>>
    where
        S: DlogState<T>,
    {
        fs::create_dir_all(&dir).ok();

        // purge existing journals for this shard.
        for item in err_at!(IoError, fs::read_dir(&dir))? {
            let file_name = err_at!(IoError, item)?.file_name();
            let (n, id) = (name.clone(), shard_id);
            match Journal::<S, T>::new_cold(dir.clone(), n, id, file_name) {
                Some(journal) => journal.purge()?,
                None => (),
            }
        }

        let (d, n) = (dir.clone(), name.clone());
        let active = Journal::<S, T>::new_active(d, n, shard_id, 1)?;

        Ok(Shard {
            dir,
            name,
            shard_id,
            journal_limit,
            batch_size,
            fsync,

            dlog_seqno: seqno,
            journals: vec![],
            active,
        })
    }

    pub(crate) fn load(
        dir: ffi::OsString,
        name: String,
        shard_id: usize,
        seqno: Arc<AtomicU64>,
        journal_limit: usize,
        batch_size: usize,
        fsync: bool,
    ) -> Result<(u64, Shard<S, T>)>
    where
        S: DlogState<T>,
    {
        let mut journals = vec![];

        for item in err_at!(IoError, fs::read_dir(&dir))? {
            let file_name = err_at!(IoError, item)?.file_name();
            let (n, id) = (name.clone(), shard_id);
            match Journal::<S, T>::new_archive(dir.clone(), n, id, file_name) {
                Some(journal) => journals.push(journal),
                None => (),
            }
        }

        let idx = seqno.load(SeqCst);

        journals.sort_by(|x, y| x.num.cmp(&y.num));
        let last_seqno = {
            let mut iter = journals.iter().rev();
            loop {
                match iter.next() {
                    None if idx == 0 => break 0,
                    None => break idx - 1,
                    Some(journal) => match journal.to_last_seqno()? {
                        Some(last_seqno) => break last_seqno,
                        None => (),
                    },
                }
            }
        };

        let num = match journals.last() {
            Some(journal) => {
                let jf = JournalFile(journal.file_path.clone()).next();
                let p: (String, String, usize, usize) = TryFrom::try_from(jf)?;
                p.3
            }
            None => 1,
        };
        let (d, n) = (dir.clone(), name.clone());
        let active = Journal::<S, T>::new_active(d, n, shard_id, num)?;

        Ok((
            last_seqno,
            Shard {
                dir,
                name,
                shard_id,
                journal_limit,
                batch_size,
                fsync,

                dlog_seqno: seqno,
                journals,
                active,
            },
        ))
    }

    pub(crate) fn into_deep_freeze(self, before: Bound<u64>) -> Result<Self> {
        let mut last_seqno = 0;
        let mut journals = vec![];
        for journal in self.journals.into_iter() {
            let seqno = match journal.to_last_seqno()? {
                Some(seqno) => seqno,
                None => break,
            };
            assert!(seqno <= last_seqno, "fatal {} > {}", seqno, last_seqno);
            let ok = match before {
                Bound::Included(before) => last_seqno <= before,
                Bound::Excluded(before) => last_seqno < before,
                Bound::Unbounded => true,
            };
            if ok {
                journals.push(journal.into_cold()?);
            } else {
                journals.push(journal);
            }

            last_seqno = seqno
        }

        Ok(Shard {
            dir: self.dir,
            name: self.name,
            shard_id: self.shard_id,
            journal_limit: self.journal_limit,
            batch_size: self.batch_size,
            fsync: self.fsync,

            dlog_seqno: self.dlog_seqno,
            journals,
            active: self.active,
        })
    }

    pub(crate) fn close(self) -> Result<()> {
        debug!(
            target: "dlogsd",
            "shard:{} {:?}/{} closed", self.shard_id, self.dir, self.name,
        );

        Ok(())
    }

    pub(crate) fn purge(self) -> Result<()> {
        for journal in self.journals.into_iter() {
            journal.purge()?
        }
        self.active.purge()?;

        debug!(
            target: "dlogsd",
            "shard:{} {:?}/{} purged", self.shard_id, self.dir, self.name,
        );

        Ok(())
    }
}

// shards are monotonically increasing number from 1 to N
impl<S, T> Shard<S, T> {
    #[inline]
    pub(crate) fn into_journals(mut self) -> Vec<Journal<S, T>> {
        self.journals.push(self.active);
        self.journals
    }
}

impl<S, T> Shard<S, T>
where
    S: 'static + Send + Default + Serialize,
    T: 'static + Send + Default + Serialize,
{
    pub(crate) fn into_thread(self) -> rt::Thread<OpRequest<T>, OpResponse, Shard<S, T>>
    where
        S: DlogState<T>,
    {
        let seqno = self.dlog_seqno.load(SeqCst);
        debug!(
            target: "dlogsd",
            "convert shard:{} {:?}/{} to thread, at seqno:{}",
            self.shard_id, self.dir, self.name, seqno
        );
        let name = format!("wal-{}-{}", self.shard_id, self.name);
        rt::Thread::new(name, move |rx| move || self.routine(rx))
    }
}

impl<S, T> Shard<S, T>
where
    S: Default + Serialize,
    T: Default + Serialize,
{
    fn routine(mut self, rx: rt::Rx<OpRequest<T>, OpResponse>) -> Result<Self>
    where
        S: DlogState<T>,
    {
        'outer: loop {
            let mut cmds = vec![];
            let mut retry = self.batch_size * 2;
            loop {
                match rx.try_recv() {
                    Ok(cmd) => {
                        cmds.push(cmd);
                        if cmds.len() > self.batch_size || retry <= 0 {
                            break;
                        }
                    }
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => break 'outer Ok(self),
                }
                retry -= 1;
            }

            match self.do_cmds(cmds) {
                Ok(false) => (),
                Ok(true) => break Ok(self),
                Err(err) => break Err(err),
            }

            thread::sleep(Duration::from_millis(1));
        }
    }

    // return true if main loop should exit.
    fn do_cmds(
        &mut self,
        cmds: Vec<(OpRequest<T>, Option<mpsc::Sender<OpResponse>>)>,
    ) -> Result<bool>
    where
        S: DlogState<T>,
    {
        use std::sync::atomic::Ordering::AcqRel;

        for cmd in cmds {
            match cmd {
                (OpRequest::Op { op }, Some(caller)) => {
                    let seqno = self.dlog_seqno.fetch_add(1, AcqRel);
                    self.active.add_entry(DEntry::new(seqno, op))?;
                    err_at!(IPCFail, caller.send(OpResponse::new_seqno(seqno)))?;
                }
                (OpRequest::PurgeTill { before }, Some(caller)) => {
                    let before = self.do_purge_till(before)?;
                    err_at!(IPCFail, caller.send(OpResponse::new_purged(before)))?;
                }
                _ => err_at!(Fatal, msg: format!("unreachable"))?,
            }
        }

        match self.active.flush1(self.journal_limit, self.fsync)? {
            None => (),
            Some((buffer, batch)) => {
                self.rotate_journal()?;
                self.active.flush2(&buffer, batch, self.fsync)?;
            }
        }

        Ok(false)
    }

    // return seqno or io::Error.
    fn do_purge_till(&mut self, before: Bound<u64>) -> Result<Bound<u64>> {
        for _ in 0..self.journals.len() {
            match (self.journals[0].to_last_seqno()?, before) {
                (Some(li), Bound::Included(before)) if li <= before => {
                    self.journals.remove(0).purge()?;
                }
                (Some(li), Bound::Excluded(before)) if li < before => {
                    self.journals.remove(0).purge()?;
                }
                (Some(_), Bound::Unbounded) => {
                    self.journals.remove(0).purge()?;
                }
                _ => break,
            }
        }

        Ok(before)
    }

    fn rotate_journal(&mut self) -> Result<()>
    where
        S: DlogState<T>,
    {
        let num = {
            let jf = JournalFile(self.active.file_path.clone()).next();
            let p: (String, String, usize, usize) = TryFrom::try_from(jf)?;
            p.3
        };
        let (d, n, i) = (self.dir.clone(), self.name.clone(), self.shard_id);
        let new_active = Journal::<S, T>::new_active(d, n, i, num)?;

        let active = mem::replace(&mut self.active, new_active);
        self.journals.push(active.into_archive()?);

        Ok(())
    }
}

pub(crate) struct Journal<S, T> {
    num: usize,               // starts from 1
    file_path: ffi::OsString, // dir/{name}-shard-{shard_id}-journal-{num}.dlog

    inner: InnerJournal<S, T>,
}

enum InnerJournal<S, T> {
    // Active journal, the latest journal, in the journal-set. A journal
    // set is managed by Shard.
    Active {
        file_path: ffi::OsString,
        fd: fs::File,
        batches: Vec<Batch<S, T>>,
        active: Batch<S, T>,
    },
    // All journals except lastest journal are archives, which means only
    // the metadata for each batch shall be stored.
    Archive {
        file_path: ffi::OsString,
        batches: Vec<Batch<S, T>>,
    },
    // Cold journals are colder than archives, that is, they are not
    // required by the application, may be as frozen-backup.
    Cold {
        file_path: ffi::OsString,
    },
}

impl<S, T> fmt::Debug for Journal<S, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        use InnerJournal::{Active, Archive, Cold};

        match &self.inner {
            Active { .. } => write!(f, "Journal<active,{:?}>", self.file_path),
            Archive { .. } => write!(f, "Journal<archive,{:?}>", self.file_path),
            Cold { .. } => write!(f, "Journal<cold,{:?}>", self.file_path),
        }
    }
}

impl<S, T> Journal<S, T>
where
    S: Default + Serialize,
    T: Serialize,
{
    fn new_active(
        dir: ffi::OsString,
        name: String,
        shard_id: usize,
        num: usize,
    ) -> Result<Journal<S, T>>
    where
        S: DlogState<T>,
    {
        let file: ffi::OsString = {
            let state: S = Default::default();
            let name = name.to_string();
            let typ = state.to_type();
            let file: JournalFile = (name, typ, shard_id, num).into();
            file.into()
        };
        let fpath = {
            let mut fpath = path::PathBuf::new();
            fpath.push(&dir);
            fpath.push(&file);
            fpath
        };

        fs::remove_file(&fpath).ok(); // cleanup a single journal file

        let fd = {
            let mut opts = fs::OpenOptions::new();
            err_at!(IoError, opts.append(true).create_new(true).open(&fpath))?
        };

        debug!(target: "dlogjn", "New active journal {:?}", fpath);

        Ok(Journal {
            num: num,
            file_path: fpath.clone().into_os_string(),

            inner: InnerJournal::Active {
                file_path: fpath.into_os_string(),
                fd: fd,
                batches: Default::default(),
                active: Batch::default_active(),
            },
        })
    }

    fn new_archive(
        dir: ffi::OsString,
        name: String,
        shard_id: usize,
        fname: ffi::OsString,
    ) -> Option<Journal<S, T>>
    where
        S: DlogState<T>,
    {
        let (nm, _typ, id, num): (String, String, usize, usize) =
            TryFrom::try_from(JournalFile(fname.clone())).ok()?;

        if nm != name || id != shard_id {
            return None;
        }

        let file_path = {
            let mut fp = path::PathBuf::new();
            fp.push(&dir);
            fp.push(fname);
            fp.into_os_string()
        };

        let mut batches = vec![];
        let mut fd = util::open_file_r(&file_path).ok()?;
        let mut fpos = 0_usize;
        let till: usize = convert_at!(fd.metadata().ok()?.len()).ok()?;

        while fpos < till {
            let n = cmp::min(DLOG_BLOCK_SIZE, till - fpos) as u64;
            let fpos_u64: u64 = convert_at!(fpos).ok()?;
            let block = read_file!(&mut fd, fpos_u64, n, "journal corrupted").ok()?;

            let mut m = 0_usize;
            while m < block.len() {
                let mut batch: Batch<S, T> = Batch::default_active();
                m += batch
                    .decode_refer(&block[m..], convert_at!((fpos + m)).ok()?)
                    .ok()?;
                batches.push(batch);
            }
            fpos += block.len();
        }

        debug!(
            target: "dlogjn",
            "Opening archive journal {:?}, loaded {} batches",
            file_path, batches.len()
        );

        Some(Journal {
            num: num,
            file_path: file_path.clone(),

            inner: InnerJournal::Archive {
                file_path: file_path.clone(),
                batches,
            },
        })
    }

    // don't load the batches. use this only for purging the journal.
    fn new_cold(
        dir: ffi::OsString,
        name: String,
        shard_id: usize,
        fname: ffi::OsString,
    ) -> Option<Journal<S, T>> {
        let (nm, _, id, num): (String, String, usize, usize) =
            TryFrom::try_from(JournalFile(fname.clone())).ok()?;

        if nm != name || id != shard_id {
            return None;
        }

        let file_path = {
            let mut fp = path::PathBuf::new();
            fp.push(&dir);
            fp.push(fname);
            fp.into_os_string()
        };

        debug!(target: "dlogjn", "Opening cold journal {:?}, loaded", file_path);

        Some(Journal {
            num: num,
            file_path: file_path.clone(),

            inner: InnerJournal::Cold {
                file_path: file_path.clone(),
            },
        })
    }

    fn purge(self) -> Result<()> {
        let file_path = self.to_file_path();
        err_at!(IoError, fs::remove_file(&file_path))?;

        debug!(target: "dlogjn", "purged {:?}", file_path);

        Ok(())
    }

    pub(crate) fn into_archive(mut self) -> Result<Self>
    where
        S: DlogState<T>,
    {
        use InnerJournal::{Active, Archive, Cold};

        match self.inner {
            Active {
                file_path, batches, ..
            } => {
                self.inner = Archive { file_path, batches };
                Ok(self)
            }
            Cold { file_path } => {
                let (dir, fname) = {
                    let fp = path::Path::new(&file_path);
                    let fname = fp.file_name().unwrap().to_os_string();
                    let dir = fp.parent().unwrap().as_os_str().to_os_string();
                    (dir, fname)
                };

                let (name, _, shard_id, _): (String, String, usize, usize) =
                    TryFrom::try_from(JournalFile(fname.clone())).unwrap();

                Ok(Self::new_archive(dir, name, shard_id, fname).unwrap())
            }
            _ => err_at!(Fatal, msg: format!("unreachable")),
        }
    }
}

impl<S, T> Journal<S, T> {
    pub(crate) fn to_last_seqno(&self) -> Result<Option<u64>> {
        use InnerJournal::{Active, Archive};

        match &self.inner {
            Active {
                batches, active, ..
            } => match active.to_last_seqno() {
                seqno @ Some(_) => Ok(seqno),
                None => match batches.last() {
                    Some(last) => Ok(last.to_last_seqno()),
                    None => Ok(None),
                },
            },
            Archive { batches, .. } => match batches.last() {
                Some(last) => Ok(last.to_last_seqno()),
                None => Ok(None),
            },
            _ => err_at!(Fatal, msg: format!("unreachable"))?,
        }
    }

    pub(crate) fn to_file_path(&self) -> ffi::OsString {
        match &self.inner {
            InnerJournal::Active { file_path, .. } => file_path,
            InnerJournal::Archive { file_path, .. } => file_path,
            InnerJournal::Cold { file_path } => file_path,
        }
        .clone()
    }

    pub(crate) fn is_cold(&self) -> bool {
        match self.inner {
            InnerJournal::Active { .. } => false,
            InnerJournal::Archive { .. } => false,
            InnerJournal::Cold { .. } => true,
        }
    }

    pub(crate) fn into_batches(self) -> Result<Vec<Batch<S, T>>> {
        let batches = match self.inner {
            InnerJournal::Active {
                mut batches,
                mut active,
                ..
            } => {
                batches.push(mem::replace(&mut active, Default::default()));
                batches
            }
            InnerJournal::Archive { batches, .. } => batches,
            _ => err_at!(Fatal, msg: format!("unreachable"))?,
        };

        Ok(batches)
    }

    pub(crate) fn add_entry(&mut self, entry: DEntry<T>) -> Result<()>
    where
        S: DlogState<T>,
    {
        match &mut self.inner {
            InnerJournal::Active { active, .. } => active.add_entry(entry),
            _ => err_at!(Fatal, msg: format!("unreachable")),
        }
    }

    pub(crate) fn into_cold(mut self) -> Result<Self> {
        use InnerJournal::{Active, Archive, Cold};

        self.inner = match self.inner {
            Archive { file_path, .. } => Cold { file_path },
            inner @ Cold { .. } => inner,
            Active { .. } => err_at!(Fatal, msg: format!("unreachable"))?,
        };
        Ok(self)
    }
}

impl<S, T> Journal<S, T>
where
    S: Default + Serialize,
    T: Serialize,
{
    // periodically flush journal entries from memory to disk.
    fn flush1(
        &mut self,
        journal_limit: usize,
        fsync: bool,
    ) -> Result<Option<(Vec<u8>, Batch<S, T>)>> {
        let (file_path, fd, batches, active, rotate) = match &mut self.inner {
            InnerJournal::Active {
                file_path,
                fd,
                batches,
                active,
            } => {
                let limit: u64 = convert_at!(journal_limit)?;
                let rotate = err_at!(IoError, fd.metadata())?.len() > limit;
                Ok((file_path, fd, batches, active, rotate))
            }
            _ => err_at!(Fatal, msg: format!("unreachable")),
        }?;

        match rotate {
            true if active.len()? > 0 => Ok(Some(active.to_refer(0)?)),
            false if active.len()? > 0 => {
                let (buffer, batch) = {
                    let fpos = err_at!(IoError, fd.metadata())?.len();
                    active.to_refer(fpos)?
                };
                batches.push(batch);
                write_file!(fd, &buffer, file_path.clone(), "wal-flush1")?;
                if fsync {
                    err_at!(IoError, fd.sync_all())?;
                }
                *active = Batch::default_active();
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn flush2(&mut self, buffer: &[u8], batch: Batch<S, T>, fsync: bool) -> Result<()> {
        let (file_path, fd, batches, active) = match &mut self.inner {
            InnerJournal::Active {
                file_path,
                fd,
                batches,
                active,
            } => Ok((file_path, fd, batches, active)),
            _ => err_at!(Fatal, msg: format!("unreachable")),
        }?;

        write_file!(fd, &buffer, file_path.clone(), "wal-flush2")?;
        if fsync {
            err_at!(IoError, fd.sync_all())?;
        }
        batches.push(batch);
        *active = Batch::default_active();

        Ok(())
    }
}

#[cfg(test)]
#[path = "dlog_journal_test.rs"]
mod dlog_journal_test;
