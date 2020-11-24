use log::debug;
use mkit::thread;

use std::{
    ffi,
    sync::{Arc, Mutex},
};

use crate::{entry, journal::Journal, Result};

pub enum Req {
    AddEntry { entry: entry::Entry },
}

pub enum Res {
    None,
}

pub struct Writer<S> {
    name: String,
    dir: ffi::OsString,
    seqno: u64,
    journals: Vec<Journal<S>>,
    journal: Journal<S>,
}

impl<S> Writer<S> {
    pub(crate) fn start(
        name: &str,
        dir: &ffi::OsStr,
        journals: Vec<Journal<S>>,
        journal: Journal<S>,
        seqno: u64,
    ) -> (Arc<Mutex<Writer<S>>>, thread::Thread<Req, Res, Result<()>>)
    where
        S: 'static + Send,
    {
        let w = Arc::new(Mutex::new(Writer {
            name: name.to_string(),
            dir: dir.to_os_string(),
            seqno,
            journals,
            journal,
        }));
        let name = format!("wral-writer-{}", name);
        // TODO: avoid magic number
        let thread_w = Arc::clone(&w);
        let t = thread::Thread::new_sync(&name, 1000, move |rx: thread::Rx<Req, Res>| MainLoop {
            w: thread_w,
            rx,
        });
        (w, t)
    }

    pub fn close(&self) -> Result<u64> {
        let n_batches: usize = self.journals.iter().map(|j| j.len_batches()).sum();
        let (n, m) = match self.journal.len_batches() {
            0 => (self.journals.len(), n_batches),
            n => (self.journals.len() + 1, n_batches + n),
        };
        debug!(
            target: "wral",
            "{:?}/{} closed at seqno {}, with {} journals and {} batches",
            self.dir, self.name, self.seqno, m, n
        );
        Ok(self.seqno)
    }

    pub fn purge(mut self) -> Result<u64> {
        let seqno = self.close()?;
        for j in self.journals.drain(..) {
            j.purge()?
        }
        self.journal.purge()?;
        Ok(seqno)
    }
}

struct MainLoop<S> {
    w: Arc<Mutex<Writer<S>>>,
    rx: thread::Rx<Req, Res>,
}

impl<S> FnOnce<()> for MainLoop<S> {
    type Output = Result<()>;

    extern "rust-call" fn call_once(self, _args: ()) -> Self::Output {
        todo!()
    }
}
