use log::debug;
use mkit::{
    cbor::{FromCbor, IntoCbor},
    thread,
};

use std::{
    borrow::BorrowMut,
    mem,
    sync::{
        atomic::{AtomicU64, Ordering::SeqCst},
        Arc, RwLock,
    },
};

use crate::{entry, journal::Journal, state, wral, wral::Config, Error, Result};

#[derive(Debug)]
pub enum Req {
    AddEntry { op: Vec<u8> },
}

#[derive(Debug)]
pub enum Res {
    Seqno(u64),
}

pub struct Writer<S> {
    config: Config,
    seqno: Arc<AtomicU64>,
    pub journals: Vec<Journal<S>>,
    pub journal: Journal<S>,
}

type SpawnWriter<S> = (
    Arc<RwLock<Writer<S>>>,
    thread::Thread<Req, Res, Result<u64>>,
    thread::Tx<Req, Res>,
);

impl<S> Writer<S> {
    pub(crate) fn start(
        config: Config,
        journals: Vec<Journal<S>>,
        journal: Journal<S>,
        seqno: u64,
    ) -> SpawnWriter<S>
    where
        S: state::State,
    {
        let seqno = Arc::new(AtomicU64::new(seqno));
        let w = Arc::new(RwLock::new(Writer {
            config: config.clone(),
            seqno: Arc::clone(&seqno),
            journals,
            journal,
        }));
        let name = format!("wral-writer-{}", config.name);
        let thread_w = Arc::clone(&w);
        let (t, tx) = thread::Thread::new_sync(
            &name,
            wral::SYNC_BUFFER,
            move |rx: thread::Rx<Req, Res>| {
                || {
                    let l = MainLoop {
                        config,
                        seqno,
                        w: thread_w,
                        rx,
                    };
                    l.run()
                }
            },
        );

        (w, t, tx)
    }

    pub fn close(&self) -> Result<u64> {
        let n_batches: usize = self.journals.iter().map(|j| j.len_batches()).sum();
        let (n, m) = match self.journal.len_batches() {
            0 => (self.journals.len(), n_batches),
            n => (self.journals.len() + 1, n_batches + n),
        };
        let seqno = self.seqno.load(SeqCst);
        debug!(
            target: "wral",
            "{:?}/{} closed at seqno {}, with {} journals and {} batches",
            self.config.dir, self.config.name, seqno, m, n
        );
        Ok(self.seqno.load(SeqCst).saturating_sub(1))
    }

    pub fn purge(mut self) -> Result<u64> {
        self.close()?;

        for j in self.journals.drain(..) {
            j.purge()?
        }
        self.journal.purge()?;

        Ok(self.seqno.load(SeqCst).saturating_sub(1))
    }
}

struct MainLoop<S> {
    config: Config,
    seqno: Arc<AtomicU64>,
    w: Arc<RwLock<Writer<S>>>,
    rx: thread::Rx<Req, Res>,
}

impl<S> MainLoop<S>
where
    S: Clone + IntoCbor + FromCbor + state::State,
{
    fn run(self) -> Result<u64> {
        use std::sync::mpsc::TryRecvError;

        // block for the first request.
        'a: while let Ok(req) = self.rx.recv() {
            // then get as many outstanding requests as possible from
            // the channel.
            let mut reqs = vec![req];
            loop {
                match self.rx.try_recv() {
                    Ok(req) => reqs.push(req),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break 'a,
                }
            }
            // and then start processing it in batch.
            let mut w = err_at!(Fatal, self.w.write())?;

            let mut items = vec![];
            for req in reqs.into_iter() {
                match req {
                    (Req::AddEntry { op }, tx) => {
                        let seqno = self.seqno.fetch_add(1, SeqCst);
                        w.journal.add_entry(entry::Entry::new(seqno, op))?;
                        items.push((seqno, tx))
                    }
                }
            }
            w.journal.flush()?;

            for (seqno, tx) in items.into_iter() {
                if let Some(tx) = tx {
                    err_at!(IPCFail, tx.send(Res::Seqno(seqno)))?;
                }
            }

            if w.journal.file_size()? > self.config.journal_limit {
                Self::rotate(w.borrow_mut())?;
            }
        }

        Ok(self.seqno.load(SeqCst).saturating_sub(1))
    }
}

impl<S> MainLoop<S>
where
    S: Clone,
{
    fn rotate(w: &mut Writer<S>) -> Result<()> {
        // new journal
        let journal = {
            let num = w.journal.to_journal_number().saturating_add(1);
            let state = w.journal.to_state();
            Journal::start(&w.config.name, &w.config.dir, num, state)?
        };
        // replace with current journal
        let journal = mem::replace(&mut w.journal, journal);
        let (journal, entries, _) = journal.into_archive();
        if !entries.is_empty() {
            err_at!(Fatal, msg: "unflushed entries {}", entries.len())?
        }
        w.journals.push(journal);
        Ok(())
    }
}
