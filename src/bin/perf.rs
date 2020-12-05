use rand::prelude::random;
use structopt::StructOpt;

use std::time;

use wral::{self};

// Command line options.
#[derive(Clone, StructOpt)]
pub struct Opt {
    #[structopt(long = "seed")]
    seed: Option<u128>,

    #[structopt(long = "name", default_value = "wral-perf")]
    name: String,

    #[structopt(long = "ops", default_value = "1000000")] // default 1M
    ops: usize,

    #[structopt(long = "payload", default_value = "32")] // default 32-bytes
    payload: usize,

    #[structopt(long = "threads", default_value = "8")]
    threads: usize,

    #[structopt(long = "size", default_value = "10000000")] // default 10M bytes
    journal_limit: usize,

    #[structopt(long = "nosync")]
    nosync: bool,
}

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Opt::from_args();
    let seed = opts.seed.unwrap_or_else(random);

    let mut config = wral::Config::new(&opts.name, dir.path().as_os_str());
    config
        .set_journal_limit(opts.journal_limit)
        .set_fsync(!opts.nosync);
    println!("{:?}", config);

    let wal = wral::Wal::create(config, wral::NoState).unwrap();

    let mut writers = vec![];
    for id in 0..opts.threads {
        let wal = wal.clone();
        let opts = opts.clone();
        writers.push(std::thread::spawn(move || writer(id, wal, opts, seed)));
    }

    let mut entries: Vec<Vec<wral::Entry>> = vec![];
    for handle in writers {
        entries.push(handle.join().unwrap());
    }
    let mut entries: Vec<wral::Entry> = entries.into_iter().flatten().collect();
    entries.sort_by_key(|a| a.to_seqno());

    let n = entries.len() as u64;
    let sum = entries.iter().map(|e| e.to_seqno()).sum::<u64>();
    assert_eq!(sum, (n * (n + 1)) / 2);

    let mut readers = vec![];
    for id in 0..opts.threads {
        let wal = wal.clone();
        let entries = entries.clone();
        readers.push(std::thread::spawn(move || reader(id, wal, entries)));
    }

    for handle in readers {
        handle.join().unwrap();
    }

    wal.close(true).unwrap();
}

fn writer(id: usize, wal: wral::Wal, opts: Opt, _seed: u128) -> Vec<wral::Entry> {
    let start = time::Instant::now();

    let mut entries = vec![];
    let op = vec![0; opts.payload];
    for _i in 0..opts.ops {
        let seqno = wal.add_op(&op).unwrap();
        entries.push(wral::Entry::new(seqno, op.clone()));
    }

    println!(
        "w-{:02} took {:?} to write {} ops",
        id,
        start.elapsed(),
        opts.ops
    );
    entries
}

fn reader(id: usize, wal: wral::Wal, entries: Vec<wral::Entry>) {
    let start = time::Instant::now();
    let items: Vec<wral::Entry> = wal.iter().unwrap().map(|x| x.unwrap()).collect();
    assert_eq!(items, entries);

    println!(
        "r-{:02} took {:?} to iter {} ops",
        id,
        start.elapsed(),
        items.len()
    );
}
