use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use std::{collections::hash_map::RandomState, ffi, mem, path, sync::mpsc, thread};

use super::*;
use crate::{
    core::{Entry, Index, Reader, Writer},
    dlog_journal::Journal,
    llrb::Llrb,
};

// TODO: fine tune Wal test cases

#[test]
fn test_state() {
    let mut state: State = Default::default();
    let entry: DEntry<Op<i32, i32>> = Default::default();
    state.on_add_entry(&entry);

    let mut buf = vec![];
    assert_eq!(state.encode(&mut buf).unwrap(), 0);
    let mut dec_state: State = Default::default();
    dec_state.decode(&buf).unwrap();
    assert!(dec_state == state);
}

#[test]
fn test_op_type() {
    let op_type: OpType = From::from(1_u64);
    assert_eq!(op_type, OpType::Set);
    let op_type: OpType = From::from(2_u64);
    assert_eq!(op_type, OpType::SetCAS);
    let op_type: OpType = From::from(3_u64);
    assert_eq!(op_type, OpType::Delete);
}

#[test]
fn test_op() {
    let mut out = vec![];
    let mut res: Op<i32, i32> = Default::default();

    let op: Op<i32, i32> = Op::new_set(34, 43);
    op.encode(&mut out).unwrap();
    assert_eq!(Op::<i32, i32>::op_type(&out).unwrap(), OpType::Set);
    let n = res.decode(&out).expect("op-set decode failed");
    assert_eq!(n, 24);
    match res {
        Op::Set { key: 34, value: 43 } => (),
        _ => unreachable!(),
    }

    let op: Op<i32, i32> = Op::new_set_cas(-34, -43, 100);
    out.resize(0, 0);
    op.encode(&mut out).unwrap();
    assert_eq!(Op::<i32, i32>::op_type(&out).unwrap(), OpType::SetCAS);
    let n = res.decode(&out).expect("op-set-cas decode failed");
    assert_eq!(n, 32);
    match res {
        Op::SetCAS {
            key: -34,
            value: -43,
            cas: 100,
        } => (),
        _ => unreachable!(),
    }

    let op: Op<i32, i32> = Op::new_delete(34);
    out.resize(0, 0);
    op.encode(&mut out).unwrap();
    assert_eq!(Op::<i32, i32>::op_type(&out).unwrap(), OpType::Delete);
    let n = res.decode(&out).expect("op-delete decode failed");
    assert_eq!(n, 12);
    match res {
        Op::Delete { key: 34 } => (),
        _ => unreachable!(),
    }
}

#[test]
fn test_wal() {
    let seed: u128 = random();

    for i in 0..10 {
        let seed = seed + (i * 100);
        let mut rng = SmallRng::from_seed(seed.to_le_bytes());
        let dir = {
            let mut dir_path = path::PathBuf::new();
            dir_path.push(std::env::temp_dir().into_os_string());
            dir_path.push("test-wal");
            let dir: &ffi::OsStr = dir_path.as_ref();
            dir.clone().to_os_string()
        };
        fs::remove_dir_all(&dir).ok();
        fs::create_dir_all(&dir).unwrap();

        let name = "users".to_string();
        let nshards = 1;
        let journal_limit = (rng.gen::<usize>() % 100_000) + 1_000;
        let batch_size = (rng.gen::<usize>() % 100) + 1;
        let fsync: bool = rng.gen();

        println!(
            "seed:{} dir:{:?} journal_limit:{} batch_size:{} fsync:{}",
            seed, dir, journal_limit, batch_size, fsync
        );

        let mut wl: Wal<i64, i64, RandomState> = {
            let dl = Dlog::<State, Op<i64, i64>>::create(
                dir.clone(),
                name.clone(),
                nshards,
                journal_limit,
                batch_size,
                fsync,
            )
            .unwrap();
            Wal::from_dlog(dl, RandomState::new())
        };

        let mut items = create_wal(seed, &mut wl, false /*nocas*/);
        let last_seqno = items.len() as u64;
        items.sort_by(|x, y| x.0.cmp(&y.0));

        {
            let dl = Dlog::<State, Op<i64, i64>>::load(
                dir.clone(),
                name.clone(),
                nshards,
                journal_limit,
                batch_size,
                fsync,
            )
            .unwrap();
            validate_dlog1(dl, items.clone());
        }

        let befr = rng.gen::<u64>() % last_seqno;
        let before = Bound::Included(befr);
        wl.purge_till(before).unwrap();

        {
            let dl = Dlog::<State, Op<i64, i64>>::load(
                dir.clone(),
                name.clone(),
                nshards,
                journal_limit,
                batch_size,
                fsync,
            )
            .unwrap();
            let mut lis: Vec<u64> = vec![];
            let iter = dl.shards.into_iter().map(|shard| shard.into_journals());
            for journal in iter.flatten() {
                lis.push(journal.to_last_seqno().unwrap().unwrap_or(std::u64::MAX));
            }
            println!("before:{:?} lis:{:?}", before, lis);
            assert!(lis.into_iter().all(|x| x > befr));
        }

        wl.close().unwrap();

        let mut wl: Wal<i64, i64, RandomState> = {
            let dl = Dlog::<State, Op<i64, i64>>::load(
                dir.clone(),
                name.clone(),
                nshards,
                journal_limit,
                batch_size,
                fsync,
            )
            .unwrap();
            Wal::from_dlog(dl, RandomState::new())
        };

        let mut load_items = load_wal(seed, &mut wl, false /*nocas*/);
        load_items.sort_by(|x, y| x.0.cmp(&y.0));
        items.extend_from_slice(&load_items);

        {
            let dl = Dlog::<State, Op<i64, i64>>::load(
                dir.clone(),
                name.clone(),
                nshards,
                journal_limit,
                batch_size,
                fsync,
            )
            .unwrap();
            validate_dlog2(dl, items)
        }

        {
            wl.purge().unwrap();

            let dl = Dlog::<State, Op<i64, i64>>::load(
                dir.clone(),
                name.clone(),
                nshards,
                journal_limit,
                batch_size,
                fsync,
            )
            .unwrap();
            assert_eq!(dl.seqno.load(SeqCst), 1);
            assert_eq!(dl.shards.len(), 1);
        }
    }
}

#[test]
fn test_wal_replay() {
    let seed: u128 = random();

    for i in 0..10 {
        let seed = seed + (i * 100);
        let mut rng = SmallRng::from_seed(seed.to_le_bytes());
        let dir = {
            let mut dir_path = path::PathBuf::new();
            dir_path.push(std::env::temp_dir().into_os_string());
            dir_path.push("test-wal-replay");
            let dir: &ffi::OsStr = dir_path.as_ref();
            dir.clone().to_os_string()
        };
        fs::remove_dir_all(&dir).ok();
        fs::create_dir_all(&dir).unwrap();

        let name = "users".to_string();
        let nshards = 1;
        let journal_limit = (rng.gen::<usize>() % 100_000) + 1_000;
        let batch_size = (rng.gen::<usize>() % 100) + 1;
        let fsync: bool = rng.gen();

        println!(
            "seed:{} dir:{:?} journal_limit:{} batch_size:{} fsync:{}",
            seed, dir, journal_limit, batch_size, fsync
        );

        let mut wl: Wal<i64, i64, RandomState> = {
            let dl = Dlog::<State, Op<i64, i64>>::create(
                dir.clone(),
                name.clone(),
                nshards,
                journal_limit,
                batch_size,
                fsync,
            )
            .unwrap();
            Wal::from_dlog(dl, RandomState::new())
        };

        let mut items = create_wal(seed, &mut wl, true /*nocas*/);
        items.sort_by(|x, y| x.0.cmp(&y.0));
        let mut index: Box<Llrb<i64, i64>> = Llrb::new_lsm("twal-replay");
        let mut ref_index: Box<Llrb<i64, i64>> = Llrb::new_lsm("twal-replay-ref");
        for item in items.into_iter() {
            match item.1 {
                Op::Set { key, value } => {
                    index.set(key, value).unwrap();
                    ref_index.set(key, value).unwrap();
                }
                Op::SetCAS { key, value, .. } => {
                    index.set(key, value).unwrap();
                    ref_index.set(key, value).unwrap();
                }
                Op::Delete { key } => {
                    index.delete(&key).unwrap();
                    ref_index.delete(&key).unwrap();
                }
            };
        }

        let mut items = load_wal(seed, &mut wl, true /*nocas*/);
        items.sort_by(|x, y| x.0.cmp(&y.0));
        for item in items.into_iter() {
            match item.1 {
                Op::Set { key, value } => {
                    ref_index.set(key, value).unwrap();
                }
                Op::SetCAS { key, value, .. } => {
                    ref_index.set(key, value).unwrap();
                }
                Op::Delete { key } => {
                    ref_index.delete(&key).unwrap();
                }
            };
        }

        let seqno = index.to_seqno().unwrap();
        wl.replay(index.as_mut(), seqno).unwrap();

        assert_eq!(index.len(), ref_index.len());
        assert_eq!(index.to_seqno(), ref_index.to_seqno());
        for (e, re) in index.iter().unwrap().zip(ref_index.iter().unwrap()) {
            check_node(&e.unwrap(), &re.unwrap())
        }
    }
}

fn create_wal(
    seed: u128,
    wl: &mut Wal<i64, i64, RandomState>, // wal
    nocas: bool,
) -> Vec<(u64, Op<i64, i64>)> {
    let (tx, rx) = mpsc::channel();
    let mut last_seqno = wl.to_seqno();
    assert_eq!(last_seqno, 1);

    let mut threads = vec![];
    for i in 0..1000 {
        let mut w = wl.to_writer().unwrap();
        let wl_tx = tx.clone();
        let thread = thread::spawn(move || {
            let mut rng = {
                let seed = seed + (i as u128);
                SmallRng::from_seed(seed.to_le_bytes())
            };
            for _ in 0..100 {
                let op: u8 = rng.gen();
                let (seqno, op) = match op % 3 {
                    0 => {
                        let key = rng.gen::<i64>().abs();
                        let value = rng.gen::<i64>().abs();
                        let seqno = w.set(key, value).unwrap();
                        (seqno, Op::new_set(key, value))
                    }
                    1 if nocas => {
                        let key = rng.gen::<i64>().abs();
                        let value = rng.gen::<i64>().abs();
                        let seqno = w.set(key, value).unwrap();
                        (seqno, Op::new_set(key, value))
                    }
                    1 => {
                        let key = rng.gen::<i64>().abs();
                        let value = rng.gen::<i64>().abs();
                        let cas = rng.gen::<u64>();
                        let seqno = w.set_cas(key, value, cas).unwrap();
                        (seqno, Op::new_set_cas(key, value, cas))
                    }
                    2 => {
                        let key = rng.gen::<i64>().abs();
                        let seqno = w.delete(&key).unwrap();
                        (seqno, Op::new_delete(key))
                    }
                    _ => unreachable!(),
                };
                wl_tx.send((seqno, op)).unwrap()
            }
        });
        threads.push(thread)
    }

    for thread in threads.into_iter() {
        thread.join().unwrap();
    }

    mem::drop(tx);

    last_seqno += 100_000;
    let mut items = vec![];
    for item in rx {
        items.push(item)
    }
    assert_eq!(items.len() as u64, last_seqno - 1);

    items
}

fn load_wal(
    seed: u128,
    wl: &mut Wal<i64, i64, RandomState>, // wal
    nocas: bool,
) -> Vec<(u64, Op<i64, i64>)> {
    let (tx, rx) = mpsc::channel();
    let last_seqno = wl.to_seqno();
    assert_eq!(last_seqno, 100_001);

    let mut threads = vec![];
    for i in 0..1000 {
        let mut w = wl.to_writer().unwrap();
        let wl_tx = tx.clone();
        let thread = thread::spawn(move || {
            let mut rng = {
                let seed = seed + (i as u128);
                SmallRng::from_seed(seed.to_le_bytes())
            };
            for _ in 0..100 {
                let op: u8 = rng.gen();
                let (seqno, op) = match op % 3 {
                    0 => {
                        let key = rng.gen::<i64>().abs();
                        let value = rng.gen::<i64>().abs();
                        let seqno = w.set(key, value).unwrap();
                        (seqno, Op::new_set(key, value))
                    }
                    1 if nocas => {
                        let key = rng.gen::<i64>().abs();
                        let value = rng.gen::<i64>().abs();
                        let seqno = w.set(key, value).unwrap();
                        (seqno, Op::new_set(key, value))
                    }
                    1 => {
                        let key = rng.gen::<i64>().abs();
                        let value = rng.gen::<i64>().abs();
                        let cas = rng.gen::<u64>();
                        let seqno = w.set_cas(key, value, cas).unwrap();
                        (seqno, Op::new_set_cas(key, value, cas))
                    }
                    2 => {
                        let key = rng.gen::<i64>().abs();
                        let seqno = w.delete(&key).unwrap();
                        (seqno, Op::new_delete(key))
                    }
                    _ => unreachable!(),
                };
                wl_tx.send((seqno, op)).unwrap()
            }
        });
        threads.push(thread)
    }

    for thread in threads.into_iter() {
        thread.join().unwrap();
    }

    mem::drop(tx);

    let mut items = vec![];
    for item in rx {
        items.push(item)
    }

    items
}

fn validate_dlog1(dl: Dlog<State, Op<i64, i64>>, items: Vec<(u64, Op<i64, i64>)>) {
    let last_seqno = items.len() as u64;
    assert_eq!(dl.seqno.load(SeqCst), last_seqno + 1);
    let mut entries = vec![];
    let journals: Vec<Journal<State, Op<i64, i64>>> = dl
        .shards
        .into_iter()
        .map(|shard| shard.into_journals())
        .flatten()
        .collect();
    for journal in journals.into_iter() {
        let mut fd = {
            let file_path = journal.to_file_path();
            let mut opts = fs::OpenOptions::new();
            opts.read(true).open(&file_path).unwrap()
        };
        let mut es: Vec<DEntry<Op<i64, i64>>> = vec![];
        for batch in journal.into_batches().unwrap().into_iter() {
            let a = {
                let a = batch.into_active(&mut fd).unwrap();
                a.into_entries().unwrap()
            };
            es.extend_from_slice(&a);
        }
        entries.extend_from_slice(&es);
    }

    for (e, (seqno, op)) in entries.into_iter().zip(items.into_iter()) {
        let (rseqno, rop) = e.into_seqno_op();
        assert!(rseqno == seqno);
        assert!(rop == op);
    }
}

fn validate_dlog2(
    dl: Dlog<State, Op<i64, i64>>,
    mut items: Vec<(u64, Op<i64, i64>)>, // includes both create and loaded items
) {
    let last_seqno = items.len() as u64;
    assert_eq!(dl.seqno.load(SeqCst), last_seqno + 1);
    let mut entries = vec![];
    let journals: Vec<Journal<State, Op<i64, i64>>> = dl
        .shards
        .into_iter()
        .map(|shard| shard.into_journals())
        .flatten()
        .collect();
    for journal in journals.into_iter() {
        let mut fd = {
            let file_path = journal.to_file_path();
            let mut opts = fs::OpenOptions::new();
            opts.read(true).open(&file_path).unwrap()
        };
        let mut es: Vec<DEntry<Op<i64, i64>>> = vec![];
        for batch in journal.into_batches().unwrap().into_iter() {
            let a = {
                let a = batch.into_active(&mut fd).unwrap();
                a.into_entries().unwrap()
            };
            es.extend_from_slice(&a);
        }
        entries.extend_from_slice(&es);
    }

    items.drain(..(items.len() - entries.len()));
    for (e, (seqno, op)) in entries.into_iter().zip(items.into_iter()) {
        let (rseqno, rop) = e.into_seqno_op();
        assert!(rseqno == seqno);
        assert!(rop == op);
    }
}

fn check_node(entry: &Entry<i64, i64>, ref_entry: &Entry<i64, i64>) {
    //println!("check_node {} {}", entry.key(), ref_entry.key);
    assert_eq!(entry.to_key(), ref_entry.to_key(), "key");

    let key = entry.to_key();
    //println!("check-node value {:?}", entry.to_native_value());
    assert_eq!(
        entry.to_native_value(),
        ref_entry.to_native_value(),
        "key {}",
        key
    );
    assert_eq!(entry.to_seqno(), ref_entry.to_seqno(), "key {}", key);
    assert_eq!(entry.is_deleted(), ref_entry.is_deleted(), "key {}", key);
    assert_eq!(
        entry.as_deltas().len(),
        ref_entry.as_deltas().len(),
        "key {}",
        key
    );

    //println!("versions {} {}", n_vers, refn_vers);
    let mut vers = entry.versions();
    let mut ref_vers = ref_entry.versions();
    loop {
        match (vers.next(), ref_vers.next()) {
            (Some(e), Some(re)) => {
                assert_eq!(e.to_native_value(), re.to_native_value(), "key {}", key);
                assert_eq!(e.to_seqno(), re.to_seqno(), "key {} ", key);
                assert_eq!(e.is_deleted(), re.is_deleted(), "key {}", key);
            }
            (None, None) => break,
            (Some(e), None) => panic!("invalid entry {} {}", e.to_key(), e.to_seqno()),
            (None, Some(re)) => panic!("invalid entry {} {}", re.to_key(), re.to_seqno()),
        }
    }
}
