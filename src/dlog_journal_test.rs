use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

use super::*;
use crate::wal;

#[test]
fn test_journal_file() {
    let name = "my-dlog".to_string();
    let typ = "wal".to_string();
    let (shard_id, num) = (10, 1);

    let journal_file: JournalFile = (name.clone(), typ.clone(), shard_id, num).into();
    assert_eq!(
        journal_file.clone().0.into_string().unwrap(),
        "my-dlog-wal-shard-010-journal-001.dlog".to_string()
    );

    let journal_file = journal_file.next();
    assert_eq!(
        journal_file.clone().0.into_string().unwrap(),
        "my-dlog-wal-shard-010-journal-002.dlog".to_string()
    );

    let (nm, t, id, num) = journal_file.try_into().unwrap();
    assert_eq!(nm, name);
    assert_eq!(t, typ);
    assert_eq!(shard_id, id);
    assert_eq!(num, 2);
}

#[test]
fn test_journal() {
    let seed: u128 = random();
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let dir = {
        let mut dir = path::PathBuf::new();
        dir.push(std::env::temp_dir());
        dir.push("test-journal");
        dir.into_os_string()
    };
    fs::create_dir_all(&dir).unwrap();

    let mut journal: Journal<wal::State, wal::Op<i64, i64>> = {
        let name = "journal".to_string();
        Journal::new_active(dir, name, 1, 1).unwrap()
    };
    let limit = 1_000_000_000;

    for i in 0..100 {
        for j in 0..1000 {
            let op = wal::Op::<i64, i64>::new_set(10 * i + j, 20 + i);
            let seqno = (i * 1000 + j) as u64 + 1;
            journal.add_entry(DEntry::new(seqno, op)).unwrap();
        }
        let fsync: bool = rng.gen();
        assert_eq!(journal.flush1(limit, fsync).unwrap().is_none(), true);
    }

    assert_eq!(journal.to_last_seqno().unwrap(), Some(100_000));
    let rf: &ffi::OsStr = "journal-wal-shard-001-journal-001.dlog".as_ref();
    assert_eq!(
        path::Path::new(&journal.to_file_path())
            .file_name()
            .unwrap(),
        rf
    );
    assert_eq!(journal.is_cold(), false);

    let mut fd = {
        let file_path = journal.to_file_path();
        let mut opts = fs::OpenOptions::new();
        opts.read(true).open(&file_path).unwrap()
    };
    for (i, batch) in journal.into_batches().unwrap().into_iter().enumerate() {
        let batch = batch.into_active(&mut fd).unwrap();
        for (j, entry) in batch.into_entries().unwrap().into_iter().enumerate() {
            let (seqno, op) = entry.into_seqno_op();
            let ref_seqno = (i * 1000 + j) as u64 + 1;
            assert_eq!(seqno, ref_seqno);
            let (k, v) = ((10 * i + j) as i64, (20 + i) as i64);
            let ref_op = wal::Op::<i64, i64>::new_set(k, v);
            assert_eq!(op, ref_op);
        }
    }
}

#[test]
fn test_shard() {
    let seed: u128 = random();
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());
    println!("seed:{}", seed);

    let dir = {
        let mut dir = path::PathBuf::new();
        dir.push(std::env::temp_dir());
        dir.push("test-shard");
        dir.into_os_string()
    };
    fs::create_dir_all(&dir).unwrap();

    for _ in 0..10 {
        let name = "myshard".to_string();
        let shard_id = 1;
        let journal_limit = 1_000_000;
        let fsync: bool = rng.gen();
        let dlog_seqno = Arc::new(AtomicU64::new(1));
        let batch_size = ((rng.gen::<usize>() % 1000) + 1) as i64;
        let n_batches = (rng.gen::<usize>() % 30) as i64;

        println!(
            "dir:{:?} fsync:{} batch_size:{} n_batches:{}",
            dir, fsync, batch_size, n_batches
        );

        let tshard = Shard::<wal::State, wal::Op<i64, i64>>::create(
            dir.clone(),
            name.clone(),
            shard_id,
            Arc::clone(&dlog_seqno),
            journal_limit,
            batch_size as usize,
            fsync,
        )
        .unwrap()
        .into_thread();

        let mut ref_entries = vec![];

        for i in 0..n_batches {
            for j in 0..batch_size {
                let op = match rng.gen::<u8>() % 3 {
                    0 => wal::Op::<i64, i64>::new_set(10 * i + j, 20 + i),
                    1 => {
                        let cas = i as u64;
                        wal::Op::<i64, i64>::new_set_cas(10 * i + j, 20 + i, cas)
                    }
                    2 => wal::Op::<i64, i64>::new_delete(10 * i + j),
                    _ => unreachable!(),
                };

                let seqno = {
                    match tshard.request(OpRequest::new_op(op.clone())).unwrap() {
                        OpResponse::Seqno(seqno) => seqno,
                        _ => unreachable!(),
                    }
                };

                ref_entries.push((seqno, op.clone()));
            }
        }

        tshard.close_wait().unwrap();
        assert_eq!(dlog_seqno.load(SeqCst), (n_batches * batch_size + 1) as u64);

        let (last_seqno, shard) = Shard::<wal::State, wal::Op<i64, i64>>::load(
            dir.clone(),
            name.clone(),
            shard_id,
            Arc::clone(&dlog_seqno),
            journal_limit,
            batch_size as usize,
            fsync,
        )
        .unwrap();
        assert_eq!(last_seqno, (n_batches * batch_size) as u64);

        let journals = shard.into_journals();
        assert_eq!(dlog_seqno.load(SeqCst), (n_batches * batch_size + 1) as u64);

        if journals.len() > 0 {
            match &journals[0].inner {
                InnerJournal::Archive { batches, .. } if batches.len() > 0 => {
                    assert_eq!(batches[0].to_first_seqno().unwrap(), 1);
                }
                InnerJournal::Archive { .. } => (),
                _ => unreachable!(),
            };
        }

        let mut entries = vec![];
        for journal in journals.into_iter() {
            let mut fd = {
                let file_path = journal.to_file_path();
                let mut opts = fs::OpenOptions::new();
                opts.read(true).open(&file_path).unwrap()
            };
            for batch in journal.into_batches().unwrap().into_iter() {
                let batch = batch.into_active(&mut fd).unwrap();
                for entry in batch.into_entries().unwrap().into_iter() {
                    entries.push(entry);
                }
            }
        }

        assert_eq!(ref_entries.len(), entries.len());

        for (r, e) in ref_entries.into_iter().zip(entries.into_iter()) {
            let (seqno, op) = e.into_seqno_op();
            assert_eq!(seqno, r.0);
            assert_eq!(op, r.1);
        }

        let (_, shard) = Shard::<wal::State, wal::Op<i64, i64>>::load(
            dir.clone(),
            name.clone(),
            shard_id,
            Arc::clone(&dlog_seqno),
            journal_limit,
            batch_size as usize,
            fsync,
        )
        .unwrap();

        shard.purge().unwrap();
    }
}
