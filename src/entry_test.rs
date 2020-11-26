use super::*;

use std::io::Write;

#[test]
fn test_entry() {
    let _r_entry = DEntry::<i64>::default();

    let r_entry = DEntry::<i64>::new(10, 20);

    {
        let entry = DEntry::<i64>::new(10, 20);
        assert_eq!(r_entry.seqno, entry.seqno);
        assert_eq!(r_entry.op, entry.op);

        let (seqno, op) = entry.into_seqno_op();
        assert_eq!(seqno, 10);
        assert_eq!(op, 20);
    }

    let mut buf = vec![];
    let n = r_entry.encode(&mut buf).unwrap();
    assert_eq!(n, 16);

    {
        let mut entry: DEntry<i64> = Default::default();
        entry.decode(&buf).unwrap();
        assert_eq!(entry.seqno, r_entry.seqno);
        assert_eq!(entry.op, r_entry.op);
    }
}

#[test]
fn test_batch1() {
    use crate::wal;

    let batch1: Batch<wal::State, wal::Op<i64, i64>> = Default::default();
    let batch2: Batch<wal::State, wal::Op<i64, i64>> = Default::default();
    assert!(batch1 == batch2);
}

#[test]
fn test_batch2() {
    use crate::wal;

    let validate = |abatch: Batch<wal::State, wal::Op<i64, i64>>| {
        abatch
            .into_entries()
            .unwrap()
            .into_iter()
            .enumerate()
            .for_each(|(i, e)| {
                let (seqno, op) = e.into_seqno_op();
                assert_eq!(seqno, (i + 1) as u64);
                assert_eq!(op, wal::Op::<i64, i64>::new_set(10, 20));
            })
    };

    let batch = {
        let mut batch = Batch::<wal::State, wal::Op<i64, i64>>::default_active();

        assert_eq!(batch.len().unwrap(), 0);

        for i in 0..100 {
            let op = wal::Op::new_set(10, 20);
            batch.add_entry(DEntry::new(i + 1, op)).unwrap();
        }
        batch
    };
    assert_eq!(batch.to_first_seqno().unwrap(), 1);
    assert_eq!(batch.to_last_seqno().unwrap(), 100);
    assert_eq!(batch.len().unwrap(), 100);

    validate(batch.clone());

    let mut buf = vec![];
    let length = batch.encode_active(&mut buf).unwrap();
    assert_eq!(length, 4099);

    let file = {
        let mut dir = std::env::temp_dir();
        dir.push("test-dlog-entry-batch2");
        fs::create_dir_all(&dir).unwrap();
        dir.push("batch2.dlog");
        dir.into_os_string()
    };
    fs::File::create(&file).unwrap().write(&buf).unwrap();

    let rbatch = Batch::<wal::State, wal::Op<i64, i64>>::new_refer(
        //
        0, length, 1, 100,
    );
    let mut fd = fs::File::open(&file).unwrap();
    let abatch = rbatch.into_active(&mut fd).unwrap();
    validate(abatch);

    let mut batch = Batch::<wal::State, wal::Op<i64, i64>>::default_active();
    let n = batch.decode_refer(&buf, 0).unwrap();
    assert_eq!(n, 4099);
    match batch {
        Batch::Refer {
            fpos: 0,
            length: 4099,
            start_seqno: 1,
            last_seqno: 100,
        } => (),
        Batch::Refer {
            fpos,
            length,
            start_seqno,
            last_seqno,
        } => panic!("{} {} {} {}", fpos, length, start_seqno, last_seqno),
        _ => unreachable!(),
    }
}
