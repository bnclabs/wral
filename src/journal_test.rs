use arbitrary::Unstructured;
use rand::{prelude::random, rngs::StdRng, Rng, SeedableRng};

use super::*;
use crate::state;

#[test]
fn test_journal() {
    let seed: u64 = random();
    println!("test_journal {}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    let name = "test_journal";
    let dir = tempfile::tempdir().unwrap();
    println!("test_journal {:?}", dir.path());
    let mut jn = Journal::start(name, dir.path().as_ref(), 0, state::NoState).unwrap();
    assert_eq!(jn.to_journal_number(), 0);
    assert_eq!(jn.len_batches(), 0);
    assert_eq!(jn.to_state(), state::NoState);

    let mut entries: Vec<entry::Entry> = (0..1_000_000)
        .map(|_i| {
            let bytes = rng.gen::<[u8; 32]>();
            let mut uns = Unstructured::new(&bytes);
            uns.arbitrary::<entry::Entry>().unwrap()
        })
        .collect();
    entries.sort();
    entries.dedup_by(|a, b| a.to_seqno() == b.to_seqno());

    let mut n_batches = 0;
    let mut offset = 0;
    for _i in 0..1000 {
        let n = rng.gen::<u8>();
        for _j in 0..n {
            let entry = entries[offset].clone();
            jn.add_entry(entry.clone()).unwrap();
            entries.push(entry);
            offset += 1;
        }

        assert_eq!(jn.to_last_seqno(), Some(entries[offset - 1].to_seqno()));

        jn.flush().unwrap();
        if n > 0 {
            n_batches += 1;
        }

        assert_eq!(jn.to_last_seqno(), Some(entries[offset - 1].to_seqno()));
    }
    assert_eq!(n_batches, jn.len_batches());

    let iter = RdJournal::from_journal(&jn, 0..=u64::MAX).unwrap();
    let jn_entries: Vec<entry::Entry> = iter.map(|x| x.unwrap()).collect();
    let entries = entries[..offset].to_vec();
    assert_eq!(entries.len(), jn_entries.len());
    assert_eq!(entries, jn_entries);

    {
        let (load_jn, _) =
            Journal::<state::NoState>::load(name, &jn.to_file_path()).unwrap();
        let iter = RdJournal::from_journal(&load_jn, 0..=u64::MAX).unwrap();
        let jn_entries: Vec<entry::Entry> = iter.map(|x| x.unwrap()).collect();
        let entries = entries[..offset].to_vec();
        assert_eq!(entries.len(), jn_entries.len());
        assert_eq!(entries, jn_entries);
    }

    jn.purge().unwrap();
    dir.close().unwrap();
}
