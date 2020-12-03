use arbitrary::Unstructured;
use mkit::cbor::IntoCbor;
use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use super::*;

#[test]
fn test_index() {
    let seed: u128 = random();
    println!("test_index {}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let index: Index = {
        let bytes = rng.gen::<[u8; 32]>();
        let mut uns = Unstructured::new(&bytes);
        uns.arbitrary().unwrap()
    };
    assert_eq!(index.to_first_seqno(), index.first_seqno);
    assert_eq!(index.to_first_seqno(), index.first_seqno);

    let val = Index::new(
        index.fpos,
        index.length,
        index.first_seqno,
        index.last_seqno,
    );
    assert_eq!(index, val);
}

#[test]
fn test_batch() {
    let seeds: Vec<u128> = vec![
        225569602694000826843969627559726108824,
        177121329091129258928668221088480874568,
        random(),
        random(),
        random(),
    ];
    let seed = seeds[random::<usize>() % seeds.len()];
    // let seed: u128 = 177121329091129258928668221088480874568;
    println!("test_index {}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let mut batches = vec![];
    for _i in 0..1000 {
        let batch: Batch = {
            let bytes = rng.gen::<[u8; 32]>();
            let mut uns = Unstructured::new(&bytes);
            uns.arbitrary().unwrap()
        };
        batches.push(batch.clone());

        assert_eq!(batch.to_state(), batch.state);
        assert_eq!(batch.to_first_seqno(), batch.first_seqno);
        assert_eq!(batch.to_last_seqno(), batch.last_seqno);
        let r = ops::Range {
            start: 0,
            end: u64::MAX,
        };
        assert_eq!(
            batch.clone().into_iter(r).collect::<Vec<entry::Entry>>(),
            batch.entries
        );

        let cbor: Cbor = batch.clone().into_cbor().unwrap();

        let mut buf: Vec<u8> = vec![];
        let n = cbor.encode(&mut buf).unwrap();
        let (val, m) = Cbor::decode(&mut buf.as_slice()).unwrap();
        assert_eq!(n, m);
        assert_eq!(cbor, val);

        let rbatch = Batch::from_cbor(val).unwrap();
        assert_eq!(batch, rbatch);
    }

    let mut batches: Vec<Batch> = batches
        .into_iter()
        .filter(|b| b.entries.len() > 0)
        .collect();
    batches.sort();
    batches.dedup_by(|a, b| a.first_seqno == b.first_seqno);

    let mut seqno = 0;
    for batch in batches.into_iter() {
        assert!(seqno < batch.first_seqno, "{} {}", seqno, batch.first_seqno);
        assert!(batch.first_seqno <= batch.last_seqno, batch);
        seqno = batch.first_seqno
    }
}

//use mkit::cbor::Cbor;
//
//let seed: u128 = random();
//println!("test_entry {}", seed);
//let mut rng = SmallRng::from_seed(seed.to_le_bytes());
//
//let mut entries = vec![];
//for _i in 0..1000 {
//    let entry: Entry = {
//        let bytes = rng.gen::<[u8; 32]>();
//        let mut uns = Unstructured::new(&bytes);
//        uns.arbitrary().unwrap()
//    };
//    entries.push(entry.clone());
//
//    assert_eq!(entry.to_seqno(), entry.seqno);
//    let (seqno, op) = entry.clone().unwrap();
//    assert_eq!(entry, Entry::new(seqno, op));
//
//    let cbor: Cbor = entry.clone().into_cbor().unwrap();
//    let mut buf: Vec<u8> = vec![];
//    let n = cbor.encode(&mut buf).unwrap();
//    let (val, m) = Cbor::decode(&mut buf.as_slice()).unwrap();
//    assert_eq!(n, m);
//    assert_eq!(cbor, val);
//
//    let entr = Entry::from_cbor(val).unwrap();
//    assert_eq!(entr, entry);
//}
//
//entries.sort();
//let mut seqno = 0;
//for entry in entries.into_iter() {
//    assert!(seqno < entry.seqno, "{} {}", seqno, entry.seqno);
//    seqno = entry.seqno
//}
