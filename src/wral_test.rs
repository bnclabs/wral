use arbitrary::Unstructured;
use mkit::cbor::IntoCbor;
use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use super::*;

#[test]
fn test_wral() {
    let seeds: Vec<u128> = vec![random()];
    let seed = seeds[random::<usize>() % seeds.len()];
    // let seed: u128 = 148484157541144179681685363423689665370;
    println!("test_wral {}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let mut config: Config = {
        let bytes = rng.gen::<[u8; 32]>();
        let mut uns = Unstructured::new(&bytes);
        uns.arbitrary().unwrap()
    };
    config.name = "test-wral".to_string();
    config.dir = tempfile::tempdir().unwrap().path().clone().into();

    println!("{:?}", config);
    let mut val = Wral::create(config.clone(), state::NoState).unwrap();

    let mut entries: Vec<entry::Entry> = (0..10_000_000)
        .map(|_i| {
            let bytes = rng.gen::<[u8; 32]>();
            let mut uns = Unstructured::new(&bytes);
            uns.arbitrary::<entry::Entry>().unwrap()
        })
        .collect();
    entries.sort();
    entries.dedup_by(|a, b| a.to_seqno() == b.to_seqno());

    for entry in entries.iter() {
        let entry = entry.clone();

        let mut buf: Vec<u8> = vec![];
        entry.into_cbor().unwrap().encode(&mut buf).unwrap();
        val.add_op(&buf).unwrap();
    }

    val.close().unwrap();
    val.purge().unwrap();
}
