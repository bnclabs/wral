use arbitrary::Unstructured;
use rand::{prelude::random, rngs::SmallRng, Rng, SeedableRng};

use super::*;

#[test]
fn test_wal() {
    let seeds: Vec<u128> = vec![275868558029936601763097336595517926704, random()];
    let seed = seeds[random::<usize>() % seeds.len()];
    // let seed: u128 = 275868558029936601763097336595517926704;
    println!("test_wal {}", seed);
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let mut config: Config = {
        let bytes = rng.gen::<[u8; 32]>();
        let mut uns = Unstructured::new(&bytes);
        uns.arbitrary().unwrap()
    };
    config.name = "test-wal".to_string();
    let dir = tempfile::tempdir().unwrap();
    config.dir = dir.path().clone().into();

    println!("{:?}", config);
    let val = Wal::create(config.clone(), state::NoState).unwrap();

    let n_threads = 1;

    let mut writers = vec![];
    for id in 0..n_threads {
        let wal = val.clone();
        writers.push(std::thread::spawn(move || writer(id, wal, 1000, seed + id)));
    }

    let mut entries: Vec<Vec<entry::Entry>> = vec![];
    for handle in writers {
        entries.push(handle.join().unwrap().unwrap());
    }
    let entries: Vec<entry::Entry> = entries.into_iter().flatten().collect();

    let mut readers = vec![];
    for id in 0..n_threads {
        let wal = val.clone();
        let entries = entries.clone();
        readers.push(std::thread::spawn(move || {
            reader(id, wal, 10, seed + id, entries)
        }));
    }

    for handle in readers {
        handle.join().unwrap().unwrap();
    }

    val.close(true).unwrap();
}

fn writer(_id: u128, wal: Wal, ops: usize, seed: u128) -> Result<Vec<entry::Entry>> {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    let mut entries = vec![];
    for _i in 1..ops {
        let op: Vec<u8> = {
            let bytes = rng.gen::<[u8; 32]>();
            let mut uns = Unstructured::new(&bytes);
            uns.arbitrary().unwrap()
        };
        let seqno = wal.add_op(&op).unwrap();
        entries.push(entry::Entry::new(seqno, op));
    }

    Ok(entries)
}

fn reader(_id: u128, wal: Wal, ops: usize, seed: u128, entries: Vec<entry::Entry>) -> Result<()> {
    let mut rng = SmallRng::from_seed(seed.to_le_bytes());

    for _i in 0..ops {
        match rng.gen::<u8>() % 2 {
            0 => {
                let items: Vec<entry::Entry> = wal.iter().unwrap().map(|x| x.unwrap()).collect();
                assert_eq!(items, entries);
            }
            1 => {
                let start = rng.gen::<usize>() % entries.len();
                let end = start + (rng.gen::<usize>() % (entries.len() - start));
                let (x, y) = (entries[start].to_seqno(), entries[end].to_seqno());
                let items: Vec<entry::Entry> =
                    wal.range(x..y).unwrap().map(|x| x.unwrap()).collect();
                assert_eq!(items, entries[start..end]);
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}
