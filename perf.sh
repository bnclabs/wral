#! /usr/bin/env bash

# Single threaded, with 100-bytes, 1K, 10K, 1M payload, with fsync true.
cargo run --bin perf --features=perf -- --payload 100 --ops 10000 --threads 1 --size 100000000
cargo run --bin perf --features=perf -- --payload 1000 --ops 10000 --threads 1 --size 100000000
cargo run --bin perf --features=perf -- --payload 10000 --ops 10000 --threads 1 --size 100000000
cargo run --bin perf --features=perf -- --payload 100000 --ops 1000 --threads 1 --size 100000000

# 100-byte payload, [1,2,4,8,16] thread write operations, fsync = true
cargo run --bin perf --features=perf -- --payload 100 --ops 10000 --threads 1 --size 100000000
cargo run --bin perf --features=perf -- --payload 100 --ops 10000 --threads 2 --size 100000000
cargo run --bin perf --features=perf -- --payload 100 --ops 10000 --threads 4 --size 100000000
cargo run --bin perf --features=perf -- --payload 100 --ops 10000 --threads 8 --size 100000000
cargo run --bin perf --features=perf -- --payload 100 --ops 10000 --threads 16 --size 100000000
