[![Documentation](https://docs.rs/wral/badge.svg?style=flat-square)](https://docs.rs/wral)

_Write ahead logging for rust applications_. Write ahead logging is a
crucial component for applications requiring data durability. Many times it
is inefficient to flush and sync new data (or modifications to existing data)
to on-disk data-structures, like an index. Write-ahead-logging facilitates
by ingesting write operations by appending and syncing it to disk and allows
applications to pre-process a batch of write-operations and write them to
on-disk structures in the most efficient manner.

Goals
-----

* [x] Serialize write operations to an append only journal file.
* [x] Generate monotonically increasing `sequence-number` and return the
  same to the application.
* [x] Configurable limit for journal file, beyond which log files are rotate.
* [x] Configurable fsync, for persistence guarantee.
* [x] Iterate over all entries persisted in the log file in monotonically
  increasing `seqno` order.
* [x] Range over a subset of entries, specified with `start-seqno` and
  `end-seqno`.
* [x] `Wal` type is parameterized over a state type `S`. This is helpful for
  using `Wal` type to be used with consensus protocol like [Raft][raft].
* [x] Concurrent readers and writers into single log instance.

**Concurrency**

A single log-instance can be cloned and shared among multiple threads
for concurrent writing and reading. All write operations are serialized.
While read operations and write-operation are mutually exclusive,
concurrent reads are allowed.

Performance
-----------

Single threaded write performance with different payload size and
`fsync` enabled.

payload |  total-entries | elapsed-time | throughput
--------|----------------|--------------|------------
  100   |   10000        |   31s        |  300/s
  1000  |   10000        |   31s        |  300/s
  10000 |   10000        |   31s        |  300/s
  10000 |    1000        |   3.1s       |  300/s

Multi-threaded write performance with constant payload size of
100-bytes per operation and `fsync` enabled.

threads |  total-entries | elapsed-time | throughput
--------|----------------|--------------|------------
  1     |   10000        |  31s         |   300/s
  2     |   20000        |  60s         |   300/s
  4     |   40000        |  59s         |   650/s
  8     |   80000        |  54s         |  1300/s
  16    |  160000        |  50s         |  3200/s

Multi-threaded read performance with constant payload size of
100-bytes per operation and `fsync` enabled.

threads |  total-entries | elapsed-time | throughput
--------|----------------|--------------|------------
  1     |   10000        |     .15s     |  66000/s
  2     |   20000        |     .28s     |  71000/s
  4     |   40000        |     .38s     | 105000/s
  8     |   80000        |     .62s     | 130000/s
  16    |  160000        |    1.10s     | 150000/s 

Contribution
------------

* Simple workflow. Fork, modify and raise a pull request.
* Before making a PR,
  * Run `cargo build` to make sure 0 warnings and 0 errors.
  * Run `cargo test` to make sure all test cases are passing.
  * Run `cargo bench` to make sure all benchmark cases are passing.
  * Run `cargo +nightly clippy --all-targets --all-features` to fix clippy issues.
  * [Install][spellcheck] and run `cargo spellcheck` to remove common spelling mistakes.
* [Developer certificate of origin][dco] is preferred.

[spellcheck]: https://github.com/drahnr/cargo-spellcheck
[dco]: https://developercertificate.org/
[raft]: https://raft.github.io
