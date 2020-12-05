[![Rustdoc](https://img.shields.io/badge/rustdoc-hosted-blue.svg)](https://docs.rs/wral)

_Write ahead logging for rust applications_. Write ahead logging is a
crucial component for applications requiring data persistence and durability.
Many times it is inefficient to flush and sync new data (or modifications
to existing data) to on-disk data-structures, like an index.
Write-ahead-logging facilitates by ingesting write operations by appending
and syncing it to disk and allows applications to pre-process a batch of
write-operations and write them to on-disk structures in the most efficient
manner.

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
* [x] `Wal` type is parameterised over a state type `S`. This is helpful for
  using `Wal` type to be used with consensus protocol like [Raft][raft].
* [x] Concurrent readers and writers into single log instance.

**Concurrency**

A single log-instance can be cloned and shared among multiple threads
for concurrent writing and reading. All write operations are serialized.
While read operations and write-operation are mutually exclusive,
concurrent reads are allowed.

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
