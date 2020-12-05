* review "use imports" and "crate imports".
* Benchmark using src/bin/perf.rs and plot graph for
  * Single threaded, [100,1K,10K,1M] payload.
  * 100-byte payload, [1,2,4,8,16] thread write operations, to write 1GB worth of data.
  * 100-byte payload, [1,2,4,8,16] thread read operations, to read 1GB worth of data.
