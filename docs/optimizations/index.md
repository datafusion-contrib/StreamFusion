# Optimizations

The running ledger of every deliberate technique StreamFusion uses to keep throughput high, one
page per currently-live technique. When a commit's purpose is speed rather than coverage, its page
gets a new entry in the same commit — what the optimization is, why it works, and the measured
improvement if benchmarked, with a reference back to the commit that introduced it.

An optimization's page describes its *current* shape; where a technique went through several
iterations, earlier steps are summarized as history within that page rather than getting pages of
their own — this ledger tracks what the code does today, not a commit-by-commit changelog (that's
what `git log` is for).

CI build reuse and runner scheduling are documented separately in the
[upstream-suite guide](../upstream-flink-suite.md#shared-builds-and-runtime-shards): compiled Flink
caching, shared build artifacts, grouped connector suites and combined coverage verification.
Those measurements describe CI job counts and elapsed time, not SQL execution throughput.

- [Optional Linux RocksDB io_uring reads](rocksdb-io-uring.md): configurable concurrent SST batch reads, with full before/after trial data.

## How these numbers are measured

- **Benchmark-gated**: a change that doesn't move the numbers is rejected, not merged with an
  aspirational justification.
- **Differential profiling**: sampling native vs. stock Flink on the same query isolates what
  native pays that Flink doesn't — this is what repeatedly localized gaps to allocator churn,
  hashing, and `ScalarValue` state rather than the compute itself.
- **Fresh-JVM, idle-machine, pinned-codegen runs**: combined runs accumulate GC pressure that
  disproportionately slows the alloc-heavier side, and unpinned Rust codegen units have swung hot
  loops by ~50% from unrelated code growth — both are controlled for.
- **Release builds only** — see [Benchmarks](../benchmarks.md); every number on these pages comes
  from a release build, never debug.
