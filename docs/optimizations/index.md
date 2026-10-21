# Optimizations

The running ledger of every deliberate technique StreamFusion uses to keep throughput high, one
page per currently-live technique. When a commit's purpose is speed rather than coverage, its page
gets a new entry in the same commit — what the optimization is, why it works, and the measured
improvement if benchmarked, with a reference back to the commit that introduced it.

An optimization's page describes its *current* shape; where a technique went through several
iterations, earlier steps are summarized as history within that page rather than getting pages of
their own — this ledger tracks what the code does today, not a commit-by-commit changelog (that's
what `git log` is for).

The [benchmark evidence index](../benchmarks/evidence.md) separates complete-job
comparisons from component measurements, profiles and rejected experiments.
Those historical records support a decision; this ledger describes the current
technique and its measured limits.

CI build reuse and runner scheduling are documented separately in the
[upstream-suite guide](../upstream-flink-suite.md#shared-builds-and-runtime-shards): compiled Flink
caching, shared build artifacts, grouped connector suites and combined coverage verification.
Those measurements describe CI job counts and elapsed time, not SQL execution throughput.

- [Copy canonical state directly](canonical-state-installation.md): remove a full temporary payload copy while retaining managed chunk ownership.
- [Reuse Iceberg boundary setup](iceberg-boundary-overhead.md): cached task schemas and serializers, shared Arrow leases and routing maps.

- [Columnar changelog operators](columnar-changelog-operators.md): pass unchanged filter batches by ownership and avoid standalone row/Arrow round trips.
- [Collection representation work](collection-representation-work.md): omit unnecessary snapshots, reuse nested-row readers and prune UNNEST expansion before copying.
- [Native basic-array searches and extrema](native-array-search.md): scan or reduce flat values without JVM row reconstruction and avoid temporary bytes for wide-precision decimals whose actual value fits a long; retain measured standalone limits.
- [Native collection edits](native-collection-edits.md): build verified collection results from Arrow offsets and values while retaining Flink NULL and ordering semantics.
- [Shared-window state costs](shared-window-state.md): exact memory deltas, batched reservations and preserved accumulator state.
- [Compact window-rank timestamp cells](window-rank-timestamp-cells.md): exact numeric digits avoid per-row timestamp struct slicing.
- [Bounded rank payload rejection](bounded-rank-payloads.md): skip full payload encoding for batches already outside fixed rank bounds.
- [Top-1 winner state](top1-winner-state.md): conditional winner updates and bounded clean decoded RocksDB state.
- [Retire dead Delta checkpoint buffers](delta-live-buffer-retirement.md): release fully superseded Arrow batches and metadata.
- [Preserve Paimon partition pruning](paimon-partition-pruning.md): carry retained partition equalities into released split planning.
- [Indexed pending rowtime deduplication](pending-dedup-index.md): incoming-key probes over bounded Arrow chunks.
- [Optional Linux RocksDB io_uring reads](rocksdb-io-uring.md): configurable concurrent SST batch reads, with full before/after trial data.
- [Fluss Arrow transport](fluss-arrow-transport.md): retained receive buffers, exactly sized serialization and bounded acknowledgement pipelining.

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
