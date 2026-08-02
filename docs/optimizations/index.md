# Optimizations

The running ledger of every deliberate technique StreamFusion uses to keep throughput high, one
page per currently-live technique. When a commit's purpose is speed rather than coverage, its page
gets a new entry in the same commit — what the optimization is, why it works, and the measured
improvement if benchmarked, with a reference back to the commit that introduced it.

An optimization's page describes its *current* shape; where a technique went through several
iterations, earlier steps are summarized as history within that page rather than getting pages of
their own — this ledger tracks what the code does today, not a commit-by-commit changelog (that's
what `git log` is for).

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
