# Memory profiling + leak detection for the native side

**Status:** open
**Source:** requested alongside the memory-accounting work (shipped — divergences/16) — accounting
tells Flink how much we hold; this ticket is about *verifying* those numbers are honest and that
nothing leaks.

## Problem
Native memory is invisible to the JVM's tooling: a leaked Arrow buffer, an operator whose state map
never shrinks, or an FFI handover that drops the release callback shows up only as slow RSS growth
and an eventual container OOM. We have no standing way to (a) observe the native footprint of a
running job, (b) prove an operator releases everything on close, or (c) catch a leak introduced by a
new operator before it ships.

## Goal
A profiling + leak-detection layer so we can say with evidence "the native side holds X bytes and
frees all of it," and so regressions are caught by tests, not production OOMs.

## What to build
1. **Allocator/pool introspection.** Expose the native side's current usage — the shared FFI Arrow
   allocator and the per-operator memory pool's reserved bytes (shipped for every stateful native
   operator — divergences/16) — over JNI, and surface it as
   Flink operator metrics so a running job's native footprint is visible in the Flink UI/metrics
   reporter next to its JVM numbers.
2. **Leak assertions in tests.** At operator `close()` the pool/allocator balance must return to
   zero. Wire an assertion into the parity/integration harness so every existing end-to-end test
   doubles as a leak test (mirrors Arrow's `BufferAllocator.close()` leak check on the Java side and
   DataFusion's pool accounting on the Rust side). A test that ends with bytes outstanding fails
   with the per-consumer breakdown.
3. **Native heap profiling recipe.** A documented, repeatable way to profile the native heap under a
   real job: jemalloc/`heaptrack`/Instruments (macOS) or `valgrind --tool=massif`/ASAN+LSAN (Linux,
   CI-friendly) against the Nexmark suite, including how to get symbols from the `bench` release
   build. Written up in `docs/` so any contributor can run it.
4. **Soak test.** An opt-in long-running job (like `SF_BENCHMARK=true`) over a keyed stateful
   operator with an unbounded key stream + eviction, asserting RSS/pool usage plateaus rather than
   grows without bound — the class of leak short tests can't see.

## Acceptance criteria
- Native usage (allocator + pool, per operator) is queryable from Java and exported as Flink metrics.
- All integration tests fail loudly if native bytes remain outstanding after close.
- A `docs/` page describes the heap-profiling workflow and it has been run once against Nexmark,
  with findings (or a clean bill) recorded.
- A soak test exists that would have caught an unbounded-state or dropped-release leak.

## Pointers
- divergences/16 (memory accounting) — the pool this ticket introspects; shipped for every
  stateful native operator.
- Comet's memory-pool stats and Arrow Java's allocator leak detection for prior art.
- `.claude/todos/20-profiling-and-benchmarks.md` item 2 (native timing/counter hook) — same
  feature-flagged native introspection surface; consider building them together.
