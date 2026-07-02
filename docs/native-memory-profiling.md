# Profiling the native side's memory

The standing checks catch most leaks automatically: every test asserts at close that all native
handles were freed and the shared Arrow FFI allocator drained to zero (`SharedFlinkCluster`), the
managed-memory budget fails a job whose *accounted* state exceeds its reservation
(divergences/16), and the opt-in soak (`SF_SOAK=true mvn test -Pbench -Dtest=NativeMemorySoakTest`)
asserts RSS and allocator use plateau during a long evicting job. Reach for a heap profiler for
what none of those see: *where* Rust-side memory goes inside a live handle — a state map that
shrinks but never returns pages, allocator churn, or growth in a dependency.

For a **running job**, check the operator metrics before reaching for a profiler: every accounted
native operator exports `nativeStateBudgetBytes` (its reserved managed-memory budget),
`nativeStateBytes` (the tracked state drawing on it, sampled per batch), and
`nativeArrowAllocatorBytes` (the process-wide Arrow FFI allocator) to the Flink UI/metrics
reporter, next to the operator's JVM numbers.

## Symbols

The `bench` release build keeps symbol names but no debug info. For readable allocation stacks with
inlined frames and line numbers, override the profile without touching the build files:

```sh
CARGO_PROFILE_RELEASE_DEBUG=true mvn test -Pbench -Dtest=...
```

## Workloads

Two standard workloads, both opt-in:

- **Query-diverse:** the Nexmark matrix — `SF_BENCHMARK=true SF_MATRIX_QUERIES=q5,q7,q8,q11,q12
  SF_MATRIX_PARQUET=false SF_MATRIX_KAFKA=false mvn test -Pbench -Dtest=NexmarkMatrixBenchmark`.
- **Long-running/evicting:** the soak — `SF_SOAK=true mvn test -Pbench
  -Dtest=NativeMemorySoakTest` (size with `SF_SOAK_ROWS`).

The profiled process is the **surefire fork**, not the `mvn` launcher — find it with
`pgrep -f surefire`.

## macOS

`leaks(1)` ships with the Command Line Tools and scans a live process for unreachable malloc
blocks. Malloc stack logging must be on in the *target* process — export it around `mvn` (children
inherit it) and expect a few-times slowdown:

```sh
MallocStackLogging=lite SF_SOAK=true SF_SOAK_ROWS=5000000 mvn test -Pbench -Dtest=NativeMemorySoakTest &
sleep 60   # let the job reach steady state
leaks $(pgrep -f surefire) > /tmp/leaks.txt
```

A JVM always shows some unreachable blocks of its own; what matters is whether any leaked stack
passes through `libstreamfusion.dylib` frames (search the report for `streamfusion`). For
where-does-memory-live rather than what-leaked, use Instruments' Allocations template instead:
`xctrace record --template Allocations --attach <pid> --output trace.trace`, then open the trace
and filter by the dylib.

## Linux

[`heaptrack`](https://github.com/KDE/heaptrack) gives per-callsite allocation profiles with
negligible setup and works on a JVM:

```sh
heaptrack --pid $(pgrep -f surefire)   # attach mid-run, Ctrl-C to stop
heaptrack_print heaptrack.*.zst | less # or heaptrack_gui
```

`valgrind --tool=massif` works but slows the JVM enough that only small row counts are practical.
ASAN/LSAN require rebuilding the Rust side with a nightly toolchain (`-Zsanitizer=leak`) and are
not part of the standard workflow; prefer heaptrack, which needs no rebuild.

## Findings log

Record each profiling run here: date, workload, tool, and what was found (or a clean bill), so the
next person knows when the exercise last ran and what normal looks like.

- **2026-07-02, soak under `leaks`, macOS: clean.** `leaks` attached mid-run to a 5M-row soak with
  `MallocStackLogging=lite` reported `0 leaks for 0 total leaked bytes` — no unreachable malloc
  blocks anywhere in the process, the Rust dylib included. The same day's full 50M-row soak
  plateaued with the steady-state and late allocator medians identical (8.4 MB of in-flight
  batches) and RSS drifting +3.7% inside the tolerance.
