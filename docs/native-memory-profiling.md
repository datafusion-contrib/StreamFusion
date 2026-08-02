# Profiling the native side's memory

## Sizing the off-heap budgets

The native side holds real memory that appears in **no Flink memory figure** — it is not heap, not
managed memory, and not network buffers. In a container sized by Flink's process model, budget it
under `taskmanager.memory.task.off-heap.size` (or equivalent container headroom), or a
backpressured job gets OOM-killed with no attribution. Per TaskManager:

- **Kafka consumer prefetch** — each native Kafka source subtask's librdkafka consumer prefetches
  message bytes into its native queue ahead of the reader. The budget is
  `streamfusion.kafka.prefetch-mb` (default 256 MiB, rendered into librdkafka's
  `queued.max.messages.kbytes`, whose ceiling clamps the knob at 2 GiB) **per source subtask**: our
  reader drains the single consumer queue all of its partitions forward into, so librdkafka's
  per-partition fetch decisions all back off against that one queue's size (the documented
  per-partition semantics apply only to separate partition queues, which we don't use). A
  TaskManager therefore holds up to `Kafka source subtasks × prefetch-mb` — and under backpressure
  on a deep topic the queues really do fill to the cap. Two footnotes: fetching also stops at
  1,000,000 queued messages (`queued.min.messages`), so small-message topics can plateau below the
  byte cap; and each fetch can overshoot by up to one `fetch.message.max.bytes` (1 MiB default).
  The knob is the only control — `properties.queued.*` on a table is refused (librdkafka-only keys
  make the source fall back to Flink's client), so raising throughput headroom means raising the
  property and the off-heap budget together.
- **Kafka producer buffer** — the native sink translates the Java client's `buffer.memory`
  (default 32 MiB) into `queue.buffering.max.kbytes` **per sink subtask**, with the message-count
  cap disabled so the byte budget governs. A stalled broker fills it before the sink blocks.
- **Arrow FFI allocator** — the process-wide allocator for buffers crossing the native↔JVM
  boundary, visible as the `nativeArrowAllocatorBytes` metric. It is uncapped by default (like
  comet's): its traffic is transient per-batch crossings, refcount-freed as each batch is consumed
  and bounded by the pipeline's in-flight batches, so steady state is small (the soak's median is
  megabytes). `streamfusion.memory.arrow.max-mb` optionally caps it; an allocation past the cap
  fails with the knob named instead of growing the process.
- **Native operator state** is the exception: with accounting on (the default) it is reserved from
  Flink's *managed memory*, so it is already inside Flink's process model and needs no extra
  off-heap allowance.

Worked example: 4 Kafka source subtasks and 2 sink subtasks on one TaskManager at the defaults need
`4 × 256 MiB + 2 × 32 MiB ≈ 1.1 GiB` of task off-heap before Arrow's in-flight batches.

## Profiling

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
