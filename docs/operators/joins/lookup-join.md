# Lookup join

**Status:** Native for append-only probes with INNER and LEFT, both sync and async connectors. `FOR SYSTEM_TIME AS OF
probe.proctime` against a dimension table (Nexmark q13) — each probe row looks the key up in an
external table as of "now," rather than holding versioned state the way the [temporal table
join](temporal-join.md) does. There is no build-side state in the Flink sense at all: the lookup goes
straight to the connector on every probe row (or is cached by the connector itself).

## How it works

The probe batch stays Arrow, but the row-level join core is **Flink's own generated lookup runner** —
key building over both field references *and* constants, the pre-filter, the connector's real
`LookupFunction`/`asyncLookup`, the projection/filter on the temporal table, the residual non-equi
condition, and LEFT null-padding — all driven by the native operator per batch. Because it *is* the
host's generated code invoked per batch rather than a reimplementation, it is byte-identical to Flink
by construction.

The **async** path overlaps lookups within an Arrow batch and awaits before emitting it, following
Arroyo's batch-scoped lookup model. No requests remain in flight across a successful batch boundary.
Admission is bounded by `table.exec.async-lookup.buffer-capacity` before invoking Flink's runner,
so its internal result buffer cannot block timeout handling. Results retain probe order in both
ordered and unordered modes; unordered mode permits this stronger ordering. Duplicate keys still
invoke the host connector independently, preserving its cache and retry behavior.

Each request uses `table.exec.async-lookup.timeout`, measured from its invocation, and Flink's own
timeout callback. A non-positive timeout disables the deadline, as on Flink. Lookup-miss retries
remain in Flink's generated retry wrapper and share the request's deadline. Any failed request
stops the batch, even if an earlier request is still pending. Failure or interruption cancels the
operator's outstanding completion handles; late results cannot emit a failed batch. The connector
owns cancellation of its external I/O and is closed during task cleanup. A partially failed open
also closes the runner, retaining the original exception and any cleanup failure.

This preserves the Arrow boundary around a row-oriented connector call. It overlaps I/O within a
batch; it does not overlap I/O across batches or claim that a connector performs vectorized lookups.

## Admission

Same equi-key/type conditions as the [regular join](regular-join.md): a supported-type equi-key and
null-dropping keys (LEFT is the only non-INNER shape here). The temporal table itself must be a
modern `TableSourceTable`, or on Flink 1.18 a legacy `LookupableTableSource`. Projection/filter on the temporal table, the pre-filter, the residual
condition, and constant lookup keys are all native — the operator drives Flink's own generated
runner, so none of these narrow admission further.

## Falls back to Flink when

- the planner produces an **upsert-materialized** (keyed-state) lookup rather than a plain
  per-row lookup;
- the probe carries updates or deletes: the current columnar lookup output is append-only;
- Flink requests **key-ordered async lookup** for updating probes, which requires its keyed
  partitioning and scheduling contract;
- the join type isn't INNER or LEFT;
- the temporal table is a legacy (pre-`TableSourceTable`) connector.

## Bounded async queue diagnostic

The timeout/cancellation correction was measured against `762efc8` using a separate synthetic
async lookup fixture: 200,000 row-source probes, five dimension keys served on four Java worker
threads, capacity 100, parallelism 1, and a rowwise blackhole sink. The release build uses mimalloc
(`-Pbench`) on Apple M1 Max/JDK 17. Both row/Arrow transposes and the native lookup are asserted in
the plan. Two warmups precede five interleaved trials per engine; before/after runs use separate
JVMs, with no other tests or native builds running during measurement.

| Implementation | Flink median (s) | Native median (s) |
| --- | ---: | ---: |
| Before bounded admission/deadlines | 0.328 | 0.288 |
| With bounded admission/deadlines | 0.301 | 0.284 |

The native difference is within run-to-run variation. This is a correctness change with no
measurable overhead in this fixture, not evidence of a speed improvement. The fixture has no
network latency and does not establish connector I/O throughput. Times include planning and the
complete row-to-columnar-to-row pipeline.

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=AsyncLookupBenchmark -Dlookup.rows=200000 -Dlookup.capacity=100 \
  -Dlookup.warmup=2 -Dlookup.runs=5
```

## Flink 1.18 compatibility

The development profile adapts Flink 1.18's generated lookup runners and lifecycle to the same
Arrow batch operators. Async capacity, timeout and cleanup remain enforced. Key-ordered async
lookup is N/A on that host line; it is not counted as a planner fallback. Legacy lookup sources
use Flink's released provider resolution and generated runners, including external-row conversion,
key ordering, synchronous/asynchronous selection and the host's retry behavior. Their probes and
results retain the same Arrow batch boundaries as modern providers. The 1.18 upstream execution
contracts require native work for both legacy and modern lookup providers. See
[Flink line compatibility](../../flink-compatibility.md) for validation status.
