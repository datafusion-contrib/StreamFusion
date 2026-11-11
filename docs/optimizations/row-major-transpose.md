# Row-major entry transpose

**Applies to:** the RowData→Arrow entry transpose

This page opens a line of work on the row↔Arrow perimeter: the transposes at a native island's
edges are the tax every rowwise-fed job pays, and this line of work took Nexmark q0–q2 from ~0.6x
to 1.1–1.6x vs Flink (`fbe714c`). The techniques below it in the nav — the Arrow unsafe-checks
flags, the zero-copy exit transpose, the string-copy reduction, and projection pruning into the
transpose — are further cuts into the same perimeter.

## What it is

The RowData→Arrow entry converter used to fill column-major, growing each Arrow vector with
`setSafe` as rows arrived. It was rewritten row-major into vectors pre-sized to the row count —
the same shape as Comet's `ArrowWriter` (`64528e7`).

Insert-only inputs omit the hidden Arrow row-kind column until a non-insert record
actually arrives. If a source advertises insert-only output but emits a retraction,
the transpose retains that row kind and carries the column on subsequent batches.
When the first retract arrives partway through a batch, already-written rows receive
explicit INSERT entries in the newly allocated sidecar; later batches keep it.
This matches Flink's runtime forwarding and keeps deletes visible downstream.

## Measured

354 → 265 µs per 4096-row batch.

## Bounded input completion

Bounded SQL transposes flush at input completion and skip streaming latency timer
registration. This avoids one unnecessary timer per partially filled batch while
preserving the final short batch. Batch parity and unchained-stage controls cover
the lifecycle. No isolated speedup is claimed; the complete
[batch scalar investigation](../benchmarks/batch-scalars.md) still found cheap
arithmetic slower than stock Flink, and that standalone path retains fallback.

## Rejected alternative

A native Rust row decoder — parsing Flink's row wire format directly in Rust instead of converting
from a JVM-materialized `RowData` — was investigated and rejected: it only ties the JVM build and
loses once JNI is counted (`f70d078`).

## Direct writes at the streaming boundary

The streaming entry transpose writes each accepted row immediately into its owned Arrow batch,
using the same row-major writers as the list converter. Previously it deep-copied each input
into a retained RowData list and converted that list at flush time. Direct writes remove the
intermediate row and nested-value copies while preserving upstream object reuse: the input is
fully consumed before `processElement` returns.

This follows Comet's `RowArrowReader` write loop. Every emitted batch receives independent
buffers; pending buffers are released on conversion failure or operator close. Row-kind bytes
are written alongside the row, and the writer independently counts zero-column rows. The
existing row-count, timer, watermark, end-input and checkpoint flush boundaries remain intact.
The `conversionTime` counter estimates time spent converting rows. Batch allocation and
finalization are timed exactly; other writes are randomly sampled at 1/64 and their measured
durations weighted by 64. Random sampling avoids systematic bias on periodic row shapes. A
profile after removing the row buffer attributed about 10% of CPU samples to the per-row clock
calls themselves; sampling keeps that instrumentation off most of the hot path. This counter
is diagnostic and is no longer an exact sum of individual write durations.

## End-to-end ownership-copy measurements

These scalar measurements were collected on [PR #263](https://github.com/datafusion-contrib/StreamFusion/pull/263),
revision `bd0b85cc`; reproducing the scalar command below requires that revision's TRY_CAST
benchmark fixtures. A Linux x86-64/Core i7-12650H diagnostic uses JDK 17, released Flink 2.2.1, release+mimalloc,
2M runtime rows, parallelism one, a 2 GiB heap, two warmups and five alternating host/native
trials. Both row/Arrow transposes and the row source/sink remain included; object reuse stays
at its default disabled setting for both engines. The timestamp query includes the separately
measured canonical parser; only the transpose implementation changes here.

| Query | Before native median, s | Updated native median (range), s | Updated stock Flink median (range), s |
| --- | ---: | ---: | ---: |
| Boolean-text identity | 0.636 | 0.460 (0.435–0.471) | 0.384 (0.377–0.391) |
| TRY_CAST STRING to BOOLEAN | 0.564 | 0.395 (0.389–0.403) | 0.386 (0.378–0.392) |
| TRY_CAST STRING to TIMESTAMP(9) | 0.855 | 0.664 (0.654–0.684) | 3.466 (3.420–3.493) |

The Boolean and timestamp native medians improve about 30% and 22%. The original stock controls
were 0.404 s and 3.444 s; runs were separate, so the smaller differences should not be overstated.
The 2M-row Boolean query still trails Flink slightly, while timestamp reaches 5.22x Flink/native.
These results do not imply every isolated scalar function outperforms stock Flink.

The intermediate direct-entry-only implementation measured Boolean at 0.553 s; also removing
the redundant exit copy reduced it to 0.486 s. Sampling conversion timing then reduced it to
0.395 s. A CPU profile of the intermediate implementation recorded 945 clock-read samples under
the entry transpose out of 9,955 total samples across its Boolean and identity diagnostic jobs.
The original Boolean parser represented under 1% of samples; changing that parser was not the
main opportunity.

```sh
SF_BENCHMARK=true mvn test -Pbench -pl streamfusion-runtime -am \
  -Dtest=ScalarFunctionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dscalar.functions=TRY_STRING_TO_BOOLEAN,TRY_STRING_TO_TIMESTAMP \
  -Dscalar.rows=2000000 -Dscalar.warmup=2 -Dscalar.runs=5 -Dsf.extraJvmArgs=-Xmx2g
```

At 20M rows with the same resources, warmup and five-trial method, Boolean TRY_CAST measures
3.337 s native (3.314–3.343) versus 3.228 s Flink (3.186–3.239), or 0.967x. Its identity
control is 3.829 s native / 3.151 s Flink. That entry-only implementation still trails Flink at the
larger size; startup alone does not explain the deficit. The subsequent
[generated exit projection](zero-copy-exit-transpose.md#generated-projection-for-primitive-and-binary-outputs)
closes the measured Boolean gap while retaining both conversions.


## Batch preference for proctime complex COUNT

CPU sampling of the initial complex OVER COUNT candidate identified Calc, C Data import/export,
allocation and transpose setup as the proctime perimeter cost. Its ARRAY running-ROWS profile
measured 0.377 s native versus 0.332 s stock with 1,024-row batches. Proctime ARRAY/ROW COUNT
input islands now prefer 4,096 rows. The transition pass carries this preference through native
operators to entry transposes, including previously pruned entries, and stops at host boundaries.
Explicit table/JVM batch limits still win. The existing 50 ms latency backstop and watermark,
checkpoint and end-input flushes remain active. Other queries keep their previous default.

Release+mimalloc complete jobs retain nullable runtime containers, both transposes, production
IPC, JNI, state, startup and the row blackhole sink. JDK 17/Flink 2.2.1, parallelism two, 2 GiB
heap and 64 keys are shared with stock and the separately compiled actual previous revision
`7513ed7b`. Two warmups precede three alternating trials. Final running and bounded profiles use 500,000 rows. Earlier 50,000-row
bounded diagnostics remain in the artifact. Median (range) seconds follow; the artifact retains every trial.

| Proctime type / frame | Stock | Native | Actual previous SF |
| --- | ---: | ---: | ---: |
| ARRAY / running ROWS | 0.358 (0.320–0.373) | 0.269 (0.264–0.292) | 0.324 (0.313–0.336) |
| ARRAY / running RANGE | 0.241 (0.231–0.302) | 0.181 (0.181–0.191) | 0.344 (0.314–0.366) |
| ARRAY / 3 preceding ROWS | 0.310 (0.292–0.313) | 0.180 (0.176–0.187) | 0.353 (0.351–0.374) |
| ROW / running ROWS | 0.271 (0.262–0.276) | 0.213 (0.212–0.253) | 0.281 (0.276–0.356) |
| ROW / running RANGE | 0.283 (0.271–0.287) | 0.216 (0.211–0.217) | 0.267 (0.260–0.268) |
| ROW / 3 preceding ROWS | 0.284 (0.276–0.290) | 0.211 (0.202–0.214) | 0.328 (0.275–0.339) |

These admitted profiles beat stock and actual previous execution. Startup and source allocation
remain measured; these results are not sustained throughput or
a universal guarantee. MAP-backed proctime did not show a reliable improvement at 4,096, 8,192 or
16,384 rows, so those arguments retain Flink ([issue #419](https://github.com/datafusion-contrib/StreamFusion/issues/419)). The larger-batch diagnostics change no defaults.
[Complete evidence](../benchmarks/results/over-complex-count-2026-10-19.json) includes the
unfavorable candidates, paired previous-version stock controls and final source/library identity.
Coverage and reproduction are documented under [OVER](../operators/over.md#complex-ordinary-count).
