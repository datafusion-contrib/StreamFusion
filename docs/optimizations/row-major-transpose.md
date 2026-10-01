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
