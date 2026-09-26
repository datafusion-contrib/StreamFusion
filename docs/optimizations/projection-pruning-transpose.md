# Projection pruning into the entry transpose

**Applies to:** the RowData→Arrow entry transpose

## What it is

When a native calc reads only a few columns or nested struct fields of a wide row, the planner
narrows the entry transpose to exactly those leaves and remaps the calc accordingly. The unread
person/auction structs of the Nexmark wide event are never materialized into Arrow at all
(`8523187`).

The entry transpose applies this projection **before copying and buffering the row**. Its
serializer uses the projected schema and recursively copies only selected fields into owned
storage. The reusable projection view releases its source and nested-row references immediately
after the copy, including on failure. Upstream GenericRowData and BinaryRowData can therefore be
reused as soon as `processElement` returns without retaining or copying their unread payload.

The Arrow writer still creates independent, pre-sized batches on the JVM, following Comet's
ownership model. This does not introduce native row decoding or reset buffers held downstream.
Row-count and latency limits, pre-watermark/checkpoint flushing, RowKind and zero-column row counts
retain their existing behavior. No new byte limit is introduced: memory still depends on the
**selected** values' sizes. This optimization removes unread payload from the buffer rather than
claiming a bound on arbitrary projected records.

## Measurement

`PrunedTransposeBenchmark` uses a source without projection pushdown that reuses full-width rows:
INT, DECIMAL(20,2), an unread STRING and unread BYTES. Its fixed query computes `id + 1`, selects
the decimal and filters `id >= 0`. It asserts that the actual native Calc plan includes a two-field
entry transpose and the exit transpose; both engines use the same rowwise blackhole sink. This
diagnostic does not modify Nexmark. The specialized native filter currently does not push its
column subset into the entry transpose; that separate planner path is outside this measurement.

On ARM64/JDK 17/Flink 2.2.1 with release native artifacts, 100,000 rows, two warmups and five
alternating host/native trials per shape, median job net-runtime seconds were:

| Source row | Bytes per unread field | Main native | Pruned-copy native | Main host | Pruned-copy host |
| --- | ---: | ---: | ---: | ---: | ---: |
| Generic | 0 | 0.084 | 0.090 | 0.068 | 0.065 |
| Generic | 1,024 | 0.098 | 0.098 | 0.072 | 0.076 |
| Generic | 65,536 | 0.774 | 0.348 | 0.320 | 0.327 |
| Binary | 0 | 0.079 | 0.089 | 0.067 | 0.068 |
| Binary | 1,024 | 0.101 | 0.105 | 0.078 | 0.075 |
| Binary | 65,536 | 0.766 | 0.390 | 0.357 | 0.360 |

The wide-payload native runs improve by 2.22x/1.96x. Small payloads show no improvement and the
zero-payload Binary case costs more: selected-field extraction replaces a cheap contiguous copy.
These short pipelines remain slower than Flink; the improvement is relative to the former native
path. Trial sets ran separately on the same host, so small differences should not be overinterpreted.

A separate same-thread allocation diagnostic buffers 512 rows without flushing. The Java
allocation per row includes the input StreamRecord and buffering overhead, excludes fixture
construction and Arrow emission, and is averaged over five trials after three warmups:

| Source row | Bytes per unread field | Main bytes/row | Pruned-copy bytes/row |
| --- | ---: | ---: | ---: |
| Generic | 0 | 338.2 | 154.2 |
| Generic | 1,024 | 2,386.2 | 154.2 |
| Generic | 65,536 | 131,410.2 | 154.2 |
| Binary | 0 | 250.2 | 310.2 |
| Binary | 1,024 | 2,298.2 | 310.2 |
| Binary | 65,536 | 131,322.2 | 310.2 |

At the largest payload, the old 512-row buffer retains 64 MiB of unread field bytes; the new
buffer retains none. This is the retained unread-payload component, not total JVM peak heap.
Allocation in the new copy path no longer grows with unread field size. Ownership regressions
cover immediate producer mutation, nested and empty projections, NULLs, Unicode, precision-38
decimals, nanosecond TIMESTAMP/LTZ, changelog kinds, timers, watermarks and checkpoint barriers.
SQL parity and boundary tests run on released Flink 2.2.1 and 1.18.1.

```bash
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=PrunedTransposeBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dprune.rows=100000 -Dprune.warmup=2 -Dprune.runs=5
```

## The pass-through bug

Pass-through columnar nodes must not hide the rowwise input from this pruning pass. The mini-batch
assigner sitting between a calc and the source silently disabled it: an unpruned transpose measured
at 7x the transpose work, and native q3 ran 2.4x slower with mini-batch on. The fix pushes the
pruning through the assigner (`ddc4f25`); a planner test pins the pruned arity.

## General rule

Any future pass-through columnar rel needs the same treatment — it must not opaquely block
projection pruning from reaching the transpose on its far side.
