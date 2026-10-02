# Projection pruning into the entry transpose

**Applies to:** the RowData→Arrow entry transpose

## What it is

When a native calc reads only a few columns or nested struct fields of a wide row, the planner
narrows the entry transpose to exactly those leaves and remaps the calc accordingly. The unread
person/auction structs of the Nexmark wide event are never materialized into Arrow at all
(`8523187`).

Row-ordered expressions evaluated by Flink's generated Calc code retain the complete nested
structs passed as arguments. That code uses the original positional row serializers, so pruning
members would change their arity. Unused top-level columns are still pruned; native field-by-name
expressions retain nested-field pruning.

The entry transpose applies this projection **before writing into the owned Arrow batch**.
Only selected fields are read and copied into Arrow; no intermediate deep-copied RowData list
is retained. The reusable projection view releases its source and nested-row references
immediately after the write, including on failure. Upstream GenericRowData and BinaryRowData
can therefore be reused as soon as `processElement` returns without retaining unread payload.

Each emitted batch has independent buffers, following Comet's ownership model. Buffers held by
downstream consumers are never reset or reused. Row-count and latency limits, pre-watermark and
checkpoint flushing, RowKind and zero-column row counts retain their existing behavior. Partial
batches now retain Arrow storage under the normal off-heap allocator rather than copied heap
rows. Closing the operator releases a partial batch without emitting it. No new byte limit is
introduced: memory still depends on the **selected** values' sizes.

## Borrowing at the synchronous consumer

For a direct physical input whose released Flink `InternalTypeInfo` matches the source row
schema, the entry edge uses a consumer-local serializer whose row-copy methods borrow the
incoming row. The transpose writes selected values into independent Arrow buffers before
returning. Flink's extra chained-input deep copy therefore adds no ownership protection on this
edge and is avoided. The source type and every sibling consumer keep their normal serializers;
global object reuse is unchanged. This also avoids copying unread fields before pruning.

The edge is a standard virtual forward partition, not another physical operator or a custom
transformation subclass. Existing virtual partition inputs retain their original serializer,
so a hash or rebalance partition cannot be replaced accidentally. Other type information and
schema mismatches also retain the original input. Network serialization, deserialization and
binary copying delegate to the released row serializer. Its original snapshot is preserved;
restoring that snapshot conservatively restores ordinary row copying. This borrowing serializer
belongs only at the synchronous transpose input, never at a consumer that retains rows or sorts,
keys, or asynchronously processes them.

The design follows Comet's row-to-Arrow reader, which writes a row before advancing its producer.
Flink's consumer-specific chained serializer is the adaptation needed here; see
[the divergence note](https://github.com/datafusion-contrib/StreamFusion/blob/feat/recovered-goal-followups/divergences/46-synchronous-arrow-input.md). Ownership checks run a
source that reuses one row and one array through both chained and network-separated forks. They
verify every buffered Arrow result and a sibling that retains its first row. Additional checks
cover serializer duplication, changelog wire bytes, conservative snapshot restoration and
partition/schema exclusions, alongside transpose and SQL parity checks on released Flink 2.2.1
and 1.18.1.

Release/mimalloc diagnostics on x86-64, JDK 17 and Flink 2.2.1 use a 2 GiB test heap, two warmups
and five alternating stock/native trials. Both transposes, the native Calc and the rowwise
blackhole sink remain in the measured job. Previous production is `1b1b5ed8` with the same
benchmark fixtures and its own release DSO; it falls back to stock expressions for these new
functions. Trial sets ran separately on the same host.

| Query | Rows | Stock seconds | Current native seconds | Previous production seconds |
| --- | ---: | ---: | ---: | ---: |
| STRING to fixed BINARY, 264-byte ASCII source, no NULLs | 2,000,000 | 0.868 | 0.379 | 0.858 |
| ELT on fixed BINARY, no NULLs | 2,000,000 | 0.307 | 0.321 | 0.321 |
| ELT on fixed BINARY, no NULLs | 10,000,000 | 1.018 | 1.280 | not measured |
| Boolean ARRAY_DISTINCT, width eight, domain two, nullable | 10,000,000 | 2.466 | 2.087 | 2.199 |

For the two-million-row cast, native trials span 0.371–0.423 seconds, stock
0.861–0.885, and previous production 0.847–0.883 (its stock control is 0.856).
The combined native cast and entry path is 2.29× faster than stock and 2.26× faster than previous
production. These comparisons include both the new expression and the entry optimization;
they do not isolate the serializer change. ELT remains slower than stock: native trials span
0.318–0.325 seconds at two million rows and 1.273–1.288 at ten million, versus stock
0.303–0.308 and 1.001–1.026 respectively. The ten-million-row fixed-BINARY identity control is
also slower (native 1.171 versus stock 1.005). Entry borrowing does not resolve that exit-boundary
performance limit, and ELT's acceleration gate remains open.

Boolean containers are NULL every eighth row and elements every seventh. The repeated native
run spans 2.081–2.097 seconds against stock 2.440–2.469; previous-production fallback spans
2.171–2.241 with its stock control at 2.183. An earlier candidate run is 2.036 against stock
2.512. Both candidate runs beat both baselines, but the changing stock controls limit the size
of the comparative claim: the repeat is 15.4% below its stock control and 5.1% below previous
production. All trials, identities and both stock controls are retained in the
[measurement CSV](../benchmarks/synchronous-arrow-entry-2026-10-02.csv). These are complete-job
measurements; the Boolean membership optimization is included alongside entry borrowing.

## Measurement

The following historical measurements compare full-row and projected-row buffering, before
direct Arrow writes replaced the intermediate row list.

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

The current allocation diagnostic reports pending Arrow storage instead of reflecting into the
removed row list. On the x86-64/JDK 17 direct-write implementation, 512 pending projected rows
retain 81,920 Arrow bytes for every unread-payload size above. Measured heap allocation is about
130.4 bytes/row for GenericRowData and 286.3 for BinaryRowData, averaged over five trials after
three warmups. It now includes Arrow writer creation and writes during row ingestion; these
numbers are not directly comparable to the earlier ARM64 copy-only diagnostic. The fixture and
query are unchanged, and ownership tests independently verify that dropped fields are never read.
