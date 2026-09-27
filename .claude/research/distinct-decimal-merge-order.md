# Wide-decimal DISTINCT merge ordering

Remaining work in #231: two-phase SUM/AVG DISTINCT over DECIMAL precision 20–38.
The current precision-19 limit is still necessary. This is an implementation investigation,
not a decision to exclude the issue's DECIMAL(20,2) case.

## Released-host evidence

`FlinkDistinctDecimalMergeOrderTest` uses released Flink's `MapView`, `MapSerializer`,
`DecimalDataSerializer`, and `DecimalDataUtils.add`. On JDK 17, both Flink 2.2.1 and 1.18.1
exhibit these two independent effects:

* Three equal membership maps can iterate differently after `MapSerializer.copy` or a
  serialize/deserialize round trip. With `L = 9 * 10^37`, the default map containing
  `[L, L - 3, -L]` folds SUM to `-L`; the copied/deserialized maps fold to `L - 3`.
  AVG's running sum over the original map overflows to sticky NULL, while the other maps
  retain `L - 3`. The membership maps themselves compare equal.
* Folding singleton local views in arrival order `[L, -L, L - 3]` retains `L - 3`, but first
  combining those views in a default `MapView` produces `-L` for SUM and NULL for AVG's sum.

The test isolates membership ordering and decimal addition. It is not an end-to-end claim
that any particular physical SQL route always selects one of those results.

## Relevant Flink structure

Reference files in the canonical Flink tree:

* `flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/codegen/agg/DistinctAggCodeGen.scala`:
  creates a fresh MapView and merges by iterating `otherAcc.entries()`; shared filters use
  a common membership view with per-aggregate flags/counts.
* `flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/aggregate/MiniBatchLocalGroupAggFunction.java`:
  creates a fresh accumulator per buffered key, accumulates input rows, and emits it at flush.
* `flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/aggregate/MiniBatchGlobalGroupAggFunction.java`:
  `addInput` merges incoming partials into a temporary local accumulator; `finishBundle`
  merges that accumulator into durable global state. The released 2.2.1 bytecode confirms
  this extra buffering stage.
* `flink-core/src/main/java/org/apache/flink/api/common/typeutils/base/MapSerializer.java`:
  copy/deserialization creates a HashMap sized for the entry count, while a fresh MapView
  starts with a default HashMap.
* `flink-table/flink-table-common/src/main/java/org/apache/flink/table/data/DecimalData.java`:
  decimal keys use BigDecimal hash codes and numerical comparison.

## Consequences for implementation

The native global currently merges each incoming membership view immediately and coalesces
output changes at flush. Exact wrapping integer sums and precision-at-most-19 decimal sums
are independent of this iteration order; wide decimal sums are not.

Sorting each native local view by decimal hash is insufficient. A complete implementation
must establish the actual copy/serialization route, preserve the temporary global union
and its logical bundle boundaries, reproduce HashMap iteration (including collisions,
resizes and tree bins), and preserve shared-filter membership ordering. Durable snapshot
restore must retain the existing running sum rather than refold it in arbitrary map order.

Keep the planner fallback until runtime-source SQL tests exercise those contracts on both
released Flink lines. The issue's precision-20 example remains open; a small non-overflowing
fixture does not justify admitting all values of that type.
