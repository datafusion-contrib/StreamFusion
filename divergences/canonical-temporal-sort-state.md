# Stock canonical temporal-sort state

The native sort keeps Arrow batches or typed persistent rows and an optional actual last-emitted
cutoff. Its ordinary checkpoints retain that representation. Canonical engine interchange instead
projects directly into released Flink 2.2.1 dataState, lastTriggeringTsState and Void event callbacks,
using the stock empty-key and full-row serializers. This is a savepoint boundary adapter, not a
rowwise native execution path or a native-checkpoint conversion pass.

Arroyo's arrow/window_fn.rs was consulted for timestamp partitioning, stable buffered data and
watermark-driven release. Its expiring-table checkpoint format cannot be exposed to stock Flink.
The released RowTimeSortOperator is the interoperability oracle: it accepts signed timestamps
before any emission, admits records newer than its actual cutoff even behind an empty watermark,
and preserves arrival order among equal-time rows when there is no secondary comparator.

DataFusion Comet's native/jni-bridge/src/arrow_array_stream.rs was consulted before adding the
bridge. The existing owned Arrow C Data handoff and release pattern is reused. Canonical export
visits batches without consuming live state and can retry after receiver failure; persistent
export decodes rows in bounded chunks directly from the existing table.

Stock Flink can retain its default maximum parallelism even though this operator runs in one task.
The portable replacement preserves that value and lets the managed projection assign the actual
empty key to stock key groups. Native raw state retains its internal singleton group; neither
representation allows the ordered buffer to split across active sort tasks. Canonical metadata
must not substitute maximum parallelism one for a stock operator that retains a larger key space.

Full qualification and planner consolidation remain in issue569. Performance validation is deferred
under the maintainer's explicit functionality-first instruction.

A singleton input can cause Flink's generic single-transformation translation to pin a row-to-Arrow
transpose to maximum parallelism one. Because that stateless transpose can head the operator chain,
it must carry the sort's preserved maximum after translation. Changing that helper metadata does
not change the sort's one active task or modify an upstream stateful operator.
