# Watermark assigner

**Status:** partial, for rowtime and subtraction of constant day-time or year-month intervals.

Native admission accepts `WATERMARK FOR rt AS rt` and `rt - INTERVAL constant` when the constant
is a non-negative **day-time** or **year-month** interval. DAY, HOUR, MINUTE, SECOND and composite
forms such as DAY TO SECOND carry milliseconds; YEAR, MONTH and YEAR TO MONTH carry calendar
months. Zero delay and chained subtractions, such as
`(rt - INTERVAL '1' MONTH) - INTERVAL '1' DAY`, are supported. Each subtraction keeps its position
in the expression: subtracting one month twice can differ from subtracting two months once.

Calendar subtraction reproduces Flink's `DateTimeUtils.addMonths`, including month-end clamping and
leap years. Each Arrow row's candidate is calculated **before** taking the running maximum:
`MAX(rt - interval)`. For example, March 30 at 23:00 and March 31 at 00:00 both map to the last
day of February when subtracting one month, but the first candidate is later. Taking the maximum
rowtime first would lose that candidate. This also preserves Flink's signed integer arithmetic
for fixed delays at the range limits. Rowtime columns keep their existing representation.

The planner encodes watermarks in the same expression format as native Calc and checks the native
output type before admission. The serialized plan contains typed operations and literals; each
operator or source reader owns a separate runtime evaluator. Calendar and composed expressions
run through the existing DataFusion projection engine and scalar registry, producing nullable
BIGINT candidates in epoch milliseconds. These values are internal to watermark evaluation;
timestamp columns retain their complete millisecond/fraction pair downstream.
A direct rowtime or single fixed-millisecond subtraction
uses an Arrow value view without allocating a candidate column, behind the same evaluator interface.

The independent assigner starts at watermark zero and slices batches at each eager watermark
boundary, including sorted batches: later rows can observe the watermark through
`CURRENT_WATERMARK` even when none is late. A batch without an internal emission boundary is
forwarded whole. A NULL rowtime fails the
job like Flink's assigner. Non-constant or negative delays, other watermark expressions, and
expressions referring to a different column fall back.

The independent assigner also falls back when `table.exec.source.idle-timeout` is positive.
Flink then retains its idle/active channel notifications, including its backpressure-aware
idle clock, so idle partitions cannot block downstream watermarks. The whole query follows
the all-or-nothing island rule in this case. Watermarks pushed into native sources retain
their own supported idle-timeout behavior; this restriction applies only to the independent
assigner.

The assigner can follow a columnar producer or a rowwise source leaf. The transition pass inserts
the source-edge transpose when needed; the whole query still has to satisfy the
[all-or-nothing island rule](index.md#the-all-or-nothing-island).

Watermarks pushed into a source use the same expression admission and candidate evaluation. Source
generators start at `Long.MIN_VALUE` and ignore NULL candidates, matching Flink's pushed generator.
The maximum candidate is separate from the batch's event timestamp and remains available after
downstream consumers release its Arrow buffers. Their additional
admission rules are documented for [Kafka](../connectors/kafka.md#source-admission-and-fallbacks)
and [Paimon](../connectors/paimon.md).

## Watermark expressions

`SOURCE_WATERMARK()` uses the source's existing watermark forwarding. `CURRENT_WATERMARK(rt)` in a
native Calc reads that Calc's last received watermark and returns NULL before the first one. It also
works in a pure filter, which uses the Calc runtime when a watermark context is needed. The value is
scoped to the synchronous evaluation, with the previous context restored afterwards, so it cannot leak between operators
sharing a task thread. See [temporal functions](temporal-functions.md) for contextual restrictions.
