# Event-time sort

**Status:** Native — `ORDER BY` on a single leading ascending rowtime; a secondary order key is
the one gap.

This is Flink's temporal `Sort`: a full re-ordering of the stream by rowtime, needed because
arbitrary-lateness elements can only be placed once the watermark has passed them, so the operator
must hold and globally order a buffer rather than emit in arrival order.

The admitted shape is exactly Flink's own: the leading order key must be an ascending rowtime. A
descending or non-time leading key isn't a fallback at all — it's a non-temporal `Sort`, which
Flink itself rejects in streaming, so declining it here is parity, not a gap.

## Gap

Any **secondary** order key beyond that leading ascending rowtime falls back.

## State layout

Because a full time order can't be partitioned by any real key, the buffer is raw keyed state
addressed under Flink's own one canonical empty key. That makes it checkpointable and restorable
exactly as Flink restores its own temporal sort, but it deliberately **cannot shard across
subtasks** — the same singleton limitation Flink's implementation has. See
[#22](https://github.com/datafusion-contrib/StreamFusion/issues/22) for that constraint.
