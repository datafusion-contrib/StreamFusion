# Integer-truncating AVG

**Kind:** semantic — output values differ from Arroyo for the same query.
**Diverges from:** Arroyo / DataFusion.
**Forced by parity:** yes.

## Their decision
Arroyo delegates `AVG` to DataFusion's `avg` aggregate. For an integer input
column, DataFusion's `avg` returns a **floating-point** result (IEEE division),
which is the standard DataFusion/Spark behavior Comet would also inherit.

## What we do instead
For `AVG` over a bigint column we compute Flink's semantics: integer division of
the sum by the count, **truncating toward zero**, returning a bigint. This lives
in a small custom `IntegerAvgAccumulator` rather than DataFusion's `avg`.

## Why
Flink's SQL returns the *input* numeric type for `AVG` of an integer column, so
`AVG` of `{1, 2}` is `1`, not `1.5`. If we used DataFusion's float `avg` our
result would differ from stock Flink, breaking the prime directive. This is the
clearest case where Arroyo's choice and Flink's behavior simply disagree and we
must follow Flink.

## Scope / consequences

Integer AVG covers TINYINT, SMALLINT, INT and BIGINT. Its widened BIGINT sum and signed count
retain Flink's wrapping arithmetic, NULL for a zero count, truncation toward zero and narrowing
back to the declared input type. Java's MIN_VALUE / -1 division result is preserved.

Window accumulators and the compact running state used by GROUP BY and OVER share this
finalization kernel. Their accumulation loops, state layouts and two-phase adapters remain
specialized. Contract tests exercise NULLs, positive and negative changelog transitions,
zero/negative counts, merge/restore and division overflow across both representations.
Supported two-phase and multi-aggregate coverage lives on the operator pages; this semantic
note does not impose a lone-aggregate or single-phase restriction.
