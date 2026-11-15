# Union

**Status:** native, with boundary restrictions.

For bounded execution, see the separate [batch SQL admission](batch.md).

`UNION ALL` substitutes to a native `Union` operator that simply concatenates its inputs' Arrow
batches — no state, no key. It admits any row type the columnar boundary can carry.

Append-only and updating branches can share a union. Each input retains its physical RowKind
and Java string-cache sidecars. Before a downstream keyed operator coalesces these batches,
native code aligns logical columns by position and sidecars by identity: missing RowKind tags
become INSERT, while missing string caches remain absent values. UPDATE_BEFORE, UPDATE_AFTER
and DELETE tags are preserved. Logical arity/type mismatches fail before Arrow concatenation.
No row conversion is added inside the island.

Batch unions give their branches the same field names and nullability before forwarding their
batches. The header adjustment retains Arrow vectors, physical types and field metadata; it does
not transpose rows or copy column buffers. Downstream bounded operators therefore see one schema
when input aliases differ, including when `UNION` adds a deduplication aggregate.

## Final JSON_VALUE projections

Direct character JSON_VALUE projections can feed a final UNION ALL and row sink when the JSON
argument is an input column and the existing native path/default-policy rules admit the call.
Multiple JSON_VALUE outputs, nested unions and ordinary STRING branches are covered. A producing
JSON Calc may also project non-character values that do not consume character inputs, or typed
NULLs. Other character outputs, additional consumers of character inputs in its projections or
filter, and JVM function callbacks remain outside this admission.

The JSON input retains original Java UTF-16 until native parsing finishes. The output is ordinary
UTF-8, matching Flink's row serialization on the fan-in edge. In the released-Flink SQL tests,
an isolated surrogate becomes `?` before the row sink observes it; preserving that surrogate
through Arrow IPC would change the sink's observed value. The tests capture code units inside
a row sink, before result collection can hide a mismatch. Fused scalar consumers still retain
original UTF-16 inside their generated expression as described in [Calc](calc-filter.md#json_value).

At the entry transpose, a parse-only document marker allows redundant caches to be omitted:
valid lazy Java strings encode losslessly, and materialized ASCII strings must match their bytes
exactly. Malformed UTF-16 and mismatched caches retain their original units. This marker is internal,
is distinct in the planner's reuse digest, and does not change general string or collection handling.

A non-final optimizer block, a consumer above the union, JSON_QUERY, composed character
results, or a native columnar sink retains the identity fallback. This is a narrow final-output
admission, not general JSON STRING support for grouping, joining or sorting.

## Fallback

- **An inconvertible row type** — a column (or nested leaf) outside the Arrow boundary's supported
  types falls the operator back, dragging the whole query with it per the
  [all-or-nothing island rule](index.md#the-all-or-nothing-island).

## Not a fallback: `UNION` (distinct)

Plain `UNION` isn't planned as this operator at all — Flink's own optimizer rewrites duplicate
elimination into a `GROUP BY` before the plan reaches us, so a distinct union accelerates through
the [GROUP BY](group-by.md) path instead. It never reaches the `Union` matcher, so there's nothing
here for it to decline.
