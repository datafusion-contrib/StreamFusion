# Coverage and fallbacks

What StreamFusion does **not** accelerate, and **every** condition that makes a query (or part of one)
fall back to stock Flink. The [Compatibility Chart](../readme.md#compatibility-chart) is the source of
truth for what *is* accelerated and under what terms; this is its complement — the boundaries.

> Keep this current. When an operator, type, expression, or connector gains or loses support, update
> this file in the same commit (alongside the readme chart). It is meant to always answer "why didn't
> my query accelerate?" precisely.

A query accelerates only if it forms **one fully-columnar island**: every operator but a rowwise
source/sink runs natively (Arrow in/out). One unsupported interior operator therefore drags the
**whole** query back to Flink (the all-or-nothing gate below). Use `NativePlanner.explain(...)` or
`-Dstreamfusion.logFallbackReasons=true` to see the recorded reason(s) for a given query.

---

## (a) What we don't support

### Whole operators with no native path
These have no matcher; any query containing one falls back entirely.

| Operator | SQL surface |
|---|---|
| `Correlate` | lateral table functions, and `UNNEST` beyond the INNER single-`ARRAY` case (`MAP`/`MULTISET` unnest, `ARRAY<ROW>`, `WITH ORDINALITY`, `LEFT` unnest, or a filter pushed into the `Correlate` as a condition). INNER `UNNEST` of a single `ARRAY` column **is** supported — see the chart. |
| `TemporalJoin` | `FOR SYSTEM_TIME AS OF` versioned-table join |
| `LookupJoin` | dimension-table / async lookup join |
| `Match` | `MATCH_RECOGNIZE` (CEP / row-pattern) |
| `GroupWindowAggregate`, `GroupWindowTableAggregate` | legacy `GROUP BY TUMBLE(...)` windowing, and proctime group windows |
| `LocalGroupAggregate` + `GlobalGroupAggregate` + `IncrementalGroupAggregate` + `MiniBatchAssigner` | two-phase / mini-batch non-windowed `GROUP BY` (only single-phase `GroupAggregate` is native) |
| `GroupTableAggregate` | `TableAggregateFunction` |
| `DropUpdateBefore`, `Values`, plain `Sort` | misc |
| `LegacyTableSourceScan`, `LegacySink` | legacy connectors |
| `Python*` (`PythonCalc`/`PythonCorrelate`/`PythonGroupAggregate`/`PythonOverAggregate`/…) | PyFlink UDFs |

### Feature gaps inside operators we *do* support
- **Nested types** (`ARRAY`/`MAP`/`ROW`) are carried through filters/projections, usable as GROUP BY /
  join / dedup keys, and as a `COUNT` value column (see §4). The remaining nested-type gap is *ordering*
  a nested value (`MAX`/`ORDER BY`/Top-N sort) — which Flink also rejects.
- **Aggregates** — `SUM`/`AVG` over decimal; two-phase `AVG`; window `AVG` only as a lone aggregate;
  non-windowed `GROUP BY` `AVG` only via the host's SUM/COUNT rewrite (not modeled natively); value
  types outside bigint/double/int/smallint/tinyint/float (see `aggregate-type-support.md`).
- **`OVER`** — only the `RANGE UNBOUNDED PRECEDING … CURRENT ROW` frame (no `ROWS` frame, no bounded
  frame); value column bigint/int/double only; `LAG` (model mismatch), `LEAD` (Flink-unsupported in
  streaming).
- **Top-N** — `ROW_NUMBER` only (Flink itself rejects streaming `RANK`/`DENSE_RANK`); rank range must
  start at 1 (no `OFFSET`); insert-only input (no retracting Top-N).
- **Deduplication** — keep-first (`ORDER BY rowtime ASC`) only; keep-last falls back.
- **Joins** — proctime interval/window joins fall back; a residual non-equi predicate must be
  expressible by the native expression engine.
- **Sources/sink** — local `file:` path only (Parquet/ORC source, Parquet sink); Kafka decode limited
  (see below); CDC only Debezium/OGG JSON.
- **Proctime** variants of essentially every event-time operator (sort, dedup, window agg, joins).

---

## (b) Every cause of fallback, by layer

### 1. Global / gate — these zero out the *entire* query
- **Master switch off**: `-Dstreamfusion.native.enabled=false`.
- **All-or-nothing island gate**: after substitution, if any operator other than a rowwise source
  (leaf) or the sink (root) is still row-wise, nothing is substituted — the whole query runs on Flink.
  So one unsupported interior operator drags the whole query back.
- **Insert-only guard**: every operator except the changelog-aware ones (`GROUP BY`, regular join,
  CDC source, `Calc`, `UNION ALL`, `Expand`, `ChangelogNormalize`, streaming Top-N / `LIMIT`) requires
  an insert-only input; a retracting/updating input falls it back.
- **Per-operator kill switch**: `-Dstreamfusion.operator.<name>.enabled=false` (e.g. `filter`,
  `groupAggregate`, `union`, `limit`, `expand`, `changelogNormalize`, `windowRank`, …). `kafkaSource`
  defaults to *false*.

### 2. Per-operator matcher declines (exact conditions)
- **OVER** — more than one window group; frame ≠ `UNBOUNDED PRECEDING…CURRENT ROW`; order key not
  exactly one ascending rowtime; aggregates not all over one shared bigint/int/double value column;
  `PARTITION BY` key outside bigint/int/string/boolean/date/timestamp/decimal.
- **Interval join** — not INNER/LEFT/RIGHT/FULL; no equi key; non-null-dropping (non-INNER) keys;
  equi-key type outside the supported set; non-equi residual not expressible; proctime bounds.
- **Window join** — same key/type/non-equi conditions; both sides must carry an event-time
  window-attached windowing.
- **Regular join** — unsupported join type; no equi key; non-null-dropping keys; non-equi residual not
  expressible; an input column type the converter can't carry.
- **Window aggregate / local / global** — window not event-time `TUMBLE`/`HOP`/`CUMULATE` (zero offset)
  over a local-time-zone rowtime; `HOP` slide / `CUMULATE` step doesn't divide size; key type outside
  bigint/int/string/boolean/date/timestamp/decimal; value type/aggregate mismatch; `AVG` (where noted);
  two-phase partials not single-field bigint/double.
- **GROUP BY (non-windowed)** — any aggregate other than SUM/MIN/MAX/COUNT (`AVG`, distinct, UDAF);
  idle-state TTL ≠ 0; an unsupported key/value column type.
- **Top-N / LIMIT** — not `ROW_NUMBER`; rank range not a constant starting at 1 (an `OFFSET`); a row
  type the converter can't carry; (`LIMIT`) missing `FETCH`, or an `OFFSET`.
- **Deduplicate** — not rowtime-`ASC` rank-1 keep-first.
- **Window Top-N / window dedup** — not `ROW_NUMBER` over a windowing-TVF; rank not starting at 1.
- **Windowing TVF** — not event-time `TUMBLE`/`HOP`/`CUMULATE` (zero offset) over a local-time-zone
  rowtime.
- **Event-time sort** — more than one order key, descending, or a non-timestamp order key.
- **Union** — not `UNION ALL`; row type the converter can't carry.
- **Expand** — any project cell that isn't a column ref, a NULL literal, or the integer expand id.
- **ChangelogNormalize** — a pushed filter condition; the source-reuse variant; a row type the
  converter can't carry.
- **Watermark assigner** — only substituted when its input is already a columnar producer (otherwise
  left on host to avoid a double transpose — a no-op, not a true fallback).

### 3. Expression level — a `Calc`/filter falls back if *any* node is un-admitted
- **Unsupported function/operator** outside the admitted set (e.g. `MD5`; `CONCAT` for a NULL-semantics
  divergence; …).
- **CAST** — anything that isn't widening numeric (integer→wider int, integer→float/double,
  float→double); narrowing, float→int, and string casts fall back.
- **Incompatible functions** — off by default, native only under
  `-Dstreamfusion.expression.<NAME>.allowIncompatible=true` (or the blanket flag): `UPPER`, `LOWER`,
  `EXP`, `LN`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `LOG10`, and `ROUND` on float/double.
- **Literal/arity guards** — unsupported literal type; `SUBSTRING` non-literal or out-of-range
  start/length; `LEFT`/`RIGHT`/`REPEAT`/`LPAD`/`RPAD` non-literal or negative count; `TRIM` other than
  default BOTH-whitespace; `POSITION` with a FROM start; wrong arity for any admitted function.

### 4. Type level
- **Scalar leaf types.** Every column (and every nested leaf) must be a type the boundary converter
  carries: tinyint/smallint/int/bigint/float/double/boolean/char/varchar/timestamp/timestamp-ltz/date/decimal.
  Anything outside that — `TIME`, interval, raw/binary — falls back. Both gates
  (`FilterCalcMatcher.convertibleRow` for filter/`Calc`, `RowDataArrowConverter.supports` for the
  keyed/stateful operators) check this recursively.
- **Nested `ARRAY`/`MAP`/`ROW` are supported** (recursively, down to supported leaves): carried through
  filters/projections, usable as a GROUP BY / join / dedup **key** (the nested value rides the row
  state as a DataFusion `ScalarValue` and is cast back to its declared column type on emit), and as a
  `COUNT` value column (counted for null-ness only). What still falls back for a nested column:
  - **Ordering a nested value** — `MAX`/`MIN` over it, `ORDER BY` it, or a Top-N/sort on it. Flink
    itself rejects `MAX(array)` and `ORDER BY array`, so this matches the host.
- **Key types** outside bigint/int/string/boolean/date/timestamp/decimal **(plus the nested types
  above)** for join/OVER/window/group keys.
- **Aggregate value types** outside the parity matrix in `aggregate-type-support.md` (esp. decimal
  SUM/AVG).

### 5. Source / sink / connector
- **Filesystem** — non-local path (`hdfs:`/`s3:`/…) for the Parquet/ORC source and Parquet sink; any
  non-Parquet/ORC source format; any non-Parquet sink format.
- **Kafka** — value format outside JSON/CSV/raw/bare-Avro/protobuf; `avro-confluent` (decoder exists,
  not wired); a `key.format`; a topic pattern; specific-offsets / unsupported bounded startup mode;
  protobuf fields needing representation reconciliation (enum/unsigned/bytes/proto3-defaults/well-known
  types).
- **CDC** — anything other than Debezium/OGG full-image JSON: Maxwell/Canal (partial-`old` not
  bit-identical), `schema-include` envelope, `ignore-parse-errors`, metadata/computed columns.
