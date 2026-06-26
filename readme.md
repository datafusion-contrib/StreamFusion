# StreamFusion

Run Apache Flink SQL faster by executing supported operators natively (Rust + Apache
Arrow/DataFusion over JNI) while Flink continues to own planning, coordination, and
everything not yet supported. Substitution is transparent and conservative: a query is
planned by Flink, the operators we can reproduce exactly are swapped for native ones, and
anything else falls back to Flink with identical results.

## Compatibility Chart

What executes natively today and the conditions under which each operator is accelerated.
An operator is substituted only when **all** of its terms (plus the global terms) hold;
otherwise it runs on Flink unchanged.

| Operator | Accelerated | Terms |
|---|---|---|
| Projection / `Calc` (`SELECT` expressions, with optional `WHERE`) | Expressions over the admitted operations | A `Calc` — an optional condition followed by arbitrary projection expressions — built from the operations the [native expression engine](src/main/java/io/github/jordepic/streamfusion/planner/RexExpression.java) admits (below). Computed columns, constants, column reorders, searched `CASE`, and widening `CAST` all run natively, columnar in and out (e.g. `SELECT v + k, CASE WHEN s <> 'a' THEN k ELSE v END FROM t WHERE v > 15`). Filtered first, then projected. Any un-admitted operation anywhere in the `Calc` makes the whole node fall back. |
| Filter (`WHERE`), with column projection | Predicate expressions over the admitted operations | A filter whose predicate is built from the operations the native expression engine admits: column refs and literals, arithmetic (`+` `-` `*` `/` `%`), the six comparisons (either operand order), `AND`/`OR`/`NOT` in any nesting, `IS NULL`/`IS NOT NULL`, searched `CASE`, `COALESCE`/`NULLIF`, **widening numeric `CAST`** (integer→wider integer, integer→float/double, float→double), `LIKE`, `POSITION`, `ABS`/`FLOOR`/`CEIL`/`SIGN` (float/double only), and the string functions `CHAR_LENGTH`/`TRIM`/`LTRIM`/`RTRIM` (default whitespace)/`SUBSTRING`/`LEFT`/`RIGHT`/`LPAD`/`RPAD` (literal bounds)/`REPLACE`/`REVERSE`/`REPEAT`/`ASCII`/`CHR` — e.g. `WHERE v + k > 25`, `WHERE (a > 1 AND b < 2) OR c <> 3`; `BETWEEN`/`IN`/ranges expand to comparisons. The predicate is encoded and compiled once into a native handle, then evaluated per batch. An optional projection may select/reorder input columns (`SELECT a, c FROM t WHERE …`); arbitrary computed projections route through the `Calc` path above. The whole row is carried through Arrow, so every input column must be a type the [whole-row converter](src/main/java/io/github/jordepic/streamfusion/operator/RowDataArrowConverter.java) handles (the primitives, boolean, string, timestamp, date, decimal). Un-admitted operations — other functions (e.g. `CONCAT`), a narrowing/float→int/string `CAST` — make the whole node fall back, as do unsupported column types (a `fallbackReasons()` entry names the cause). Integer arithmetic is computed in the operand's declared width and wraps on overflow exactly as the host does, verified at the `INT` boundary and for narrow-int (`TINYINT`/`SMALLINT`) columns; `/` and `%` match on all finite operands (divide-by-zero and `INT_MIN/-1` edges noted in [divergences/07](divergences/07-expression-encoding-and-compile-once.md)). |
| Tumbling window aggregate | Yes | Event-time `TUMBLE` over a local-time-zone (rowtime) attribute; one or more aggregates — `SUM` / `MIN` / `MAX` / `COUNT` (and `AVG` only as a lone aggregate) — each over its own value column (so `SUM(a), SUM(b)` over different columns works); grouped by the window, optionally plus one or more bigint/int/string/boolean/date/timestamp/decimal keys. `COUNT(*)` is supported, including alongside value aggregates (it counts rows over a synthesized non-null column) and two-phase (`TUMBLE`/`HOP`/`CUMULATE`). Value-type support is the parity intersection in [docs/aggregate-type-support.md](docs/aggregate-type-support.md): all five aggregates over every non-decimal numeric type (bigint/double/int/smallint/tinyint/float), with custom accumulators where DataFusion would diverge — integer `SUM` wraps at the input width, integer `AVG` truncates, float `SUM` keeps 4-byte precision, and float/double `AVG` sum in double (float narrows the result); decimal carries `MIN`/`MAX`/`COUNT` (its `SUM`/`AVG` fall back on precision rules). |
| Hopping window aggregate | Yes | Same as tumbling, with `HOP`. One-phase assigns each row to its overlapping windows; two-phase (the default plan) pre-aggregates per slice and combines the shared slices of each window, requiring the slide to divide the size (other ratios fall back). |
| Session window aggregate | Yes | Same aggregate/key/value terms as tumbling, with `SESSION` (optionally `PARTITION BY` one or more bigint keys). Each element opens a gap-wide window; overlapping or touching windows merge, including when a late element bridges two open sessions. Always single-phase (the host never splits sessions), so no `ONE_PHASE` is needed. |
| Cumulative window aggregate | Yes | Same terms as tumbling, with `CUMULATE` (zero offset only). Nested windows share a bucket start and grow by the step up to the max size. One- and two-phase: the two-phase global re-buckets each slice partial into every nested window whose end it reaches (and unlike `HOP`, `CUMULATE` carries no synthetic count column, so the partials are just the user aggregates). |
| `OVER` aggregate / window function | Yes | Event-time `OVER ([PARTITION BY k] ORDER BY rt) UNBOUNDED PRECEDING`: running `SUM`/`MIN`/`MAX`/`COUNT`/`AVG` over a bigint/int/double value column, and the window functions `ROW_NUMBER()`/`RANK()`/`DENSE_RANK()`. Optionally partitioned by one or more bigint/int/string keys, emitting each input row with the running value appended. Columnar — input columns pass through Arrow and it rides the keyed columnar shuffle with no transpose (native source → watermark assigner → exchange → OVER all flow Arrow). Each row is held until the watermark passes its rowtime, then folded per partition in rowtime order. Aggregates use DataFusion accumulators; window functions keep a small per-key state (a counter for `ROW_NUMBER`, rank/last-value for `RANK`/`DENSE_RANK`) — both incremental, matching Flink, since DataFusion's window evaluators can't be checkpointed ([divergences/11](divergences/11-over-incremental-vs-window-exec.md)). `FIRST_VALUE`/`LAST_VALUE` next; bounded frames and proctime fall back. |
| Interval join (event-time) | Yes | Event-time INNER interval join — `a JOIN b ON a.k = b.k AND a.rt BETWEEN b.rt - X AND b.rt + Y` — with one or more bigint/int/string equi-join keys and an event-time interval on the rowtimes. Columnar: each input is shuffled by its join key through a native columnar exchange and the join emits Arrow batches of the matched pairs (left columns then right columns), so the whole two-input pipeline flows Arrow with no transpose (native sources → watermark assigners → exchanges → join). Both sides buffer; a row is joined against the other side's buffer (a DataFusion hash join on the keys with the interval as a residual filter) as it arrives, so a pair is emitted once, and rows are evicted once the combined (min) input watermark passes the point no future row could match. Outer/semi/anti joins, proctime intervals, and a residual non-equi predicate fall back to the host. |
| Window join (event-time) | Yes | Event-time INNER window join — two windowing-TVF inputs (`TUMBLE`/`HOP`/`CUMULATE`) joined on their key within the same window: `a JOIN b ON a.k = b.k` where both sides carry matching `window_start`/`window_end`. One or more bigint/int/string equi-join keys. Both sides buffer; when the combined watermark closes a window its rows are joined (a DataFusion hash join keyed on the user key plus the window bounds, so only same-window rows match) and evicted. The join is columnar (Arrow pairs out); the window assignment (windowing TVF) stays on the host. Outer/semi/anti joins, proctime, and a residual non-equi predicate fall back. |
| Streaming Top-N (`ROW_NUMBER`) | Yes (emits a changelog) | An append-only streaming Top-N — `ROW_NUMBER() OVER (PARTITION BY … ORDER BY …) <= N` with the rank number **not** projected. Keeps the best `N` rows per partition (sorted by the order keys, ties in arrival order) and emits a changelog as that set changes: an `INSERT` for a row entering the top-N and a `DELETE` for one displaced (delete before insert, and nothing when the new row is itself rank N+1) — matching the host's append-only bounded buffer, the closest analog being RisingWave's `TopNCache` ([divergences/14](divergences/14-standalone-streaming-engines.md)) though we hold the full set in memory. The order comparator honors per-column ascending/descending and nulls-first independently. `ROW_NUMBER` only (not `RANK`/`DENSE_RANK`), rank range starting at 1 (no offset), insert-only input; projecting the rank number, an offset, or a retracting input falls back. Columnar (Arrow in/out); the bounded per-partition buffers are checkpointed. |
| Regular (non-windowed) join | Yes (changelog in and out) | A regular INNER equi-join `a JOIN b ON a.k = b.k` (no window/interval), the "updating join". Keeps a per-side keyed multiset of live rows and probes it incrementally per input row — not the DataFusion batch-hash-join the time-bounded interval/window joins use, because retract correctness needs per-row counts ([divergences/14](divergences/14-standalone-streaming-engines.md)). On each input row it emits one output per matching row on the other side (by that row's count), carrying the input row's kind — so over append-only inputs the output is insert-only, and over changelog inputs (e.g. two aggregations joined) a retract on either side retracts the matched pairs. INNER only (outer/semi/anti fall back); a residual non-equi predicate falls back; null join keys never match (host INNER semantics). Join keys may be any converter-supported type. Columnar (Arrow in/out), per-side state checkpointed. Unbounded state (no time eviction) — like the host's regular join. |
| Non-windowed `GROUP BY` aggregate | Yes (changelog in and out) | A plain `GROUP BY` (or a global aggregate with no grouping): `SUM`/`MIN`/`MAX`/`COUNT` (and `AVG`, which the host rewrites into `SUM`/`COUNT` plus a division) over bigint/int/double value columns, `COUNT(*)` included, grouped by any keys the [whole-row converter](src/main/java/io/github/jordepic/streamfusion/operator/RowDataArrowConverter.java) handles. Unlike the windowed aggregates it **emits a retract changelog**: a key's first row inserts; a later change retracts the previous value then appends the new (the `UPDATE_BEFORE` gated on the host's per-node flag, an unchanged result suppressed); and a key whose record count reaches zero is deleted. It also **consumes a changelog**: each input row's `RowKind` (carried as a four-way byte column across the row/Arrow boundary, [divergences/13](divergences/13-rowkind-carriage-meta-column.md)) selects accumulate (`+I`/`+U`) or retract (`-U`/`-D`), so a `GROUP BY` over another aggregate's or a join's output runs natively too — every row processed in order so the change sequence matches the host's exactly. Per-key state, checkpointed; columnar (Arrow in/out) — the row↔Arrow conversion is paid only at host edges. `SUM`/`COUNT` retract a running value (`SUM` keeps a non-null count so a fully-retracted `SUM` reports `NULL`); `MIN`/`MAX` retract via a per-key value→count multiset that recovers the next extreme when the current one is retracted (Flink's `*WithRetract` `MapView`); a record count drives the delete. All four therefore run over either an append-only or a changelog input. Substituted only at zero idle-state TTL (a TTL changes the host's refresh/expire semantics). The `AVG` division `Calc` sits on the retracting output and stays on the host. |
| Parquet source (`SELECT … FROM`) | Local filesystem | A `filesystem`-connector source with `'format' = 'parquet'` reading from a local (`file:` or scheme-less) `path`. Read natively through DataFusion's file scan straight to Arrow batches (columnar), so the data never becomes `RowData` — feeding a fully columnar pipeline (a copy into a Parquet sink runs columnar end to end). Flink's file source owns discovery, split assignment, and checkpointing; the native reader is handed one file's byte range and reads only the row groups starting in it (splittable — a large file is read by several subtasks at once), with the projection pushed into the decode. Remote filesystems (e.g. `hdfs:`/`s3:`) fall back to the host. |
| ORC source (`SELECT … FROM`) | Local filesystem | A `filesystem`-connector source with `'format' = 'orc'` reading from a local (`file:` or scheme-less) `path`. Same path as the Parquet source — read natively through DataFusion's file scan (via a DataFusion ORC source) straight to Arrow, split at stripe granularity with the framework owning enumeration/assignment/checkpointing and the projection pushed into the decode. Remote filesystems fall back to the host. |
| Parquet sink (`INSERT INTO`) | Local filesystem | A `filesystem`-connector sink with `'format' = 'parquet'` writing to a local (`file:` or scheme-less) `path`. The incoming rows are written to Parquet natively (Arrow → Parquet), committed exactly once via two-phase commit on checkpoint. Remote filesystems (e.g. `hdfs:`/`s3:`) and other formats fall back to the host. |
| Kafka source native decode (`SELECT … FROM`) | CSV / raw / bare-Avro value formats | A `kafka`-connector source whose `value.format` is `csv`, `raw`, or `avro`: Flink's own Kafka source consumes the raw value bytes — owning offsets, checkpointing, and auth — and a native operator decodes a whole batch straight to Arrow, replacing Flink's per-record `RowData` materialization with one native decode per batch. Bare Avro decodes against the reader schema derived from the table's `RowType` by the same converter Flink's own `avro` format uses, so the result matches. Requires an explicit topic, a supported startup mode, an unbounded or `latest-offset` bounded scan, and no `key.format`. (A JSON value table takes the native Kafka *source* instead where that is eligible.) The registry-framed `avro-confluent` format has a native decoder but is not wired into this routing yet, so it falls back. |
| Kafka protobuf source (`SELECT … FROM`) | Flat scalar messages | A `kafka`-connector source with `value.format = protobuf` and a `protobuf.message-class-name`. Flink consumes the raw protobuf messages and a native operator decodes them straight to Arrow via the descriptor reflectively extracted from that generated class (the descriptor → Arrow walk is descriptor-driven, no per-record object tree). Verified identical to Flink's own `protobuf` format for flat scalar messages (host-baseline parity test). Same topic/startup/bounded/`key.format` prerequisites as the row above. Parity for enums, nested/repeated/map fields, well-known types, proto3 missing-field defaults, and a column order differing from the proto field order is not yet verified — see the fallback note. |
| Kafka CDC source (`SELECT … FROM`, emits a changelog) | Debezium / OGG JSON, reproduced identically to Flink | A `kafka`-connector source whose `value.format` is `debezium-json` or `ogg-json`. Flink consumes the raw bytes and a native operator decodes the `{before, after, op}` envelope straight to a columnar changelog — physical columns plus the `RowKind` byte, an update fanning out to UPDATE_BEFORE + UPDATE_AFTER — with no `RowData` envelope decode. Because a CDC source emits a changelog itself, it is substituted even though it is not insert-only, and the downstream changelog (e.g. into a native `GROUP BY`/join) flows with no row materialization. Substituted **only** where the result is identical to Flink's own decoder: full-image dialects only (see Maxwell/Canal below), no `schema-include` wrapper, default error handling (`ignore-parse-errors` unset — an unknown op or a null pre-image *fails* like Flink rather than being dropped), and no metadata/computed columns; anything else falls back. Same topic/startup/bounded/`key.format` prerequisites as the decode row above. |

Two-phase (local + global) aggregation is accelerated too: the native local
pre-aggregate emits partial state, the host shuffles by key, and the native
global merges — for `SUM`/`MIN`/`MAX`/`COUNT` (not `AVG`, whose partial is
multi-field). This is the default planning, so tumbling, hopping, and cumulative
window aggregation no longer need `ONE_PHASE`. Hopping and cumulative use the
host's slice-sharing model (a per-slice local, a global that re-buckets each
slice into the windows it belongs to — the overlapping windows for hopping, the
nested windows up to the max size for cumulative).

When a window (rowtime over a local-time-zone attribute) sits on a columnar
producer — e.g. a native Parquet source through a watermark assigner — the keyed
shuffle before it is kept columnar too: a native exchange splits each Arrow batch
by the grouping keys and routes it, feeding a columnar window with no row
transpose anywhere. This covers both one- and two-phase plans:

- **One-phase:** source → watermark assigner → exchange → window, all Arrow.
- **Two-phase:** a columnar local pre-aggregate emits partial-state Arrow batches,
  a columnar exchange splits them by key, and a columnar global merges — the whole
  local → shuffle → global path flows Arrow with no transpose on either side.

The shuffle only co-locates each key on a channel — the window re-groups by key in
operator state — so its hash need not match Flink's. The exchange is kept columnar
only when its downstream is a native columnar window/merge; otherwise the window
stays row-fed and the shuffle stays on the host.

### Global terms (all native execution)

- **Insert-only input**, with two exceptions: the non-windowed `GROUP BY` aggregate both emits and consumes a changelog, and the Kafka CDC source (Debezium/OGG) *emits* a changelog from its raw input (see their rows above). Every other operator consumes insert-only input; a retracting or updating *input* to them falls back to Flink.

### Not yet accelerated (falls back to Flink)

- Expressions using operations the engine does not admit: scalar functions outside the admitted set and narrowing/float→int/string `CAST`. Functions where DataFusion diverges from JVM semantics only at a precision/locale edge — `UPPER`/`LOWER` (locale-sensitive case folding), `ROUND` on float/double (`BigDecimal` vs binary rounding), and transcendental math (`SIN`/`EXP`/`POWER`/… last-ULP) — fall back by default but are **opt-in** via `-Dstreamfusion.expression.<NAME>.allowIncompatible=true` (Comet's `allowIncompatible` model). `CONCAT` (NULL handling) is a true value divergence and never opt-in. All documented in [divergences/07](divergences/07-expression-encoding-and-compile-once.md)
- The native columnar sources/sink only on a local (`file:`) path — remote filesystems (`hdfs:`/`s3:`) fall back. Sources read Parquet and ORC; the sink writes Parquet only — other formats fall back
- Kafka value formats on the native decode path beyond CSV/raw/bare-Avro/protobuf: the registry-framed `avro-confluent` decoder exists but isn't planner-wired yet; a `key.format`, a topic pattern, an unsupported startup/bounded mode, or untranslatable consumer properties also fall back
- Protobuf parity beyond flat scalar messages — enums, nested/repeated/map fields, well-known types (Timestamp/wrappers), proto3 missing-field defaults, and a SQL column order that differs from the proto field order — is not yet verified against Flink, so those should be treated as unverified until a parity test covers them (the descriptor → Arrow mapping and Flink's `PbToRowTypeUtil` can differ, e.g. enum-as-int vs enum-as-name). The host-baseline protobuf parity test lives in the main module; the ORC host-baseline test is isolated in the `orc-baseline` module because Flink's ORC and protobuf formats need incompatible protobuf-java versions
- Kafka CDC formats other than Debezium/OGG JSON, and CDC tables we can't reproduce bit-identically: **Maxwell and Canal** JSON (their partial-`old` pre-image can't be recovered from the decoded image alone — a field changed *to* null is indistinguishable from an unchanged one), a `schema-include` envelope wrapper, `ignore-parse-errors` (Flink skips bad rows; the native decoder fails on them, matching only Flink's default), and tables with metadata/computed columns. The Maxwell/Canal decoders exist and are unit-tested, but the planner falls these back to Flink
- Two-phase hopping where the slide does not divide the size
- `SUM`/`AVG` over decimal (precision rules), and grouping keys outside bigint/int/string/boolean/date/timestamp/decimal
- Any aggregate over decimal — see [docs/aggregate-type-support.md](docs/aggregate-type-support.md)
- Two-phase `AVG` (multi-field partial state)
- Other operators that *consume* a changelog — regular (non-windowed) joins and streaming Top-N — still need retract *input* handling (the non-windowed `GROUP BY` aggregate consumes one; these operators do not exist natively yet). The non-windowed `GROUP BY` does not model `AVG`'s multi-field running state directly (it works via the host's `SUM`/`COUNT` rewrite).

### Seeing why a query fell back

Substitution is silent by default, so a query that does not accelerate gives no signal on its own.
Two ways to see *why* a node stayed on Flink (mirroring DataFusion Comet's fallback-reason reporting):

- **Per-plan list.** `NativePlanner.install(env)` returns the `PhysicalPlanScan`; its
  `fallbackReasons()` lists each candidate node that fell back and why — a precise expression reason
  for a `Calc` (e.g. `"Calc: unsupported function/operator: ABS"`) and an operator-level reason for a
  stateful node (e.g. `"interval join: needs an INNER equi-join …"`).
- **Plan-time log.** Run with `-Dstreamfusion.logFallbackReasons=true` and each reason is logged as
  it is decided: `[streamfusion] falls back to host — Calc: unsupported function/operator: ABS`.

### Controlling acceleration

Acceleration is configured by JVM system properties (mirroring DataFusion Comet's config surface):

- **Master switch** — `-Dstreamfusion.native.enabled=false` turns off all native substitution; the
  query runs entirely on Flink.
- **Per operator** — `-Dstreamfusion.operator.<name>.enabled=false` keeps one operator on the host
  (`filter`, `calc`, `parquetSource`, `orcSource`, `parquetSink`, `watermark`, `over`, `intervalJoin`,
  `windowJoin`, `windowAggregate`, `localWindowAggregate`, `globalWindowAggregate`). Useful to leave a
  lone row-source operator that measures below 1× on the host rather than pay the transpose round-trip.
- **Opt-in incompatible expressions** — `-Dstreamfusion.expression.<NAME>.allowIncompatible=true` (or
  the blanket `-Dstreamfusion.expression.allowIncompatible=true`) runs natively a function that
  diverges from the host only at a precision/locale edge (`UPPER`/`LOWER`, `ROUND`, transcendental
  math), for data that avoids the edge. Default off.

### Determinism (the one parity caveat)

Results are byte-identical to stock Flink for everything admitted, with a single
boundary in event-time operators (windows, `OVER`, interval and window joins):
**late-data dropping on out-of-order streams.** A row is dropped when the watermark has already passed its
window; we replicate Flink's *deterministic* (eager, per-jump) watermark emission
exactly, slicing batches so a window-closing watermark precedes any row it makes
late. But Flink *also* emits watermarks on a periodic processing-time timer, which
makes **Flink itself non-deterministic** there — two runs of the same job can drop
different rows depending on wall-clock timing. We don't reproduce that
non-determinism (there is no single correct answer to match); we match the
deterministic path, which is what governs in-order data and bounded jobs. In-order
/ monotonic rowtimes — the common case, and every benchmark — have no late rows and
no divergence at all. Details: [divergences/09](divergences/09-per-batch-watermark-assignment.md).

## Benchmarks

Two measurements per operator: the native hot loop in isolation, and the whole job
against stock Flink. Both matter, and right now they tell different stories.

### Native operator hot loop (Criterion)

The native compute over an in-memory Arrow batch (no JVM bridge, no job scheduling) —
run with `cd native && cargo bench`. Method and running table:
[docs/benchmarks.md](docs/benchmarks.md).

| Operator | Benchmark | Batch | Time | Throughput |
|---|---|---|---|---|
| Filter (`WHERE`) | compiled predicate `v > 0`, ~50% pass | 4096 rows | 2.56 µs | ~1.60 Gelem/s |
| Tumbling window aggregate | `SUM` over 16 windows, no key | 4096 rows | 106 µs | ~38.6 Melem/s |
| Tumbling window aggregate | `SUM` over 16 windows, 64 bigint keys | 4096 rows | 252 µs | ~16.3 Melem/s |
| Interval join | INNER, 1:1 on key, equi-key + interval filter | 4096 rows | 100 µs | ~41 Melem/s |
| Window join | INNER, 1:1 on key + window bounds | 4096 rows | 175 µs | ~23 Melem/s |
| Non-windowed `GROUP BY` | `SUM`, 256 string keys (changelog out) | 4096 rows | 1.85 ms | ~2.2 Melem/s |
| `OVER` running `SUM` | running aggregate (specialized fold), 64 keys | 4096 rows | 0.60 ms | ~6.8 Melem/s |
| `OVER` `ROW_NUMBER` | per-key counter, 64 keys | 4096 rows | 465 µs | ~8.8 Melem/s |
| Session window aggregate | `SUM`, 64 bigint keys, 500 ms gap | 4096 rows | ~3 ms | ~1.4 Melem/s |

The native compute is fast where it batches (a filter clears ~1.6 G elem/s; the joins
delegate to a DataFusion hash join at 20–40 Melem/s). The running `OVER` aggregate folds a
small typed state per row (matching DataFusion's accumulators — wrapping integer sum,
null-skipping — without the per-row accumulator call), at ~6.8 Melem/s. The session
aggregate, which merges open windows over a per-key `BTreeMap`, is the remaining per-row
outlier (~1.4 Melem/s, high-variance).

### End to end vs. Flink

The same query over 5M rows, native substitution on vs. off, single slot — run with
`SF_BENCHMARK=true mvn test -Pbench -Dtest=ThroughputBenchmark`. The `-Pbench` profile is
required: it loads the **release** native library (the debug build is ~10–20× slower and
gives misleading numbers — see [docs/benchmarks.md](docs/benchmarks.md)).

| Operator | Query | Flink | Native | Native vs. Flink |
|---|---|---|---|---|
| Parquet copy (columnar source → sink) | `INSERT INTO parquet SELECT * FROM parquet` | 1.32 M rows/s | 6.54 M rows/s | **4.97×** |
| Parquet sink (row source) | `INSERT INTO parquet SELECT *` | 1.23 M rows/s | 2.76 M rows/s | **2.24×** |
| Windowed aggregate over a columnar source | `SUM` by 1s window from a Parquet table | 1.80 M rows/s | 3.29 M rows/s | **1.82×** |
| Interval join (event-time) | `a JOIN b ON a.k=b.k AND a.rt BETWEEN b.rt ± 1s` | 0.37 M rows/s | 0.63 M rows/s | **1.71×** |
| `OVER` running `SUM` (row source) | `SUM(v) OVER (ORDER BY rt)` | 0.91 M rows/s | 1.42 M rows/s | **1.56×** |
| Tumbling window aggregate (row source) | `SUM` by 1s window | 1.69 M rows/s | 2.10 M rows/s | **1.24×** |
| Filter (`WHERE`) | `SELECT * FROM f WHERE v > 50` | 3.23 M rows/s | 2.41 M rows/s | **0.75×** |
| Non-windowed `GROUP BY` (row source) | `SUM(v) … GROUP BY k`, 1024 keys | 2.21 M rows/s | 1.48 M rows/s | **0.67×** |
| Non-windowed `GROUP BY` (columnar source) | `SUM(v) … GROUP BY k` from Parquet | 2.16 M rows/s | 1.84 M rows/s | **0.85×** |

The gain tracks how much of the pipeline stays columnar. The **fully-columnar paths lead**:
the Parquet copy at **4.97×** (read as Arrow, through the native sink, written as Arrow —
never `RowData`, while Flink round-trips every row at both ends), the windowed aggregate over
a columnar source at **1.82×** (the whole source → watermark assigner → keyed shuffle →
local/global window pipeline stays Arrow), and the event-time **interval join at 1.71×** (Flink's
interval join is slow; ours buffers per key and delegates the match to a DataFusion hash join).
The **Parquet sink reaches 2.24×** even from a row source: it writes Arrow → Parquet natively and
coalesces batches into size-targeted files (rolling on a row target / checkpoint) rather than one
file per batch, so per-file footer/syscall overhead no longer scales with batch count. **Other
row-source ops still pay a `RowData → Arrow` transpose at the input**, though it is cheaper since the
converter was made row-major + pre-sized (~25% faster build): `OVER` running `SUM` lands at **1.56×**
and tumbling at **1.24×**. The lone **stateless filter remains below 1× at 0.75×** — a single cheap
predicate cannot earn back the `RowData → Arrow → RowData` round-trip; leave it on the host with
`-Dstreamfusion.operator.filter.enabled=false`. The **non-windowed `GROUP BY` from a row source is the
same story at 0.67×**: the operator is columnar, but a row source feeds it through a transpose (and
out through another to the host sink), and a per-key `SUM` is too cheap to earn back those transposes
(a retract stream is up to ~2× the input rows) plus the per-row key read
([ticket 20](.claude/todos/20-profiling-and-benchmarks.md)). **From a columnar (Parquet) source through
a native keyed shuffle the input transpose is gone and it reaches 0.85×** — the same move that put the
windowed aggregate and `OVER` ahead. It is still short of 1× only because the changelog output still
transposes at the host sink edge and the per-row key read remains; a fully-native downstream removes
that last transpose. It can be disabled meanwhile with
`-Dstreamfusion.operator.groupAggregate.enabled=false`. Closing the gap generally is the columnar-flow
work: keep Arrow across adjacent native operators so the transpose is paid once at the edges, not per
operator ([divergences/08](divergences/08-columnar-flow-transitions.md)).

_Apple M1 Max; numbers are comparable only within a machine._

## Related work

Two commercial native Flink accelerators exist, both **closed source**:

- **Iron Vector** (Irontools) — the same stack as us: Rust + Arrow + DataFusion
  over zero-copy JNI, transparent fallback. It serializes the plan to the native
  side with [Substrait](https://substrait.io/) and integrates at the
  StreamTask/subtopology level, exchanging Arrow batches over Flink's network
  rather than `RowData`. Today it is **stateless only** (projections, filters,
  expressions); windows, joins, stateful operators, and exactly-once are
  described as planned. It claims ~97% higher throughput on a stateless ETL
  pipeline.
  ([blog](https://irontools.dev/blog/introducing-iron-vector/))
- **Vera X** (Ververica, the original Flink creators) — a proprietary native
  vectorized engine with a drop-in compatibility layer and a new state store.
  It supports stateful workloads and claims 5–10× on Nexmark SQL and ~52% lower
  resource usage. The implementation (language, columnar library, plan format) is
  undisclosed.
  ([blog](https://www.ververica.com/blog/vera-x-introducing-the-first-native-vectorized-apache-flink-engine))

Where StreamFusion differs: it is **open source**, and every substitution is
gated and verified for identical results against stock Flink by a parity harness
rather than asserted. It is already native on **stateful windowing** — tumbling,
hopping, and cumulative windows (one- and two-phase) plus session windows — and
event-time interval and window joins, which Iron Vector (stateless only) has not yet shipped.
It is earlier-stage than Vera X and does not
match its operator breadth or have published benchmarks, but its acceleration is
auditable and parity-first by construction.
