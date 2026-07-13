# Coverage and fallbacks

What StreamFusion does **not** accelerate, and **every** condition that makes a query (or part of one)
fall back to stock Flink. This file is the **source of truth for coverage**: everything not excluded
here runs natively. The [readme](../readme.md) gives the high-level picture of what *is* accelerated;
this is the precise complement — the boundaries.

> Keep this current. When an operator, type, expression, or connector gains or loses support, update
> this file in the same commit. It is meant to always answer "why didn't my query accelerate?"
> precisely.

A query accelerates only if it forms **one fully-columnar island**: every operator but a rowwise
source/sink runs natively (Arrow in/out). One unsupported interior operator therefore drags the
**whole** query back to Flink (the all-or-nothing gate below). Use `NativePlanner.explain(...)` or
`-Dstreamfusion.logFallbackReasons=true` to see the recorded reason(s) for a given query.

**What counts as a fallback.** A fallback is something **Flink executes that we don't accelerate** —
a real gap we could close. It is *not* a fallback when Flink itself rejects the query in streaming
(e.g. `RANK`/`DENSE_RANK` Top-N, `LEAD`/non-time/`FOLLOWING` `OVER`, non-temporal `ORDER BY`): we match
Flink by also not running it, which is **parity**, not a gap. Nor is it a fallback when the feature
already accelerates via another plan shape (e.g. `UNION` distinct, which the host rewrites to a
`GROUP BY`). This file lists only real gaps; parity cases are called out as such where they'd otherwise
look like one.

---

## (a) What we don't support

### Whole operators with no native path
These have no matcher; any query containing one falls back entirely.

| Operator | SQL surface |
|---|---|
| `Correlate` | lateral table functions, and `UNNEST` with a pushed condition the expression engine can't encode (or any condition over a LEFT unnest). INNER **or LEFT** `UNNEST` of a single `ARRAY` (scalar or `ROW` element, flattened), `MAP` (key+value), or `MULTISET` (element by count) column — optionally `WITH ORDINALITY`, INNER including a pushed element filter — **is** supported (see the chart). |
| `Match` | `MATCH_RECOGNIZE` (CEP / row-pattern) |
| `GroupWindowAggregate` (most), `GroupWindowTableAggregate` | the legacy group-window syntax — `GROUP BY TUMBLE(...)`/`HOP(...)`, and proctime group windows. **Exception:** a legacy event-time `SESSION(...)` group-window routes natively (reusing the session operator), when its only window properties are `(window_start, window_end[, rowtime][, proctime])` in that order |
| `IncrementalGroupAggregate` | the five-node chain a distinct aggregate plans to **only when `table.optimizer.distinct-agg.split.enabled` is on** (partial local → incremental → final global over a bucket key) — a deliberate non-goal: the knob mitigates state-backend hot-key skew our in-process distinct set doesn't exhibit (`wontdos/52-distinct-split-chain.md`). The default mini-batch plan for distinct aggregates — `LocalGroupAggregate` + `GlobalGroupAggregate` with a distinct MapView partial — **is** native, as is the whole ordinary two-phase family (+ `MiniBatchAssigner`); see the feature gaps and §2 below |
| `GroupTableAggregate` | `TableAggregateFunction` |
| `DropUpdateBefore`, `Values` | misc (a non-temporal `Sort` is parity — Flink rejects it in streaming) |
| `LegacyTableSourceScan`, `LegacySink` | legacy connectors |
| `Python*` (`PythonCalc`/`PythonCorrelate`/`PythonGroupAggregate`/`PythonOverAggregate`/…) | PyFlink UDFs |

### Feature gaps inside operators we *do* support
(Real gaps only — Flink runs these and we don't yet. Ordering a nested value, `MAX(array)`/`ORDER BY
array`, is **not** here: Flink rejects it too, so we're at parity.)
- **Aggregates** — non-windowed `GROUP BY` `SUM`/`MIN`/`MAX`/`COUNT` over `DECIMAL` **are** native
  (`SUM` → `DECIMAL(38, s)` with overflow → NULL; `MIN`/`MAX` → `DECIMAL(p, s)`; an i128 at scale `s`,
  matching Flink). **`AVG` is native** for the single-phase non-windowed `GROUP BY`: a running sum
  (widened to bigint for any integer input, double for float/double) plus the non-null count, emitting
  `count == 0 ? NULL : sum / count` with the result cast back to the input type and **integer division
  truncating toward zero** — a faithful port of Flink's `AvgAggFunction`, over
  bigint/int/smallint/tinyint/float/double, retract-aware — and **two-phase `AVG`** now runs native
  too, over the same numeric family (see the mini-batch bullet below). **Decimal `AVG`** is also native
  for the single-phase non-windowed `GROUP BY`: the sum is SUM's `DECIMAL(38, s)` accumulator and
  the emit divides by the non-null count with Flink's exact decimal division (38-significant-digit
  quotient, HALF_UP rescale), reporting `DECIMAL(38, max(6, s))` — `findAvgAggType`'s type. The
  **single-phase windowed** aggregates run decimal `SUM`/`AVG` the same way, and the **two-phase
  (mini-batch) non-windowed** split now carries decimal too: SUM's partial is the i128 running sum
  as `DECIMAL(38, s)` (a bundle overflow emits NULL and latches the merged AVG NULL, skipped by the
  SUM merge — the host's own null-propagation), MIN/MAX partials keep `DECIMAL(p, s)` through the
  extremes multiset, and AVG merges the `(DECIMAL(38, s), bigint)` pair into the exact-division
  emit. The **windowed** two-phase split carries the same full value-type family: every custom SUM
  accumulator's state is Flink's own buffer — the nullable sum alone — so the windowed local emits
  it as the single-field partial and the global merges it with Flink's semantics (a NULL partial is
  skipped; an overflowed decimal sum goes NULL and the next value resets it, not a sticky latch).
  Still falling back: value types outside bigint/double/int/smallint/tinyint/float/decimal (see
  `aggregate-type-support.md`).
- **Two-phase (mini-batch) `GROUP BY`** — all four operators run native: a native `MiniBatchAssigner`
  emits the proc-time marker, the local is a transient in-memory bundle flushed on that marker / a
  `mini-batch.size` trigger / before each checkpoint (no checkpointed state, like Flink's
  `MapBundleOperator`), the keyed shuffle is a native exchange, and the global reuses the single-phase
  group-aggregate operator (`COUNT` merges as a `SUM` over partial counts).
  Scope: SUM/MIN/MAX/COUNT over bigint/int/double value columns, and **AVG** over any of
  `AvgAggFunction`'s numerics — bigint/int/smallint/tinyint/float/double, the single-phase set.
  (Flink's SUM partial keeps the value's own type — nothing is lost to widening.) An `AVG` spans
  **two positional partials** — the widened running sum (bigint for the integer inputs, double for
  float/double) plus the bigint non-null count: the local runs them as a widened-sum state and a
  COUNT over the same column, and the global folds the pre-summed pair into the ordinary AVG state
  (the count partial bumps the non-null count), so the final divide/truncate/cast-back — including
  the cast back to a narrow integer or float result — is byte-identical to the single-phase AVG.
  Decimal SUM/MIN/MAX/AVG carry through the split too (see the decimal bullet above). Both assigner
  modes are native: proc-time (markers generated from the clock) and row-time (upstream event-time
  watermarks filtered to the mini-batch interval — a pure function of the input watermarks, so
  results stay deterministic). **Distinct aggregates ride the split natively** in the default
  (no-split) plan: the local's bundle set travels as a trailing view column — the local's distinct
  (value, count) entries as a list of structs, the Arrow form of Flink's serialized MapView
  partial — and the global folds the entries into its per-key distinct state with multiplicities,
  so values repeating across bundles count once. Scope: `COUNT(DISTINCT)` over
  bigint/int/smallint/tinyint/float/double/string/decimal values, `SUM(DISTINCT)` over bigint/int
  (the merge folds in set-iteration order, so order-sensitive float/double sums stay on the host).
  **Per-aggregate `FILTER (WHERE …)` rides the split too**, on plain and distinct aggregates alike:
  the predicate is a boolean column the local gates every fold on, so the merge stays filter-blind.
  Filtered distinct instances each get their own native view/set per (args, filter) pair — same
  final output as Flink's shared bitmask view, since a filtered distinct is an unfiltered distinct
  over the filtered row subset.
  **A retracting local input** (the aggregate consumes another aggregate's changelog, q4's shape)
  is native for COUNT and AVG — their accumulators are layout-invariant under retraction; the
  local subtracts -U/-D rows, and the appended (or reused) count1 COUNT(*) partial drives per-key
  liveness in the global (`-D` and state drop when the merged count reaches zero, Flink's
  RecordCounter semantics).
  The durable global `GROUP BY` state stays as a Rust hot map but checkpoints through Flink **raw
  keyed state**: each non-empty Flink key group gets its own snapshot payload, and a rescaled task
  restores exactly the payloads assigned to its new key-group range. The native columnar exchange
  uses the same BinaryRow hash/key-group calculation, so the two layouts agree.
  The same per-key-group raw state layout now covers every native state shape partitioned by user
  keys: fixed and session windows, `OVER`, deduplication, Top-N/window rank, and regular, window,
  interval, updating, and temporal joins. Event-time sort follows Flink's special case: raw keyed
  state under one canonical empty key, so its global sort buffer can recover but cannot shard. Raw
  processing-time operators copy their latest cleanup deadline into every raw key-group payload, so
  Flink redistributes and re-arms cleanup after recovery without a separate operator-state fallback.
  Row-to-Arrow and JSON decode batching boundaries likewise drain before their checkpoint barrier.
  Raw payloads are a new checkpoint format, so they are not wire-compatible with the prior
  development-only whole-handle snapshots; versioned native-state migration remains tracked in
  [#22](https://github.com/datafusion-contrib/StreamFusion/issues/22).
  Still falling back: the opt-in `distinct-agg.split.enabled` incremental chain (a deliberate
  non-goal — `IncrementalGroupAggregate` above), MIN/MAX/AVG over DISTINCT under two-phase,
  smallint/tinyint/float SUM/MIN/MAX partials, and — under a retracting input — any aggregate
  other than COUNT/AVG (Flink's SUM/MIN/MAX retract variants declare extra accumulator fields,
  and a monotonicity-exempt MIN/MAX ignores retractions in ways the native fold would not) plus
  DISTINCT (its view value switches to per-filter live counts).
- **`OVER`** — the unbounded `RANGE … CURRENT ROW` frame (running fold), the bounded
  `ROWS BETWEEN n PRECEDING AND CURRENT ROW` frame (recomputed over the row slice), **and** the
  bounded `RANGE BETWEEN INTERVAL n PRECEDING AND CURRENT ROW` frame (recomputed over the rowtime
  interval), over one ascending rowtime, each aggregate over its own (possibly different)
  bigint/int/smallint/tinyint/double/float value column (narrow ints / 4-byte float keep the host's
  narrow result type); `FIRST_VALUE`/`LAST_VALUE` and the window functions
  `ROW_NUMBER`/`RANK`/`DENSE_RANK` (no value column, unbounded frame) are admitted too. **Proctime**
  order is native as well (arrival order, eager emit) for the running and bounded-ROWS frames. Real
  gaps: **`AVG`**, **`COUNT(*)`**, and a decimal/non-numeric value column (the matcher declines them
  — see §2). A bounded-RANGE frame over proctime (wall-clock interval, non-deterministic), more than
  one window group, decimal bounded frames, `FOLLOWING` frames, non-time/descending order, and
  `LAG`/`LEAD` are all parity (Flink rejects or single-groups them in streaming).
- **Deduplication** — all four variants are native: rowtime keep-first (insert-only, watermark-
  released) and keep-last (retracting), and proctime keep-first/keep-last (arrival order, no
  watermark). The proctime order key is materialized by the native `PROCTIME()` expression.
- **Joins** — regular/interval/window joins: a residual non-equi predicate must be expressible by the
  native expression engine (event-time and proctime interval and window joins are all native).
  Under mini-batch, a regular join coalesces replacements only when Flink metadata proves that both
  join keys contain an input upsert key; every non-unique/multiplicity-bearing join retains the
  immediate changelog path.
  **Temporal table join** (`FOR SYSTEM_TIME AS OF probe.rowtime`) is native for INNER and LEFT over
  event time: the build side is held as per-key versioned state (changelog `+I`/`+U`/`-D`, indexed by
  rowtime), and on a watermark each buffered probe row joins the version valid at its time — a faithful
  port of Flink's `TemporalRowTimeJoinOperator`, deterministic and value-compared to the host. A
  residual non-equi predicate beyond the temporal condition (e.g. `… AND o.amount < r.rate`) is applied
  natively, like the other joins. Real gap: none — a **processing-time** temporal table join is parity
  (Flink itself rejects it — FLINK-19830), as is the legacy proctime temporal *function* join.
- **Lookup join** (`FOR SYSTEM_TIME AS OF probe.proctime`, the dimension-table join, Nexmark q13) —
  native for INNER and LEFT against **both synchronous and async** connectors. The probe batch stays
  Arrow; the row-level join core is **Flink's own generated lookup runner** (key building over field
  references *and constants*, the pre-filter, the connector's real `LookupFunction`/`asyncLookup`,
  the **projection/filter on the temporal table**, the **residual non-equi condition**, LEFT
  null-padding), driven by the native operator per batch — byte-identical to the host by
  construction. The async operator fires every probe row's lookup **concurrently** within the batch
  and awaits on the task thread before emitting (the Arroyo/RisingWave within-batch model, no
  operator mailbox needed since nothing is in flight across a batch; concurrency bounded by Flink's
  own `table.exec.async-lookup.buffer-capacity`). This is not vectorizable compute — it is a JVM
  upcall into the host connector — but it keeps the island unbroken, and the async path overlaps a
  batch's I/O. Still falls back: an **upsert-materialized** (keyed-state) lookup.
- **Sources/sink** — Parquet source reads local `file:` paths only. The Parquet **sink** writes to
  **any path scheme Flink has a filesystem for** (`file:`/`s3:`/`gs:`/`abfs:`/`hdfs:`/`oss:`/…):
  the native side only encodes Parquet bytes, drained into Flink's own recoverable output streams,
  so filesystem plugins, credentials, exactly-once commit, and partition commit are the host's own
  code. `PARTITIONED BY` tables are fully supported, including `sink.partition-commit.*` triggers
  and policies (`_SUCCESS` files) via Flink's verbatim `PartitionCommitter`. Sink fallback causes
  are enumerated in §5. Kafka decode limited (see below); CDC covers the four JSON dialects
  (Debezium/OGG/Maxwell/Canal — the latter two for flat scalar schemas); Fluss log tables via the
  native fluss-rs scanner (ARROW log format, a verified scalar-type whitelist — see §5).
- **Proctime** support, by operator:
  - **Deduplication** and **`OVER`** (running / bounded-ROWS) — native; they emit eagerly in arrival
    order, no wall-clock timer needed.
  - **`TUMBLE`/`HOP`/`CUMULATE` window aggregate** — native; each row is assigned to the window(s)
    covering the operator's current processing-time clock and fired on a processing-time timer. `HOP`
    and `CUMULATE` leave several windows open at once, so the timer chains: each firing emits the
    earliest-ending open window and schedules the next slide boundary, until the clock has passed the
    latest open window's end (the slide must divide the size so every window end lands on a slide
    boundary). Non-deterministic, so routing/execution are tested but the result is not byte-compared
    to the host (see the CLAUDE.md note).
  - **Session window aggregate** — native; the gap is measured on the processing-time clock and each
    batch registers a cleanup timer at its `now + gap`, the earliest the session could close with no
    further input. A later element extends the session (merging in the native aggregator) and
    registers its own later timer, so a firing emits only the sessions the clock has truly left behind
    by a full gap. Non-deterministic, so routing/execution are tested but not byte-compared.
  - **Windowing TVF, window join, window Top-N / dedup** — native; the windowing TVF assigns each row
    to the window(s) covering the clock (instead of reading a rowtime column), and the downstream
    window join (two-input) and window rank close those windows on a chained processing-time timer
    (the same next-slide-boundary model as the window aggregate) rather than a watermark. The slide
    must divide the size. Non-deterministic, so routing/execution are tested but not byte-compared.
  - **Interval join** — native; each row is timed by the operator's processing-time clock (its time
    column is stamped with the clock at push, so the interval is measured in processing time), and
    eviction advances on the clock — each batch registers a cleanup timer at `now + max(upper, -lower)`
    (the latest a row buffered now could still match), the tail draining at the last timer / on finish.
    Non-deterministic, so routing/execution are tested but not byte-compared.
  - A proctime bounded-RANGE `OVER` frame falls back: with processing time materialized as a fixed
    per-batch timestamp, a wall-clock-interval frame has no meaningful definition.
  - **Temporal table join** is event-time only by design — Flink itself rejects a processing-time
    temporal table join (FLINK-19830), so a proctime one is parity, not a gap.

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
  `groupAggregate`, `union`, `limit`, `expand`, `changelogNormalize`, `windowRank`,
  `localGroupAggregate`, `miniBatchAssigner`, `lookupJoin`, …). The two-phase global half reuses the
  `groupAggregate` switch. All default on, `kafkaSource` included — it activates only when the
  `streamfusion-kafka` extension (with its Kafka native library) and the matching StreamFusion value
  format JAR are installed; otherwise the plan falls back to Flink's Kafka path. `flussSource`
  behaves the same way for `streamfusion-fluss`.

### 2. Per-operator matcher declines (exact conditions)
- **OVER** — a frame not of the form `… PRECEDING .. CURRENT ROW` (a `ROWS`/`RANGE` lower bound that
  is not a constant preceding offset); a bounded-RANGE frame over a proctime order (wall-clock
  interval, non-deterministic); an aggregate that is `AVG`, `COUNT(*)`, or reads a non-numeric /
  decimal column (numeric value columns are bigint/int/smallint/tinyint/double/float); `PARTITION BY`
  key outside bigint/int/string/boolean/date/timestamp/decimal. (More than one window group, decimal
  bounded frames, non-time/descending order, `FOLLOWING` frames, and `LAG`/`LEAD` never reach us —
  Flink rejects or single-groups them in streaming.)
- **Interval join** — not INNER/LEFT/RIGHT/FULL; no equi key; non-null-dropping (non-INNER) keys;
  equi-key type outside the supported set; non-equi residual not expressible. (Event-time and proctime
  bounds are both native — proctime times rows by the clock and evicts on a processing-time timer.)
- **Window join** — same key/type/non-equi conditions; both sides must carry a window-attached
  windowing of the same time semantics (both event-time or both proctime). Proctime closes the
  window on a processing-time timer instead of a watermark.
- **Temporal join** — not INNER/LEFT (Flink rejects RIGHT/FULL); no equi key; non-null-dropping keys;
  equi-key type outside the supported set; a residual non-equi predicate beyond the `FOR SYSTEM_TIME`
  condition that the native engine can't express; a processing-time temporal join (parity — Flink
  rejects it for a versioned table).
- **Lookup join** — an upsert-materialized (keyed-state) lookup; not INNER/LEFT; a temporal table
  that isn't a non-legacy `TableSourceTable`. (Projection/filter on the temporal table, residual and
  pre-filter conditions, and constant lookup keys are all native — the operator drives Flink's own
  generated runner; both the sync and async processing-time forms are native — §(a).)
- **Regular join** — unsupported join type; no equi key; non-null-dropping keys; non-equi residual not
  expressible; an input column type the converter can't carry.
- **Window aggregate / local / global** — window not event-time `TUMBLE`/`HOP`/`CUMULATE` (zero offset)
  over a local-time-zone **or plain `TIMESTAMP`** rowtime (the bounds render in the session zone for a
  local-time-zone attribute, in UTC — the raw wall-clock — for a plain `TIMESTAMP`) — or, for
  **proctime**, a single-phase `TUMBLE`/`HOP`/`CUMULATE` whose
  slide divides its size, or a single-phase `SESSION`; anything else proctime (the two-phase local/
  global path) is not yet on the processing-time-timer path; `HOP` slide / `CUMULATE` step doesn't
  divide size; key type outside bigint/int/string/boolean/date/timestamp/decimal; value type/aggregate
  mismatch; `AVG` under two-phase (its (sum, count) buffer spans two positional partial columns;
  single-phase `AVG` is native as a lone aggregate); a **windowed
  `DISTINCT` aggregate** (`SUM(DISTINCT …)` etc. inside a window — it dedups per window, which the
  native window operators' every-row fold would over-count; the non-windowed `GROUP BY` handles
  `DISTINCT` natively). A **zero-aggregate**
  grouping-only window (`GROUP BY key + window`, no aggregate function) is a windowed distinct and **is**
  supported (single- and two-phase), emitting one row per (key, window).
- **GROUP BY (non-windowed)** — a UDAF, or `AVG`/`SUM`/`MIN`/`MAX` over a value type outside its
  supported set (see `aggregate-type-support.md`); `AVG(DISTINCT)` (the only
  non-native `DISTINCT` form — `COUNT(DISTINCT x)` keeps a per-key value set, `SUM(DISTINCT x)` adds
  a running sum folded as values enter/leave it, and `MIN`/`MAX(DISTINCT)` run as their plain forms,
  the extreme being multiplicity-blind); an approximate aggregate;
  idle-state TTL ≠ 0; an unsupported key/value column type. `SUM`/`MIN`/`MAX`/`COUNT` all admit
  `DECIMAL` (`SUM` → `DECIMAL(38, s)`, `MIN`/`MAX` → `DECIMAL(p, s)`); **`MIN`/`MAX` also admit a
  string** (`CHAR`/`VARCHAR`), ordered byte-lexicographically — matching Flink's `BinaryStringData`
  byte comparison (its common binary path; the materialized-Java-object path differs only for
  supplementary-plane characters, divergences/07); `COUNT(DISTINCT x)` keeps a per-key value set. A
  per-aggregate **`FILTER (WHERE …)`** is native — the operator folds a row into
  an aggregate only where that aggregate's filter (a boolean input column) is true.
- **Local group aggregate** (two-phase local half) — any aggregate other than SUM/MIN/MAX/COUNT/AVG;
  a SUM/MIN/MAX value type outside bigint/int/double/decimal (MIN/MAX also admit a string, merged
  byte-lexicographically on both halves), or an AVG value type outside
  bigint/int/smallint/tinyint/float/double/decimal; a `COUNT(DISTINCT)` value type outside
  bigint/int/smallint/tinyint/float/double/string/decimal or a `SUM(DISTINCT)` value outside
  bigint/int; `MIN`/`MAX`/`AVG` over DISTINCT; a partial
  whose declared type differs from what the native side emits (the value's own type for SUM/MIN/MAX
  — decimal SUM widens to `DECIMAL(38, s)` — bigint for COUNT, the widened `(sum, count)` pair for
  AVG — defensive, not seen from Flink's planner); a retracting input with any aggregate other
  than plain COUNT/AVG; an unsupported grouping-key/input column type.
- **Global group aggregate** (two-phase merge) — any merge other than SUM/MIN/MAX/COUNT/AVG; a
  partial column outside bigint/int/double/decimal (strings allowed under MIN/MAX); an AVG whose
  partial pair isn't
  `(bigint, bigint)` for an integer (bigint/int/smallint/tinyint) average, `(double, bigint)` for a
  float/double one, or `(decimal(38, s), bigint)` for a decimal one; a distinct merge outside the
  local half's COUNT/SUM(DISTINCT) scope; a retracting merge with any aggregate other than plain
  COUNT/AVG (those merge natively, with the count1 partial driving per-key liveness); an
  unsupported grouping-key/output column type. (Both halves must match for the query to
  accelerate — one staying on the host drags the whole query back via the gate.)
- **Top-N** — a non-constant (variable) rank range; a row type the converter can't carry. (Insert-only
  and changelog input, an `OFFSET`, and a projected rank number are all handled. `RANK`/`DENSE_RANK`
  never reach us — Flink rejects them in streaming.)
- **LIMIT** — missing `FETCH`, or a retracting input (`OFFSET` is handled — it uses the retracting
  ranker over the insert-only input).
- **Deduplicate** — not a time-ordered rank-1. Rowtime and proctime, keep-first (`ASC`) and keep-last
  (`DESC`), are all native; a value-ordered rank-1 is a Top-N (handled separately).
- **Window Top-N / window dedup** — rank not starting at 1 (an `OFFSET`).
- **Windowing TVF** — not `TUMBLE`/`HOP`/`CUMULATE` (zero offset) over a local-time-zone time
  attribute. Both event-time (assign by rowtime) and proctime (assign by the clock) are native.
- **Event-time sort** — a secondary order key beyond the leading ascending rowtime. (A descending or
  non-time leading key is a non-temporal `Sort`, which Flink rejects in streaming — parity.) Its
  global time-ordering buffer uses raw keyed state under Flink's one canonical empty key. It is
  therefore checkpointed and restored by Flink, but deliberately cannot shard across subtasks—the
  same singleton limitation as Flink's temporal sort; see
  [#22](https://github.com/datafusion-contrib/StreamFusion/issues/22).
- **Union** — a row type the converter can't carry. (`UNION` distinct is not a fallback — the host
  rewrites it to a `GROUP BY`, which routes through the aggregate path.)
- **Expand** — any project cell that isn't a column ref, a NULL literal, or the integer expand id.
- **ChangelogNormalize** — a pushed filter condition; the source-reuse variant; a row type the
  converter can't carry.
- **Watermark assigner** — only substituted when its input is already a columnar producer (otherwise
  left on host to avoid a double transpose — a no-op, not a true fallback).

### 3. Expression level — a `Calc`/filter falls back if *any* node is un-admitted
- **Unsupported function/operator** outside the admitted set (e.g. `MD5`; `CONCAT` for a NULL-semantics
  divergence; …).
- **CAST** — native for: widening numeric (integer→wider int, integer→float/double, float→double);
  **narrowing integer→integer and float/double→integer** (the `NarrowingCast` kernel reproduces Flink's
  primitive Java cast — two's-complement wrap for an integer source, round-toward-zero-and-saturate with
  NaN→0 for a float source — which arrow's own cast can't, as it errors on overflow); **CHAR/VARCHAR→
  VARCHAR** when the target length ≥ source (an unpadded no-op, e.g. `COALESCE(s,'x')`); **→DECIMAL
  from an exact source** (DECIMAL or integer, rescaled HALF_UP); and — **host-exact by default via the
  columnar JVM upcall** — **number↔string** in both directions (`CAST(x AS VARCHAR)`,
  `CAST(s AS INT)`, decimals included), **narrowing a VARCHAR** (truncation), **casting to CHAR(n)**
  (space-padding), and **→DECIMAL from a float/double**: these run Flink's own `CastExecutor`
  (`CastRuleProvider`), so trailing zeros, scientific-notation thresholds, trim semantics, and
  failure behavior (an unparsable string fails the job, like the host's default cast) are the host's
  by construction — Java's float/double rendering is even JDK-version-dependent, so no native port
  could match the running host. The upcall casts decline (fall back) when the deprecated
  `table.exec.legacy-cast-behaviour` is enabled — its null-on-failure semantics differ. Still
  falling back: casts between strings and the non-numeric types (boolean/date/time/timestamp↔string)
  and any other pair not listed above.
- **Decimal arithmetic** — **all native and byte-exact by default, *not* a fallback.** `+`/`-`/`*`
  whose result type is `DECIMAL` (e.g. Nexmark q1's `0.908 * price`): operands are Decimal128
  (columns already are; literals emit as an exact Decimal128), Arrow's Decimal128 add/sub/mul carry
  Flink's scales, and the wrapping cast to the declared `DECIMAL(p, s)` rounds HALF_UP as Flink does.
  **Division/modulo** (`/`/`%`) run through a fused native kernel reproducing Flink's exact runtime
  (`DecimalDataUtils.divide`/`mod`): the quotient to 38 *significant digits* with HALF_UP
  (`BigDecimal`'s `MathContext(38, HALF_UP)`), then the rescale to the declared `DECIMAL(p, s)` with
  HALF_UP, NULL when the result exceeds `p` digits, and a division by zero failing the job — all as
  the host. (The old `decimalArithmetic.approximate` flag is retired entirely — the float/double→
  DECIMAL cast it last gated now runs host-exact through the cast upcall; see the CAST bullet.)
- **Case folding & regex — native by default, *not* a fallback.** `UPPER`/`LOWER` and `REGEXP_EXTRACT`
  run natively **by default** via a columnar JVM upcall to Flink's own `BinaryStringData` case folding /
  `SqlFunctionUtils.regexpExtract`, so they are byte-identical to the host and the rest of the expression
  stays native. Each also has a faster **pure-Rust** path (Rust case folding / the `regex` crate) that is
  opt-in under `-Dstreamfusion.expression.<NAME>.allowIncompatible=true` (or the blanket flag) — it can
  diverge from the JVM on non-ASCII case folding / advanced regex (backreferences, lookaround, some
  Unicode classes), so it is not the default. Neither falls back to the host for supported argument types
  (a non-string argument, or the pure-native `REGEXP_EXTRACT`'s non-literal pattern/index, does fall back).
- **`DATE_FORMAT`/`EXTRACT` over `TIMESTAMP_LTZ` — native by default, *not* a fallback.** A local-zoned
  timestamp's calendar fields depend on the session time zone (`table.local-time-zone`), which the naive
  native formatter (UTC wall-clock) can't reproduce. Like case folding/regex, the **default** routes the
  LTZ case through Flink's own zone-aware `DateTimeUtils.formatTimestamp`/`extractFromTimestamp` via the
  columnar JVM upcall — byte-identical. The **pure-Rust `chrono-tz`** path is opt-in under
  `-Dstreamfusion.expression.<DATE_FORMAT|EXTRACT>.allowIncompatible=true` (or the blanket flag); it can
  diverge from the JVM at tz-database edges (bundled-tzdb-version skew, DST beyond ~2100, deep history) —
  see [divergences/17](../divergences/17-ltz-datetime-session-zone.md). A **legacy zone form** the native
  parser can't read (`GMT+1`, `PST`) makes the opt-in path fall back; the default (upcall) handles any
  zone. A plain `TIMESTAMP` argument stays on the pure-native path (no zone, no upcall) as before.
- **Incompatible math — off by default, native only under
  `-Dstreamfusion.expression.<NAME>.allowIncompatible=true` (or the blanket flag):** `EXP`, `LN`, `SIN`,
  `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `LOG10`, `POWER`/`SQRT` (last-ULP libm divergence), and `ROUND`
  on float/double (`BigDecimal` vs binary-float rounding). Unlike case folding/regex there is no cheap
  byte-exact path, so these **fall back** unless opted in.
- **Literal/arity guards** — unsupported literal type; `SUBSTRING` non-literal or out-of-range
  start/length; `LEFT`/`RIGHT`/`REPEAT`/`LPAD`/`RPAD` non-literal or negative count; `TRIM` other than
  default BOTH-whitespace; `POSITION` with a FROM start; `SPLIT_INDEX` empty/non-literal separator;
  `DATE_FORMAT` a non-literal pattern, or (on the pure-native path) a non-translatable pattern
  (text/fraction/zone fields) — the JVM-upcall LTZ path accepts any pattern Flink's formatter does;
  `EXTRACT` a fractional/convention-divergent field (`SECOND`/`DOW`/`WEEK`/`QUARTER`); a `TIMESTAMP_LTZ`
  argument to either now runs natively (session-zone aware — see the datetime bullet above);
  `TO_TIMESTAMP_LTZ` precision ≠ 3; a **non-literal subscript** in `array[i]` / `map[key]` (a runtime
  negative index counts from the end in DataFusion but is NULL in Flink, and the native map lookup
  binds its key at compile time — literal subscripts run natively, `array[i]` requiring the literal
  ≥ 1); wrong arity for any admitted function.

### 4. Type level
- **Boundary leaf types.** Every column (and every nested leaf) must be a type the Arrow boundary
  carries: tinyint/smallint/int/bigint/float/double/boolean/char/varchar/binary/varbinary/date/time,
  year-month interval/day-time interval, timestamp/timestamp-ltz, or decimal. Both gates
  (`FilterCalcMatcher.convertibleRow` for filter/`Calc`, `RowDataArrowConverter.supports` for the
  keyed/stateful operators) check this recursively. Individual stateful operators can impose a
  narrower key-type restriction while their native key encoder is being brought to parity.
- **`RAW<T>` falls back as a column type.** A raw value's bytes, equality, and hash are defined by its
  arbitrary Flink `TypeSerializer`, while its snapshot controls state evolution. Native sources do
  not carry that serializer contract: Fluss/Paimon expose their own binary types rather than Flink
  RAW, and the native Kafka decoders only understand their wire schemas. A generic RAW field therefore
  keeps the affected island on Flink's host path; this is a deliberate exclusion, documented in
  [the WONTDO](../.claude/wontdos/22-generic-raw-type-acceleration.md).
- **Nested `ARRAY`/`MAP`/`ROW`/`MULTISET` are supported** (recursively, down to supported leaves; a
  `MULTISET<E>` rides the Arrow boundary as a `MAP<E, INT>`): carried through filters/projections,
  usable as a `GROUP BY` **key** (the native state uses the Flink BinaryRow bytes and gathers the
  original Arrow key values on emit), and as a `COUNT` value column (counted for null-ness only).
  **Extracting a scalar field from a `ROW` column** in an expression
  (`bid.price`, nested `a.b.c`) is native — the expression engine encodes it as DataFusion's
  `get_field`, returning NULL for a null struct, matching Flink. (This is what lets the Nexmark
  `person`/`auction`/`bid` views — `SELECT bid.price … FROM events WHERE event_type = N` — accelerate.)
  **Subscripting with a literal** — `array[1]`, `map['key']` — is also native (DataFusion
  `array_element` / map `get_field`: NULL on a null collection, an out-of-range index, or an absent
  key, matching Flink); a non-literal subscript falls back (see the literal/arity guards above).
  What still falls back for a nested column:
  - **Ordering a nested value** — `MAX`/`MIN` over it, `ORDER BY` it, or a Top-N/sort on it. Flink
    itself rejects `MAX(array)` and `ORDER BY array`, so this matches the host.
- **Composite `GROUP BY` keys** (`ARRAY`/`MAP`/`ROW`/`MULTISET`) use the recursive Rust
  `BinaryRowData` writer for equality, exchange routing, and raw keyed-state partitioning. The
  parity suite covers nested nulls, inline and long variable-width values, wide decimals,
  precision-9 timestamps and times, and map/multiset layouts. `RAW<T>` is separately excluded above.
- **Aggregate value types** outside the parity matrix in `aggregate-type-support.md`. The non-windowed
  `GROUP BY` (single-phase and the two-phase mini-batch split) and the single-phase windowed
  aggregates cover `DECIMAL` for `SUM`/`MIN`/`MAX`/`COUNT`/`AVG`; the windowed two-phase decimal
  split still falls back.

### 5. Source / sink / connector
- **Filesystem source** — non-local path (`hdfs:`/`s3:`/…) for the Parquet source; any non-Parquet
  source format. (An ORC source existed and was removed: its scan engine, datafusion-orc, lags
  DataFusion releases, and keeping it meant carrying a fork pin through every DataFusion bump —
  restoring it is https://github.com/datafusion-contrib/StreamFusion/issues/19.)
- **Filesystem Parquet sink** — any path scheme Flink has a filesystem for is accepted (Flink's own
  recoverable streams do the IO). The sink's config translator honors every option 1:1 or declines
  with a recorded reason; `parquet.*` keys that stock Flink itself never reads (its writer builder
  bypasses the Hadoop-config-driven properties: bloom filters, page row limits, statistics/column
  index truncation, page checksums, per-column `#col` overrides, …) are ignored exactly as the host
  ignores them. Every fallback cause:
  - **Timestamp columns without `'parquet.write.int64.timestamp' = 'true'`** — Flink's default
    encoding is INT96, which the Rust writer cannot produce (INT96 support via the low-level writer
    is a tracked follow-up).
  - **Timestamp columns without `'parquet.utc-timezone' = 'true'`** — the default shifts values
    through the JVM's local timezone, not safely reproducible off-JVM.
  - **Nested written columns** (`ARRAY`/`MAP`/`MULTISET`/`ROW`/`RAW`) — Parquet group-encoding
    parity unverified; scalar columns cover BOOLEAN/TINYINT/SMALLINT/INT/BIGINT/FLOAT/DOUBLE/
    CHAR/VARCHAR/BINARY/VARBINARY/DECIMAL(any precision, minimal fixed-width binary like the
    host)/DATE/TIME/TIMESTAMP/TIMESTAMP_LTZ. Partition-key columns are exempt (their values become
    directory names via Flink's own code, never Parquet bytes).
  - **`'auto-compaction' = 'true'`** — Flink's compaction topology has no native counterpart.
  - **`parquet.compression` outside UNCOMPRESSED/SNAPPY/GZIP/ZSTD** (LZO/LZ4/BROTLI/LZ4_RAW);
    zstd/gzip levels are honored, including DDL and cluster-Hadoop-config overrides.
  - **Multithreaded zstd** (`parquet.compression.codec.zstd.workers` ≠ 0) — changes the compressed
    frame layout.
  - **`'parquet.validation' = 'true'`**, an unrecognized `parquet.writer.version` or
    `parquet.timestamp.time.unit`, a non-numeric size option, or any unrecognized non-`parquet.`
    sink option.
  - **`INSERT OVERWRITE`** and any unmodeled sink ability — falling back reproduces the host's own
    streaming-mode rejection.
  - A **changelog (retracting) input** — defense-in-depth; Flink's own validation rejects it first.
- **Filesystem sink, non-Parquet format** — any non-Parquet sink format falls back.
- **Kafka** — missing `streamfusion-kafka` or the matching `streamfusion-*` format JAR; a value format
  outside JSON/CSV/raw/bare-Avro/`avro-confluent`/protobuf; a `key.format`;
  a `scan.bounded.mode` other than unbounded/latest-offset; protobuf fields needing representation
  reconciliation (enum/unsigned/bytes/proto3-defaults/well-known types); **`ignore-parse-errors` on a
  protobuf table** (Flink skips malformed messages; that native decoder fails on them — the
  JSON-decoded formats honor the per-message skip, and CSV reproduces Flink's finer per-field
  granularity natively: a bad value nulls the field, a short row pads, a record-level error drops
  the row; a JSON table with the flag set takes the decode path, not the native source).
- **Kafka CSV** — the decode splits records with csv-core and converts fields with Flink-exact text
  parsers (parity-pinned against Flink's own deserializer — `CsvDecodeParityTest`, divergences/21),
  honoring `field-delimiter` (incl. `\t`/`\uXXXX` forms), `quote-character`,
  `disable-quote-character`, `allow-comments`, and `null-literal` natively. Still falling back:
  **`escape-character`** (Jackson unescapes in unquoted fields, csv-core can't); a **non-ASCII**
  delimiter or quote char; a `null-literal` containing a newline; a column outside the scalar
  family — **ARRAY/ROW** (Jackson's `array-element-delimiter` layer) or any non-boundary type.
- **Kafka JSON (and the CDC envelopes)** — the decode follows Flink's converters exactly
  (parity-pinned — `JsonDecodeParityTest`, divergences/21): both `timestamp-format.standard`
  modes (`SQL`/`ISO-8601`) are native on every JSON-decoded path; the
  scalar coercion envelope (string-encoded numbers with trimming, `Infinity`/`NaN`/suffix floats,
  never-failing booleans, strict `ISO_LOCAL_DATE`, integer/boolean/container echo under STRING) is
  Flink's; DECIMAL columns parse the exact raw literal with `BigDecimal`'s HALF_UP-or-NULL (the
  old arrow-json truncation was a silent value divergence); and `ignore-parse-errors` skips with
  Flink's per-field granularity. Still falling back: **`fail-on-missing-field = true`** (a missing
  field is null natively — Flink's default mode; the fail mode isn't modeled).
  `avro-confluent` (decode
  path only, not the native source) fetches each frame's writer schema from the registry by id at
  runtime — the same lazy per-id lookup Flink's deserializer makes, following mid-stream schema
  evolution — but falls back when the registry options need more than a plain URL: an explicit
  reader `schema`, basic/bearer auth, SSL stores, or pass-through client `properties`. All startup
  modes are supported (earliest/latest/group-offsets/timestamp/specific-offsets),
  as are `topic` lists and `topic-pattern` — discovery and offset resolution run in Flink's own
  reused enumerator (`scan.topic-partition-discovery.interval` honored, including `0` to disable),
  so the native paths inherit its semantics; mid-job partition additions reach the native consumer
  as incremental split assignments.
- **Kafka watermarks / event time** — a pushed `WATERMARK` clause is regenerated inside the native
  source for the reproducible shapes: bounded out-of-orderness (`rt` or `rt - INTERVAL const`) over a
  physical rowtime column or `TO_TIMESTAMP_LTZ(bigintCol, 3)` (the epoch-millis computed-rowtime
  idiom), periodic emit, with `scan.watermark.idle-timeout` / `table.exec.source.idle-timeout`
  honored. The in-poll format decode hands the connector typed batches, so the split reader stamps
  each per-partition batch's max rowtime as its record timestamp and Flink's own per-split machinery
  (one generator per split, min combination, idleness) reproduces the pushed strategy — the same
  shared path the Fluss source uses. Still falling back, each with a precise recorded reason: an
  `on-event` emit strategy, watermark alignment, `SOURCE_WATERMARK()`, any other rowtime expression;
  a watermarked CDC table (CDC decodes in an operator downstream of Flink's source, which cannot
  regenerate watermarks); and a watermarked table the native source can't consume at all (the
  decode-operator path regenerates no watermarks either).
- **CDC** — all four JSON dialects route natively: Debezium/OGG (full pre/post images, nested
  columns included) and Maxwell/Canal (post-image + partial `old`, whose UPDATE_BEFORE follows
  Flink's findValue key-presence rule via a native per-message key scan of the raw `old` — an
  explicit null is kept, an absent key copies the post-image, and Canal's presence spans the whole
  `old` array; envelope pinned by `CdcDecodeParityTest`). `ignore-parse-errors` is native with
  Flink's exact granularity: a structurally bad message drops whole, a bad value inside an image
  nulls just that field, and a failure mid-fan-out keeps the rows emitted before it. Still falling
  back: the `schema-include` envelope wrapper; metadata/computed columns; **nested Maxwell/Canal
  columns** (findValue's recursive search could false-match a column name inside another field —
  flat scalar schemas only, up to 128 columns); Canal's `database.include`/`table.include` regex
  filters; and `debezium-avro-confluent` (Avro-bodied envelope).
- **Fluss** — the native source (fluss-rs' `RecordBatchLogScanner`) replaces a Fluss **log-table**
  scan behind a two-level gate: the `flussSource` operator switch
  (`-Dstreamfusion.operator.flussSource.enabled`, default on) **and** the `streamfusion-fluss` JAR
  with a native library for the deployment platform. The planner probes
  `NativeFluss.featureBuilt` and records `the native Fluss extension is unavailable` when it cannot
  load that extension. Coordination stays on the JVM by design — split assignment, startup-offset
  resolution, partition discovery/acknowledgement, snapshot leases, and checkpointing run in the
  Flink-side enumerator, so `scan.startup.mode`/`scan.startup.timestamp`,
  `scan.partition.discovery.interval`, and `scan.kv.snapshot.lease.*` are honored there and never
  translated into the native config (streaming and bounded scans, static and dynamically discovered
  partitions, and column projection are all native). Still falling back, each recorded as
  `fluss source: <reason>`:
  - **Primary-key tables** — they read a changelog the native log scanner does not carry
    (append-only log tables only).
  - **Datalake-enabled tables** — a `lakeSource` reads through the lake, not the log.
  - **Pushdown the native reader can't honor** — a pushed single-row filter, a modification scan
    type, a row-count scan, a pushed-down `LIMIT`, pushed partition filters, an empty projection;
    plus metadata/computed columns (not produced natively).
  - **A pushed-down `WATERMARK` outside the regenerated shapes** — in current Flink a Fluss
    table's `WATERMARK` clause is *not* pushed into the scan (unlike Kafka): the assigner survives
    as its own plan node and runs natively as the columnar watermark assigner above the native
    source, so watermarked log tables route with no source involvement. Defensively, if a pushed
    spec does appear on the scan, the source regenerates it with the same shared per-split
    machinery the Kafka source uses (per-batch max rowtime → one generator per split, min-combined,
    idle timeout honored, periodic emit) for the supported shapes — `rt` or `rt - INTERVAL const`
    over a physical rowtime column — and leaves the whole scan on Flink for anything else, rather
    than silently losing watermarks.
  - **A column type outside the verified whitelist** — BOOLEAN, TINYINT, SMALLINT, INT, BIGINT,
    FLOAT, DOUBLE, CHAR, VARCHAR, DECIMAL, DATE, TIME, plain TIMESTAMP, VARBINARY, and nested ROWs
    whose leaves are also whitelisted: the intersection of fluss-rs' Arrow export and what the
    vendored `ArrowConversion` readers accept, parity-pinned by `NativeFlussTypeParityTest`.
    Notable exclusions: **TIMESTAMP_LTZ** (fluss-rs exports a zoned timestamp vector, which
    `ArrowConversion` currently rejects), **BINARY** (the source's fixed-size-binary handover has
    not yet been parity-tested), and **nested ARRAY/MAP** (unverified across the boundary).
  - **`table.log.format` other than `ARROW`** — fluss-rs' scan validation errors on any other log
    format.
  - **Client config the native client can't mirror** — an unrecognized `client.*` option; a known
    option with no fluss-rs `Config` field (`client.id`, `client.request-timeout`,
    `client.scanner.log.check-crc`, `client.scanner.io.tmpdir`, `client.metrics.enabled`,
    `client.security.sasl.jaas.config`); a `client.security.protocol` other than PLAINTEXT or SASL;
    a SASL mechanism other than PLAIN. `client.writer.*`/`client.lookup.*` options are **ignored**,
    not fallbacks — a read-only source never runs the write or lookup paths.

  Every decline above surfaces through the usual channel — `NativePlanner.explain(...)` /
  `-Dstreamfusion.logFallbackReasons=true`.
