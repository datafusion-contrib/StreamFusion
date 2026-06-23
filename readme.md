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
| Filter (`WHERE`), with column projection | Predicate expressions over the admitted operations | A filter whose predicate is built from the operations the native expression engine admits: column refs and literals, arithmetic (`+` `-` `*`), the six comparisons (either operand order), `AND`/`OR`/`NOT` in any nesting, `IS NOT NULL`, searched `CASE`, and **widening numeric `CAST`** (integer→wider integer, integer→float/double, float→double) — e.g. `WHERE v + k > 25`, `WHERE (a > 1 AND b < 2) OR c <> 3`; `BETWEEN`/`IN`/ranges expand to comparisons. The predicate is encoded and compiled once into a native handle, then evaluated per batch. An optional projection may select/reorder input columns (`SELECT a, c FROM t WHERE …`); arbitrary computed projections route through the `Calc` path above. The whole row is carried through Arrow, so every input column must be a type the [whole-row converter](src/main/java/io/github/jordepic/streamfusion/operator/RowDataArrowConverter.java) handles (the primitives, boolean, string, timestamp, date, decimal). Un-admitted operations — other functions, `/`, `%`, a narrowing/float→int/string `CAST`, or a bare `IS NULL` (the host encodes it as a null `Sarg` the engine does not yet decode) — make the whole node fall back, as do unsupported column types. Integer arithmetic is computed in the operand's declared width and wraps on overflow exactly as the host does (verified at the `INT` boundary); arithmetic between narrow-int columns (`TINYINT`/`SMALLINT`) is the one unverified case — see the [type-support notes](docs/aggregate-type-support.md). |
| Tumbling window aggregate | Yes | Event-time `TUMBLE` over a local-time-zone (rowtime) attribute; one or more aggregates over the same value column — `SUM` / `MIN` / `MAX` / `COUNT` (and `AVG` only as a lone aggregate); grouped by the window, optionally plus one or more bigint/int/string keys. Value-type support is the parity intersection in [docs/aggregate-type-support.md](docs/aggregate-type-support.md): all five over bigint/double, and `SUM`/`MIN`/`MAX`/`COUNT` over int (`SUM` keeps the host's wrapping int semantics, `AVG` its integer-truncating semantics). `AVG` applies only to integer values (bigint/int), never double. |
| Hopping window aggregate | Yes | Same as tumbling, with `HOP`. One-phase assigns each row to its overlapping windows; two-phase (the default plan) pre-aggregates per slice and combines the shared slices of each window, requiring the slide to divide the size (other ratios fall back). |
| Session window aggregate | Yes | Same aggregate/key/value terms as tumbling, with `SESSION` (optionally `PARTITION BY` one or more bigint keys). Each element opens a gap-wide window; overlapping or touching windows merge, including when a late element bridges two open sessions. Always single-phase (the host never splits sessions), so no `ONE_PHASE` is needed. |
| Cumulative window aggregate | Yes | Same terms as tumbling, with `CUMULATE` (zero offset only). Nested windows share a bucket start and grow by the step up to the max size. One- and two-phase: the two-phase global re-buckets each slice partial into every nested window whose end it reaches (and unlike `HOP`, `CUMULATE` carries no synthetic count column, so the partials are just the user aggregates). |
| `OVER` aggregate / window function | Yes (columnar) | Event-time `OVER ([PARTITION BY k] ORDER BY rt) UNBOUNDED PRECEDING`: running `SUM`/`MIN`/`MAX`/`COUNT`/`AVG` over a bigint/int/double value column, and the window functions `ROW_NUMBER()`/`RANK()`/`DENSE_RANK()`. Optionally partitioned by one or more bigint/int/string keys, emitting each input row with the running value appended. Columnar — input columns pass through Arrow and it rides the keyed columnar shuffle with no transpose (native source → watermark assigner → exchange → OVER all flow Arrow). Each row is held until the watermark passes its rowtime, then folded per partition in rowtime order. Aggregates use DataFusion accumulators; window functions keep a small per-key state (a counter for `ROW_NUMBER`, rank/last-value for `RANK`/`DENSE_RANK`) — both incremental, matching Flink, since DataFusion's window evaluators can't be checkpointed ([divergences/11](divergences/11-over-incremental-vs-window-exec.md)). `FIRST_VALUE`/`LAST_VALUE` next; bounded frames and proctime fall back. |
| Interval join (event-time) | Yes (columnar) | Event-time INNER interval join — `a JOIN b ON a.k = b.k AND a.rt BETWEEN b.rt - X AND b.rt + Y` — with one or more bigint/int/string equi-join keys and an event-time interval on the rowtimes. Columnar: each input is shuffled by its join key through a native columnar exchange and the join emits Arrow batches of the matched pairs (left columns then right columns), so the whole two-input pipeline flows Arrow with no transpose (native sources → watermark assigners → exchanges → join). Both sides buffer; a row is joined against the other side's buffer (a DataFusion hash join on the keys with the interval as a residual filter) as it arrives, so a pair is emitted once, and rows are evicted once the combined (min) input watermark passes the point no future row could match. Outer/semi/anti joins, proctime intervals, and a residual non-equi predicate fall back to the host. |
| Window join (event-time) | Yes (columnar) | Event-time INNER window join — two windowing-TVF inputs (`TUMBLE`/`HOP`/`CUMULATE`) joined on their key within the same window: `a JOIN b ON a.k = b.k` where both sides carry matching `window_start`/`window_end`. One or more bigint/int/string equi-join keys. Both sides buffer; when the combined watermark closes a window its rows are joined (a DataFusion hash join keyed on the user key plus the window bounds, so only same-window rows match) and evicted. The join is columnar (Arrow pairs out); the window assignment (windowing TVF) stays on the host. Outer/semi/anti joins, proctime, and a residual non-equi predicate fall back. |
| Parquet source (`SELECT … FROM`) | Local filesystem | A `filesystem`-connector source with `'format' = 'parquet'` reading from a local (`file:` or scheme-less) `path`. Files are read natively as Arrow batches (columnar), so the data never becomes `RowData` — feeding a fully columnar pipeline (a copy into a Parquet sink runs columnar end to end). Remote filesystems (e.g. `hdfs:`/`s3:`) fall back to the host. |
| Parquet sink (`INSERT INTO`) | Local filesystem | A `filesystem`-connector sink with `'format' = 'parquet'` writing to a local (`file:` or scheme-less) `path`. The incoming rows are written to Parquet natively (Arrow → Parquet), committed exactly once via two-phase commit on checkpoint. Remote filesystems (e.g. `hdfs:`/`s3:`) and other formats fall back to the host. |

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

- **Insert-only streams.** Retracting or updating (changelog) streams fall back to Flink.

### Not yet accelerated (falls back to Flink)

- Expressions using operations the engine does not admit: arbitrary functions, `/` and `%` (integer-division/modulo, divide-by-zero semantics), narrowing/float→int/string `CAST`, and a bare `IS NULL` (encoded by the host as a null `Sarg` the engine does not yet decode)
- The native columnar source/sink only on a local (`file:`) path — remote filesystems (`hdfs:`/`s3:`) and non-Parquet formats fall back
- Two-phase hopping where the slide does not divide the size
- Grouping keys other than bigint/int/string (e.g. decimal, timestamp), aggregates over different value columns, or `COUNT(*)`
- `AVG` over int, and any aggregate over smallint/tinyint/float/decimal — see [docs/aggregate-type-support.md](docs/aggregate-type-support.md)
- Two-phase `AVG` (multi-field partial state)

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
| Parquet copy (columnar source → sink) | `INSERT INTO parquet SELECT * FROM parquet` | 1.35 M rows/s | 6.34 M rows/s | **4.68×** |
| Parquet sink (row source) | `INSERT INTO parquet SELECT *` | 1.23 M rows/s | 2.76 M rows/s | **2.24×** |
| Windowed aggregate over a columnar source | `SUM` by 1s window from a Parquet table | 1.80 M rows/s | 3.29 M rows/s | **1.82×** |
| Interval join (event-time) | `a JOIN b ON a.k=b.k AND a.rt BETWEEN b.rt ± 1s` | 0.37 M rows/s | 0.63 M rows/s | **1.71×** |
| `OVER` running `SUM` (row source) | `SUM(v) OVER (ORDER BY rt)` | 0.91 M rows/s | 1.42 M rows/s | **1.56×** |
| Tumbling window aggregate (row source) | `SUM` by 1s window | 1.69 M rows/s | 2.10 M rows/s | **1.24×** |
| Filter (`WHERE`) | `SELECT * FROM f WHERE v > 50` | 3.23 M rows/s | 2.41 M rows/s | **0.75×** |

The gain tracks how much of the pipeline stays columnar. The **fully-columnar paths lead**:
the Parquet copy at **4.68×** (read as Arrow, through the native sink, written as Arrow —
never `RowData`, while Flink round-trips every row at both ends), the windowed aggregate over
a columnar source at **1.82×** (the whole source → watermark assigner → keyed shuffle →
local/global window pipeline stays Arrow), and the event-time **interval join at 1.71×** (Flink's
interval join is slow; ours buffers per key and delegates the match to a DataFusion hash join).
The **Parquet sink reaches 2.24×** even from a row source: it writes Arrow → Parquet natively and
[coalesces batches into size-targeted files](.claude/todos/22-parquet-sink-file-coalescing.md)
(rolling on a row target / checkpoint) rather than one file per batch, so per-file footer/syscall
overhead no longer scales with batch count. **Other row-source ops still pay a `RowData → Arrow`
transpose at the input**, though it is cheaper since the converter was made row-major + pre-sized
(~25% faster build — [ticket 28](.claude/todos/28-native-row-transpose-and-shuffle.md)): `OVER`
running `SUM` lands at **1.56×** and tumbling at **1.24×**. The lone **stateless filter remains below
1× at 0.75×** — a single cheap predicate cannot earn back the `RowData → Arrow → RowData` round-trip.
Closing that last gap is the native-operator-chaining work in
[ticket 21](.claude/todos/21-native-operator-chaining.md): keep Arrow across adjacent native
operators so the transpose is paid once at the edges, not per operator.

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
