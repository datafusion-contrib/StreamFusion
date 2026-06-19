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
| Projection | Demo only | Single integer input column; the projection is exactly `col * 2`. (A proof of the projection path, not a general projection yet.) |
| Filter (`WHERE`), with column projection | Predicate expressions over the admitted operations | A filter whose predicate is built from the operations the [native expression engine](src/main/java/io/github/jordepic/streamfusion/planner/RexExpression.java) admits: column refs and literals, arithmetic (`+` `-` `*`), the six comparisons (either operand order), and `AND`/`OR`/`NOT` in any nesting (e.g. `WHERE v + k > 25`, `WHERE (a > 1 AND b < 2) OR c <> 3`); `BETWEEN`/`IN`/ranges expand to comparisons. The predicate is encoded and compiled once into a native handle, then evaluated per batch. An optional projection may select/reorder input columns (`SELECT a, c FROM t WHERE …`). The whole row is carried through Arrow, so every input column must be a type the [whole-row converter](src/main/java/io/github/jordepic/streamfusion/operator/RowDataArrowConverter.java) handles (the primitives, boolean, string, timestamp, date, decimal). Any un-admitted operation (other functions, `CAST`, `/`, `%`) makes the whole filter fall back. Equality (`=`) on the filtered column still falls back via a different path (the column is constant-folded into the projection — pending general expression projection); projections with computed expressions or constants, and other column types, fall back. A projection with no filter is left to Flink (no native gain). Integer arithmetic is computed in the operand's declared width and wraps on overflow exactly as the host does (verified at the `INT` boundary); arithmetic between narrow-int columns (`TINYINT`/`SMALLINT`) is the one unverified case — see the [type-support notes](docs/aggregate-type-support.md). |
| Tumbling window aggregate | Yes | Event-time `TUMBLE` over a local-time-zone (rowtime) attribute; one or more aggregates over the same value column — `SUM` / `MIN` / `MAX` / `COUNT` (and `AVG` only as a lone aggregate); grouped by the window, optionally plus one or more bigint/int/string keys. Value-type support is the parity intersection in [docs/aggregate-type-support.md](docs/aggregate-type-support.md): all five over bigint/double, and `SUM`/`MIN`/`MAX`/`COUNT` over int (`SUM` keeps the host's wrapping int semantics, `AVG` its integer-truncating semantics). `AVG` applies only to integer values (bigint/int), never double. |
| Hopping window aggregate | Yes | Same as tumbling, with `HOP`. One-phase assigns each row to its overlapping windows; two-phase (the default plan) pre-aggregates per slice and combines the shared slices of each window, requiring the slide to divide the size (other ratios fall back). |
| Session window aggregate | Yes | Same aggregate/key/value terms as tumbling, with `SESSION` (optionally `PARTITION BY` one or more bigint keys). Each element opens a gap-wide window; overlapping or touching windows merge, including when a late element bridges two open sessions. Always single-phase (the host never splits sessions), so no `ONE_PHASE` is needed. |
| Cumulative window aggregate | One-phase only | Same terms as tumbling, with `CUMULATE` (zero offset only). Nested windows share a bucket start and grow by the step up to the max size. Like `HOP`, two-phase slice-sharing is not native, so set `table.optimizer.agg-phase-strategy = ONE_PHASE`. |

Two-phase (local + global) aggregation is accelerated too: the native local
pre-aggregate emits partial state, the host shuffles by key, and the native
global merges — for `SUM`/`MIN`/`MAX`/`COUNT` (not `AVG`, whose partial is
multi-field). This is the default planning, so tumbling and hopping window
aggregation no longer need `ONE_PHASE`. Hopping uses the host's slice-sharing
model (a per-slice local, a global that combines each window's slices).

### Global terms (all native execution)

- **Insert-only streams.** Retracting or updating (changelog) streams fall back to Flink.

### Not yet accelerated (falls back to Flink)

- `WHERE` predicates using operations the expression engine does not admit (functions, `CAST`, `/`, `%`); general projections beyond the doubling demo, including the constant-folded `=` on the filtered column
- Two-phase (slice-sharing) cumulative windows, and two-phase hopping where the slide does not divide the size
- Grouping keys other than bigint/int/string (e.g. decimal, timestamp), aggregates over different value columns, or `COUNT(*)`
- `AVG` over int, and any aggregate over smallint/tinyint/float/decimal — see [docs/aggregate-type-support.md](docs/aggregate-type-support.md)
- Two-phase `AVG` (multi-field partial state)

## Benchmarks

Each operator is benchmarked in isolation so its acceleration is measured, not asserted,
and regressions show up commit-to-commit. These are native operator hot loops over an
in-memory Arrow batch (no JVM bridge, no job scheduling), via Criterion — run with
`cd native && cargo bench`. Method and the running table: [docs/benchmarks.md](docs/benchmarks.md).

| Operator | Benchmark | Batch | Time | Throughput |
|---|---|---|---|---|
| Filter (`WHERE`) | compiled predicate `v > 0`, ~50% pass | 4096 rows | 2.56 µs | ~1.60 Gelem/s |
| Tumbling window aggregate | `SUM` over 16 windows, no key | 4096 rows | 110 µs | ~37.4 Melem/s |
| Tumbling window aggregate | `SUM` over 16 windows, 64 bigint keys | 4096 rows | 262 µs | ~15.7 Melem/s |

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
hopping, session, and cumulative windows, one- and two-phase — which Iron Vector
(stateless only) has not yet shipped. It is earlier-stage than Vera X and does not
match its operator breadth or have published benchmarks, but its acceleration is
auditable and parity-first by construction.
