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
| Tumbling window aggregate | Yes | Event-time `TUMBLE` over a local-time-zone (rowtime) attribute; one or more aggregates over the same bigint or double value column — `SUM` / `MIN` / `MAX` / `COUNT` (and `AVG` only as a lone aggregate); grouped by the window, optionally plus a single integer key. `AVG` follows Flink's integer-division semantics and so stays bigint-only. Double values are accelerated on the one-phase path only. |
| Hopping window aggregate | Yes | Same as tumbling, with `HOP`. One-phase assigns each row to its overlapping windows; two-phase (the default plan) pre-aggregates per slice and combines the shared slices of each window, requiring the slide to divide the size (other ratios fall back). |
| Session window aggregate | Yes | Same aggregate/key/value terms as tumbling, with `SESSION` (optionally `PARTITION BY` a single bigint key). Each element opens a gap-wide window; overlapping or touching windows merge, including when a late element bridges two open sessions. Always single-phase (the host never splits sessions), so no `ONE_PHASE` is needed. |
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

- SQL filters (a native filter exists but is not yet wired into planning)
- Two-phase (slice-sharing) cumulative windows, and two-phase hopping where the slide does not divide the size
- More than one grouping key, non-integer keys, aggregates over different value columns, `COUNT(*)`, or value columns that are neither bigint nor double
- Two-phase (local + global) aggregation over a double value column
- Two-phase `AVG` (multi-field partial state)
