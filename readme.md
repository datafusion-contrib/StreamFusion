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
| Tumbling window aggregate | Yes | Event-time `TUMBLE` over a local-time-zone (rowtime) attribute; a single integer value column; one of `SUM` / `MIN` / `MAX` / `COUNT` / `AVG`; grouped by the window only (no additional keys); a single aggregate call. `AVG` follows Flink's integer-division semantics for integer inputs. |

### Global terms (all native execution)

- **Insert-only streams.** Retracting or updating (changelog) streams fall back to Flink.
- **Single-phase aggregation.** With the engine's default two-phase (local + global) window
  aggregation, the window falls back; set `table.optimizer.agg-phase-strategy = ONE_PHASE`
  to make window aggregation eligible.

### Not yet accelerated (falls back to Flink)

- SQL filters (a native filter exists but is not yet wired into planning)
- Hopping, session, and cumulative windows
- Multiple grouping keys, multiple aggregates per window, or non-integer value columns
- Two-phase (local + global) aggregation across a shuffle
