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
| Tumbling window aggregate | Yes | Event-time `TUMBLE` over a local-time-zone (rowtime) attribute; one or more aggregates over the same bigint value column — `SUM` / `MIN` / `MAX` / `COUNT` (and `AVG` only as a lone aggregate); grouped by the window, optionally plus a single integer key. `AVG` follows Flink's integer-division semantics. |

Two-phase (local + global) tumbling aggregation is accelerated too: the native
local pre-aggregate emits partial state, the host shuffles by key, and the
native global merges — for `SUM`/`MIN`/`MAX`/`COUNT` (not `AVG`, whose partial
is multi-field). This is the default planning, so window aggregation no longer
needs `ONE_PHASE`.

### Global terms (all native execution)

- **Insert-only streams.** Retracting or updating (changelog) streams fall back to Flink.

### Not yet accelerated (falls back to Flink)

- SQL filters (a native filter exists but is not yet wired into planning)
- Hopping, session, and cumulative windows
- More than one grouping key, non-integer keys, aggregates over different value columns, `COUNT(*)`, or non-integer value columns
- Two-phase `AVG` (multi-field partial state)
