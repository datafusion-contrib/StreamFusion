# Operators

Every page in this section marks one Flink operator (or operator family) as **native**, **partial**
(native with specific, enumerated gaps), or **unsupported** — see [Unsupported
operators](unsupported.md) for the operators with no native path at all. Together these pages are
the precise answer to "why didn't my query accelerate?" — everything not called out as a gap here
runs natively.

## The all-or-nothing island

A query accelerates only if it forms **one fully-columnar island**: every operator but a rowwise
source/sink runs natively, exchanging Arrow batches, with the row↔Arrow transpose paid once at
each host edge and never between native operators. **One unsupported interior operator drags the
whole query back to Flink** — there's no partial acceleration of a single query. Use
`NativePlanner.explain(...)` or `-Dstreamfusion.logFallbackReasons=true` to see the recorded
reason(s) for a given plan.

**What counts as a fallback.** A fallback is something *Flink executes that StreamFusion doesn't
accelerate* — a real gap that could be closed. It is **not** a fallback when Flink itself rejects
the query in streaming (e.g. `RANK`/`DENSE_RANK` Top-N, non-time `ORDER BY`) — matching Flink by
also not running it is parity, not a gap.

## Global switches

- **`-Dstreamfusion.native.enabled=false`** — master switch; run entirely on Flink.
- **`-Dstreamfusion.operator.<name>.enabled=false`** — keep one specific operator on the host. See
  [Configuration](../configuration.md) for the full flag surface.
- **Insert-only guard** — every operator except the changelog-aware ones (`GROUP BY`, regular join,
  a CDC source, `Calc`, `UNION ALL`, `Expand`, changelog normalize, streaming Top-N/`LIMIT`)
  requires an insert-only input; a retracting/updating input falls it back.

## Idle-state TTL

`table.exec.state.ttl` runs **natively** everywhere Flink applies `StateTtlConfig`: non-windowed
`GROUP BY`, changelog normalize, deduplication, the regular join, Top-N/`LIMIT`, `OVER`, and the
temporal join. Semantics match Flink exactly — every stored value carries its last-**write**
wall-clock timestamp (reads never refresh it), expiry happens at `last_write + ttl` inclusive, and
expired state reads as absent and is deleted on read. Each operator's page notes any
operator-specific expiry-granularity wrinkle (e.g. the temporal join's single per-key deadline
instead of per-row TTL).
