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

## Timestamp values and event time

Timestamp readers expose Flink's signed epoch milliseconds plus a non-negative
nanosecond remainder within the millisecond. For example, `-1` nanosecond is
`(-1, 999999)`, not `(0, -1)`. Flink BinaryRow key encoding reads these components
directly, including nested timestamp keys; it does not multiply milliseconds into
an `i64` nanosecond count. Event-time readers for sort, window-aggregate input and
watermarks use the millisecond component. The JVM temporal-function bridge preserves
both components for generated expressions and reads milliseconds for millisecond-only builtins.
Readers accept Arrow second, millisecond, microsecond and nanosecond timestamp
columns without interpreting the Arrow timezone label as a timezone conversion.
The default SQL layout is a nullable timestamp struct containing non-null `millis: BIGINT` and
`nano_of_milli: INT` buffers. Component metadata distinguishes it from user ROW values. This keeps
Flink's full signed-millisecond range and fractional nanos in projections, expressions, keys,
windows and saved state. Its millisecond view shares the original buffer, and its writer preserves
hidden fractions even when the logical precision is three. Explicit Arrow unit casts check overflow;
connector formats follow their host's documented precision and physical-unit limits.
See [the timestamp contract](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/39-timestamp-value-contract.md).

## Global switches

- **`-Dstreamfusion.native.enabled=false`** — master switch; run entirely on Flink.
- **`-Dstreamfusion.operator.<name>.enabled=false`** — keep one specific operator on the host. See
  [Configuration](../configuration.md) for the full flag surface.
- **Insert-only guard** — every operator except the changelog-aware ones (`GROUP BY`, regular join,
  a CDC source, `Calc`, `UNION ALL`, `Expand`, changelog normalize, streaming Top-N/`LIMIT`)
  requires an insert-only input; a retracting/updating input falls it back.
- **`table.optimizer.delta-join.strategy = FORCE`** — no substitutions are made in an optimizer
  block containing an ordinary join and no delta join. Flink validates this strategy *after* our
  pass across all statement roots; removing the ordinary joins could hide its intended rejection.
  Blocks containing a delta join, or no ordinary join, are **not rejected by this FORCE guard**;
  ordinary admission and island checks still apply. **`DeltaJoin` remains unsupported.** The guard
  is deliberately conservative per block: a regular-join block still falls back when a delta join
  exists only in another block, even if Flink's statement-wide check accepts the statement. The
  fallback reason names the optimizer block. `AUTO` and `NONE` are unaffected by this guard.

## State-backend admission

Flink's changelog state backend is unsupported for native keyed operators on both supported lines.
Enabling it makes an affected query fall back in full; stateless queries remain eligible. This is
separate from the SQL changelog row kinds used by updating queries. On 1.18, stock RocksDB selections
are automatically adapted to [native RocksDB](../backends/rocksdb.md); unverified custom backends and
custom RocksDB factories still cause keyed SQL to fall back.

## Idle-state TTL

`table.exec.state.ttl` runs **natively** everywhere Flink applies `StateTtlConfig`: non-windowed
`GROUP BY`, changelog normalize, deduplication, the regular join, Top-N/`LIMIT`, `OVER`, and the
temporal join. Semantics match Flink exactly — every stored value carries its last-**write**
wall-clock timestamp (reads never refresh it), expiry happens at `last_write + ttl` inclusive, and
expired state reads as absent and is deleted on read. Each operator's page notes any
operator-specific expiry-granularity wrinkle (e.g. the temporal join's single per-key deadline
instead of per-row TTL).

## Flink line availability

The 1.18 development profile retains global idle-state retention but has no per-relation
`STATE_TTL` hints, `VARIANT`, delta join or `SESSION` table function. Grouped session windows
remain available. See [Flink line compatibility](../flink-compatibility.md) for the explicit host
N/A cases and validation status.
