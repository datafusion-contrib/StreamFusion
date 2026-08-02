# Deduplication

**Status:** Native — all four variants (rowtime/proctime × keep-first/keep-last), mini-batch
included, idle-state TTL included.

Flink recognizes `ROW_NUMBER() OVER (PARTITION BY key ORDER BY time_col ASC|DESC) = 1` as its
`Deduplicate` operator rather than a general rank filter whenever the order key is exactly one time
column (rowtime or `PROCTIME()`) and the rank is 1. A rank-1 filter ordered by anything else is a
value-ordered dedup, which Flink plans — and StreamFusion runs — as [Top-N](top-n.md) instead.

## The four variants

- **Rowtime keep-first** (`ORDER BY rowtime ASC`) — insert-only, watermark-released: a key's first
  row is held until the watermark passes it, then emitted once.
- **Rowtime keep-last** (`ORDER BY rowtime DESC`) — retracting: each later row for a key replaces
  the previous emission.
- **Proctime keep-first** / **keep-last** — arrival order, no watermark involved. The order key
  itself is materialized by the native `PROCTIME()` expression, so both proctime shapes run as
  ordinary time-ordered dedup over that generated column.

All four emit eagerly in arrival order — proctime dedup needs no wall-clock timer, unlike the
windowed operators that fire on a processing-time clock.

## Mini-batch

Every mini-batch shape replicates Flink's per-mode emission exactly:

- Under mini-batch, a **rowtime** dedup — keep-first included — becomes Flink's bundled retracting
  function. Its flush emits every kept row's transition by default, or one net transition per key
  per bundle under `table.exec.deduplicate.mini-batch.compact-changes-enabled`.
- A **proctime** flush emits one net transition per key.

## Insert-sensitivity

`table.exec.deduplicate.insert-update-after-sensitive-enabled` (default `true`) is replicated too.
With the option off, under a consumer that requests only `UPDATE_AFTER` (an upsert sink), every
emission becomes a bare `+U` — a fresh key's first row included — and the proctime identical-row
suppression is disabled, exactly as Flink's own helpers behave.

## Idle-state TTL

Idle-state TTL runs natively here — see [TTL semantics](index.md#idle-state-ttl) and
[Configuration](../configuration.md) for the flag surface. One shape needs a wrinkle beyond the
standard last-write-timestamp rule: the watermark-buffered **rowtime keep-first** dedup TTLs only
its emitted *markers*. The buffered candidate row itself mirrors Flink's deliberately un-TTL'd timer
state — the watermark is what cleans it up, and expiring it early would lose data. The marker,
written once when a key fires and never refreshed afterward, expires a fixed retention after that
firing — which is what lets the key emit a second "first" row once its earlier marker has aged out.

## Gap

A rank-1 filter that is not time-ordered — i.e. ordered by a value column rather than rowtime or
`PROCTIME()` — is not a fallback for Deduplication; it is a different query shape, handled by
[Top-N](top-n.md).
