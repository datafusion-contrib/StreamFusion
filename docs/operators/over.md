# OVER

**Status:** Native across all three frame shapes, with the gaps enumerated below.

`OVER` runs over one ascending order (rowtime or, where noted, proctime) and one window group. Each
aggregate reads its own — possibly different — value column of type
bigint/int/smallint/tinyint/double/float (narrow ints and 4-byte float keep the host's narrow result
type rather than being widened). `FIRST_VALUE`/`LAST_VALUE` and the window functions
`ROW_NUMBER`/`RANK`/`DENSE_RANK` (no value column, unbounded frame) are admitted alongside the
aggregates below.

## Frame shapes

### Unbounded `RANGE … CURRENT ROW` (running fold)

A running fold over the whole partition-to-date — every prior row is folded into the aggregate as
the current row arrives.

### Bounded `ROWS BETWEEN n PRECEDING AND CURRENT ROW`

Recomputed over the row slice — a fixed count of preceding rows plus the current one.

### Bounded `RANGE BETWEEN INTERVAL n PRECEDING AND CURRENT ROW`

Recomputed over the rowtime interval — every row within `n` of the current row's rowtime.

## Proctime order

The running and bounded-ROWS frames are native on proctime as well: arrival order, eager emit, no
wall-clock timer needed.

A **bounded-RANGE frame over proctime** falls back — with processing time materialized as a fixed
per-batch timestamp, a wall-clock-interval frame has no meaningful definition.

## Gaps

The matcher declines:

- `AVG` and `COUNT(*)`.
- A decimal or other non-numeric value column.
- A `PARTITION BY` key outside bigint/int/string/boolean/date/timestamp/decimal.
- A frame not of the form `… PRECEDING .. CURRENT ROW` (a `ROWS`/`RANGE` lower bound that isn't a
  constant preceding offset).
- A bounded-RANGE frame over a proctime order.

## Parity, not gaps

Flink itself rejects or single-groups these in streaming, so not running them natively matches Flink
rather than falling short of it: more than one window group, decimal bounded frames, `FOLLOWING`
frames, non-time or descending order, and `LAG`/`LEAD`.

## Idle-state TTL

`OVER` runs `table.exec.state.ttl` natively across all three frame shapes, but the mechanics differ
by shape:

- **Rowtime frames and the proctime bounded-ROWS frame** share a per-key cleanup deadline (the same
  scheme as the temporal join): registered on every element, with hysteresis and a
  `minRetentionTime > 1` enablement threshold, checked lazily and swept, clearing the key's
  accumulator and frame buffer silently. One wrinkle: at the deadline, the rowtime shapes *defer*
  while the key still has buffered rows the watermark hasn't folded (the timer re-registers and
  waits), whereas the proctime bounded-ROWS frame clears its retract frame unconditionally — so that
  frame can observably restart short.
- **The proctime unbounded fold** instead puts a per-value TTL (`> 0` enables, refreshed on last
  write) directly on its accumulator. An expired key visibly restarts its running fold — and its
  `ROW_NUMBER`/`RANK` numbering — from zero, exactly as Flink's `NeverReturnExpired` state does.
- **The bounded-RANGE rowtime frame** takes no retention at all: Flink's own function accepts none,
  since its frame eviction already bounds state, so `table.exec.state.ttl` changes nothing there.

With that, nothing declines a nonzero retention setting. See [Configuration](../configuration.md) for
the TTL flag surface, and [window aggregate](window-aggregate.md) for the (unaffected — no idle-state
TTL applies) window operators.
