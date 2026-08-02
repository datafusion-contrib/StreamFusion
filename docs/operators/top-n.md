# Top-N

**Status:** Native — all three rank strategies, idle-state TTL included; a small set of matcher
gaps below.

Flink lowers a rank filter (`ROW_NUMBER()`/`RANK()`/`DENSE_RANK() OVER (PARTITION BY ... ORDER BY
...)` restricted by `WHERE rn <= n` or `rn BETWEEN offset+1 AND offset+n`) to one of three ranker
implementations, chosen from the input's changelog kind and key structure. StreamFusion runs all
three natively:

- **Append-only ranker** — the input is insert-only; a per-partition sorted list of the current
  top rows. TTL expires per sort-key list — every list write refreshes all of that list's tie rows.
- **Update-fast ranker** — the input carries a unique key and the sort key is inferred monotonic
  against updates on that key (e.g. ranking by a descending `COUNT(*)`), mirroring Flink's
  `UpdatableTopNFunction`. For the `rn <= 1` special case this is `FastTop1Function`: rather than
  keeping bounded state for every row, a new row for a key is dropped immediately — no state
  update, no emission — the moment it fails to outrank the currently-held top row, since a
  monotonic sort key means a non-improving challenger can never later become the top row. TTL
  expires per row-key entry.
- **Retracting ranker** — the general case for an arbitrary retracting input, mirroring Flink's
  `RetractableTopNFunction`. TTL expires the *whole* per-partition buffer at once, on a clock
  refreshed by every record processed for that partition — modeling Flink's own per-record
  `SortedMap` rewrite (see
  [divergences/28](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/28-state-ttl-clock-and-granularity.md)).

Idle-state TTL is native across all three — see [TTL semantics](index.md#idle-state-ttl) and
[Configuration](../configuration.md) for the flag surface.

Also native: an `OFFSET` on any non-update-fast shape, a projected rank number, and both
insert-only and retracting changelog input. `RANK`/`DENSE_RANK` never reach the matcher at all —
Flink itself rejects them in streaming, so that's parity, not a gap.

## Gaps

- A non-constant (variable) rank range.
- A row type the native converter can't carry.
- An **update-fast** rank paired with an `OFFSET` — every other update-fast shape, including plain
  `rn <= 1`, is native.

[LIMIT](limit.md) reuses this same operator — a plain row-count limit is Top-N with a constant
rank range starting at 1.

## Window Top-N and window dedup

Window Top-N ranks within a windowing TVF's windows (`PARTITION BY window_start, window_end, key
ORDER BY ...`); window dedup is the same shape specialized to a time-ordered rank-1, keeping the
first or last row per key per window. Both are native for **event-time and proctime** windowing:
the windowing TVF assigns each row to the window(s) covering its rowtime (or, under proctime, the
operator's clock), and the rank operator closes each window on the same chained
processing-time-timer model as the [window aggregate](window-aggregate.md) — the slide must divide
the size. As with the other proctime-driven window operators, this is non-deterministic, so it's
tested for routing/execution but not byte-compared to the host.

The one gap is a rank that doesn't start at 1 — i.e. an `OFFSET` on the window rank.

The `-Dstreamfusion.operator.windowRank.enabled` switch covers both shapes; window dedup reuses the
window-rank operator rather than getting its own switch — see [Configuration](../configuration.md).
