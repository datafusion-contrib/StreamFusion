# LIMIT

**Status:** Native — `LIMIT`/`FETCH`, with or without `OFFSET`, over an insert-only input.

Flink lowers a `LIMIT`/`FETCH` clause to a rank filter and reuses the same rank operator family as
[Top-N](top-n.md) — a plain `LIMIT n` is nothing more than a rank filter with a constant range
starting at 1. Everything the Top-N page describes about ranker selection and idle-state TTL
applies here unchanged, since it *is* the same operator underneath.

`OFFSET` is handled: it runs over the retracting ranker, applied to the (insert-only) input.

## Idle-state TTL

Native here too, for the same reason: a `LIMIT` lowers to a rank, and that rank's TTL machinery
runs regardless of which SQL surface produced it. See [Top-N](top-n.md) for the ranker-specific
expiry granularity, [TTL semantics](index.md#idle-state-ttl) for the general rule, and
[Configuration](../configuration.md) for the flag surface.

## Gap

- A `LIMIT`/`OFFSET` with no `FETCH` (row count) at all — an unbounded skip.
- A retracting input — `LIMIT` requires insert-only (`OFFSET` is the one exception already
  described above, since it runs over the retracting ranker on top of that insert-only input).
