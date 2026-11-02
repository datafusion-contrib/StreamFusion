# Changelog normalize

**Status:** native, with gaps.

`ChangelogNormalize` turns an upsert/CDC changelog into Flink's normalized `+I`/`-U`/`+U`/`-D`
stream by keeping per-key state. It's one of the changelog-aware operators exempt from the
insert-only guard — see the [operator index](index.md#global-switches) — since accepting a
retracting/updating input is the entire point of the operator.

Mini-batch-enabled translation validates `table.exec.mini-batch.size` through the selected
released Flink planner. Unset/default `-1`, zero and negative sizes fail during planning
with the host exception and message. Disabled mini-batching does not validate an unused size.

## Fallback

- **A pushed filter condition** on the operator.
- **The source-reuse variant** (where the normalizer shares state with a scan rather than owning
  it outright).
- **An inconvertible row type** — a column outside the Arrow boundary's supported types.

Any of these falls the operator back, dragging the whole query with it per the
[all-or-nothing island rule](index.md#the-all-or-nothing-island).

## Idle-state TTL

`table.exec.state.ttl` runs natively here, with Flink's exact semantics: every stored value carries
its last-**write** wall-clock timestamp (a read never refreshes it), a value expires at
`last_write + ttl` inclusive, and expired state reads as absent and is deleted on read — the next
row for that key restarts as a fresh `+I`. See [Idle-state TTL](index.md#idle-state-ttl) for the
mechanics shared across every operator TTL applies to.


## Canonical savepoint adapter

A direct Flink 2.2.1 state adapter preserves the full retained INSERT row and its original TTL
write age in the released `deduplicate-state` VALUE descriptor. Native memory and typed RocksDB
share this stock schema; export reads live state without a native snapshot intermediate.
Whole-view restore validates keys against payloads and checks all rows before inserting any
into a fresh native target. Batches remain bounded, including raw expired persisted rows.

All 16 direct Java controls pass across both native stores, including retained payloads, null keys,
whole-view rejection, bounded transfer, receiver retry and original TTL age/expiry. Fourteen existing
processing/checkpoint controls also pass. This adapter is not yet installed automatically by
normal job submission. Compiled/ordinary binding, released normalizer fixtures, complete-job
mini-batch/TTL/failure and connector matrices remain open in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Existing processing and
native checkpoint representations remain unchanged. Benchmarking is deferred.
