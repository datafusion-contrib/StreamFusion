# Changelog normalize

**Status:** native, with gaps.

`ChangelogNormalize` turns an upsert/CDC changelog into Flink's normalized `+I`/`-U`/`+U`/`-D`
stream by keeping per-key state. It's one of the changelog-aware operators exempt from the
insert-only guard — see the [operator index](index.md#global-switches) — since accepting a
retracting/updating input is the entire point of the operator.

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
