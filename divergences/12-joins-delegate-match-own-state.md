# Event-time joins: delegate the match to DataFusion, own the state

**Kind:** structural — what we delegate vs. what we own.
**Diverges from:** Arroyo (in state ownership and scope; the *match* is aligned).
**Forced by parity:** the state ownership is (we are a Flink guest); the INNER-only
scope is a current limitation, not a requirement.

## The match — aligned with Arroyo (no divergence)
Both of Arroyo's joins run the actual match as a **DataFusion `ExecutionPlan`**
over the batches they have buffered (`JoinWithExpiration` for interval,
`InstantJoin` per window). We do the same: the interval join and the window join
each build a DataFusion `HashJoinExec` — interval = equi-keys + the time interval
as a residual `JoinFilter`; window = equi-keys plus `window_start`/`window_end`
folded into the keys so only same-window rows match. An earlier version
hand-rolled a per-key cross product; that was a divergence with no good reason and
was removed in favor of delegating, per the "rip operators out of Arroyo" default.

## What we own, and why it diverges from Arroyo
Arroyo keeps join state in its own **`expiring_time_key_table`** abstraction — a
keyed, time-tiered store with TTL expiry, tied to Arroyo's table manager and
checkpoint coordinator. We cannot lift that: as a **guest inside Flink** we use
Flink operator state and Flink's checkpoint/watermark machinery (see
[04](04-synchronous-stateful-execution.md)). So we own the buffering and the
**watermark-driven eviction** ourselves:
- **Interval join** — buffer both sides; on each batch, join it against the other
  side's buffer; evict a row once the combined watermark passes the point no future
  row could match (`left.rt - lower`, `right.rt + upper`).
- **Window join** — buffer both sides; when the combined watermark closes a window,
  join that window's rows and evict them.

This is the same "own the streaming state, borrow the compute" split we use for
aggregations (we drive DataFusion accumulators; Flink owns the state model). The
divergence from Arroyo is in *whose* state abstraction — forced by being a guest —
not in *what computes the join*.

## Scope (current limitation, not a divergence of intent)
INNER only; one or more equi-join keys of supported types (bigint/int/string);
no residual non-equi predicate beyond the interval/window. Outer/semi/anti joins
emit nulls on watermark expiry, which needs either null-emitting state or the
retract path ([06-changelog](../.claude/todos/06-changelog-retract-support.md));
they fall back to the host until then. Null keys never match — `HashJoinExec` is
built with `NullEquality::NullEqualsNothing`, matching Flink's `filterNulls`.

## Shared divergences that still apply
- The per-input keyed shuffle uses our own hash, not Flink's key-group hash
  ([10](10-columnar-exchange-own-hash.md)) — safe because the join re-groups by key
  itself; for two inputs, a matching left/right key co-locates on both sides because
  the hash is over key *values*, not column positions.
- Late-data dropping on out-of-order streams follows the watermark caveat
  ([09](09-per-batch-watermark-assignment.md)); the parity tests use a lagging
  watermark so nothing is dropped and the result is the full match set.

## Verification
Parity harness: interval and window joins over DataStream and Parquet sources, at
parallelism 1 and 2 (cross-input co-location), plus LEFT-join fallback assertions.
