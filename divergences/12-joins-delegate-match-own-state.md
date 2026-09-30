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
emit nulls on watermark expiry, which needs null-emitting state; they fall back to
the host until then. Null keys never match — `HashJoinExec` is built with
`NullEquality::NullEqualsNothing`, matching Flink's `filterNulls`.

The regular (non-windowed) **updating** join takes the opposite implementation
choice — a keyed multiset probed incrementally rather than a batch hash join — for
the reason recorded in [divergences/14](14-standalone-streaming-engines.md): a
changelog join needs per-row retract bookkeeping a batch join can't give.

## Shared parity guarantees that apply
- The per-input keyed shuffle follows Flink's BinaryRow/key-group assignment
  ([10](10-columnar-exchange-own-hash.md)); matching left/right key values therefore co-locate on
  the same channel and key group.
- Late-data dropping on out-of-order streams follows the watermark caveat
  ([09](09-per-batch-watermark-assignment.md)); the parity tests use a lagging
  watermark so nothing is dropped and the result is the full match set.

## Verification
Parity harness: interval and window joins over DataStream and Parquet sources, at
parallelism 1 and 2 (cross-input co-location), plus LEFT-join fallback assertions.

## Updating inner joins on disk

Arroyo's `join_with_expiration` keeps separate input-side state and probes the
opposite side. Immediate updating inner joins follow that separation: point reads
for input counts and lazy opposite-side record scans, with bounded per-bundle reuse.
The record metadata and physical key-group prefix differ because Flink requires
multiset retractions, unique-key replacement, independent write-based TTL, and
portable key-group savepoints. Host-pool reservations follow Comet's reserve/release
pattern; a cache miss never authorizes unbounded full-key hydration.

Probe reuse has no separate fixed byte ceiling: allocations are charged to the
shared host memory pool, following Comet’s reservation model. Failure to reserve
a complete group retains the lazy RocksDB probe path. Historical 8 MiB/64 MiB
comparisons remain test-only controls.
