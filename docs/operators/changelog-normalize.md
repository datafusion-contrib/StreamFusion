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

The shared adapter also binds released Flink 1.18.1. Native RocksDB snapshots pin their read view
at the barrier and convert retained rows asynchronously; memory captures use bounded spill storage.
The older release's generated equality compares row kinds, so unchanged UPDATE_AFTER input retains
its released emission behavior. The runtime comparison mode is set again after restore and does
not change the stored row schema. Wider complete-job qualification remains separate.

A direct Flink 2.2.1 state adapter preserves the full retained INSERT row and its original TTL
write age in the released `deduplicate-state` VALUE descriptor. Native memory and typed RocksDB
share this stock schema; export reads live state without a native snapshot intermediate.
Whole-view restore validates keys against payloads and checks all rows before inserting any
into a fresh native target. Batches remain bounded, including raw expired persisted rows.

All 16 direct Java controls pass across both native stores, including retained payloads, null keys,
whole-view rejection, bounded transfer, receiver retry and original TTL age/expiry. Fourteen existing
processing/checkpoint controls also pass. Ordinary SQL and stock compiled plans now bind this
contract automatically when all other native state owners in the graph have qualified bindings.
The original stock artifact remains portable; operator IDs, serializers, loaded retention and
maximum parallelism are captured from the actual stock plan. Wider mini-batch/TTL/failure and
connector matrices remain open in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Existing processing and
native checkpoint representations remain unchanged. Benchmarking is deferred.


All 52 isolated normalizer/join scenarios pass across both submission forms/stores/orders,
including native checkpoint recovery, no-input returns, empty state, partial mini-batch barriers
and rescaling at 1→3→2/maxP128. Changed join values and key-only tombstones exercise the retained
full row. Custom sources, transactional sink effects, both transposes and clean stock classpaths
remain in the tests. The rescale harness's initial family-list error is retained; its correction
passes eight rescalings and 126 controls after 44 other jobs passed. These overlapping runs are
not cumulative counts. Long-retention jobs are separate from direct expiry controls.

All five unchanged released normalizer restore programs pass per engine: three actual native
imports/input paths and two filtered stock paths. Wider keys/backends, old native graph upgrades,
shared stateful roles and unqualified consumers remain partial under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).


## ROW-key handoff behavior

ROW-bearing unique keys retain their exact stock binary representation through the input exchange
and direct canonical bridge, including nested row kinds and reserved bytes. Both paths use the
same input schema, including representation-preserving nested strings. Stock key serializers,
key groups and the retained row descriptor remain unchanged. Duplicate detection compares row
content through the shared stock-compatible equaliser instead of treating Java representation
flags as changed data. TTL refresh behavior retains its existing rules.

Ordinary SQL resolves ROW-key normalization from the portable stock template, rebuilding both
its input representation and state binding. Qualified native consumers remain native; consumers
without a qualified canonical adapter retain their stock implementation and state identities. The existing mini-batch retracting aggregate
and ROW-bearing regular-join exclusions are preserved. They do not disable the normalizer's
portable binding or permit unmatched real state to be discarded.

The shipping tests retain both input representations, actual nested row kinds, both stock/native
stores, exact immediate/continued state, native checkpoint recovery, custom source/coordinator and
sink state, transpose boundaries and clean stock workers. Benchmarking remains deferred.

When ROW-key input crosses a network edge during rescaling, Arrow Java may rename the internal
UTF-16 list child. The normalizer preserves the original values and reconciles decoded retained
rows with the output schema; both canonical restore and subsequent update-before emission accept
that representation-only rename. Logical schema changes remain rejected.
