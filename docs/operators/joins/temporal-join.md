# Temporal table join

**Status:** Native for INNER and LEFT, event-time only. `FOR SYSTEM_TIME AS OF probe.rowtime` against
a versioned table — each probe row joins the build side as it existed at the probe's own rowtime, not
the latest value. Like the [interval](interval-join.md) and [window](window-join.md) joins, this
operator is not changelog-aware on the probe side; the build side, by contrast, is expected to be a
changelog.

## How it works

The build side is held as **per-key versioned state**: every `+I`/`+U`/`-D` change on the build
changelog is kept indexed by rowtime rather than overwriting in place. On a watermark that fires a registered callback for its key, each due
buffered probe row joins the build version that was valid **at its own rowtime** — a faithful port of
Flink's `TemporalRowTimeJoinOperator`, deterministic and value-compared against the host. A residual
non-equi predicate beyond the `FOR SYSTEM_TIME` condition itself (e.g. `... AND o.amount < r.rate`)
is applied natively, same as the other join shapes.

## Admission

Equi-keys use the [verified scalar whitelist](../window-aggregate.md#scalar-grouping-and-partition-keys),
including narrow integers, FLOAT/DOUBLE, TIME, binary and both interval families.
Collection/ROW/object key columns retain fallback. Keys must drop NULLs (LEFT is the only
non-INNER shape Flink itself allows here — see below). The
residual beyond the `FOR SYSTEM_TIME` condition must be expressible by the native expression engine.

## Idle-state TTL

The temporal join runs `table.exec.state.ttl` natively, but with a coarser scheme than the other
TTL-bearing operators (see [Idle-state TTL](../index.md#idle-state-ttl)): Flink bounds it with **one
per-key processing-time cleanup deadline**, not a per-value one, and the native operator replicates
the scheme exactly:

- every touch — either side's changelog, or a watermark firing that leaves state for the key —
  registers the deadline at `now + 1.5× retention` (Flink's planner-derived max idle retention);
- an existing deadline only moves when a touch lands within one retention of it;
- when the clock reaches the deadline, the key's **entire** state clears silently — both the
  buffered probe rows and every build-side version;
- the host schedules a callback for the minimum native cleanup deadline; expiry clears all keys
  due at that timestamp and re-arms the next minimum, even without input or watermarks;
- moving the minimum cancels the old callback; native recovery re-arms from retained per-key
  clocks, including already-overdue clocks, without persisting an extra host timer;
- callback-driven execution disables lazy expiry so mailbox ordering controls which callback
  or input runs first; deadline arithmetic preserves Java long overflow;
- the deadline rides checkpoints exactly as Flink's does;
- cleaning only activates when retention exceeds one millisecond — Flink's own literal
  `minRetentionTime > 1` quirk.

See [Configuration](../../configuration.md) for the TTL flag surface.

## Processing-time boundary

Stock Flink rejects a processing-time versioned-table `FOR SYSTEM_TIME AS OF` join
(FLINK-19830). Its legacy processing-time temporal-function form is supported by stock and
retains that stock state owner here. Native temporal conversion currently covers event time,
including the rowtime temporal-function form. For a processing-time dimension-table lookup,
see [Lookup join](lookup-join.md).

## Falls back to Flink when

- the join type isn't INNER or LEFT (Flink itself rejects RIGHT/FULL for a versioned-table join);
- there's no equi key;
- the key columns aren't null-dropping;
- the equi-key type is outside the supported set;
- a residual non-equi predicate beyond the `FOR SYSTEM_TIME` condition isn't expressible by the
  native expression engine;
- the join is processing-time against a versioned table (stock rejects it), or uses the legacy
  processing-time temporal-function operator (retained stock owner — see above).

## Independent native cleanup state

Partitioned native snapshots preserve cleanup-only key groups independently of probe rows and
build versions. Both memory and persistent logical exporters include those groups; restore and
rescaling retain the original deadline and silent expiry. Regressions cover both backend
switch directions, 64 keys, empty/mixed row sides and three-way redistribution.

These controls cover native recovery. The separate canonical adapter described below preserves
probe indices/cursors, versioned row kinds, registered timer values and queued callbacks that can
outlive TTL cleanup. The compiled-plan path binds that contract automatically; full job
qualification remains open in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).
Benchmarking is deferred.

## Released cursor and callback contract controls

The actual released stock operator passes 18 cases covering a shared 48-transition metadata trace
on both backends, with 96 canonical backend restores. The trace preserves cursors without rows,
signed cursor overflow and output ordering, independent callbacks after TTL cleanup, right-only
cleanup refresh, timer reordering and extreme timestamps. It also preserves an older build version
when later watermarks arrive without a queued callback, so a later probe can still match it.
Live Rocks queue head/size checks and complete freshly restored canonical queue views avoid
mistaking the Rocks iterator's seek hint for missing state.

Both native stores now retain the cursor and physical callback set during execution and native
recovery. Build versions are pruned only for keys whose event callbacks fire; unrelated watermarks
preserve versions needed by late probes. Probe rows use signed per-key indices, including Java-long
wraparound and ordering. Cleanup can leave callbacks without any cached rows or cleanup deadline.
Those metadata-only key groups survive backend switches and redistribution.

Native memory snapshots use four sections: indexed probes, build versions, optional cleanup
clocks, and versioned cursor/callback metadata. Persistent checkpoints retain equivalent typed
tables. Source retention metadata distinguishes deliberately absent deadlines from migration
out of disabled retention. Frozen snapshots from the previous implementation verify both legacy
raw layouts and both persistent restore paths without fabricating old bytes with the new writer.
The shared released trace runs through native recovery after every transition, including backend
switches and aligned/redistributed persistent checkpoints. Orphan callback memory remains accounted
until the callback fires.

## Canonical interchange: partial

The released 2.2.1 adapter reads live native rows and bookkeeping directly into stock map/value
states and Void timer queues, with original key/row serializers and signed probe-map indices.
It creates no intermediate native snapshot. Preflight validates the complete stock inventory,
serializers, row keys/timestamps and callback ownership before native mutation.

Eighty positive operator cases cover INNER/LEFT, both stock/native stores, the shared metadata
trace, unchanged-state handoff, ordinary native recovery, NULL payloads, all four row kinds,
version overwrites and continued execution in both engines. Thirty-two malformed-state controls
verify rejection without partial native import. Serializer snapshots restore with StreamFusion
and planner classes blocked; Arrow allocation checks remain strict. The independent clean build
passes 1,184 native tests (two existing ignores) and 185 Java controls. Shared 1.18 runtime checks
also pass, but do not qualify canonical temporal transfer on that version.

The operator fixtures use BIGINT keys/payloads and compact time fields. The compiled-plan loader
also passes all four unchanged released 2.2.1 temporal restore programs, including scalar string
keys derived from rows/maps and the rowtime temporal function. Each imports four saved rows and
processes four new rows in the native temporal operator. Portable stock JSON, original operator
identity/serializer and resolved maximum parallelism are retained automatically. Twenty-five
planner/boundary controls and 91 neighboring controls pass. Unsupported backend/exchange modes
and the legacy processing-time function keep their stock owner.

Isolated complete-job switches and the broader schema/rescale/failure/version matrix remain
pending under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking remains deferred.
