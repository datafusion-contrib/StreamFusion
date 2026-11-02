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

Representative isolated complete-job switches and rescaling are qualified below. The broader
schema/failure/version/connector matrix and ordinary-SQL identities remain pending under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking remains deferred.

## Complete-job handoff matrix

Twenty-six isolated complete-job scenarios pass on released Flink 2.2.1: 16 continued-input
round trips across INNER/LEFT, both native stores, both engine orders and residual predicates;
four LEFT no-input re-export round trips; two INNER native checkpoint/cancel/restore handoffs;
and four LEFT rescale round trips. Rescale distributes 64 actual temporal keys through 1 → 3 → 2
tasks at maximum parallelism 128 while the two custom sources and transactional sink remain at
one task. Every advancing native leg requires actual temporal input; each claimed rescale task
must process input. The portable stock plan stays unchanged and stock processes exclude
StreamFusion. All canonical metadata artifacts, including no-input returns, are checked for
StreamFusion class names. External committed rows match both an uninterrupted stock run and an
independent version-history lookup oracle.

These cases retain BIGINT payloads, TIMESTAMP_LTZ(3), both transposes, duplicate/NULL probe keys,
version history, residual rejection and unmatched LEFT output. Watermarks keep probes pending
until final drain so every handoff must preserve their state. They do not qualify intermediate
temporal-output commit failures, full-job TTL expiry, sink-writer rescaling, wider schemas or
cross-version handoffs. The clean combined selection passes 240 checks with zero failures,
errors or skips: these 26 jobs, four interval job controls and 210 temporal/stock/planner controls.
Component and upstream cohorts overlap this selection and are not added cumulatively. Every
operator family remains partial under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.


The compiled-plan matrix also passes all 26 scenarios with explicit SQL UID generation disabled.
Automatic state registration uses the actual native primary IDs, while the canonical artifact
keeps the original stock IDs. Both engine orders/stores, no-input re-export, native checkpoint
recovery and 64-key rescaling retain the scope above. Generated-ID rescaling disables chaining
from the initial job on both engines, preserving stock IDs across changes in parallelism. Stock
itself rejects the original chained generated-ID graph when its chain boundaries change. Six
cross-family identity controls compare registered contract keys with the real runtime hashes; explicit-UID controls still pass. This
removes the UID prerequisite from the qualified compiled-plan workflow. Ordinary SQL optimizer
integration and the remaining #569 matrix stay open. See
[generated operator IDs](../../backends/canonical-state.md#generated-operator-ids-in-qualified-compiled-plans).
