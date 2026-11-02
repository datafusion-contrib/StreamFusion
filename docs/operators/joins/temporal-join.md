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
- enforcement is lazy at each key touch, plus a once-per-retention silent sweep (equivalent to
  Flink's timer, since firing emits nothing);
- the deadline rides checkpoints exactly as Flink's does;
- cleaning only activates when retention exceeds one millisecond — Flink's own literal
  `minRetentionTime > 1` quirk.

See [Configuration](../../configuration.md) for the TTL flag surface.

## Released processing-time restriction

A **processing-time** temporal table join (`FOR SYSTEM_TIME AS OF probe.proctime` against a
versioned table) and the legacy proctime temporal *function* join are both parity, not a gap — Flink
itself rejects them (FLINK-19830). For the proctime *dimension-table* shape that Flink does support,
see [Lookup join](lookup-join.md).

## Falls back to Flink when

- the join type isn't INNER or LEFT (Flink itself rejects RIGHT/FULL for a versioned-table join);
- there's no equi key;
- the key columns aren't null-dropping;
- the equi-key type is outside the supported set;
- a residual non-equi predicate beyond the `FOR SYSTEM_TIME` condition isn't expressible by the
  native expression engine;
- the join is processing-time against a versioned table (parity, not a real gap — see above).

## Independent native cleanup state

Partitioned native snapshots preserve cleanup-only key groups independently of probe rows and
build versions. Both memory and persistent logical exporters include those groups; restore and
rescaling retain the original deadline and silent expiry. Regressions cover both backend
switch directions, 64 keys, empty/mixed row sides and three-way redistribution.

This is native snapshot correctness, not stock canonical-savepoint qualification. Complete
stock state projection must additionally preserve probe ordering/cursors, versioned row kinds,
the registered event-timer value and queued callbacks that can outlive TTL cleanup. That adapter
and automatic temporal plan binding remain open in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.

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

Stock temporal savepoint transfer, host cleanup-timer scheduling and automatic plan binding remain
unfinished. Native recovery correctness does not count as qualified stock conversion coverage.
Benchmarking remains deferred.
