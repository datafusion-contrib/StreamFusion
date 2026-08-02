# Temporal table join

**Status:** Native for INNER and LEFT, event-time only. `FOR SYSTEM_TIME AS OF probe.rowtime` against
a versioned table — each probe row joins the build side as it existed at the probe's own rowtime, not
the latest value. Like the [interval](interval-join.md) and [window](window-join.md) joins, this
operator is not changelog-aware on the probe side; the build side, by contrast, is expected to be a
changelog.

## How it works

The build side is held as **per-key versioned state**: every `+I`/`+U`/`-D` change on the build
changelog is kept indexed by rowtime rather than overwriting in place. On each watermark, every
buffered probe row joins the build version that was valid **at its own rowtime** — a faithful port of
Flink's `TemporalRowTimeJoinOperator`, deterministic and value-compared against the host. A residual
non-equi predicate beyond the `FOR SYSTEM_TIME` condition itself (e.g. `... AND o.amount < r.rate`)
is applied natively, same as the other join shapes.

## Admission

Same equi-key/type conditions as the [regular join](regular-join.md): a supported-type equi-key and
null-dropping keys (LEFT is the only non-INNER shape Flink itself allows here — see below). The
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

## Real gap: none

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
