# Paimon state backend performance

**Applies to:** the experimental [Paimon backend](../backends/paimon.md)

This page tracks the performance arc of the Paimon state backend, in the order the fixes landed. Each
section names the bottleneck a differential profile localized, the change that removed it, and the
measured improvement. The backend's architecture, coverage, and restore rules live on the
[Paimon backend](../backends/paimon.md) page; this page covers only why it got fast.

## Maintenance moved off the barrier onto a background thread

The state-table compactor originally ran synchronously in every checkpoint's sync phase, paying a
table open, scan plan, and writer/commit lifecycle per operator per barrier — measured *slower* than
no maintenance at all on the q4 backend A/B (124.1 s vs. 99.6 s at 500 ms checkpoints). Maintenance
now runs on a dedicated background thread per operator backend, kicked after each barrier's data
commit — the RocksDB model, where each barrier's new sorted run is the analog of a flushed L0 file
and compaction never blocks the write path; Paimon's optimistic commit retry resolves races with the
barrier's data commits. Measured on the same A/B: **124.1 s → 55.6–66.1 s** across two runs, roughly
2× the Paimon backend's end-to-end throughput, with tables equally maintained.

## State tables are de-bucketed

The original bucket-per-key-group layout made rescale free (whole-bucket file adoption) but wrote one
file per touched key group per commit — fragmentation proportional to max parallelism, for a property
rescale rarely uses. Flink never physically partitions RocksDB by key group either: the group is a
key prefix in one column family, and rescale clips. The tables now default to one bucket per subtask
(`streamfusion.state.paimon.buckets`); key-group locality survives because `kg` leads the primary key
(hydration prunes by key-group predicate over kg-clustered row groups), aligned restores keep
wholesale file adoption, and rescale pays a one-time key-group-range clip rewrite at recovery.
Measured on the q4 backend A/B (500 ms checkpoints, 2M events, two runs): **8.99 s → 3.08–3.71 s**,
which puts the Paimon backend at **1.16–1.27× the memory backend** — faster than memory state,
because each barrier now commits one small delta file where the memory backend's raw snapshots
serialize and upload the whole state. Round cumulative: 124.1 s → 3.1 s.

## Checkpoints hard-link only the files actually uploaded

Every barrier used to hard-link the pinned snapshot's whole reachable file set into the
per-checkpoint directory — one `mkdir` + `linkat` per live file per checkpoint, growing with table
size — when the only files ever read from that directory are the ones the async phase actually
uploads. The reuse decision (which files ride as placeholders against the last confirmed checkpoint,
which upload) now happens once in the sync phase — the backend stashes the checkpoint options and
stream factory that the runner interface only hands to the async phase — and the sync phase links
exactly the non-reusable set, so the linked set and the upload set are identical by construction;
savepoints and non-file-sharing modes upload everything and therefore link everything. Link volume per
checkpoint drops from O(live files) to O(new files). End-to-end movement sat within the session's
thermal noise (the paimon/memory ratio spanned 0.34–0.48 across control runs whose memory baseline
itself spanned 4.4–8.7 s); the maintenance-pacing default deserves a cool-machine A/B for the same
reason.

## Directory ensures are cached

From the post-fix profile: the commit path re-ensures the same table directories on every commit
(snapshot/manifest mkdirs — a fifth of the remaining `mkdir` samples). The custom fs service now
remembers directories it has already ensured, with the write path's missing-parent retry as the
staleness backstop. (A companion change from the same round — pacing a background maintenance thread
— was later removed outright: maintenance is now synchronous at the barrier in deletion-vector mode;
see [Deletion vectors](#deletion-vectors-make-every-committed-read-a-raw-scan) below.)

## The custom local-fs backend

The object-store layer's stock filesystem service calls `create_dir_all(parent)` — a `mkdir` plus its
companion `stat`, each a blocking-pool round trip — on every file it writes, and state tables write
one file per touched bucket per commit plus manifests and snapshot documents, all serialized on the
barrier path. The state tables' directory skeleton lives for the table's whole life, so a custom
opendal service now delegates everything to the stock fs service except `write`, which opens the file
directly and creates a missing parent only on the rare first miss (one retry). The hook this needs —
handing paimon-rust a prebuilt operator — did not exist upstream; the pinned fork carries a 21-line
`FileIOBuilder::with_operator` (pending upstream contribution). Measured on the q4 backend A/B:
**35.4 s → 9.85 s** — the mkdir storm cost far more than its CPU-sample share because commits blocked
on it — bringing the profiling round's cumulative total to **124.1 s → 9.85 s (12.6×)**, the Paimon
backend at 0.44× the memory backend end to end.

## Deletion vectors make every committed read a raw scan

*(2026-07-27)* Two read-path pathologies shared one root: reads through Paimon's merge reader. The
miss-heavy, high-cardinality workload (Nexmark q18's `(bidder, auction)` dedup) re-decoded whole key
columns per batch — range stats cannot prune a uniformly-spread probe set — and any bucket holding
several sorted runs paid the merge itself.

Two resident-memory indexes were tried and rejected first: a bloom file index cannot prune
batch-sized probe sets, and an exact per-file key set holds memory proportional to disk rows (see
`.claude/wontdos/58`). The shipped design removes the merge instead: `deletion-vectors.enabled` on
the state tables makes every level-1+ file standalone-correct (stale rows are masked by vector files,
maintained by stock Java Paimon's lookup compaction), so every committed read is a raw parquet scan
with the exact `IN` probe pushed to the decoder and the vectors applied as row masks.

Deletion-vector reads skip level 0, so the compactor runs a **minimal round synchronously inside the
barrier** — up-level the barrier's runs, universal triggers disabled, delta-proportional by
construction (Paimon's own `lookup-wait` model) — while **discretionary shaping merges run on a
background thread**, serialized against the barrier rounds on one mutex because Paimon supports a
single compactor per table. Shaping can lag arbitrarily without affecting results; an early variant
that ran universal picks inside the barrier re-created the old feedback loop (slow batches → more
barriers → more in-barrier rewrites) and sent q18 to 0.30× under load.

Measured on the Flink-on-RocksDB comparison (exactly-once Kafka, 500K events): q18 deterministic
across independent reruns (**1.7–2.4×**, formerly bimodal between 0.3× and 2.3×), no resident index
memory, suite at **22/23 wins** with a **~2.0× geometric mean**.

## The long-lived maintenance writer session

*(2026-07-27)* Profiling q9 (the hit-heaviest state shape: updating join feeding a retracting top-1)
showed Paimon's manifest reader pool burning more than a core continuously: every barrier's minimal
round re-opened the table, re-planned against the full manifest chain, and restored writers from
scratch — times three subtables. The compactor now opens a session per table holding the table and
one minimal writer across barriers, the dedicated-compaction-job pattern: after each native data
commit it reads just that snapshot's delta manifest and folds the new files in via `notifyNewFiles`,
so the full-chain scan happens once per session (verified by instrumentation: one open per job, never
per barrier), and the lookup-file caches stay warm. Shaping rounds still use a throwaway writer and
invalidate the session when they commit — a long-lived writer must be its table's only compactor —
costing one rescan per trigger-gated shaping commit instead of one per barrier.

Honest measurement: the wasted core is gone, but q9's end-to-end number moved only **~6%** (5.8 s →
5.5 s cool-to-cool; the ratio is dominated by RocksDB's own run variance) — this was a
latency-hiding fix, not a throughput one. The churn was off the critical path, and q9 remains bound by
synchronous per-batch read-through on hit-heavy keys. The change is kept as the prerequisite shape for
fully-async maintenance (deletion-vector-aware merge reads in paimon-rust would remove the barrier
round entirely).

## Incremental checkpoint file listing from delta manifests

*(2026-07-27)* Every barrier must name the table files the checkpoint upload pins — twice, because
the synchronous compaction round commits a second snapshot. That listing used to be a full scan plan
(walk the snapshot's entire manifest chain, merge adds against deletes across every manifest file), so
its cost grew with table history and hit **~170 ms per call** on q9's join tables — and with two
listings per table per barrier across three tables, the barrier tail alone could exceed the
one-second checkpoint interval, re-creating the slow-batches→more-barriers feedback loop that
maintenance was split to avoid.

The store now keeps the live file set as state: each barrier reads only the *delta* manifests of the
snapshots committed since the last listing (its own data commit and the compactor's minimal round —
both small by construction) and folds adds/deletes into the tracked set, re-reading the
deletion-vector index manifest only when its name changes; the full-chain walk happens once, to seed
the set. The pinned paimon-rust fork exposes the manifest entry kind/bucket accessors this walk needs.

Measured on q9 (`SF_STATE_PROFILE` wall-clock instrumentation, exactly-once Kafka, 500K events):
listings **~170 ms → 2–3 ms** each, per-operator barrier sync **0.9–1.6 s → 80–680 ms** — back under
the checkpoint interval, loop broken. q9's end-to-end ratio itself barely moves (0.83× vs. RocksDB;
it sits at the ~0.86× memory-state structural ceiling), but the win applies to every query's barrier
path on the backend.

## The per-batch key-probe read path

The store is exactly **two components** — the write buffer (everything written since the last
barrier) and the committed disk table. Each input batch's keys missing from the buffer are read with
one scan whose `IN` predicate the reader enforces exactly at parquet decode: file/page stats prune,
the key column decodes first, and value columns decode only for matching rows.

Two techniques make per-batch probing affordable where it originally was not:

- The pinned paimon-rust fork evaluates `IN`/`NOT IN` literal sets with **one hash-set pass** over the
  column instead of one comparison kernel per literal (the stock loop is O(rows × literals) —
  quadratic for a pushed key batch).
- The store **plans its scan splits once per pinned snapshot** (the table is immutable between
  barriers), so probes pay no per-batch manifest walk.

Re-reads of hot files are served by the OS page cache rather than an application-side copy of
committed state, and the operator memory bound drops from touched-state-per-interval to
written-state-per-interval. Measured at **parity** on the q4 backend A/B (same session, 500 ms
checkpoints, 2M events): **2.400 s → 2.393 s** — on this shape nearly every read key is also written,
so the superseded design's retained clean rows saved nothing.

### Superseded: bucket-granular resident hydration

*(Superseded, kept for the record.)* Before the per-batch key probe above shipped in its current
form, an earlier design read whole buckets on first miss and kept everything resident until the
barrier. That design existed because the original per-batch key probe on the bucket-per-key-group
layout re-opened the same bucket files for every input batch — the file-open storm the first backend
flame graph was dominated by — so switching to bucket-resident hydration fixed it at the time
(measured then: **55.6–66.1 s → 35.4 s**). That evidence was later confounded: the storm actually came
from the 128-bucket layout, the per-write mkdir storm, and per-literal `IN` evaluation — all since
fixed independently (see the sections above). Once they were fixed, the per-batch probe measured
identical to residency while holding a working set bounded by writes instead of touches, so the
resident map (a third copy of state between the write buffer and the page cache) was removed. This
design is not live; the per-batch key-probe path above is what ships today.

## Per-entry map-state flush diffing

The Paimon backend's map store (join state: one table row per stored row under PK `[kg, key, row]`)
initially flushed a touched key by rewriting its whole bucket at the barrier, so one matched row in a
hot join key rewrote every row stored under that key. The store now keeps the bucket image it
hydrated and diffs against it at the barrier: only entries that differ are upserted and only vanished
rows are tombstoned — the analog of RocksDB `MapState`'s per-entry puts and deletes, derived from the
image instead of tracked per mutation, so the operator still mutates a plain hydrated map. Write
volume per checkpoint drops from O(bucket) to O(changed rows) per touched key (a unit test pins 1 row
flushed from a 3-row bucket, and zero for a reverted mutation).

No end-to-end number is claimed for this one yet — the Paimon backend is opt-in and its benchmark
pass is still pending.
