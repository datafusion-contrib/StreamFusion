# Disaggregated state store (remote/tiered operator state, likely on Apache Fluss)

**Status:** TODO (not started, longer-horizon / architectural). Requested 2026-06-30.

Today every native stateful operator holds its state **in-memory** in the worker process — the
arrow-row byte-keyed maps (group-by, join, Top-N, dedup), the interval/window join `RecordBatch`
buffers, the MIN/MAX multisets. State size is bounded by worker RAM, checkpoints snapshot the whole
state to the JVM via IPC, and a rescale reshuffles it. That caps how large a keyspace we can run and
ties recovery time to total state size.

**Goal:** a **disaggregated** state backend — operator state lives in a remote/tiered store with a local
working-set cache, so state size is decoupled from worker memory, checkpoints are incremental (only
dirty keys), and recovery/rescale is fast (lazy fetch on access instead of bulk reload). This is the
direction Flink 2.0 took with **ForSt** (async, DFS/S3-backed state) and RisingWave with its
Hummock/object-store state — consult both before designing (extend the `.claude/research/` notes).

**Why Apache Fluss is the likely backing store.** Fluss already gives us (a) a columnar streaming log
and (b) a **primary-key (upsert) table** model — i.e. a durable, partitioned, versioned KV store with a
Flink-native client. An operator's keyed state maps cleanly onto a Fluss PK table keyed by
`(operator, key)`. Crucially, the state we now hold is **already arrow-row bytes** (memcomparable key +
value-encoded payload — from the Top-N/join/dedup/group-by refactors): that byte layout is exactly what
a remote KV store wants on the wire, so the encode/decode is mostly already paid. Pairs naturally with
ticket 36 (Fluss as a *source*) — one Fluss dependency serving both ingest and state.

**Sketch of the work (to be designed, not prescribed):**
- A state-access abstraction over the operators' maps (get/put/delete/scan by key) so the in-memory map
  and a remote-backed map are interchangeable.
- A local working-set cache (LRU) over the remote store; async prefetch on the mailbox model
  (ticket 01) so remote latency overlaps record processing (ForSt's async pattern).
- Incremental checkpoint: flush only dirty keys to Fluss; checkpoint = the committed Fluss offset/version.
- Rescale by key-range reassignment without bulk reload (lazy fetch).
- Reuse the existing arrow-row key/value byte encoding as the stored format.

**Open questions:** does Fluss's PK table give the point-lookup latency + range-scan we need, or is an
object-store + LSM (Hummock/ForSt-style) the better primitive with Fluss only for the changelog? Benchmark
state-heavy Nexmark (q4/q9/q16 — large keyspaces) under disaggregated vs in-memory before committing.

Relates to: ticket 36 (Fluss source), ticket 05 (memory accounting), ticket 01 (mailbox/async),
and the changelog-operator byte-state refactors (the on-the-wire format is ready).
