# 28 — State TTL: clock sampling and expiry granularity

Native idle-state TTL (`table.exec.state.ttl`) replicates Flink's `StateTtlConfig` as the table
runtime configures it — `OnCreateAndWrite`, `NeverReturnExpired`, expired ⟺ `last_write + ttl <=
now` — but diverges from Flink's mechanics in five deliberate ways. All five are invisible outside
timing windows that wall-clock TTL already makes non-deterministic in Flink itself.

## Per-call clock sampling

Flink reads `System.currentTimeMillis()` inside every state access (`TtlTimeProvider.DEFAULT`).
The native operators sample the clock once per ingest call — the host passes its
`ProcessingTimeService` reading (the same wall clock in production) as a JNI argument, and every
row of the batch shares it. The difference is bounded by one batch's processing time, well inside
the run-to-run jitter Flink's own per-access reads have. The win: the test harness steers the
service's clock, so expiry is deterministically testable at the operator level with no test-only
hooks. Corollary: the mini-batch flush paths replay staged work under the bundle's last ingest
clock instead of widening the flush ABI with a second clock argument.

## Temporal join and OVER: lazy check for the cleanup timer

Flink retention-bounds the temporal join not with per-value TTL but with one per-key
processing-time cleanup timer (deadline `now + 1.5×retention`, moved under a hysteresis rule,
clearing the key's whole state when it fires). The native operator keeps the same deadline in a
per-key map and enforces it lazily — at each key touch, plus a once-per-retention silent sweep —
instead of registering a timer. Firing emits nothing and only affects what later probes see, so
the substitution is observably identical within the per-call clock sampling above. One narrowing:
Flink also re-arms the deadline when a key's event-time timer fires; we replicate that for keys
firing buffered probe rows, but a build-only key's single fire-time re-arm — observable only when
the watermark trails the wall clock by more than half the retention — is not replicated, so such
a key expires at the deadline its last push registered (never sooner than one full max retention
after its last write).

On the persistent RocksDB backend the same lazy + sweep scheme narrows once more: the memory
path visits every buffered key at each watermark advance and can clear an expired key whose rows
all sit above the watermark, while the backend only sees the keys the watermark actually fires
(visiting the rest would mean a full state read per advance). Such a key's state lingers until
its next fire or the once-per-retention sweep — silent either way, since a fired deadline emits
nothing and the expiry decision at the eventual fire uses the same deadline.

The OVER aggregate's deadline shapes (rowtime frames and proctime bounded ROWS) make the same
lazy + sweep substitution. Flink's fired timer defers a rowtime key that still has buffered rows
awaiting a watermark, re-registering from the fire time (`fire + max`); our deferral re-arm runs
at the next lazy check or sweep instead (`check-time now + max`), which keeps the state
marginally longer — silent, and bounded by the lazy-check granularity. Bounded RANGE over rowtime
takes no retention because Flink's own function accepts none: its cleanup is the event-time frame
eviction derived from the frame bound, which the native buffer already applies.

## Retracting Top-N: whole-buffer expiry

Flink's `RetractableTopNFunction` splits its state: a `ValueState<SortedMap>` treemap written on
every record for the partition, and a per-sort-key `MapState` written only when that sort key is
touched. Under TTL the two halves can expire independently, leaving internally inconsistent state
whose observable output is a hardcoded-lenient warn-and-skip. We model the treemap's clock only:
the whole buffer expires atomically on a head-entry timestamp refreshed by every processed record.
A partition idle past the retention loses everything at once (a stale retraction then finds
nothing and emits nothing — the same observable as Flink's lenient path); a partition Flink would
half-expire keeps its rows here. Replicating the half-expired output exactly would mean a second
count structure whose only purpose is reproducing state corruption.

## Proctime keep-last dedup: which Flink to match

Flink's identical-row suppression in proctime keep-last dedup compares `RowKind` through its
generated equaliser, and its heap state backend aliases the stored row with the emitted one — so a
key suppresses duplicates only until its first update mutates the stored kind to `UPDATE_AFTER`,
after which identical rows re-emit forever. On RocksDB the stored bytes keep the pre-mutation
kind and suppression keeps working. Flink's own output therefore differs across its state
backends; we match the heap backend (the one the parity harness runs against), and persist the
stored kind in snapshots as the heap backend effectively does.

## No resurrection when the retention grows across a restore

Physical cleanup (the memory sweep or RocksDB compaction filter) removes rows that
were expired under the retention in force at the time. Restoring with a larger
`table.exec.state.ttl` cannot bring them back. Flink documents the identical caveat for its
RocksDB compaction filter; keeping the retention out of the persistent table schema (it is passed
per session, never stamped) ensures a stale value can at least never drive future drops.
