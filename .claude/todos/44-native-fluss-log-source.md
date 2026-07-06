# Native Fluss log source (Rust scanner → Arrow, zero-transpose ingest)

**Status:** designed, not started. Feasibility investigated 2026-07-05 —
`.claude/research/fluss-native-source-findings.md` holds the full findings (class/crate
references, discovery mechanics, capability matrix). Requested as "a Fluss log source that
works in practice very similar to Kafka".

## Why

Fluss log tables are Arrow on the wire. The stock connector decodes to `ColumnarRow` and then
converts per record to `RowData`; our native reader can hand the batch across the C Data
Interface untouched — the first source with a ~zero ingest perimeter (no decode, no
RowData→Arrow transpose). Ticket 36 (Nexmark-on-Fluss) is the measurement of exactly this.

## Shape: the Kafka pattern, one-to-one

Mirror `NativeKafkaSource`: reuse Fluss's JobManager-side coordination verbatim, swap only the
per-subtask reader.

- **Reuse verbatim** (all public, directly instantiable): `FlinkSourceEnumerator`,
  `SourceSplitSerializer` (null LakeSource), `SourceEnumeratorState` + its serializer, the
  split types. Our `Source` impl delegates `createEnumerator`/serializers exactly like
  `NativeKafkaSource` does with `KafkaSourceEnumerator`.
- **Replace** `FlinkSourceSplitReader` with a native split reader: JNI handle over a
  fluss-rust `RecordBatchLogScanner`; `SplitsAddition` → `subscribe_partition(partitionId,
  bucket, startingOffset)` / `subscribe(bucket, offset)`; poll returns per-bucket Arrow
  batches → per-split `ArrowBatch` records (split state advances per bucket, as in
  `NativeKafkaSplitReader.fetch()`).
- **Dynamic partition discovery comes free**: the reused enumerator polls
  `listPartitionInfos` every `scan.partition.discovery.interval` (default 1 min) and assigns
  new-partition splits (FLIP-288: post-first-round partitions start EARLIEST). fluss-rust
  subscriptions are an RwLock map separate from the poll path, so incremental mid-poll
  subscribe works — same reconcile shape as the Kafka reader, except Fluss's subscribe is
  already incremental (no full-assignment replace, no offset-tracking union needed natively).
- **Partition removal (no Kafka analog)**: dropped partitions arrive as
  `PartitionsRemovedEvent`; the reader must unsubscribe those buckets and ack with
  `PartitionBucketsUnsubscribedEvent` (the fetcher manager's `removePartitions` path). The
  native reader needs `unsubscribe` plumbing plus split-state cleanup so checkpoints stop
  carrying removed splits.

## Scope (first cut)

- **Append-only log tables, ARROW log format** (the only format fluss-rust reads), streaming
  mode. Startup modes: full/earliest, latest, timestamp — all map to subscribe offsets or the
  admin `list_offsets(Earliest|Latest|Timestamp)` the enumerator resolves anyway.
- Projection pushdown: wire `SupportsProjectionPushDown` → `TableScan::project` (server-side
  pruning on the local path; remote/tiered reads decode full schema — prune client-side, note
  in coverage doc).
- Watermarks: same per-split regeneration we built for Kafka (per-bucket batch-max rowtime).
- **Fall back** on: PK tables (`HybridSnapshotLogSplit` — fluss-rust has no snapshot-bootstrap
  scanner; changelog-only reads exist in record mode but the Arrow surface rejects PK), lake
  union reads, bounded/batch mode, non-PLAIN auth / TLS, non-default ZSTD level, `INDEXED`
  log format, any Fluss type our boundary schema can't carry. Matcher must mirror the
  connector's schema `sanityCheck`.

## Known frictions

- **arrow-rs 57 (fluss-rust) vs 58 (ours)**: try bumping fluss-rust to 58 (upstream if it
  compiles clean); otherwise bridge zero-copy via Arrow C FFI inside the native lib
  (fluss-rust ships a sync `RecordBatchReader` FFI adapter built for this).
- fluss-rust spawns untracked fetch tasks (no cancel-on-drop) — verify clean task shutdown.
- Parity referee: harness comparing native output vs the stock Fluss connector on the same
  table (Testcontainers Fluss cluster), including a partition created mid-job (discovery) and
  a partition dropped mid-job (removal round trip), changelog kinds (+A only for log tables),
  and every startup mode.

## Sequencing

1. Bump/bridge arrow; JNI binding over `RecordBatchLogScanner` (model: the cpp binding's
   C ABI + Arrow C Data surface).
2. Source + split reader + planner matcher (`connector = 'fluss'`), fall-back gates, coverage
   doc section.
3. Parity harness incl. dynamic discovery/removal scenarios.
4. Ticket 36: Nexmark matrix rung on Fluss — the payoff measurement (native vs stock
   Flink-on-Fluss, honest perimeter).

Relates to: ticket 36 (the benchmark this enables), ticket 33 (the Kafka pattern being
mirrored), ticket 24 (columnar endpoints), ticket 37 (Fluss PK KV as a state-store candidate).
