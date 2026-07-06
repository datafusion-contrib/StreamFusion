# Fluss native source: feasibility findings (2026-07-05)

Investigated `~/data/fluss` (Apache Fluss, `main`) and `~/data/fluss-rust` (the Rust client,
`main`, workspace v0.2.0, targets Fluss server 0.8.0+) to answer: can we build a native Fluss
**log source** shaped like the native Kafka source — Flink-side enumerator reused verbatim,
per-subtask reader swapped for a JNI/Rust one — with dynamic partition discovery? **Yes**, and
the fit is better than Kafka's was: Fluss's log is Arrow on the wire, so the native reader can
emit Arrow batches with no decode and no transpose. The Rust client was recently adopted into
the Fluss monorepo (FIP-40), so it is the blessed native client; the standalone repo is ahead
of the monorepo copy (PK-changelog subscribe, Fluss 1.x admin protocol) — track standalone.

## The Kafka pattern maps one-to-one

All connector logic is in `fluss-flink/fluss-flink-common` (version modules are shims;
`fluss-flink-2.2` matches our Flink 2.2.1). The reusable JobManager side is public and
directly instantiable, like Flink's `KafkaSourceEnumerator`:

- `FlinkSource<OUT>` (`source/FlinkSource.java`) — public ctors; `createReader` is the one
  method to override. `FlussSource` (DataStream builder wrapper) has package-private ctors.
- `FlinkSourceEnumerator` (`source/enumerator/FlinkSourceEnumerator.java`) — public ctors,
  takes a Fluss `Configuration` and builds its own `Connection`/`Admin` in `start()`.
- `SourceSplitSerializer` (pass `null` LakeSource) and `SourceEnumeratorState` +
  `FlussSourceEnumeratorStateSerializer` (v3) — public.
- Split types: `LogSplit` = one `TableBucket` (`tableId`, nullable `partitionId`, `bucket`) +
  `startingOffset`/`stoppingOffset`; `HybridSnapshotLogSplit` for PK tables (snapshot + log).
  Offsets are per-bucket sequential longs, exactly like Kafka partitions; `EARLIEST = -2`.
- The class to replace is `FlinkSourceSplitReader` (`source/reader/FlinkSourceSplitReader.java`)
  — one `LogScanner` per subtask multiplexing all buckets, `subscribe(partitionId, bucket,
  offset)` on `SplitsAddition`, standard `SingleThreadMultiplexSourceReaderBase` above it.

## Dynamic partition discovery: built into the enumerator we'd reuse

- Periodic loop in `FlinkSourceEnumerator.start()`: `listPartitionInfos` on a worker thread
  every `scan.partition.discovery.interval` (**default 1 min**; non-positive disables; javadoc
  warns small intervals hammer ZooKeeper), diffed against assigned ∪ pending partitions.
- FLIP-288 semantics: first-round partitions use the user's `OffsetsInitializer`; partitions
  discovered later always start EARLIEST (`initialDiscoveryFinished` flag, persisted in state).
- Non-partitioned tables: buckets are static (`0..numBuckets-1`), splits generated once.
- **Partition REMOVAL exists (no Kafka analog)**: dropped partitions → enumerator broadcasts
  `PartitionsRemovedEvent` → reader unsubscribes those buckets and acks with
  `PartitionBucketsUnsubscribedEvent` → enumerator prunes its assigned state. A native reader
  must honor this round trip (the fetcher manager routes it via `removePartitions`).

## fluss-rust can play the librdkafka role

`crates/fluss` (`fluss-rs`), tokio-based, arrow-rs **v57** (we're on 58 — see risks):

- **`RecordBatchLogScanner`** (`client/table/scanner.rs`) polls `Vec<ScanBatch>` where
  `ScanBatch` wraps a real `arrow::RecordBatch` + `TableBucket` + base/last offset — the
  columnar jackpot. Log tables only; `INDEXED` log format rejected (ARROW only).
- Subscribe model matches split assignment exactly: `subscribe(bucket, offset)`,
  `subscribe_partition(partition_id, bucket, offset)`, `_buckets` batch variants,
  `unsubscribe[_partition]`. Subscriptions live behind an `RwLock` map **separate from the
  poll path, so mid-job incremental subscribe works while polling** — the property dynamic
  discovery needs.
- Offsets: seek = the offset passed to subscribe (`-2` earliest); admin `list_offsets` /
  `list_partition_offsets` with `Earliest | Latest | Timestamp(ms)` — covers Fluss startup
  modes (`full` == earliest for log tables).
- Admin: `list_partition_infos` (what the Java enumerator's discovery calls — but discovery
  stays on the JM in reused Java code; the native side never needs it).
- Projection pushdown: `TableScan::project(&[usize])` — server-side for Arrow log tables.
  **Not applied on remote/tiered reads** (full schema decoded, then pruned client-side).
- Tiered storage read works: `FetchLog` responses pointing at remote segments are downloaded
  via OpenDAL (S3/OSS features) and stitched into the same fetch buffer transparently.
- Compression: ZSTD (default level 3 only — non-default levels error, upstream issue #109)
  and LZ4_FRAME.
- PK tables: record-mode `LogScanner` reads the changelog with `ChangeType` per record
  (+A/+I/-U/+U/-D); the Arrow batch surface rejects PK tables (no change-type slot). **No
  snapshot-bootstrap scanner exists** (only KV-snapshot admin/lease RPCs) — so Flink's
  `HybridSnapshotLogSplit` path cannot be served natively today.
- Auth: SASL/PLAIN only, **no TLS**. Bindings exist for python/cpp/elixir — no JNI; the cpp
  binding (C ABI + Arrow C Data) is the model to follow.

## What the Java path pays that we would not

The Java connector decodes the Arrow batch client-side into `ColumnarRow`s, then converts
**per record** to `GenericRowData` (`FlussRowToFlinkRowConverter`) before Flink even sees it.
A native reader hands the already-Arrow batch across the C Data Interface untouched — no
per-record materialization, no RowData→Arrow transpose at the island edge. This is the first
source where the ingest perimeter cost can genuinely go to ~zero (ticket 36's hypothesis).

## Risks / frictions

- **arrow-rs 57 vs our 58**: two majors can't share types. Either bump fluss-rust to 58 (try
  first; likely upstreamable) or bridge zero-copy through the version-stable Arrow C FFI
  inside the native lib (fluss-rust already ships a sync `RecordBatchReader` FFI adapter).
- **Type/schema parity**: Fluss Arrow types (Decimal128, Timestamp/LTZ, nested List/Map/
  Struct...) must map onto our boundary schema exactly; the connector's `sanityCheck`
  validates Flink-vs-Fluss schema agreement — mirror it in the matcher.
- Watermarks: connector uses standard per-split machinery (no alignment/pause support) — same
  pattern we already replicate for Kafka.
- Fetch tasks in fluss-rust are spawned untracked (not cancelled on drop) — check shutdown
  behavior when a TM task closes mid-fetch.
- Non-default ZSTD level tables fail to decode (gate on it, or fix upstream).

See `.claude/todos/44-native-fluss-log-source.md` for the build plan.
