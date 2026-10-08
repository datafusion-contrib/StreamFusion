# Fluss client settings audit

Audited against the released `fluss-flink-1.18:1.0.0` and
`fluss-flink-2.2:1.0.0` client/connector sources and the Apache Fluss checkout at
`64b3b4611`. StreamFusion admits only the released 1.0.0 implementation; the
checkout is reference material, not a runtime dependency. No Fluss source is
modified or forked.

## Admission rule

The Arrow transport delegates connection configuration to the released Java client.
The following explicit settings are admitted: `bootstrap.servers`, `client.id`,
`client.connect-timeout`, `client.request-timeout`, `netty.client.num-network-threads`,
`netty.client.allocator.heap-buffer-first`, the released `client.security.*` options
listed below, and `client.writer.dynamic-create-partition.enabled`. Explicit heap
preference selects the copying Arrow decoder instead of overriding that preference.
Allowed `netty.*` table options are restored into the Java configuration because the
stock table factory drops them.

Other explicit settings retain the stock endpoint, including defaults, arbitrary
filesystem settings and unknown keys. The source and sink are checked separately.
Diagnostics identify a key without its value, including credentials. A fallback does
not imply that stock Fluss accepts an invalid or arbitrary option.

`FlinkTableFactory.toFlussClientConfig` forwards all `client.*` table properties.
`FlinkCatalog.getTable` includes stored table properties and catalog security
properties in the resolved table options. Admission checks the resulting
`flussConfig` and resolved table options, then copies admitted connection settings to
the runtime configuration.
The factory drops `netty.*` table properties, so the raw table-options gate is
necessary even when the resulting SDK configuration contains only bootstrap
servers. Programmatically supplied network settings also decline acceleration.
Stock Fluss remains
responsible for validating a declined setting or rejecting an invalid combination.
A fallback is not a claim that stock Fluss accepts every arbitrary key.

The policy separates connection ownership from unverified data-plane policies. The audit adds registry-wide
regression coverage, precise diagnostics, a gate for factory-dropped settings, and a
missing table-statistics gate. It also replaces the Arrow writer's deterministic batch rotation with the released
`StickyBucketAssigner`, explicitly sends the default fetch read preference, and
acknowledges duplicate-sequence replies as already committed writes like the SDK.
Do not broaden the whitelist based solely on finding a `config.get(...)` call:
the full batching, routing, recovery and observability contract needs verification.

## Complete released client/network inventory

Keys marked **admitted** delegate to the released connection or the verified partition creator.
Every other key causes planning-time fallback when explicitly present.
The notes describe the unconfigured default path and why forwarding the setting to
the Java connection alone does not establish support. The test enumerates the
installed released `ConfigOptions` registry, so newly exposed options cannot escape
coverage. The 46 options below are supplemented by filesystem and unknown-key tests.

| Setting | Default-path audit |
| --- | --- |
| `netty.client.num-network-threads` | **Admitted.** Java RPC transport owns its event loop. |
| `netty.client.allocator.heap-buffer-first` | **Admitted.** Explicit heap reception is respected and uses the copying Arrow decoder. |
| `client.id` | **Admitted.** Java connection creates its own client identity and RPC metrics group. |
| `client.connect-timeout` | **Admitted.** Java RPC transport owns connection establishment. |
| `client.writer.buffer.memory-size` | Acknowledgement queue applies the default byte ceiling, plus a 64-batch ceiling and task memory accounting. |
| `client.writer.buffer.page-size` | Arrow/wire allocations replace SDK segment-page allocation; no page allocator setting is reproduced. |
| `client.writer.buffer.per-request-memory-size` | Arrow/wire allocations replace SDK segment reservations. |
| `client.writer.buffer.wait-timeout` | Default unbounded buffer wait is retained; a finite allocation-wait timeout is not implemented. |
| `client.writer.batch-size` | Slices Arrow roots toward the default batch target. This is a batching target, not a per-row wire-size guarantee. |
| `client.writer.dynamic-batch-size.enabled` | SDK adaptive segment allocation is replaced by Arrow-root slicing and bounded coalescing; explicit allocation policy stays stock. |
| `client.writer.batch-timeout` | Sealed batches are scheduled immediately; StreamFusion does not reproduce a configured SDK linger schedule. |
| `client.writer.bucket.no-key-assigner` | Uses the released sticky assigner at sealed Arrow-batch boundaries, with current cluster metadata; per-row round robin is not supported. |
| `client.writer.acks` | Requests require all in-sync replicas (`-1`); zero/leader-only acknowledgement modes are not implemented. |
| `client.writer.request-max-size` | Encoded batches and coalesced records are bounded by the default request-record limit; oversized single records fail. |
| `client.writer.retries` | Default transient retries retain the same encoded batches and sequences; configurable retry combinations stay stock. |
| `client.writer.enable-idempotence` | Writer IDs and per-bucket batch sequences deduplicate retries within a writer lifetime. Explicit enable/disable and conflicting-option validation stay stock. |
| `client.writer.max-inflight-requests-per-bucket` | One request per bucket is in flight, within the SDK default ceiling of five. Configurable concurrency stays stock. |
| `client.writer.dynamic-create-partition.enabled` | **Admitted.** The released dynamic partition creator handles enabled/disabled creation and auto-partition validation. |
| `client.writer.kv-backpressure.max-throttle` | KV writes remain stock; the Arrow sink is append-only. |
| `client.request-timeout` | **Admitted.** Java RPC transport and append requests use the SDK default timeout. Request timeout is forwarded to the connection and append RPC; writer retry policy remains outside admission. |
| `client.scanner.log.check-crc` | Complete unprojected batches are CRC-checked. Broker-projected replies skip CRC like the SDK, whose projection invalidates the original checksum. |
| `client.scanner.log.max-poll-records` | The reader emits one Arrow batch at a time, rather than a row polling API; it does not promise configurable row poll granularity. |
| `client.security.protocol` | **Admitted.** Authentication/handshake remain on the released Java transport; broker tests cover SASL/PLAIN with credentials and heap/direct reception. |
| `client.security.enable-plugin-discovery` | **Admitted.** Authentication/handshake remain on the released Java transport; broker tests cover SASL/PLAIN with credentials and heap/direct reception. |
| `client.scanner.log.fetch.max-bytes` | FetchLog sends the default total byte limit. |
| `client.scanner.log.fetch.max-bytes-for-bucket` | FetchLog sends the default per-bucket byte limit. |
| `client.scanner.log.fetch.wait-max-time` | FetchLog sends the default broker wait limit. |
| `client.scanner.log.fetch.min-bytes` | FetchLog sends the default minimum byte threshold. |
| `client.scanner.kv.fetch.max-bytes` | Snapshot/KV scans remain stock. |
| `client.scanner.kv.batch-strategy` | Snapshot/KV scans remain stock. |
| `client.lookup.queue-size` | Lookup joins remain stock; the scan substitution does not replace a lookup provider. |
| `client.lookup.max-batch-size` | Lookup joins remain stock; the scan substitution does not replace a lookup provider. |
| `client.lookup.max-inflight-requests` | Lookup joins remain stock; the scan substitution does not replace a lookup provider. |
| `client.lookup.batch-timeout` | Lookup joins remain stock; the scan substitution does not replace a lookup provider. |
| `client.lookup.max-retries` | Lookup joins remain stock; the scan substitution does not replace a lookup provider. |
| `client.scanner.remote-log.prefetch-num` | Uses the released downloader, requesting segments sequentially, within its default prefetch ceiling. |
| `client.scanner.log.read-preference` | FetchLog explicitly sends the default LOCAL_FIRST preference; alternative tier preferences stay stock. |
| `client.scanner.io.tmpdir` | Released remote downloader owns its default temporary files. |
| `client.remote-file.download-thread-num` | Released connection owns the remote file downloader. |
| `client.filesystem.security.token.renewal.backoff` | Released connection owns the security token manager; configured credentials/renewal policy stays stock. |
| `client.filesystem.security.token.renewal.time-ratio` | Released connection owns the security token manager; configured credentials/renewal policy stays stock. |
| `client.metrics.enabled` | Default is false. Native queues/Arrow decoding do not reproduce every SDK scanner/writer metric; enabled SDK metrics stay stock. |
| `client.security.sasl.mechanism` | **Admitted.** Authentication/handshake remain on the released Java transport; broker tests cover SASL/PLAIN with credentials and heap/direct reception. |
| `client.security.sasl.jaas.config` | **Admitted.** Authentication/handshake remain on the released Java transport; broker tests cover SASL/PLAIN with credentials and heap/direct reception. |
| `client.security.sasl.username` | **Admitted.** Authentication/handshake remain on the released Java transport; broker tests cover SASL/PLAIN with credentials and heap/direct reception. |
| `client.security.sasl.password` | **Admitted.** Authentication/handshake remain on the released Java transport; broker tests cover SASL/PLAIN with credentials and heap/direct reception. |

The newer Fluss checkout additionally defines
`client.scanner.remote-log.fetch.max-retries`. The 1.0.0 transport is not upgraded
implicitly: this key is also rejected by the blanket explicit-entry rule, and a
newer connector implementation version is declined altogether.

## Connector, table and planner contracts

| Configuration or ability | Disposition |
| --- | --- |
| `bootstrap.servers` | Forwarded unchanged; multiple bootstrap addresses remain supported. |
| `scan.startup.mode`, `scan.startup.timestamp` | Uses the factory's parsed startup options and SDK offsets initializers, including the configured SQL timezone. EARLIEST/LATEST/TIMESTAMP log reads are admitted; FULL is earliest for append tables. FULL primary-key snapshots fall back. |
| `scan.bounded.mode`, `scan.bounded.timestamp` | Uses the released stopping-offset initializer, boundedness and enumerator. Snapshot/hybrid combinations stay stock. |
| `scan.partition.discovery.interval` | Forwarded to the SDK enumerator. Native readers handle the released partition-removal event and acknowledge unsubscribed buckets. |
| `scan.split.assignment.batch-size` | Forwarded to the SDK enumerator, which pushes all assignment batches eagerly. |
| `scan.kv.snapshot.lease.id`, `scan.kv.snapshot.lease.duration` | SDK lease context is retained by the delegate. Snapshot reads remain stock; there is no native KV scan. |
| `lookup.async`, `lookup.insert-if-not-exists`, Flink `lookup.*` cache/async settings | Lookup nodes are never substituted by the Arrow scan rule. |
| `sink.ignore-delete` | Enabled mode falls back; the Arrow sink admits only insert-only input. |
| `sink.producer-id` | Explicit identity falls back; undo recovery is not implemented. |
| `sink.distribution-mode`, deprecated `sink.bucket-shuffle` | The factory resolves these into DistributionMode. AUTO/NONE on append tables with scalar bucket keys or sticky routing are admitted; BUCKET/PARTITION_DYNAMIC stay stock. |
| `bucket.num` | Uses each partition's broker metadata count, including mixed counts after rescaling the default. |
| `bucket.key`, primary keys, partition keys | Scalar bucket keys use native compacted-key hashing and stable Arrow grouping. Complex bucket keys and primary-key sinks stay stock. Partitioned append sinks use actual per-partition bucket counts; primary-key log sources preserve change types. |
| `auto-increment.fields`, `table.auto-increment.cache-size` | Auto-increment is an upstream primary-key write contract, which remains stock. |
| `table.log.format` | Source requires resolved ARROW; sink requires an explicitly resolved ARROW table. INDEXED/COMPACTED_LOG stay stock. |
| `table.log.arrow.compression.type`, `table.log.arrow.compression.zstd.level` | NONE/LZ4_FRAME/ZSTD encoding uses the current broker table compression settings; existing interop tests cover all three. |
| `table.statistics.columns` | **Native streaming writes emit V1 statistics.** Arrow column scans find min/max indexes and null counts; the released SDK serializes only the extrema. Each bucket/size split recomputes its own statistics. Sources can decode statistics-bearing V1/V2 batches; batch-filter pushdown is sent to the broker with the full schema ID. |
| `table.replication.factor` | Broker owns replication; native appends require all in-sync replica acknowledgements. |
| `table.log.ttl`, `table.log.tiered.local-segments`, `table.log.local-ttl` | Broker owns retention/tiering; reads retain offsets/high-watermarks and use the released remote downloader when directed to tiered logs. No selective remote column range reads are implemented. |
| `table.kv.format`, `table.kv.format-version`, `table.kv.value-layout-version`, `table.kv.standby-replica.enabled`, `table.kv.ttl`, `table.kv.ttl.time-column` | KV storage and expiry remain broker-owned; primary-key writes and KV snapshots stay stock. Live log reads consume the resulting broker changelog. |
| `table.changelog.image`, `table.delete.behavior` | Broker owns changelog generation/delete behavior. Read sidecar preserves emitted kinds; FULL/WAL primary-key log parity is tested. |
| `table.merge-engine`, `table.merge-engine.versioned.ver-column` | Merge-engine sources and sinks fall back, including aggregation undo recovery. |
| All `table.auto-partition.*` options | Released enumerator/partition creator own discovery, expiry and validation; native readers handle removal and native writers resolve actual partition bucket counts. |
| All `table.datalake.*` options | Lake/hybrid sources and lake sinks stay stock. Catalog-side lake creation/tiering is not replaced. |
| Flink source projection | Physical top-level and zero-column projections are supported. Nested SQL field access runs natively on the containing struct; Fluss 1.0 does not prune individual nested fields on the wire. Evolved schemas use full batches with local nullable trailing-column handling. |
| Flink filter/limit/count pushdowns | Partition pruning and statistics-based streaming log batch filtering are supported, with residual predicates retained. Point lookup, limit and count pushdowns stay stock. |
| Flink watermark pushdowns/options | Shared ScanWatermarkSpec admits verified periodic expressions and idle timeouts. Source watermarks, alignment and on-event emit fall back. |
| Flink sink materialization, row modifications, target-column/constraint abilities | Shared constraint gate and sink ability gate retain the stock sink when host enforcement is needed. |
| Checkpoint/recovery | Source uses SDK split/enumerator serializers; positions advance after successful collection, including offset-only progress across skipped batches. Sink flush waits for acknowledgements; restart delivery is at-least-once, as for the stock append connector, not transactional exactly-once. |

Server-side table settings must be resolved through the normal Fluss catalog. The writer reads
its statistics mapping and bucket keys from table metadata at startup, including statistics
enabled after planning. Later schema/statistics/key configuration changes require a job restart. New partitions
continue to resolve their own actual bucket counts, including runtime bucket-count changes.

## Verification and limits

`FlussTablesTest` enumerates admitted and excluded released client/network settings for append sources,
primary-key log sources and append sinks, plus explicit defaults, unknown keys,
filesystem options, bootstrap lists and table statistics. `FlussSqlTest` executes
configured consumer/producer fallback and statistics-enabled native writes against
the released broker and checks both plans and output. `FlussArrowClientTest` verifies
one- and four-bucket sticky batches, partition removal/recovery, filtered offset progress,
SASL/PLAIN authentication, heap/direct reception, SDK-compatible bucket placement and broker
pruning of native-produced statistics under NONE/LZ4/ZSTD compression.
Partitioned SQL tests cover mixed bucket counts, live partition discovery and projected
primary-key changelogs. The Arrow splitter tests composite names, stable row order,
timestamps and null-key rejection. These tests
and `FlussProduceResponseTest`'s duplicate acknowledgement/identity/fatal-error regressions
are included in the existing Java and Fluss SQL CI jobs on both supported Flink lines.

The optimization intentionally changes physical allocation, column-batch scheduling,
connection sharing and network teardown; it does not promise identical internal
SDK page allocation, row poll boundaries or linger behavior. Explicit settings for
those policies remain stock. Passing SQL parity and this inventory do not prove
arbitrary failure scenarios or future SDK compatibility. Expand supported settings
only with tests of their observable contract, including failure/recovery behavior.
The published Nexmark numbers predate this audit; this change does not re-label or
replace those measurements.
