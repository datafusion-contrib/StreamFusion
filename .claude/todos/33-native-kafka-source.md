# Fully native Kafka source (Rust consumer → Arrow, no JVM round-trip)

**Status:** in progress — **two of three layers built and tested; FLIP-27 `Source` wiring remains
(one design fork to settle).** A benchmark-grade native rdkafka consumer (`Native.benchmarkKafkaConsume`,
behind the `kafka-bench` cargo feature) was measured head-to-head against the shallow path
(`KafkaIngestBenchmark`): on 500k three-field JSON messages, **native ~2.7M msgs/s vs shallow ~531k =
5.08x** (local broker, so the gap is pure JVM-client + off-heap-copy overhead; decode is shared and
cancels). The 5x justifies building the real source.

## Delivered so far
- **Config translator (JVM, unit-tested).** `kafka.KafkaConfigTranslator`: Flink consumer `Properties`
  → librdkafka `Map<String,String>`, or a fallback reason. Pins Java defaults for the silent-divergence
  keys, renames/value-maps, parses PLAIN/SCRAM + Kerberos JAAS, falls back on JKS / unrecognized login
  modules / no-analog keys. 12 unit tests, no broker. (commit d22a790)
- **Native split reader (Rust, integration-tested).** `KafkaSplitReader` behind the `kafka` cargo
  feature (`kafka-bench` now implies it): an rdkafka `BaseConsumer` that manually `assign()`s + seeks a
  fixed set of (topic, partition) at explicit offsets (never `subscribe()`), polls payloads into an
  Arrow binary column, decodes to typed Arrow in Rust, and writes back per-split next-offsets each poll.
  FFI: `openKafkaConsumer` / `pollKafkaBatch` / `closeKafkaConsumer`. An opt-in Testcontainers IT
  (`NativeKafkaSourceTest`) drives open→poll→checkpoint→reopen-at-offset→resume over 5000 msgs and
  asserts **exactly-once across the simulated restore** (every id once, no gap/overlap). (commit c67771b)

## Remaining: the FLIP-27 `Source` wiring — and the design fork to settle first
`flink-connector-kafka:5.0.0-2.2` is resolvable and targets Flink 2.2.x (dependency added, `provided`).
Its `KafkaSource.createReader` (line 177) builds a `KafkaSourceReader` whose element type is hardcoded
`ConsumerRecord<byte[],byte[]>` and whose `RecordEmitter` updates **per-record** offset state. Emitting
Arrow batches instead means a fully custom `SourceReaderBase` stack (reader + `SplitFetcherManager` +
`SplitReader<ArrowBatch, KafkaPartitionSplit>` + emitter + split-state), reusing only the
`KafkaSourceEnumerator` + `KafkaPartitionSplit*Serializer` (coordination/state, not the hot path).

**The fork — how the native consumer maps onto a subtask's splits:**
- **(A) One consumer per partition-split.** Each `SplitReader` owns one rdkafka consumer for one
  partition; `fetch()` returns one Arrow batch + that split's next offset; the emitter sets the split
  state's offset to it. Simple and obviously correct (no cross-partition batch, offset-state is 1:1),
  but N consumers per subtask and N rdkafka instances.
- **(B) Multiplex all of a subtask's partitions into one consumer** (what Flink does). Needs native
  *incremental* assign/seek on `handleSplitsChanges` and per-partition batch splitting so the emitter
  can update each split's offset state. Most efficient, but more native surface and the batch↔per-record
  -state impedance to handle carefully.

This intersects the repo principle of *not transposing at edges* (the native reader already produces
Arrow directly — good); the open question is purely consumer↔split cardinality.

## Relationship to the shallow path (ticket 32) and the deciding benchmark
The shallow path (Flink's `KafkaSource` + byte-passthrough deserializer → row→Arrow transpose →
native format-decode operator) is the **fallback**: Flink owns the consumer, offsets, checkpointing,
and — critically — all the SASL/SSL/auth config, so it works everywhere. The native path replaces
only the fetch+decode with rdkafka, reusing Flink's `KafkaSourceEnumerator` + `KafkaPartitionSplit` +
checkpointed offsets, and **degrades to the shallow path** for any source whose consumer settings it
can't faithfully translate (see the config-fidelity checklist below). The native path's whole
justification is throughput over the shallow path; **benchmark both on the same topic first** (the
heap→off-heap copy + JVM Kafka-client overhead + GC it removes vs the rdkafka fetch it adds) before
committing to the config-translation work.

## Config-parity plan: translate Flink's consumer `Properties` → librdkafka
Flink hands the reader a `Properties` (user `setProperties` + Flink's forced overrides). The native
consumer must produce *identical* behavior. A **JVM-side translator** converts it to a librdkafka
config map (the native side just applies the map) — the JVM is where the inputs live: the raw
`Properties`, the JAAS string, the `KeyStore` API for cert conversion, and the `OffsetsInitializer`.
Anything it can't faithfully translate routes that table to the shallow fallback (ticket 32) with a
logged reason — we never *silently* mis-translate. Cross-referenced against
`~/data/kafka` (`ConsumerConfig`/`CommonClientConfigs`/`SaslConfigs`/`SslConfigs`) and
`librdkafka 2.x CONFIGURATION.md`. Four things to get right, ordered by how easy they are to get wrong:

**(1) Default divergence — the subtle trap.** Several keys share a *name* but have *different
defaults*, so a user relying on the Java default diverges silently if librdkafka uses its own. The
translator pins Java's default for any such key the user left unset:

| key (same name both sides) | Java default | librdkafka default | risk if unfixed |
|---|---|---|---|
| `isolation.level` | `read_uncommitted` | `read_committed` | **native hides uncommitted records / wrong EOS read** |
| `check.crcs` | `true` | `false` | native skips corruption checks |
| `allow.auto.create.topics` | `true` | `false` | consumer-side topic auto-create differs |
| `connections.max.idle.ms` | `540000` | `0` (disabled) | connection lifecycle differs |
| `metadata.max.age.ms` | `300000` | `900000` | metadata refresh cadence |
| `socket.connection.setup.timeout.ms` | `10000` | `30000` | connect-failure timing |
| `reconnect.backoff{,.max}.ms` | `50`/`1000` | `100`/`10000` | reconnect cadence |
| `send`/`receive.buffer.bytes` | `131072`/`65536` | `0` (OS) | socket buffer sizes |

**(2) Name / value translation.**
- **1:1 (copy as-is):** `bootstrap.servers`, `group.id`, `group.instance.id`, `client.id`,
  `client.rack`, `enable.auto.commit`, `fetch.min.bytes`, `fetch.max.bytes`, `max.partition.fetch.bytes`,
  `max.poll.interval.ms`, `session.timeout.ms`, `heartbeat.interval.ms`, `request.timeout.ms`,
  `retry.backoff{,.max}.ms`, `metadata.max.age.ms`, `security.protocol`,
  `sasl.kerberos.{service.name,kinit.cmd,min.time.before.relogin}`, `ssl.{key,keystore}.password`,
  `ssl.cipher.suites`, `ssl.endpoint.identification.algorithm`.
- **Renamed:** `fetch.max.wait.ms` → `fetch.wait.max.ms`; `sasl.mechanism` → `sasl.mechanisms`.
- **Value-mapped:** `auto.offset.reset` (`earliest`→`smallest`, `latest`→`largest`, `none`→`error`;
  **`by_duration:…` has no analog → fall back**). `partition.assignment.strategy` is irrelevant — we
  use manual `assign()`.

**(3) Hard gaps — translate JVM-side, else fall back.**
- **`sasl.jaas.config`** (a JAAS string; librdkafka has no JAAS): parse it — `PlainLoginModule`/SCRAM →
  `sasl.username`+`sasl.password`; `Krb5LoginModule` (keyTab/principal/serviceName) →
  `sasl.kerberos.{keytab,principal,service.name}`. A custom/unrecognized `LoginModule` → **fall back**.
- **JKS/PKCS12 `ssl.truststore.location`/`ssl.keystore.location`** (librdkafka wants PEM): read via
  `KeyStore`, write temp PEM → `ssl.ca.location` (CA) + `ssl.certificate.location`/`ssl.key.location`
  (client). `ssl.truststore.type=PEM` maps directly. Conversion failure/unsupported store → **fall back**.
- **No librdkafka equivalent** → fall back if set to a non-default: `ssl.protocol`,
  `ssl.enabled.protocols`, `ssl.{key,trust}manager.algorithm` (JSSE-specific), `exclude.internal.topics`.
- **`max.poll.records`** (Java-only, no analog): honor it as our native batch cap, or ignore (document).

**(4) Flink forced overrides — replicate exactly.** `enable.auto.commit=false` (offsets live in
checkpoints), `auto.offset.reset` = the `OffsetsInitializer` strategy (mapped per (2)), `client.id` =
prefix+subtask, partition discovery handled by the enumerator (not the consumer), byte deserializers a
no-op natively. The model: **manual `assign()` + `seek()` to the split's checkpointed offset, never
`subscribe()`/group rebalance** (the group id is only for committed-offset reads).

**Architecture.** The translator is JVM-side and emits a flat `Map<String,String>` of librdkafka keys
(including temp PEM paths) — or a `cannot-translate: <key/reason>` that routes the table to the shallow
fallback (logged like the expression-layer `fallbackReasons`). Native receives the ready map and
applies it to rdkafka `ClientConfig`; it stays a dumb applier, and JAAS-parsing / JKS-conversion live
in the JVM where the libraries are. **Test against the real Kerberos cluster** (CLAUDE.md keytab) —
SASL/SSL is where a silent divergence bites.

## Why this is its own ticket (and not just "finish ticket 32")
Ticket 32 keeps Flink's `KafkaSource` and accepts the one off-heap copy — cheap relative to the
`RowData` materialization it removes, and it reuses all of Flink's hard-won connector semantics. A
native source throws that reuse away and must **reimplement the parts that make Kafka correct**:

- **Split (partition) discovery and assignment** across parallel subtasks, with dynamic rebalancing
  as partitions/topics appear.
- **Offset management** — committed offsets, `auto.offset.reset` semantics, starting-offset modes
  (earliest/latest/timestamp/specific).
- **Checkpoint / restore integration** — offsets must be part of Flink's checkpoint so exactly-once
  (or at-least-once) holds across failure; the FLIP-27 `SourceReader`/`SplitEnumerator` state model.
- **Watermark generation** per split, with idleness, aligned to Flink's event-time model.
- **Consumer group coordination, TLS/SASL auth, schema-registry fetch** for the wire formats.

This is exactly the surface FLIP-27 `KafkaSource` + the Kafka client already cover. Reimplementing it
is justified only by the zero-copy win, so it is deliberately back-burner until the decode-operator
path (ticket 32) has proven the throughput ceiling and shown the copy is the next bottleneck.

## Reference-first (per repo CLAUDE.md)
- **Arroyo** (`~/data/arroyo`) already has a native Rust Kafka source feeding DataFusion — this is the
  primary thing to rip out and adapt, not reinvent. Map its source reader, offset/checkpoint model,
  and how it emits Arrow batches before designing ours. Record any deviation in `divergences/`.
- Rust Kafka clients: `rdkafka` (librdkafka bindings — what Arroyo uses) vs a pure-Rust client.
- Decode reuses ticket 32's native decoders (`arrow-json`/`arrow-avro`/…), so this ticket is
  "native *consumption* → Arrow", layered on the decode primitives already built.

## Shape (rough)
- A FLIP-27 `Source<ArrowBatch, KafkaSplit, EnumState>` whose `SourceReader` runs the Rust consumer
  over JNI: native `poll` returns Arrow batches (decoded in Rust), the Java reader forwards offsets to
  the checkpoint and emits `ArrowBatch` downstream. The `SplitEnumerator` may stay on the JVM (it is
  coordination, not hot path) while the per-split fetch+decode is native.
- Alternatively, a thinner step: a custom `KafkaRecordDeserializationSchema` that hands raw bytes to
  Rust in bulk (ticket 32) is the bridge; this ticket is the larger "own the consumer" move.
- Offsets are the checkpointed state; the native reader is restored to them, like the Parquet
  source's file cursor but with Kafka's commit semantics.

## Parity / correctness
- Byte/row-identical output to Flink's `KafkaSource` + stock format over the same topic/offsets.
- Exactly-once across a checkpoint/restore cycle (kill-and-recover test): no duplicates, no loss.
- Watermark/event-time behavior matches Flink's per-partition watermarking incl. idleness.
- A vs-Flink ingest throughput number (the payoff: zero-copy vs the ticket-32 one-copy path).

## Dependencies
- Build ticket 32 first (native decoders + the off-heap decode-operator path). This ticket only
  becomes worthwhile once that path is in and the copy is measurably the bottleneck.
