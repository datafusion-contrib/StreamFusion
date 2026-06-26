# Fully native Kafka source (Rust consumer → Arrow, no JVM round-trip)

**Status:** BUILT (v1, green end-to-end) but **SHELVED in favor of the shallow path (2026-06-25).** The
native rdkafka consumer works — planner → FLIP-27 source → consume+decode in Rust → Arrow, with
checkpointed exactly-once across restore — but the fair re-benchmark (see "Throughput investigation"
below) showed the native *consumer* carries a structural per-message malloc/free tax that the JVM Kafka
client avoids via bulk GC, so it only wins when decode is expensive (JSON) and loses when decode is
cheap (Avro). **Decision: the shallow path — consume in the JVM, hand raw bytes to Rust, decode to Arrow
there (the shared `MessageDecoder`) — is the chosen production approach.** It is simpler (Flink owns the
consumer, offsets, checkpointing, all SASL/SSL/auth), portable, and ties-or-beats native on both formats
once you strip the pipelining overhead. The native consumer stays in-tree behind the `kafka` feature as
a **long-term TODO** to revisit if/when raw ingest throughput becomes the proven bottleneck (and ideally
against a multi-broker cluster, where librdkafka's parallel per-broker fetch — untested here — could
change the verdict).

**LONG-TERM TODO (native consumer, deferred):** revisit only if raw Kafka ingest is the measured
bottleneck. Open levers from the investigation: (a) the per-message `rd_kafka_message_destroy` malloc/
free is ~45% of the serial consume thread and unaddressable from Rust (it's librdkafka's C `free`; a
Rust `#[global_allocator]` does not intercept it — confirmed); a process `malloc` interposer
(DYLD/jemalloc) is not shippable. (b) multi-broker parallel fetch is untested. (c) the decode thread
should be adaptive (inline by default, spill to a thread only under backpressure).

## Throughput investigation — consume + decode profiling (2026-06-25)

Re-ran the benchmark with a **fair methodology** (the earlier "5.08x" compared native→Arrow against
Flink→RowData — not apples-to-apples): same plain-JVM harness, both paths terminate at **Arrow batches
counted in Rust** (no per-batch JVM export), the **same `MessageDecoder`** on both sides, and the
**exact production librdkafka config** (`KafkaConfigTranslator` output + the throughput tuning
`KafkaTables` folds in). 10M three-field messages (avg 52.7 B), single partition, local cp-kafka.

Two consume models for the native path: **pipelined** (the production split reader — rdkafka poll on
one thread, decode on a background thread via an mpsc channel) vs **serial** (rdkafka poll + decode
inline on one thread, no handoff). Shallow = Flink's Java consumer → bytes → native decode (serial).

| Path | JSON (expensive decode) | Avro (cheap decode) |
|---|---|---|
| shallow (Java consume → native decode) | 4.61s / 2.17M/s | 2.82s / 3.54M/s |
| native **pipelined** (poll ‖ decode thread) | **3.94s / 2.54M/s** | 3.67s / 2.72M/s |
| native **serial** (poll + inline decode) | 4.15s / 2.41M/s | **2.82s / 3.55M/s** |

**Findings (all profiled with macOS `sample` on frame-pointer release builds; librdkafka v2.3.0, the
exact bundled source at rust-rdkafka 0.36.2's submodule pin):**

1. **Raw consume (no decode) is the floor: native ~3.2M/s vs Java ~4.5–5.9M/s.** Java's
   `ConsumerRecords.count()` does *zero* per-message work (sums batch counts); librdkafka *materializes
   and frees a message struct per record*. Both consume threads spend ~57% waiting on the broker
   (`cnd_timedwait` / `kevent`) and ~37% on-CPU — the difference is *what* the CPU does: Java =
   TLAB bump-alloc + near-free young GC (GC threads ~99% parked); native = `malloc` per message (broker
   thread) + `free` per message (consume thread), the structural tax of librdkafka's C API.
2. **Multi-partition does NOT help on a single broker.** Java scales with partitions (4.5M→5.9M, bigger
   fetches); native stays flat ~3.2M and the gap *widens*. One broker = one fetch thread = no parallel
   fetch; native's single merged consumer queue + per-message destroy caps it. (Parallel-broker-thread
   scaling needs a multi-broker cluster — untested.)
3. **There is no single-threaded librdkafka mode** (confirmed in `CONFIGURATION.md`): the broker I/O
   thread + coordinator thread are architectural. And it wouldn't help — the socket `poll()` wait is the
   same regardless of which thread does it, and the threaded model *overlaps* fetch with processing.
4. **The "native loses on Avro (0.77x)" was the pipelining handoff — NOT arrow-avro and NOT consume.**
   In the native Avro profile the **decode thread is 81.5% idle** (starved, parked in `mpsc::recv`);
   arrow-avro decodes 10M rows in a sliver of CPU. The pipelined path's per-poll overhead (partition
   bucketing, building the body `RecordBatch` on the consume thread, two channel hops, decode-thread
   wakeup) costs *more* than the cheap Avro decode it overlaps. **Strip the decode thread → serial native
   Avro ties shallow exactly (2.82s).** Pipelining only pays when decode is expensive enough to hide
   behind: JSON pipelined beats serial by 5% and shallow by 17%; Avro serial beats pipelined by 30%.
5. **Serial native consume thread budget (Avro, the format where it ties shallow):**
   - **44.6% per-message `rd_kafka_message_destroy`** (`free` → `free_tiny`; ~5% of the thread is the
     allocator returning pages to the kernel via `madvise`/`mach_vm_deallocate`). The dominant cost.
   - 17.8% idle `cnd_timedwait` (broker delivery wait — structural, single partition).
   - 19.9% inline decode (arrow-avro) · 8.6% dequeue machinery (locks/`message_get`/interceptors) ·
     7.9% copy into builder + `RecordBatch` build + count.

**Implications / candidate work:**
- **Make the decode thread adaptive/opt-in.** It's a *conditional* optimization: a net win for
  expensive decoders (JSON), a 30% loss for cheap ones (Avro). Default to inline; spill to the decode
  thread only under backpressure (decode can't keep up with poll). A single serial path already
  ties-or-beats shallow on *both* formats; pipelined only wins JSON.
- **Attack the per-message destroy (45% of the serial thread).** It's librdkafka's malloc/free-per-
  message — the one structural tax Java avoids via bulk GC. A retaining global allocator
  (mimalloc/jemalloc) would kill the `madvise`/`vm_deallocate` syscalls (~5%) and speed the rest of the
  free path. No batch-destroy API exists in librdkafka.
- **The raw-count comparison flatters Java** (it does no per-record work). With real downstream work the
  malloc/free delta and handoff latency become noise — the JSON case (real decode) is where native wins.

Benchmark legs live in `KafkaIngestBenchmark` (shallow/pipelined/serial × JSON/Avro; `SF_KAFKA_PROFILE`
modes for sampling each thread; `SF_KAFKA_PARTITIONS` for the partition sweep).

**Linking:** rdkafka uses the **bundled static librdkafka** (rdkafka-sys mklove build — needs only a C
toolchain + make, no system librdkafka, no cmake, no pkg-config), so any dev who clones can build the
`kafka`/`kafka-bench` features. (Earlier dynamic-linking against a brew librdkafka + stub .pc files is
gone.) The default build still pulls in no Kafka code at all.

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

## FLIP-27 `Source` wiring (built — design **(B) multiplex**, the chosen fork)
`flink-connector-kafka:5.0.0-2.2` (targets Flink 2.2.x, `provided`) supplies the enumerator + split /
enum-state serializers, reused verbatim. The element type is `NativeKafkaRecord` (Arrow batch + next
offset), so emitting Arrow needs a custom `SourceReaderBase` stack — built on the
`SingleThreadMultiplexSourceReaderBase(Supplier<SplitReader>, …)` constructor (no custom fetcher
manager needed):
- `NativeKafkaSplitReader` — **one rdkafka consumer per subtask** (B). `handleSplitsChanges` →
  `assignKafkaSplits` (full re-`assign()` of the TPL — ground-truthed against rust-rdkafka 0.36.2:
  `rd_kafka_assign` is the static-assignment replace; `incremental_assign` is the cooperative path we
  don't use). Each `fetch()` polls once, **buckets by partition**, decodes one Arrow batch per
  partition, and emits per-split records so each split's offset state advances independently.
- Offset markers: Flink's enumerator hands `EARLIEST_OFFSET=-2 / LATEST=-1 / COMMITTED=-3` (not concrete
  offsets) for the reader to resolve; the native side maps them to rdkafka `Offset::Beginning / End /
  Stored` (a raw `Offset::Offset(-2)` is rejected by librdkafka — the bug that proved this).
- Poll errors (e.g. transient `AllBrokersDown` while connecting) are logged-and-continued, matching the
  rdkafka examples (librdkafka reconnects internally); never panic across the FFI.
- Planner: `KafkaTables` maps a `connector='kafka'`, JSON-value, explicit-topic, supported-startup-mode
  table whose consumer config **translates** → `StreamPhysicalNativeKafkaSource` (ColumnarOutput) →
  `NativeKafkaSourceExecNode` (`env.fromSource`). Untranslatable/unsupported tables aren't substituted,
  so Flink's own Kafka source runs (the fallback). Gated by `NativeConfig.operatorEnabled("kafkaSource")`.

## Follow-ups (hardening, not core path)
- **Kerberos/SSL at runtime — DEFERRED, not needed for our cluster.** Our Kafka is PLAINTEXT (Kerberos
  is only for HDFS), so the bundled librdkafka being built without SSL/SASL is fine. If a SASL/SSL
  cluster ever matters, add the `ssl` + `gssapi-vendored` rdkafka features (heavier build) so the
  translator's SASL output is honored; until then SASL tables translate but can't connect natively.
- **Watermarks/event-time.** The source emits with `noWatermarks()`; per-partition watermarking +
  idleness (matching Flink's model) is not wired yet.
- **Specific-offsets / topic-pattern startup**, and `key.format`/multi-format tables → currently fall back.
- **wakeUp()** is a no-op (bounded poll timeout makes it unnecessary); revisit if poll timeouts grow.

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
