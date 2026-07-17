# Plan: Native exactly-once Kafka producer (native data plane, Java commit plane)

Status: design and local-source audit complete 2026-07-17; implementation not started.

Research snapshots:

- Flink runtime `c404d7b08e4`, especially Sink V2 writer/committer checkpoint ordering.
- flink-connector-kafka `2960af0e` (v5.0.0), built with kafka-clients 4.2.0.
- rust-rdkafka `5ca6ff5d` (0.36.2) and its checked-out librdkafka 2.3.0 submodule.
- standalone librdkafka `95a542c8` (2.3.0).
- **Build authority:** StreamFusion's `native/Cargo.lock` actually resolves rust-rdkafka 0.36.2,
  `rdkafka-sys 4.10.0+2.12.1`, and bundled librdkafka 2.12.1. The 2.12.1 source under Cargo's
  registry is authoritative where it differs from either local 2.3.0 checkout.
- Kafka 4.2.0 `ProducerConfig.java` from the connector's Maven source artifact, plus the local Kafka
  checkout for coordinator protocol behavior.

All source references below are repository-relative; the local research roots are `~/data/flink`,
`~/data/flink-connector-kafka`, `~/data/rust-rdkafka`, and `~/data/librdkafka`.

## Goal

Replace the Java record-production path of the Kafka sink with a native rust-rdkafka transactional
producer while preserving Flink's exactly-once contract and Kafka producer configuration behavior.
Rust owns serialization, partition selection, buffering, delivery callbacks, transaction initialization,
record production, and barrier flush/precommit. Java retains only Flink checkpoint coordination,
checkpointed writer/committable state, recovery cleanup, and the final `EndTxn(commit)` after checkpoint
completion.

Today's native Kafka sink is encode-only: native code serializes key/value bytes and hands them to
Flink's unmodified `KafkaSink` (`NativeKafkaSinkExecNode`, `NativeKafkaJsonSerializationOperator`), so
the Java producer remains the data-plane hot path.

## The load-bearing cross-client contract

librdkafka cannot resume an externally prepared transaction, but it does not need to. A Kafka
transaction is committed with `(transactionalId, producerId, epoch)`:

- Flink's `KafkaCommittable` serializes those values, and `KafkaCommitter` builds a fresh Java producer,
  reflectively injects producer ID/epoch through `FlinkKafkaInternalProducer.resumeTransaction`, and
  sends `EndTxn`. The committer does not need the writer's Java producer instance.
- The broker validates `EndTxn` against the tuple in transaction coordinator state; there is no
  connection or client-implementation binding. A transaction opened and flushed by librdkafka can
  therefore be committed by kafka-clients.
- librdkafka 2.12.1 reports `producer_id` and `producer_epoch` in EOS statistics, and rust-rdkafka maps
  them to `Statistics.eos`. It does **not** expose a stable direct getter. Periodic statistics are useful
  diagnostics, but correctness should not depend on waiting for a timer-driven callback. Phase 0 must
  either add a small tested rust-rdkafka/librdkafka accessor or prove a forced statistics snapshot is
  reliable; a direct upstreamable accessor is preferred.
- Destroying librdkafka with an open, flushed transaction does not commit or abort it. The broker keeps
  it ONGOING until the Java committer commits it, recovery fences it, or it times out.

So: **Rust writes and flushes; Java is the Flink-coordinated commit courier.** “Precommit in Rust” means
all delivery results have succeeded and the producer identity is captured. It does not introduce a new
Kafka protocol operation between flush and Java's final `EndTxn`.

Rejected alternatives:

- KIP-939 prepared-transaction handoff is not available in the broker/client versions in scope.
- Arroyo's in-memory pending-commit producer loses the recovery case; we must persist a real Flink
  committable instead.
- RisingWave's Kafka sink is at-least-once and does not supply a transactional recovery design.
- A custom Rust `EndTxn` implementation would duplicate Flink's commit/retry/recovery integration. It
  can be revisited only if the Java commit courier becomes a measured bottleneck.

## Flink lifecycle contract to preserve

The contract is defined jointly by the Flink runtime and flink-connector-kafka:

1. `SinkWriterOperator.prepareSnapshotPreBarrier` calls `writer.flush(false)` and then emits
   committables before the barrier. `snapshotState` subsequently snapshots writer state.
2. `ExactlyOnceKafkaWriter.prepareCommit` emits a committable only for a transaction containing records,
   after flush/precommit; empty transactions are recycled without a committable.
3. The writer snapshots ownership and pending transaction information and begins the next checkpoint's
   transaction. Transaction IDs use `prefix-subtaskId-checkpointOffset`.
4. `CommitterOperator` durably snapshots received committables and commits them only from
   `notifyCheckpointComplete`. On restore it retries recovered completed-checkpoint committables before
   accepting normal progress.
5. `KafkaCommitter` retries retriable errors and preserves Flink's classifications for fenced, invalid,
   timed-out, and unknown producer transactions. Its backchannel tells the writer which IDs completed.
6. Restore abort probing uses `initTransactions` on transaction IDs that are owned but not recovered as
   completed committables. This fences/aborts Rust-opened transactions just as it does Java-opened ones.

The native writer must plug into these exact callbacks; it must not independently decide when a
checkpoint is complete.

## Producer-property parity contract

`KafkaSinkTranslator` currently collects all `properties.*` options and passes them to
`KafkaSinkBuilder`. The builder then supplies ByteArray serializers and a one-hour
`transaction.timeout.ms` default. A native planner cannot translate only the raw table options because
that would miss Flink's normalized defaults.

Introduce a producer-specific `KafkaProducerConfigTranslator`. It may share security helpers with the
consumer `KafkaConfigTranslator`, but producer mapping, defaults, and fallbacks must be independent.
The translator produces two views from one normalized input:

- **Java commit properties:** the complete normalized kafka-clients `Properties`, including bootstrap,
  security, timeouts, and retry settings. These stay in the Java sink/committer because the commit
  producer must authenticate, find the coordinator, and send `EndTxn` with the same policy.
- **Native producer envelope:** a versioned UTF-8 key/value map containing validated librdkafka settings
  plus explicit runtime-owned settings. It crosses JNI once when each one-shot producer is created.
  Passwords and key material must never appear in explain output, logs, committables, or checkpoint
  state.

Do not let Java and Rust independently infer defaults. Normalize kafka-clients/Flink defaults in Java,
translate them explicitly, then have Rust ask librdkafka to validate the effective configuration at
producer construction.

### Initial mapping matrix

| Java/Flink property | Native treatment | Parity requirement |
|---|---|---|
| `bootstrap.servers`, `client.id`, `client.rack` | same-name pass-through | Keep full bootstrap/auth connectivity on both sides. |
| `acks`, `retries`, `retry.backoff.ms`, `retry.backoff.max.ms` | supported aliases/same names | Preserve explicit values; librdkafka must reject the same incompatible exactly-once combinations. |
| `compression.type` | librdkafka alias to `compression.codec` | Verify requested codec is compiled into the static build. |
| codec-specific compression levels | translate the selected codec's level to `compression.level` | Fall back for conflicting/multiple explicit level controls. |
| `linger.ms` | supported alias | Explicitly pin Kafka 4.2's 5 ms default. |
| `batch.size` | same name | Explicitly pin Kafka 4.2's 16,384-byte default; librdkafka otherwise uses 1,000,000. |
| `delivery.timeout.ms` | alias to `message.timeout.ms` | Explicitly pin 120,000 ms. With a transactional ID librdkafka otherwise expands it to the one-hour transaction timeout. |
| `request.timeout.ms` | same name | Pin 30,000 ms and test timeout classification. |
| `transaction.timeout.ms` | same name | Apply Flink builder's one-hour default before translation; preserve explicit override. |
| `max.in.flight.requests.per.connection` | same name | Pin Kafka's 5; exactly-once rejects values above 5. |
| `enable.idempotence` | same name | Transactional mode requires true. Do not silently override an explicit false; fail like kafka-clients. |
| `send.buffer.bytes`, `receive.buffer.bytes` | `socket.send.buffer.bytes`, `socket.receive.buffer.bytes` | Pin Kafka defaults (128 KiB/32 KiB); translate Java `-1` to librdkafka's OS-default sentinel `0`. |
| reconnect backoffs, `metadata.max.age.ms`, `connections.max.idle.ms` | same-name pass-through | Pin Kafka defaults because librdkafka defaults differ; validate range/type differences. |
| `client.dns.lookup` | same-name values | Pin Kafka's value and integration-test multi-address and canonical-name modes. |
| `socket.connection.setup.timeout.ms` | same name | Pin Kafka's initial timeout. The Java `*.max.ms` cap has no exact native control and is an explicit-property fallback initially. |
| `max.request.size` | `message.max.bytes` plus native serialized-record validation | librdkafka's request-size enforcement is not identical; reject oversize records at the writer boundary to match Java. |
| `buffer.memory` | `queue.buffering.max.kbytes` plus writer accounting | Byte-to-KiB conversion is approximate in librdkafka; enforce a Java-compatible byte budget in StreamFusion. |
| `max.block.ms` | native enqueue/metadata deadline | No config-only equivalent. On `QueueFull`, poll/wait with a deadline and surface failure after the same 60 s default. |
| default partitioner | runtime `murmur2_random` for keyed records | Exact Java Murmur2 keyed placement. Null-key sticky/adaptive selection cannot yield the identical partition sequence across different clients; preserve Kafka's distribution semantics, not trace identity. |
| explicit record partition | pass directly on produce | Exact. |
| SASL PLAIN/SCRAM/Kerberos and supported SSL PEM | translate with shared security helpers | Integration-test produce and Java commit under the same credentials. |
| `key.serializer`, `value.serializer` | reserved; native produces bytes | Accept only the builder's ByteArray serializers. Any custom serializer falls back. |
| `transactional.id` | runtime-owned per epoch | Reject a user property; derive only from Flink's transactional ID prefix/naming strategy. |
| `partitioner.class`, `interceptor.classes` | unsupported Java callbacks | Fall back when nonempty; never ignore them. |
| `partitioner.ignore.keys`, adaptive partitioner controls | no exact librdkafka equivalent | Accept defaults covered by our partitioning contract; fall back for explicit non-default behavior. |
| `transaction.two.phase.commit.enable` | unsupported client-specific protocol mode | Native path falls back whenever enabled. |
| `flink.disable-metrics`, `register.producer.metrics` | consume on Java side, do not send to librdkafka | Register equivalent native metrics or fall back when the requested metric contract cannot be met. |

The implementation must build a generated/maintained allowlist from the exact kafka-clients 4.2
`ProducerConfig` keys. Every supplied property is classified as translated, deliberately Java-only,
runtime-owned, or unsupported. An unknown property is a planner fallback, not a warning. Tests should
iterate the ProducerConfig key set so upgrades cannot silently reduce coverage.

“Parity” here means the same durability, ordering, timeout, partitioning-for-keyed-records, security,
failure, and explicit-configuration contract. It cannot mean identical internal retry timing, metadata
refresh scheduling, memory overhead, or null-key partition trace between two independent Kafka client
implementations. For an explicitly supplied property that affects behavior, StreamFusion must either
honor that behavior or fall back. For unavoidable algorithmic differences under defaults, tests define
the accepted observable envelope and the coverage document names the difference.

Security starts conservatively: PLAIN, SCRAM, Kerberos, and PEM-backed TLS are native-eligible after
cross-client tests. JKS, custom SSL engines/providers, OAuth callbacks/login modules, custom DNS
resolvers, metric reporters, and interceptors fall back until explicitly implemented. PKCS#12 support
may be added after verifying the exact static librdkafka/OpenSSL build. Secrets remain transient JNI
configuration and are never checkpointed.

## Architecture

### Native producer (`native/src/kafka.rs` and focused submodules)

Use one-shot transactional `ThreadedProducer`s: create with the translated envelope plus a runtime-owned
transactional ID, `init_transactions`, capture producer ID/epoch, `begin_transaction`, produce one
checkpoint epoch, flush, hand off identity, then destroy the native producer. The pending transaction
continues to live in the broker and Java owns its final disposition. Never reuse a
producer whose transaction was committed externally because librdkafka's local transaction state machine
does not observe Java's `EndTxn`.

- Prefer `ThreadedProducer` initially so a dedicated poll thread continuously executes delivery
  callbacks. A `BaseProducer` is eligible only if profiling justifies it and the Flink task path polls
  often enough under backpressure and checkpoint stalls.
- Delivery callbacks record the first asynchronous failure. `flushAndGetCommittable` succeeds only when
  the queue is empty, every delivery succeeded, and the current producer ID/epoch still matches the
  captured identity.
- Classify transaction errors with rust-rdkafka's `is_retriable`, `txn_requires_abort`, and `is_fatal`.
  An abort-required or fatal error fails the checkpoint/job according to Flink's existing behavior; it
  must never emit a committable.
- Enforce Java-compatible `max.block.ms`, memory, and record-size behavior around librdkafka's asynchronous
  queue. `QueueFull` is backpressure, not immediate data loss.
- Pre-warm the next one-shot producer while the current epoch runs so `init_transactions` and identity
  capture stay off the barrier path. Never initialize an ID that may still have a pending committable.
- Start with Flink's INCREMENTING naming strategy. POOLING/LISTING is a separate parity phase.

### JNI configuration and epoch surface

Use an explicit versioned ABI rather than a Java `Properties` object or ad hoc JSON:

- `createProducer(configVersion, keys[], values[], transactionalId) -> handle`
- `beginEpoch(handle)`
- `produceBatch(handle, encodedBatch)` (serialization and produce remain in the same native call path)
- `flushAndGetCommittable(handle) -> empty | {transactionalId, producerId, epoch}`
- `destroyAfterHandoff(handle)` and `abortEpoch(handle)`

Validate equal key/value lengths, UTF-8, duplicate keys, reserved-key attempts, and config version before
creating librdkafka. Redact values for keys classified as secrets in every exception and diagnostic.
The configuration envelope is construction-time data, not “per row over the network”; only encoded
records traverse the hot JNI path.

### Java sink (Flink control and commit plane)

Implement a Sink V2 two-phase-committing writer that drives native producers and emits real
`KafkaCommittable`s. Reuse these connector contracts rather than inventing parallel state:

- `KafkaCommittable` and its serializer.
- `KafkaCommitter`, constructed with the full Java commit properties.
- `KafkaWriterState`-compatible ownership, prefix, and pending-transaction state.
- `ExactlyOnceKafkaWriter`'s backchannel and `TransactionAbortStrategyImpl` probing behavior.

The Java writer assigns checkpoint/transaction IDs, invokes native barrier flush, snapshots state, and
forwards commit confirmations. It does not serialize records, enqueue sends, flush a Java producer, or
own a data-plane `KafkaProducer`. Restore probing may create short-lived Java producers because it is a
control-plane abort/fencing operation; final commit always remains the stock Flink Java committer.

Resolve access to `org.apache.flink.connector.kafka.sink.internal.*` in Phase 0: direct construction if
possible, otherwise a same-package shim, otherwise the smallest copied classes with a documented
divergence and connector-upgrade tests.

### Planner and fallback

Extend `KafkaSinkTranslator`/`KafkaSinkMatcher` only when delivery guarantee is EXACTLY_ONCE, a
transactional-ID prefix exists, the format/topic shape is already native-eligible, and every producer
property is classified as parity-safe. Otherwise retain today's encode-only native serializer plus stock
Java `KafkaSink` fallback. Keep Java `Properties` and the native translated envelope together in the
planned sink so they cannot drift.

Update `docs/coverage-and-fallbacks.md` in the same implementation commits. Planner tests must assert the
specific unsupported property in each fallback reason without exposing its value.

## Lifecycle walkthrough

- **Startup/restore:** restore writer ownership/state; let the Java probing strategy abort lingering
  uncompleted IDs; let recovered completed-checkpoint committables flow to `KafkaCommitter`; only then
  initialize a safe new native transactional ID.
- **Steady state:** native code serializes and produces records into the open Rust transaction. Before the
  checkpoint barrier, flush all deliveries and AddPartitionsToTxn work, emit
  `KafkaCommittable(transactionalId, producerId, epoch)` only if nonempty, destroy the old native
  handle, record that the transaction ID is pending, and begin the next safe epoch.
- **Checkpoint complete:** stock `KafkaCommitter` creates/resumes a Java producer using the same Java
  connectivity/security properties and sends `EndTxn(commit)`. Its backchannel releases bookkeeping for
  the pending native transaction ID.
- **Crash before barrier:** no completed committable; restore probing aborts/fences the native transaction.
- **Crash after barrier but before checkpoint completion:** the checkpoint is not recoverable as complete;
  restore probing aborts it.
- **Crash after checkpoint completion but before/during commit:** the committable is restored and the
  stock committer retries `EndTxn`. This is the crucial exactly-once recovery case.

## Invariants

1. Never call `init_transactions` on a transactional ID with a pending committable; it would fence and
   abort the transaction Java is supposed to commit.
2. Never emit a committable for an empty or unsuccessfully delivered epoch.
3. Never emit after an abort-required/fatal error or an observed producer-epoch change.
4. Never silently discard a producer property. Translate it, handle it explicitly on Java/native code,
   or fall back before execution.
5. Keep the complete Java commit properties available until every committable is resolved, but keep
   secrets out of operator/checkpoint state and diagnostics.
6. Complete commit within `transaction.timeout.ms`; preserve Flink's one-hour default and explicit user
   overrides.
7. The native path must preserve native operator coverage: no row-wise Java serialization bridge is
   reintroduced.

## Implementation phases (small commits, each independently tested)

**Phase 0A — cross-client transaction proof.** In the embedded Kafka harness, open/write/flush/destroy
with rust-rdkafka, then resume and commit with `FlinkKafkaInternalProducer`; verify `read_committed`
visibility. Cover Java abort-by-`initTransactions`, destroy-before-commit, duplicate commit, timeout,
fencing, and crash-after-checkpoint-before-commit. Cross-check the tuple with
`AdminClient.describeTransactions` where supported.

**Phase 0B — stable producer identity API.** Add and test a direct producer ID/epoch accessor in the
smallest appropriate layer (prefer rust-rdkafka backed by a public librdkafka API; otherwise carry a
minimal pinned patch and propose it upstream). Do not ship timer-driven statistics polling as the sole
correctness source.

**Phase 0C — producer configuration inventory.** Implement the classification table against kafka-clients
4.2.0's complete ProducerConfig set. Unit-test normalization, Flink's one-hour timeout default, aliases,
unit conversions, reserved keys, secret redaction, and fallback for every unsupported category. Add
cross-client config validation tests using both Java KafkaProducer and librdkafka.

**Phase 1 — native transactional producer.** Versioned JNI config envelope, one-shot threaded producer,
delivery-error latch, queue backpressure/deadlines, begin/flush/handoff/abort, pre-warming, metrics, and
Rust/broker tests.

**Phase 2 — Java Sink V2 control plane.** Native writer, real Kafka committables, compatible writer state,
stock committer, backchannel, and probing abort. Tests for normal completion, empty epochs, restart commit,
restart abort, rescale/downscale, authentication failure, and commit retry.

**Phase 3 — planner and coverage.** Exact eligibility/fallback reasons, native plan assertions, full
property-parity tests, committed-output parity (append and upsert), and documentation updates.

**Phase 4 — failure matrix.** Inject failure before barrier, after native flush, after checkpoint
completion, during Java commit, and after commit acknowledgement. Assert no duplicates/loss with
`read_committed`, no leaked transactions after restore, and stock-equivalent exception classification.

**Phase 5 — performance and profiles.** Criterion-test the native encode+produce primitives and run the
release `-Pbench` exactly-once Kafka Nexmark matrix against the same cluster/config for stock Flink and
StreamFusion. Measure throughput, CPU, allocation/JNI cost, barrier flush latency, transaction-init
latency, producer churn, queue occupancy, and broker request/batch sizes. Profile Java and native sides;
then tune producer pre-warming, batch geometry, and JNI batch size without changing the property contract.
Update the README headline chart only when the exactly-once end-to-end result is reproduced.

## Open questions and explicit boundaries

- Can a public producer ID/epoch getter be added without relying on librdkafka internals? Phase 0B is a
  release gate.
- Can connector internals be reused safely, or is a same-package shim required? Phase 0A decides.
- Cross-client null-key partition *sequence* is not reproducible because Java 4.2 uses sticky/adaptive
  selection while librdkafka has its own sticky timing. Kafka's default contract does not promise the
  sequence; applications requiring it need explicit partitions or Java fallback.
- POOLING/LISTING ownership is deferred until INCREMENTING is proven; it requires broker transaction
  listing and a separate recovery matrix.
- JKS, callback-based OAuth, custom serializers/partitioners/interceptors/reporters, and properties with
  no validated native analogue remain Java fallback, not partial native support.
- The Java commit producer remains on the checkpoint-completion path. This design removes Java from the
  per-record hot path; it does not claim a zero-Java Kafka sink.
