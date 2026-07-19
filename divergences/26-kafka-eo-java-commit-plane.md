# Kafka exactly-once: native data plane, Flink commit plane

**Kind:** architectural — the transaction hand-off differs from Arroyo's Kafka sink.
**Diverges from:** Arroyo.
**Forced by parity:** yes — Flink's recovery guarantee cannot be met Arroyo's way.

## Their decision

Arroyo's exactly-once Kafka sink owns the whole transaction in Rust: rust-rdkafka begins a
transaction per checkpoint epoch, the barrier flushes it, and the controller tells the sink to
`commit_transaction` after the checkpoint is durable. The producer awaiting commit is an in-memory
`Option<FutureProducer>` that is never persisted. When a job restarts between checkpoint completion
and commit, the replayed commit request finds no producer and is dropped
(`"Restoring from commit phase not yet implemented"`), and the new session's `init_transactions`
fences or orphans the pending transaction — the committed checkpoint's output is lost.

The gap is structural, not an oversight: librdkafka has no way to adopt a previous session's
transaction (no producer-id/epoch injection, no KIP-939), so a restarted Rust process *cannot*
commit what the dead process flushed.

## What we do instead

We split the transaction across the JNI boundary. Rust owns only the data plane — serialize,
produce, flush, and surface the broker-assigned `(producer id, epoch)`; the transaction is then
deliberately abandoned by the native producer (librdkafka sends neither commit nor abort on
destroy, so it stays ONGOING on the broker). The identity rides in a real Flink `KafkaCommittable`,
and Flink's stock `KafkaCommitter` commits it after checkpoint completion by resuming the
transaction with that identity — the broker validates `EndTxn` against
`(transactional.id, producer id, epoch)` alone, with no session binding, so it accepts a commit for
a librdkafka-written transaction from a kafka-clients connection. Because the committable is
checkpointed state, a job restarted between checkpoint completion and commit recovers it and
commits — the exact case Arroyo loses. Restore-time cleanup of never-committed transactions is
Flink's own probing abort, which fences native-opened transactions identically to Java-opened ones.

## Why

Our contract is Flink parity: `read_committed` output must be identical to stock Flink's across
every crash timing. Keeping the commit in the Java host reuses the connector's committer, retry
classification, backchannel, and recovery machinery unchanged, and removes the one thing Rust
cannot do (resume) from Rust's responsibilities. The hand-off contract is pinned broker-level by
`NativeKafkaTransactionHandoffTest`.
