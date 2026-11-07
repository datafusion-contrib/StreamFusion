# Bind original identities for sources that retain stock state

A connector replacement can preserve its state serializers yet change the surrounding graph.
Kafka's native decoder retains the released stock source's partition split and enumerator
serializers, but inserting an Arrow boundary changes automatic identities of downstream state
owners. Ordinary SQL must capture the original stock graph and project its source and host
writer/committer identities into canonical savepoints.

An optional planner extension explicitly declares when its substituted source preserves the
original stock reader/coordinator state contract. The default is false; Kafka opts in for its
existing split-aware decoder. Core planning depends on that extension contract rather than on an
optional Kafka implementation class. The original source admission rules still apply, and this
binding retains the native source and its columnar data path.

Reader state follows released Flink's SourceOperator/SimpleVersionedListState framing and the
KafkaPartitionSplitSerializer. Coordinator state remains Flink's KafkaSourceEnumStateSerializer.
Native checkpoints retain their ordinary graph identities. Canonical artifacts project stock
identities; their state can be restored without StreamFusion on the stock classpath. No native
snapshot intermediate is introduced.

The real-broker source subset and remaining transaction/failure/format/version limits are tracked
on [the Kafka coverage page](../docs/connectors/kafka.md). Broader sink variants and both-native pipeline combinations
still require independent qualification under [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).
Benchmarking remains deferred by the maintainer.


The sink follows the same explicit extension contract. The native Kafka serializer immediately
produces stock Kafka record bytes and holds no input across callbacks or barriers. Its extra
physical node may therefore be contracted during logical graph matching, with the existing
metadata projection still rejecting any durable helper state. Stock writer and committer roles
retain their original identities; no Kafka transaction state converter is introduced.

For portable plans, released Flink 2.2.1 dynamic sink specifications supply the table, abilities,
input schema and changelog mode. A schema-only relational input lets the existing ordinary sink
admission and constraint gate inspect exactly that contract. It is never translated or executed;
the native sink receives the actual lowered input edge. Existing materialization, metadata,
constraints and changelog repartitioning exclusions continue to apply. The core planner uses the
optional extension API and does not depend on Kafka classes.


The qualified both-native source/sink cohort uses the same stock state and logical identities
without representation boundaries. Its startup exposed Arrow buffers created before the process
budget was known; the [shared-memory authority](16-task-off-heap-memory-authority.md) now adopts
that early live balance before publishing capacity. This is lifetime accounting, not another
snapshot format or a connector-specific state converter. Broader transaction-failure and connector
variants remain under #569.

Failure qualification retains the same stock-owned protocol. Real TaskManager termination tests
verify the last valid canonical savepoint against external committed rows before retry. A fenced
writer is a distinct negative boundary: the released committer can mark a transaction failed
without failing savepoint completion. Its advanced source positions cannot stand in for successful
delivery. Tests reject that artifact, retain the original prefix and retry earlier state only
after proving there were no new committed effects. No state converter or permissive restore mode
can repair competing ownership. See the [Kafka failure coverage](../docs/connectors/kafka.md#canonical-task-manager-recovery-partial).

Boundary tests explicitly disable the latency flush timer and observe actual transpose input
and native encoding before the barrier; waiting for a source alone cannot prove pending rows.
Abrupt JVM termination separately bypasses normal cleanup and leaves real transactions to the
released writer's restore protocol. Its source offset and the externally committed prefix must
agree before retrying older canonical state. Neither fixture adds a native snapshot intermediate
or changes production memory/flush ownership. The exact partial scope remains on the Kafka page.

Client-result loss after completion uses the completed artifact and the stock protocol unchanged.
The tests first prove that new external effects committed, then restore that completed canonical
cut on the other engine without new input. The subsequent return and new input must neither
replay the earlier prefix nor lose pending state. Choosing an older snapshot after those commits
would be an invalid rollback; no identity projection or state converter can make it safe.
