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
on [the Kafka coverage page](../docs/connectors/kafka.md). Broader sink variants and both-native pipelines
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
