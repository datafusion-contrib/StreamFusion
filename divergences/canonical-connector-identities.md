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
on [the Kafka coverage page](../docs/connectors/kafka.md). Sink state and both-native pipelines
still require independent qualification under [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).
Benchmarking remains deferred by the maintainer.
