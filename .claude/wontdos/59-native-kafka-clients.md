# Native Kafka clients

StreamFusion deliberately does not own Kafka consumers or producers for now. Flink's Java
`KafkaSource` and `KafkaSink` retain all broker I/O, configuration, security, offset, metric, and
transaction behavior; Rust owns only serialization and deserialization.

The native clients required a large Java-to-librdkafka translation surface whose defaults and
semantics continually diverged from kafka-clients. Exactly-once production also split one Flink
sink contract across a Rust transaction data plane and Java commit plane. That complexity was not
justified by the measured source results: structured decode sometimes benefited, but raw and small
records exposed the byte-boundary/client overhead, and parity required ongoing property-by-property
auditing.

The retained byte-array boundary is an explicit tradeoff. Structured format parsing and encoding
are normally more expensive than the copies; tiny or raw records are the case to watch. Reopen this
only if a controlled profile shows JVM byte-array materialization is the dominant end-to-end cost
after codec/operator optimization, and require a design that does not recreate an incomplete
kafka-clients compatibility layer.
