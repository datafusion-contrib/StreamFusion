# 25 — Flink-style format artifacts over a private cross-DSO ABI

## Reference pattern

Flink distributes connector and format implementations as separate JARs. The table runtime discovers
them through `META-INF/services/org.apache.flink.table.factories.Factory`, so a job installs only the
connector and serialization formats it uses.

## StreamFusion decision

StreamFusion follows that deployment shape for native Kafka serialization and deserialization.
`streamfusion-kafka` owns the planner and byte-array boundary, while `streamfusion-json`,
`streamfusion-csv`, `streamfusion-raw`, `streamfusion-avro`,
`streamfusion-avro-confluent-registry`, and `streamfusion-protobuf` register
`NativeFormatProvider` implementations through Java `ServiceLoader`. The planner selects a provider
only when its artifact and supported options are present; otherwise it leaves the table on stock
Flink.

Flink's `KafkaSource<byte[]>` and `KafkaSink` own all broker I/O. Source bytes cross into the selected
native decoder downstream of the source, and native sink serialization produces final byte arrays for
Flink's sink. No Rust Kafka client or connector-to-format poll ABI is involved.

## Why keep the formats separate?

Linking every format into the Kafka extension would make the base deployment unable to follow Flink's
optional-format convention. Passing Rust-owned decoder objects between format DSOs would also exchange
allocator state across dynamic-library boundaries, which is not a stable ABI.

Arrow's C Data Interface is already the ownership-safe JNI boundary in this project. Each format DSO
imports or exports Arrow data through that boundary while every native handle remains private to its
creator. The JVM byte-array boundary adds copies, but it keeps Kafka settings and runtime semantics
identical to Flink and keeps each format independently installable, testable, and fallback-safe.
