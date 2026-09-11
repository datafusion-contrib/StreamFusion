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
creator.

Each library also exports only the JNI entry points of its own Java class. The JVM binds a native
method, on its first call, to whichever loaded library exports the mangled symbol, so a connector
library that also exported the core class's entry points could capture some of them once both were
loaded, leaving the core's handle registry, captured JVM, and memory accounting split between two
copies. The engine and each extension therefore have separate Cargo packages, each owning its
Java class's entry points. Extensions have no dependency on the engine or DataFusion. A shared
bridge crate supplies JNI guards, C Data ownership, numeric/text semantics, and ABI types;
format lifecycle and composition live in a separate format-support crate without third-party codecs.
The release build checks extension symbols, and CI verifies the package dependency boundaries.
This is the export half of the
ADBC driver discipline (one init symbol per driver, everything else through a table) without its
manual loading, which we do not need because every library ships from one build at one version. The JVM byte-array boundary adds copies, but it keeps Kafka settings and runtime semantics
identical to Flink and keeps each format independently installable, testable, and fallback-safe.

## Workspace boundary decisions

The split follows Comet's separate JNI bridge and engine crates, while retaining one statically
linked bridge and allocator per deployable library. Rust decoder trait objects remain entirely
inside their owning library; the format-driver contract still passes only C Data addresses and
opaque handles. The driver init is handed out by its owner's JNI facade, so it needs no common
unmangled symbol that would collide when Rust integration tests link multiple format crates.

Kafka's existing sink encoders remain in the Kafka package. Moving encoding behind a new format ABI
would change the runtime contract and is separate from enforcing the crate boundaries. The decoder
packages do not depend on Kafka, and Kafka does not link their decoder implementations. The raw
decoder primitive is shared with key/value composition because a keyed JSON value can contain a raw
key; this needs no third-party codec dependency.

Tests live with their owning packages, with connector/format round trips in an integration-test
package. Java tests load the same distinct libraries as production, including separate handle
registries; no all-features engine library can satisfy an extension's missing native methods.

The migration's measurable benefit is dependency isolation. Parquet's normal dependency graph
falls from 253 packages, including 30 DataFusion packages, to 79 with none from DataFusion.
The historical large binary-size saving in issue #51 was not reproduced against the current
pre-workspace build: Linux arm64 release+mimalloc on Rust 1.94.0 measured 8,119,360 bytes before
and 8,404,984 bytes after the split. The prior JNI export gating already let the linker discard
unused engine code. This workspace change is retained for independent builds and compile-time
boundaries; it makes no throughput or binary-size improvement claim.
