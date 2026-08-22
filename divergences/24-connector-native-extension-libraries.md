# 24 — Connector-native extension libraries instead of one process-wide payload

## Reference pattern

DataFusion Comet packages one native library with its JVM integration. That fits Spark's stable
extension API and a distribution that enables Comet as one unit.

## StreamFusion decision

StreamFusion's Flink base image ships only `streamfusion-core`. Native-bearing Kafka, Parquet, and
format modules each ship a Java extension and a same-named native library. The Delta module is
Java-only and reuses the Parquet extension for data-file bytes. Each native-bearing extension owns
its JNI entry points and opaque handles; core never invokes an extension handle.

This lets a Flink deployment install exactly the official connector and StreamFusion extension it
uses, without pulling connector-specific native dependencies into the base image. It also makes a
missing extension a normal planner fallback instead of a runtime linkage failure.

## Why separate DSOs

The current native engine is one Rust crate, so an extension DSO links the shared engine code into
it as well as its feature-specific code. We deliberately do not pass Rust objects or allocations
between DSOs: Arrow crosses the boundary through the C Data Interface and every native handle is
returned to the library that created it. This avoids relying on Rust's unstable cross-DSO ABI or a
process-wide allocator override.

The cost is duplicate code pages when several extensions are installed. A future Rust workspace can
factor a stable C ABI for shared engine services if measurements show that cost matters; it must
preserve the same ownership rule.
