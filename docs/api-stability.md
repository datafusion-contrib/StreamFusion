# Alpha API and artifact boundaries

The alpha JARs are Flink deployment components, not a general-purpose Java library. Installing the
loader and selected extension JARs is the supported integration path. Public Java visibility is
sometimes required across Flink's isolated planner classloader and between the core and extension
JARs; it does **not** by itself make a class a stable downstream API.

The application-facing alpha surface is intentionally small:

- `NativePlanner.install(...)` and `NativePlanner.explain(...)` for embedded clients that cannot use
  the distribution loader;
- the documented `streamfusion.*` configuration keys;
- the documented state-backend factory identifier.

Everything else under `tech.streamfusion`, including JNI declarations, planner substitutions,
operators, serializers, wire codes, and connector SPIs, is internal and may change between alpha
releases. The connector SPI currently exists to enforce artifact boundaries, not as a third-party
plugin compatibility promise.

`streamfusion-runtime` is the all-in-one development and test assembly. Do not deploy or depend on
it downstream. Deploy `streamfusion-loader`, the self-contained `streamfusion-core` runtime JAR,
and only the connector and format artifacts listed in [Deployment](deployment.md). In particular,
`streamfusion-avro-confluent-registry` depends on `streamfusion-avro` for its native codec.

The loader embeds the self-contained core planner payload for Flink's isolated planner
classloader. The separate core runtime JAR exposes serialized operators and their dependencies to
the JobManager and TaskManager runtime classloaders; neither artifact replaces the other.
