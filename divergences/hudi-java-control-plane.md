# Hudi keeps its Java/Flink source control plane

Arroyo's filesystem source streams Parquet batches with file progress kept
separately. StreamFusion follows that payload structure while retaining Hudi's
released Java timeline, file selection, assignment and state serializers.
The existing Iceberg and Paimon connectors establish the same boundary.

Comet's explicit C Data import ownership informed the shared file-batch import
helper. The Hudi reader calls the existing Rust Parquet decoder through Hudi's
Java storage streams. No payload row is materialized between file decode and
the Arrow pipeline. Java's row-oriented source lifecycle is adapted to count
emitted logical rows, and cancellation releases unclaimed prefetched batches.

The pinned Hudi SQL source builds its generic Source V2 through a private factory.
Following Iceberg's version-gated adapter, StreamFusion invokes that factory on
the original copied source and delegates the resulting enumerator and serializers.
This avoids copying Hudi's file-planning implementation. Unknown versions and
unsupported source contracts retain the stock connector at planning time.

MOR merge algorithms are deliberately left upstream. The published hudi-rs reader
does not expose caller-supplied storage and resolved schema/visibility through
its public facade. Recreating its merge/spill machinery to bypass that boundary
would defeat the integration's purpose. Current native coverage and validation
are recorded on the [connector page](../docs/connectors/hudi.md).
