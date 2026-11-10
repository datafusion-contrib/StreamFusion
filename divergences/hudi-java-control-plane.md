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
does not accept caller-supplied storage through its public facade. A prototype
instead supplies a local view populated from Java-selected files and Java-loaded
bytes, using the public table factory to resolve the schema. It matches stock
Hudi on real version-9 MOR fixtures without copying merge logic. Cache-inclusive
performance and checkpoint ordering still require validation before admission.
Current native coverage and validation are recorded on the
[connector page](../docs/connectors/hudi.md).

The immutable COW append adapter follows Iceberg's released-graph decoration. It
retains Hudi's append function and coordinator and gives Java only borrowed Arrow
row views for keys/counters. A whole batch reaches the shared Parquet encoder once;
Java retains authenticated output I/O, markers, file handles and commit metadata.
File rolling is deferred until the current Arrow batch has been counted completely.
This avoids duplicating Hudi's write/commit protocol or transposing payload rows.

Continuous COW discovery stays in the released Java enumerator. Its exact selected
instant set becomes a predicate in the existing native Calc engine, with a hidden
commit-time input projected away before emission. No custom native file reader or
commit filtering kernel is introduced. End-commit windows remain stock because
they can produce a different range contract; only the verified version-9 exact
instant-set path is admitted.

The MOR sink also keeps the released Java bucket index, row conversion control,
log framing, key-delete blocks and coordinator. Existing Rust routing and append
buffers retain payloads; the Java log serializer borrows those vectors and adds
only generated metadata before Rust Parquet encoding. Hudi's real buffer pool
and Flink's paged serializer still determine flush/preemption boundaries. A
version-pinned serializer hook skips payload writes while reserving identical
control pages. Original cross-bucket input order is preserved because changing
flush boundaries changes delete/reinsert semantics. No custom index, memory-pool,
spill or winner-selection algorithm is introduced.
