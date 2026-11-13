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

MOR sources use released Java Hudi's public file-group reader and reader context.
The context decodes Parquet into Arrow and presents stable row-reference controls
to the original Java merger. Its spill codec retains batch/row locations, keys,
versions and operation tags. Final projection and winner gathering operate on
Arrow batches. Java-generated key-delete metadata becomes nullable Arrow columns.
Hudi's original fetcher prepares bounded Arrow batches, sharing the COW emitter
and logical-row checkpoint offsets. Recovery repeats the Java merge and skips
previously emitted rows using the released source's contract. Log payloads stay
resident within the configured Arrow budget while Hudi can spill
its controls. The earlier hudi-rs local-view prototype remains historical research.
Coverage and measurements are recorded on the connector page.
[Hudi connector](../docs/connectors/hudi.md)

The immutable COW append adapter follows Iceberg's released-graph decoration. It
retains Hudi's append function and coordinator and gives Java only borrowed Arrow
row views for keys/counters. Each partition writer passes its selected batch rows to the shared Parquet encoder;
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
log framing, key-delete blocks and coordinator. Java buckets retain shared Arrow
batch references and selected-row bitsets. The log serializer borrows those vectors and adds
only generated metadata before Rust Parquet encoding. Hudi's real buffer pool
and Flink's paged serializer still determine flush/preemption boundaries. A
version-pinned serializer hook skips payload writes while reserving identical
control pages. Original cross-bucket input order is preserved because changing
flush boundaries changes delete/reinsert semantics. No custom index, memory-pool,
spill or winner-selection algorithm is introduced.

Append and MOR writers share one adapter for the released graph, preserving the
original writer coordinator and identities. Multiple MOR tasks use Hudi's own
bucket partitioner for channel assignment and the existing Arrow routing kernel
for payload selection. Channel IDs go through Flink's custom partitioner rather
than being interpreted as Flink key groups. Partition discovery and pruning
remain in the original source. SQL predicates remain explicit residual Calcs,
matching the pinned source's filter contract.

Physical timestamp encoding is derived from Hudi's Parquet schema converter;
StreamFusion supplies its existing Rust encoder with unit/UTC/INT96 metadata.
This avoids maintaining a second Hudi logical-to-physical type policy. Parquet
footer admission uses Hudi's own Avro converter with standard list unwrapping.

Read merging and write precombine share an Arrow-preserving Hudi record context.
Write precombine calls the released deduplication helper and buffered merger.
The context retains stable row references instead of making binary-row payload
copies. Disabling precombine retains stock behavior, including delete/reinsert
effects at the original buffer's flush boundaries.

Restored writers query Hudi's original coordinator for inflight instants and await
older checkpoint commits before their first bucket assignment. Task startup can precede replayed commit completion, and early
index bootstrap would miss that commit's file groups. Hudi's wait helper honors the configured commit acknowledgement timeout.
Commit and index decisions remain in the original coordinator and writer.
