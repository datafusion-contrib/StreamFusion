# Reuse Iceberg boundary metadata and Arrow leases

The Iceberg source and sink retain Java discovery, routing, rolling,
delete writers and commits. This optimization removes repeated boundary setup while
keeping payloads in Arrow and data-file encoding in Rust.

The source and file-closing path share a configured-once Jackson mapper, preserving
serializer caches across files. This follows Iceberg's `JsonUtil` pattern; the published
Flink runtime shades its Jackson types, so the connector owns a mapper for its existing
unshaded task JSON contract. There is no global task/schema cache or mutable serializer
configuration after publication.

Each source task supplies its fixed Flink Arrow output schema before decoding.
Rust already adapts every decoded batch to that schema. It now exports only the
C Data array; Java creates a fresh root using the known schema and imports its buffers.
Fresh roots remain independently owned by the fetcher/emitter cancellation protocol.

The sink lazily imports one native batch per borrowed input root. All file selections
and rolling-triggered drains borrow that batch. The lease closes before the input
root, on success and on failure. It never crosses a checkpoint or input-batch boundary.
Parquet encoding, retained-memory accounting, Java's 1,000-row rolling checks and
file-closing metrics still run at their original points. Pending appenders enter the
batch's set on their first pending row rather than on every row.

For hash distribution without unaligned recovery labels, the key-group-to-writer
mapping is computed once during task opening. Java retains the original partition
and key hashing semantics. Recovery paths keep exact original key-group labels; reopening after rescaling rebuilds the mapping.

For the released partition-key selector with integer partition tuples (including
bucket and time transforms), a bounded cache stores the exact host key group for
up to 1,024 tuples. Java still applies the original transforms on every row. On a
miss, the normal Java partition path and Flink hash determine the group; the tuple
is copied before caching. This avoids formatting/escaping the same bucket path for
every row. Other selectors and partition types use the original key calculation.
The cache is task-local, derived state, rebuilt after opening/rescaling; clearing it
at capacity changes cost rather than any routing result. No Arrow row/buffer is cached.

The ownership model follows Comet's retained Arrow references and the existing
StreamFusion bridge. Array-only export uses the existing cached-schema import
contract; source root cleanup follows the core keyed upsert buffer's import pattern.
No payload row encoding, delete semantics or routing policy changes.

[Paired release observations](../benchmarks/iceberg-boundary-20261017/README.md)
retain the original implementation, the metadata-cache probe, and intermediate
shared-batch results, including unfavorable checkpoint waits. Criterion's C Data
fanout cases isolate allocation requests and buffer sharing; they do not estimate
whole-job speedups.

The matched fast loop uses 262,144 rows, four buckets, 16 source files, 8,192-row
fetches, two warmups and five alternating measurements in release + mimalloc.
Read medians move from 52.72 to 47.25 ms (1.12× previous, 2.60× stock);
write medians from 156.65 to 141.36 ms (1.11× previous, 1.40× stock).
Destination calculation moves from 26.69 to 12.58 ms (2.12× previous, 2.18× stock).
These component boundaries exclude Flink scheduling, state, checkpoints and commits.
The linked report retains full ranges, stock cohort drift and excluded costs.

Focused streaming q0/q1 medians are 0.616/0.601 s versus stock 1.175/1.208 s,
with committed-output and native/Arrow IPC witnesses. Before/after ranges overlap;
this does not establish gains across the earlier complete Nexmark matrix. Dynamic
writer diagnostics still trail stock (0.97× append, 0.88× keyed). Supported paths
remain accelerated by default. Validation passes 302 Java/upstream streaming tests,
with 13 batch exclusions, six Rust unit tests and the native fixture smoke checks.

Source inspiration: Comet's
[`execution/utils.rs`](https://github.com/apache/datafusion-comet/blob/main/native/core/src/execution/utils.rs)
for Arrow buffer ownership; Iceberg's
[`JsonUtil`](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/util/JsonUtil.java)
for shared serializers; and the released Iceberg partition selector plus Flink
key-group assignment for exact host routing. StreamFusion's existing cached-schema
bridge and keyed-upsert import cleanup supply the local ownership conventions.

The follow-on [four-candidate investigation](../benchmarks/iceberg-boundary-round2-20261017/README.md)
transfers the existing selection vector into sparse Arrow indices and skips index
construction for contiguous slices. This removes a selection clone at the production
JNI boundary; repeated writer timings overlap, so no throughput improvement is claimed.
A bounded task-local parsed-identifier cache reduces dynamic generator medians from
34.12 to 9.90 ms (3.45× previous), while preserving fresh routing metadata and payload
ownership on every row. It caches no catalog/schema/spec state. The combined-size
JNI prototype has no consistent gain and is removed; the checksummed local source
stream has no direct-buffer capability. Original rolling checks and FileIO remain.

The subsequent [complete unchanged Iceberg Nexmark rerun](../benchmarks/iceberg-optimized-20261017/README.md)
passes all 120 cells / 480 paired validations. README-selection median geometric
means versus stock are 1.22× / 1.81× memory and 2.80× / 2.94× RocksDB (mini-batching
off/on). Direct previous-native/current-native geometric means are 1.03× / 1.19× /
1.22× / 0.90×: RocksDB/on regresses about 10%. These cohorts include all boundary
changes since the previous complete matrix, with storage/JVM variation and large
ranges; they do not isolate each edit. q18/q20 memory regressions and state/checkpoint
follow-up #391 remain. Default-on acceleration is unchanged.

The [performance follow-up](../benchmarks/iceberg-perf-followups-20261018/README.md)
removes reflection argument arrays with typed handles for the pinned released dynamic
control-record contract. Sampled metadata-extraction allocations fall from 2.76 to
0.315 GB (8.78× reduction) in the retained whole-JVM diagnostics; these are allocation
estimates, not copied bytes or wall-time shares. A bounded task-local cache reuses immutable target metadata
across rows and batches, using actual schema/spec identity and all destination/key fields.
It copies equality IDs before insertion and retains no control records or Arrow payloads.
The original Java callback and metadata refresh still execute per row. Each batch assigns
route labels with one map lookup instead of two.

The released dynamic selector LRU is decorated when selectors are created. Bucket tuples reuse the exact original selector's writer key when their maximum
cardinality, including nulls, fits the 1,024-entry partition cache. The host still owns selector invalidation, table salt,
parallelism, metadata refresh, upsert distribution and eviction. Large or unbounded partition cardinalities and other selector shapes retain the original
selector, avoiding repeated transform/cache work on nearly every row. The two-second `dynamic-key` fixture
compares every destination with the released host selector, including Arrow row traversal.
Final four-bucket medians are 30.19 ms stock and 12.55 ms cached (2.41×).
The 16,384-bucket diagnostic first exposed cache thrashing (0.60–0.62× stock); after
bounding admission its medians are 29.45 ms stock and 29.49 ms Arrow traversal (1.00×). This isolates
key calculation; it excludes host catalog/processor, Rust splitting, shuffle and writing.
Repeated end-to-end results remain the acceptance boundary for overall dynamic claims.

Source inspiration is the released Iceberg `HashKeyGenerator` selector LRU and target-limited
writer mapping, together with StreamFusion's existing exact integer-partition cache.
The adapter inspects only the pinned released partition-selector shape; it does not
reimplement Iceberg's table hash, equality-key encoding or partition path representation.

Schema/spec JSON encodings and derived Flink row types remain scoped to the current input batch, as in the original
adapter. Evicting the bounded persistent target cache does not clear those encodings
mid-batch: cycling through more destinations than the target cache can hold must not
serialize the same schema/spec or rebuild its row type on every miss. The encodings clear at batch boundaries;
only the bounded resolved targets survive between batches.


## Columnar equality sets for maintenance

Maintenance reads equality files with Iceberg's existing Parquet decoder, then builds
DataFusion's static membership filter for integer/long/string keys. Composite keys
wrap their Arrow columns in a non-null struct view; DataFusion hashes and compares
the tuples, including inner nulls. Nested struct leaves use get-field expressions
after parent validity has been overlaid on children. Payload columns stay Arrow
through filtering, sorting and encoding. There is no independent hash-table or
Parquet implementation in this adapter.

The previous RC3 delete implementation expands composite delete values into a
predicate tree evaluated against each batch. Static membership avoids repeatedly
walking that tree. Flat streaming sources keep their existing path; nested-schema
sources and maintenance use the verified membership route. Unsupported nested key
types retain the stock worker/reader. Retained filter arrays and per-key objects have
an estimated off-heap charge; transient allocations and decoder retention remain opaque.

The [sorted/equality maintenance measurements](../benchmarks/iceberg-nested-20261029/README.md)
retain previous-native comparisons separately from stock Flink and distinguish file
worker timings from commit latency in running streaming graphs.


At 2M rows, composite-key file-worker medians fall from 0.910 to 0.266 s for v2
(3.42× previous/current) and from 0.984 to 0.365 s for v3 (2.70×). Stock medians
are 1.023 and 1.203 s. These are separate cohorts with repeated ranges retained;
flat single-key timings remain near the previous version. Continuous composite-v3
commit medians are 1.766 s stock and 0.624 s native (2.83×). The full 2M standalone
forced-spill cohort is slower than stock and highly variable; isolated repeats and
running-job results are reported separately, without replacing the slower observations.
