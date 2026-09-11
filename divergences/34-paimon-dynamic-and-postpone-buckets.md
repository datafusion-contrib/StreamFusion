# Dynamic assignment stays Paimon's; postpone staging stays columnar

## Reference boundary

Checked Apache Paimon master and paimon-rust main against their canonical remotes on 2026-09-11;
both checkouts were current. Implementation targets released Java Paimon 2.0.0. Its
`DynamicBucketSink`, `HashBucketAssignerOperator`, `DynamicBucketIndexMaintainer`,
`PostponeBucketSink`, and `PostponeBucketFileStoreWrite` define the lifecycle. Comet's Arrow
C Data transfer remains the reference for the native batch hand-off.

paimon-rust 0.3.0 and current main implement dynamic assignment and postpone files, but those
assigners, index maintainers, and file writers are crate-private. Its public table writer owns
storage, routing, a different physical schema, and commit construction. As in [32](32-paimon-pk-l0-through-the-compactor-hook.md),
we reuse released Java components at the connector boundary instead of adding a Git dependency
or replacing Flink's checkpoint and commit protocol.

## Dynamic buckets

The native first shuffle uses Paimon's partition/key-hash channel formula. A columnar operator
passes the resulting key hashes to the released `HashBucketAssigner` and gathers its bucket IDs
into Arrow sub-batches for the second shuffle. Paimon retains bucket growth, index restoration,
the target and maximum bucket sizes, initial assigner count, and inactive-partition cleanup.
Commit-user union state and pre-barrier preparation mirror Paimon's own assigner operator.

The native level-zero writer must also update the hash index: handing new files to the compactor
does not notify Paimon's index maintainer. We feed the merged flush's distinct keys to the public
`DynamicBucketIndexMaintainer` and include its output in the same data increment. Its released API
takes a key row rather than a hash, so this boundary materializes only the distinct key metadata;
the payload remains Arrow. Paimon supplies index encoding, restoration, and replacement at commit.
An abandoned checkpoint's uncommitted index files are never loaded on recovery.

## Postpone buckets

Postpone staging is an ordered log, not a merge-tree run. Sorting or deduplicating it before
Paimon's compactor sees it would lose intermediate changes. The native buffer instead emits all
accepted rows in arrival order, retaining their row kinds and the stock unknown sequence number.
Files use the stock commit-user/writer-ID naming convention and the stock stateless writer and
restore-only committer. They are committed directly into bucket -2; they are not handed to the
ordinary in-job compactor.

Paimon's postpone reader orders a writer's files by millisecond creation time, with scan order
breaking ties. The native writer describes multiple rolled files after encoding a batch, which
can give them equal timestamps; manifest scan order can then replay older values last. Native
postpone creation times therefore increase strictly per partition/writer and resume after the
largest committed timestamp for that writer. The counter survives table replacement and is
restored from committed files, not abandoned checkpoint files. A fixed-clock test reverses file
scan order and checks Paimon's actual grouping, then restores with the clock moving backwards.

The deliberate physical difference is Parquet staging. Java 2.0.0 normally forces Avro for
postpone files as an internal optimization, despite a Parquet table format. Both encodings are
supported by its released readers and compactor. Keeping the existing native Parquet encoder
preserves Arrow batches through ingestion without porting a second staging format. SQL tests
compact both native Parquet and stock Avro staging through Paimon and compare merged rows and
changelogs, including repeated keys across rolled files. This is not a claim of identical staging
file bytes or identical metadata across encodings.

Cross-partition dynamic keys and postpone keys that omit partition columns remain planning-time
fallbacks. Batch-mode postpone writes also retain the stock connector and its distinct fixed-bucket
batch optimization. The supported streaming shapes retain the existing deduplicate and format/type
whitelists; no native compactor or new third-party dependency is introduced.

## Performance tradeoff

Release SQL ingestion diagnostics at one million changes measured 0.96× stock throughput for
dynamic buckets and 0.81× for postpone buckets. The latter also compares native Parquet staging
with stock Avro staging. These results justify treating this as columnar coverage and a precursor
to optimization, not a demonstrated speedup: assignment retains Java index calls, and postpone
cannot benefit from reducing repeated keys before encoding. Existing Arrow pipelines can now
reach these sinks without a payload transpose back to rows, but that performance benefit has not
been measured. Method, timings, and the exclusion of postpone compaction from the timer are
documented in the connector page.
