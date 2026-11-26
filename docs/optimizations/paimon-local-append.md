# Local Paimon append routing

Bucket-unaware append tables have no key-to-writer ownership requirement. Released Paimon's
default append topology writes locally when input and writer parallelism match. StreamFusion
preserves that behavior by feeding partition-grouped Arrow batches directly to the native writer.
It avoids row/channel reassignment, a custom network edge, and its channel-stripping map.
The native file encoder and Paimon's checkpoint, commit and compaction lifecycle are unchanged.

Changing `sink.parallelism` still uses the row-level round-robin path. Its router inherits the
input parallelism, while the custom shuffle targets the configured writers. Explicit hash and
dynamic partition strategies, fixed/dynamic buckets and postpone routing keep their separate
required exchanges.

## Structural evidence

The plan regression builds stock and native jobs with input parallelism 3 or 786 and a
matching environment default. Paimon 2.1's stock row adapter inherits that default, so this
comparison isolates the append routing. Separate native cases use environment parallelism 1
to verify that local writing and redistribution retain the actual input parallelism. It checks partitioned/unpartitioned append tables with unspecified,
equal, smaller and larger writer counts, actual JobGraph vertex parallelisms, and a forward
bucket-router-to-writer edge for local writes. The previous implementation fails the local-path
assertions. Different writer counts retain their necessary distribution.

For the 786-writer, write-only shape, native vertex count changes from **3 to 2** and task count
from **1,573 to 787**, matching stock. These are plan-level structural observations. They do not
establish a TPS speedup, runtime worker count, peak RSS reduction, or a new memory limit. The
remaining source, calculation, file encoding, filesystem and commit costs depend on the workload.
Checkpoint-failure tests compare full committed rows and NULLs against stock, require a restart,
and inspect Parquet footers to verify that native file writing actually ran.
