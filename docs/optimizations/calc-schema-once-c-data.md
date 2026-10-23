# Export Calc schemas once

**Applies to:** native Calc operators

Arrow C Data transfers the batch buffers without copying them, but an `ArrowSchema` is a separate
tree of field metadata, native strings, and release callbacks. Most batches reuse the same schema,
yet the original bridge rebuilt and exported that identical tree for every physical batch.

The first batch of each distinct input schema exports both its `ArrowArray` and `ArrowSchema`.
The operator keeps a separate compiled handle for each schema, since a `UNION ALL` can mix batches
with and without the hidden row-kind column. Rust caches the resulting struct type alongside that
compiled Calc. Later batches with the same schema use Arrow Java's array-only export and
Arrow Rust's `from_ffi_and_data_type`, so only buffer ownership and child-array layout cross JNI.
This preserves the standard C Data ownership contract: the Java array's release callback is still
consumed exactly once, and the schema remains owned by the native Calc handle until close.

The Q8 profile that identified the issue measured repeated Calc schema export at 120 samples in a
30-second profile (4.00 samples/second). After the change it fell to 14 samples in 25 seconds (0.56
samples/second), an 86% reduction; the remainder is the required first schema for each new job in
the profile loop. Inclusive Calc CPU fell by roughly 12% after normalizing against completed jobs.

The 2-million-event exactly-once Kafka Q8 timing remained within run-to-run noise: 1.472 seconds
before and 1.499 seconds after, while the adjacent Flink baseline moved from 1.617 to 1.845 seconds.
The optimization is retained because it removes measured allocator and schema-tree work without
changing batch contents, but no end-to-end Q8 gain is claimed from that noisy pair.

Generated row evaluators also capture their result schema when the function is built. Each task
parses it once during evaluator registration and reuses that immutable schema for result batches.
The entry transpose similarly captures its input schema during planning and parses it once at
open. This prevents task-local configuration from changing representation layouts and removes
repeated Flatbuffer schema decoding. No isolated speedup is claimed for these additional caches.

Represented nested-row vectors retain one row reader for the batch lifetime rather than constructing
new column-batch and row wrappers for every element getter. Each returned nested row is materialized
before the reader moves to another index. This preserves ownership while removing repeated adapter
construction; no isolated throughput gain is claimed.

## Cache Calc output schemas

The bounded investigation for [#248](https://github.com/datafusion-contrib/StreamFusion/issues/248)
also removes repeated output-schema construction. Each compiled Calc now retains the immutable
Java schema imported from its first result. Later results use the existing array-only C Data
export and import into a fresh `VectorSchemaRoot` created from that schema. A distinct JNI entry
point selects cached-schema execution, so an older native library fails with a linkage error
instead of interpreting a zero schema pointer. Inside the bridge, a zero output-schema address
requests array-only export; existing callers with a schema address retain their full export.
The input-schema key still selects a separate compiled handle and output schema, including when
UNION alternates the presence of the hidden RowKind column.

Only metadata is cached. Each result owns its array, vectors and release callbacks independently;
retained batches survive subsequent calls and operator shutdown. Key-group tags, RowKinds,
watermarks, callbacks and ordinary Flink operator lifecycles retain their existing paths.
Dictionary-bearing output schemas, including nested dictionaries, retain full per-batch schema
import because dictionary IDs and mutable provider values are not immutable metadata. This change
does not qualify new dictionary support. Comet's separate array/schema export and its ownership
transfer were consulted before changing the bridge. No fused operators or native-handle protocol
are introduced.

The investigation allowed ten minutes for capture/analysis and thirty minutes for one prototype.
One CPU/allocation capture covered Calc, Calc–GROUP BY–Calc, Calc–join–Calc and a JVM-callback
control at 64- and 1,024-row entry batches. Actual physical plans and consumed native row counts
are retained with the evidence; exchange/coalescing bundles keep their normal production sizes.
No further profiling or broader ownership redesign was attempted.

The release/mimalloc baseline profile uses Flink 2.2.1/JDK 17, 500,000 runtime source rows per
case, one warmup and one diagnostic stock/native pair. Async-profiler 4.6 samples CPU at 1 ms and
Java allocations at 512 KiB. Of 3,500 Calc-inclusive CPU samples, 147 include output-schema import,
896 include the Calc JNI entry and 635 include native Calc evaluation. Schema import accounts for
166,723,584 bytes of sampled Java allocation weight. Inclusive counts overlap; the JNI count
includes useful work and does not isolate pure transition latency. Allocation weight is not
copied bytes. Profile durations, compilation and startup samples are excluded from throughput claims.

The existing native C Data Criterion fixtures now exercise the same optional-schema export used
by Calc. Their allocation requests and shared/new output-buffer inventory distinguish metadata
work from buffer ownership. Full-job measurements retain runtime row sources, both perimeter
transposes, production JNI/IPC/state and rowwise blackhole sinks, with stock Flink and the preceding
StreamFusion implementation as controls. The JVM-callback control retains the existing dispatch route; caching does not remove
user-function invocation, argument/result boxing or row conversion costs.


### Release complete-job measurements (2026-10-23)

Flink 2.2.1 and 1.18.1 use JDK 17.0.20.1, release/mimalloc libraries, parallelism one,
a 1 GiB heap and CPU affinity to four physical cores on an i7-12650H/WSL2 host. Calc and
callback cases use 4M runtime rows, GROUP BY uses 1M, and the join uses 250k plus a 64-row
dimension. Each revision/release/case starts a fresh JVM; each entry size has two warmups
and five measured alternating stock/native pairs. The larger combined preliminary run was
stopped when a concurrent benchmark caused memory pressure; it is retained as excluded
evidence. No task compilation or tests overlap the final timings. The host is not reserved,
and separate-JVM stock drift and startup costs limit attribution.

All numbers below are complete-job median seconds; negative change means less elapsed time.

| Flink | Case | Entry batch | Previous SF | Updated SF | SF change | Previous stock | Updated stock |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2.2.1 | calc | 64 | 1.984294 | 1.850694 | -6.7% | 0.498676 | 0.465660 |
| 2.2.1 | calc | 1024 | 0.603192 | 0.583747 | -3.2% | 0.467000 | 0.464728 |
| 2.2.1 | aggregate | 64 | 0.582222 | 0.553360 | -5.0% | 0.403525 | 0.414550 |
| 2.2.1 | aggregate | 1024 | 0.216724 | 0.206724 | -4.6% | 0.304495 | 0.379728 |
| 2.2.1 | join | 64 | 0.366332 | 0.379765 | +3.7% | 0.394095 | 0.406522 |
| 2.2.1 | join | 1024 | 0.161290 | 0.163784 | +1.5% | 0.336585 | 0.310936 |
| 2.2.1 | callback | 64 | 2.180053 | 2.130729 | -2.3% | 0.474880 | 0.454918 |
| 2.2.1 | callback | 1024 | 0.627600 | 0.615197 | -2.0% | 0.457886 | 0.473037 |
| 1.18.1 | calc | 64 | 1.997777 | 1.912284 | -4.3% | 0.720339 | 0.694046 |
| 1.18.1 | calc | 1024 | 0.621503 | 0.595904 | -4.1% | 0.724671 | 0.681947 |
| 1.18.1 | aggregate | 64 | 0.581803 | 0.540237 | -7.1% | 0.396096 | 0.393583 |
| 1.18.1 | aggregate | 1024 | 0.211366 | 0.215440 | +1.9% | 0.321019 | 0.328698 |
| 1.18.1 | join | 64 | 0.341671 | 0.376045 | +10.1% | 0.254590 | 0.318826 |
| 1.18.1 | join | 1024 | 0.170527 | 0.194292 | +13.9% | 0.251497 | 0.281173 |
| 1.18.1 | callback | 64 | 2.178188 | 2.063224 | -5.3% | 0.659811 | 0.667726 |
| 1.18.1 | callback | 1024 | 0.623704 | 0.614031 | -1.6% | 0.659429 | 0.650819 |

All four Calc medians decrease 3.2–6.7%; ranges and stock-control drift limit attribution.
The 64-row native Calc cases remain slower than stock. GROUP BY and callback results are mixed;
the final Flink 1.18 join medians worsen by 10%/14%, with broad overlapping ranges. No join
speedup or universal pipeline improvement is established.

An earlier sentinel-based prototype produced 21%/39% worse Flink 1.18 join medians. Its single
bounded reverse-order check returned 0.229 s candidate versus 0.257 s previous while stock
shifted from 0.324 to 0.279 s. This did not reproduce the earlier regression, but it does not
resolve the final join attribution. Retain all outcomes and their startup/arrival-scheduling
limits. Keep the small cache for its eliminated metadata work and consistent Calc improvements;
no further optimization pass or broader ownership redesign was attempted.

Final API review added the distinct cached-schema JNI entry point to avoid reinterpreting a
pointer argument in older libraries. A stale-library probe verifies a normal linkage failure.
The full matrix and focused tests were repeated for that compatibility correction; the earlier
prototype's complete measurements remain archived separately and are not mixed into this table.

The native handoff fixtures report 58 allocation requests/3,723 requested bytes with schema
export/import versus 40/2,784 with a cached schema for the sliced two-column case. Shared and
new output-buffer inventories are unchanged (308 shared bytes/2 new bytes at 16 rows). These
are allocation requests and buffer identities, not copied bytes or Java/JNI timings. The native
roundtrip includes Rust schema export/import, unlike the JVM output boundary, and does not
measure pure JNI latency. In a separate release run, the 16-row full/cached component estimates
are 1.794/1.330 microseconds; that component result is not a complete-job speedup. All
168 handoff fixtures pass, including nullable, sliced, wide, nested and retained-value cases.

[All 448 primary observations, warmups, ranges, identities and profile counters](../benchmarks/results/native-boundaries-248-2026-10-23.json)
and [full plans, native work observations, folded CPU/allocation stacks, excluded attempt and confirmation logs](../benchmarks/results/native-boundaries-248-evidence-2026-10-23.json.gz)
are retained. The profile and native component diagnostics are separate from complete-job timings.
The [bounded closing decision and reopening criteria](https://github.com/datafusion-contrib/StreamFusion/blob/main/.claude/wontdos/248-native-handle-redesign.md)
defer broader ownership/handle redesign; this does not claim that required array wrappers or
all JNI costs have disappeared.

Reproduce a single complete-job case after building the released native library and Java tests
on the selected Flink line (`-Pflink-1.18` for 1.18):

```sh
SF_BENCHMARK=true taskset -c 0,2,4,6 mvn -pl streamfusion-runtime -Pbench \
  -Dtest=NativeBoundaryBenchmark -Dboundary.cases=calc -Dboundary.rows=4000000 \
  -Dboundary.batches=64,1024 -Dboundary.warmup=2 -Dboundary.runs=5 \
  '-Dsf.extraJvmArgs=-Xmx1g' surefire:test@default-test
```

Use `aggregate`/1M, `join`/250k and `callback`/4M for the other cases. Pin equivalent available
cores on another host. The benchmark saves both engines' actual plans and asserts consumed
native work. Native bridge allocation/buffer fixtures run with
`cargo bench -p streamfusion-bridge --bench handoffs -- --test` from `native/`.
