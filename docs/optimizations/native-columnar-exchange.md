# Native columnar keyed shuffle

**Applies to:** keyed operators reached through a shuffle

A keyed exchange splits each Arrow batch by the projected key's Flink BinaryRow hash and Murmur
key-group mix, then gathers the rows into at most one order-preserving batch per destination
channel, so a keyed operator's input stays columnar across the shuffle instead of transposing to
rows and back. Matching the host key-group layout now lets native raw
keyed state rescale safely while the hot map stays in Rust.

The fully-columnar windowed pipeline (source → watermark → shuffle → window) measured **1.91x**
vs Flink, where the row-fed window was **1.21x**.

Each serialized Arrow record carries a representative key group from its destination's range. A
random-key 8192-row input therefore produces at most the downstream parallelism in network records,
instead of nearly one IPC stream per row. Rows retain their original order within every destination.

## Finalized output topology

The splitter resolves its destination count at task startup, not during SQL translation. A resource
planner may change operator parallelism on the generated StreamGraph before submission. The splitter
identifies its own columnar keyed output in the operator configuration, maps its dataset identity to
the task's output writer, and uses that writer's actual channel count. This also works when the
splitter is chained into a producer with different parallelism or other task outputs. Missing,
replaced, ambiguous, or non-pipelined outputs fail initialization rather than guessing a count.
See [issue #358](https://github.com/datafusion-contrib/StreamFusion/issues/358) for the stale-routing
reproduction.

Maximum parallelism is a separate state contract. Changing ordinary parallelism is supported;
changing maximum parallelism after native planning is rejected before processing or restoring
native state. Splitter, partitioner, reassembler, and native keyed state retain the planned key-group
namespace. Set `pipeline.max-parallelism` before SQL translation if the deployment requires a
particular count; external resource planners must preserve it too. This does not migrate existing
state to a new key-group count. Splitter gauges `target_channels` and `target_key_groups` expose the
bound routing parameters.

SINGLETON exchanges retain exactly one destination and use the downstream operator's planned state
namespace, not a universal key-group count. Global aggregates use the configured/default count;
event-time sort and keyless joins retain their fixed one-key-group namespace, even when the job-wide
maximum parallelism is larger. The matcher carries that contract through physical-plan copies into
the splitter, partitioner, and optional reassembler. Their startup consistency checks remain enabled.
Changing a SINGLETON destination to multiple subtasks is rejected rather than producing independent
global results. Previously an unaligned global-aggregate reassembler used maximum parallelism 1
even when its downstream accumulator planned a larger count.
Existing checkpoints of that old topology need an explicit upgrade/recovery decision; this fix does
not migrate their state or claim that they restore into the corrected topology. Ordinary HASH
exchange layouts and the one-key-group sort/keyless-join layouts are unchanged.

### Singleton metadata for external resource planners

`ColumnarKeyGroupPartitioner.requiresSingleton()` exposes the exchange's distribution constraint
independently of its current channel count. Exchanges with no key columns use the `singleton(...)`
factory; existing public constructors continue to describe ordinary HASH/key-group distribution.
A HASH exchange running at parallelism one is still rescalable and does not become SINGLETON.
Copies and Java serialization preserve this metadata. Partitioner equality includes distribution,
key-group count, and recovery protocol so Flink cannot share outputs with different contracts.
Regenerate serialized execution plans created before this metadata existed; this does not change
checkpoint state or Arrow wire formats. The diagnostic name remains
`columnar-key-group`; consumers must use the typed accessor rather than interpret that string.

An external resource planner must preserve one downstream state instance for a SINGLETON exchange
and keep its planned maximum parallelism unchanged. Setting maximum parallelism to one is not a
substitute: a global aggregate can require one subtask while retaining 128 or 256 state key groups.
With unaligned checkpoints enabled, the exchange first feeds an `OrderedKeyGroupReassembler`;
both that receiver and the actual singleton stateful consumer must remain at parallelism one.
The accessor describes the exchange, not arbitrary downstream topology rewrites. It does not
automatically enforce constraints in third-party resource planners. Splitter startup checks reject
disagreement with the partitioner's distribution metadata and still reject multiple network
destinations; they do not validate every possible mutation beyond the exchange receiver.

Source and local aggregation parallelism need not be one. Stateless operators and sinks after the
global aggregate may also run in parallel if a valid redistribution edge is supplied at that
boundary. For example, a two-phase CUMULATE without business grouping keys can have a parallel local
aggregate, a SINGLETON global aggregate, and a parallel projection/sink reached through rebalance.
Do not cap the entire downstream chain solely because the aggregate is singleton.

Regression coverage includes HASH versus SINGLETON copies/serialization, distribution mismatch at
startup, rejection of a 32-channel SINGLETON deployment, and a two-phase 10-second/60-second
CUMULATE with late parallelism adjustment. The window test compares exact DOUBLE sums and window
boundaries against stock Flink, preserves 128/256 key groups, allows parallel output after the global
aggregate, and requires completed checkpoints in aligned and unaligned-enabled modes. It does not
claim a production deployment or a singleton window restore test.

The singleton metadata change passes a focused 69-test selection on each of public Flink 1.18.1
and 2.2.1 with JDK 11 on macOS ARM. Both runs apply the Java patch to an isolated upstream snapshot
and use a freshly built upstream debug native core, with no failures, errors, or skips. The
selection includes the window parity checks, key-group reassembly, and aligned/unaligned exchange
checkpoint restore with rescaling. These are correctness checks, not full-suite or performance runs.

SQL regression tests change StreamNode parallelism after translation from 1 to 4, 2 to 4, 4 to 3,
3 to 1, and 1 to 5, including different producer/consumer parallelism. They verify every key's actual
Flink owner and exact COUNT/SUM values. Aligned and unaligned-enabled SQL jobs must complete
checkpoints after the late change. Exchange recovery tests also apply parallelism after graph
generation before checkpointing and rescaling in both directions.

Sort and keyless-join SQL regressions additionally cover both aligned and unaligned-enabled
checkpoints with the default job maximum parallelism and an explicit value of 256. They require
native execution, matching one-key-group exchange metadata, exact ordered-sort/cross-product rows,
and a real completed checkpoint. The existing temporal-sort and keyless-join parity suites are also
part of the regression selection.

On public Flink 1.18.1 / JDK 11, adding only the SQL reproduction to the unmodified upstream
baseline makes four of five routing cases fail with incorrect owners; the 3-to-1 control passes.
With the fix, focused regressions on macOS ARM run 327 tests on each released line: Flink 1.18.1
has 324 passes and three existing SESSION/STATE_TTL capability skips; Flink 2.2.1 has 327 passes.
Both runs use JDK 11 and a freshly built debug native core from the upstream source, with no
failures or errors. They include RocksDB state, stateful SQL rescale, exchange recovery,
window/Top-N/join, serializer, and one-/two-phase aggregate checks. They are not full-suite runs
or performance measurements.

### Preliminary downstream performance evidence

The 2026-10-15 measurements below use a downstream Flink 1.18-based distribution, not public Flink
1.18.1 or the current upstream main. They are diagnostic evidence for this Java routing fix, not
upstream performance acceptance. Both Java variants use the same release+mimalloc native library;
there are no Rust or JNI changes in this fix.

The environment is Linux x86_64, Xeon E5-2682 v4, an unpinned 8-CPU quota, 16 GiB container, 8 GiB
heap, and JDK 11.0.22. A bounded rowwise datagen source emits 64 million sequential BIGINT ids:

```sql
SELECT k, COUNT(*) AS c, SUM(id) AS s
FROM (SELECT CAST(MOD(id, 1024) AS BIGINT) AS k, id FROM gen)
GROUP BY k
```

All variants use four runtime source/consumer subtasks, 128 key groups, a rowwise blackhole sink,
no logical mini-batching or checkpoints, 1024-row source transposes, and the default 4096-row/50 ms
exchange coalescer. Both native transposes remain in the measured path; process-local exchange
handles are disabled. The late variant plans at parallelism one and changes all StreamNodes to four
after graph generation; other variants plan at four. Each of three trials uses a fresh JVM after
two 2-million-row warmup jobs, alternating variant order. Submission-to-completion timing includes
deployment, source batching, both transposes, JNI, IPC, state, and sink, but excludes SQL/graph
compilation. Each measured job verifies all aggregate input counts and actual native engagement.

| Variant | Median (s) | Range (s) |
|---|---:|---:|
| Stock row execution | 29.107 | 28.828-33.658 |
| Previous native, planned/runtime 4 | 28.707 | 26.815-30.437 |
| Fixed native, planned/runtime 4 | 32.883 | 32.646-33.096 |
| Previous native, late 1-to-4 (incorrect ownership) | 64.996 | 64.652-70.606 |
| Fixed native, late 1-to-4 | 28.687 | 28.310-33.151 |

Correcting late routing reduces that defective path's median elapsed time by 55.9%, but it is
effectively tied with the stock control within variation, not a general native speedup. The normally
planned fixed path is 14.5% slower than the previous native median and 13.0% slower than stock in this
matrix. That unresolved performance risk still needs investigation on released Flink. A separate
paired JFR run measures 32.205 s previous versus 33.874 s fixed, with 2.500 versus 2.823 s of GC pauses;
samples include JNI updates, row conversion/copying, and shuffle IPC but do not establish the cause.

Before timing, 65,536-row checks verify every COUNT/SUM result for BIGINT and STRING keys. For 1024
BIGINT keys the old late path routes 1024/0/0/0 keys, while the fixed path matches the stock control's
251/254/262/257; STRING changes from 1024/0/0/0 to 236/266/247/275. These synthetic downstream runs do
not validate a production job or establish the performance of this branch on released Flink.

## Single-destination forwarding

For an aligned exchange with one actual runtime destination, the splitter forwards the existing Arrow root with
representative key group zero. Every key group belongs to that destination, so hashing, gathering,
and the split's JNI export/import round trip are unnecessary. Empty batches are consumed without
emitting a record. The forwarded batch keeps the split operator's handle owner and encoding metrics;
normal IPC serialization and downstream coalescing still apply. Unaligned exchanges retain their
per-key-group fragments even at parallelism one, because recovery may change the destination count.

A release+mimalloc benchmark of two-phase `AVG(DISTINCT)` with DECIMAL(38,2), 20 million runtime
rows, 64 keys, 1,024-row mini-batches, and both transposes measured native elapsed time falling from
18.737 s to 15.938 s (14.9% less time). Five alternating-engine measured trials followed two warmups
on JDK 17 / Flink 2.2.1, with process-local exchange handles disabled. Native trials changed from
18.705–18.853 s to 15.933–16.019 s; the Flink control changed from 13.283 s to 13.683 s. This improves
the existing native path but does not close its gap to Flink. CPU profiling identified split,
serialization, and coalescing costs alongside membership updates; this change removes only the
unnecessary single-destination split, without changing the query, source, sink, or batch settings.

The same setup with two million rows measured:

| Two-phase DISTINCT workload | Previous native (s) | Forwarding native (s) | Flink control (s) |
|---|---:|---:|---:|
| COUNT/SUM, DECIMAL(38,2) | 1.903 | 1.556 | 1.441 |
| AVG, DECIMAL(19,2) | 1.680 | 1.499 | 1.710 |
| AVG, DECIMAL(38,2) | 1.965 | 1.673 | 1.618 |

Native trial ranges were 1.552–1.603 s, 1.357–1.597 s, and 1.665–1.891 s respectively;
Flink ranges were 1.417–1.521 s, 1.677–1.828 s, and 1.538–1.878 s. The previous native
medians are the [typed membership optimization baseline](aggregate-specialization-fast-paths.md).
Narrow AVG now beats its matched Flink control; the wide cases still need further optimization.

The planner selects the wire shape from Flink's checkpoint configuration. With unaligned
checkpoints disabled (Flink's default), the exchange keeps the fast destination batches above and
forces that edge aligned. With unaligned checkpoints enabled, every parent batch instead emits one
record per non-empty key group. Each fragment carries a parent epoch/sequence, its original row
ordinals, and the parent's non-empty key groups. Flink's ordinary `RANGE` channel-state filter can
therefore reroute every whole fragment after rescaling. During normal execution a checkpointed
reassembler uses that compact manifest to identify its destination-local siblings and restores
their original row order with a k-way merge of the already-sorted ordinal streams.
After recovery it delivers old-attempt fragments independently: some sibling groups may already
have been applied before the checkpoint and now live in downstream operator state. The restored
producer uses a fresh epoch, so newly produced parents immediately resume ordered reassembly.

The recovery-safe representation is used for the whole execution because Flink may start a
checkpoint aligned and switch it to unaligned after its alignment timeout; records already buffered
when that happens must be independently recoverable. Process-local handle-table transfer stays off
in this mode because its handles cannot survive restore. Native keyed operators honor Flink's
`pipeline.max-parallelism` setting as the stable key-group count.

Recovery tests cover both protocols. The aligned test proves that destination batches leave no
Arrow channel state. The unaligned test creates backpressure, proves that the checkpoint captured
Arrow channel state, fails the job, restores from parallelism 2 to 3 and 3 to 2, and verifies every source id
exactly once. Operator-harness tests separately restore a partially assembled parent and hold a
watermark until all of its key-group fragments arrive.

Partitioner copies retain their configured channel count. This is required by Flink 1.18's
recovery filter, which sets up a partitioner before copying it and uses that copy directly.


## Avoid full-payload heap copies in IPC transport

The IPC serializer writes its serialization buffer directly to Flink's output view,
avoiding `ByteArrayOutputStream.toByteArray()` and its full-payload copy. A serializer
instance reuses that heap buffer after a successful write, starting at 4 KiB and
retaining at most 1 MiB of capacity. Oversized or failed writes discard the buffer.
Only encoded heap storage is reused; the input Arrow root still closes after encoding.
This follows Comet's exposed/reusable shuffle serialization buffer pattern, with a
retention limit because Flink duplicates serializers across edges.

The reader exposes the framed input directly to Arrow instead of first allocating
and filling a complete payload byte array. Reads cannot cross the declared frame
length, and the remaining end-of-stream marker is consumed before returning a batch.
The stream does not own or close Flink's input view. Legacy frames, key-group tags,
ordered recovery metadata and process-local handle frames keep their wire formats.

A matched 20M-row wide-AVG diagnostic isolates removal of payload copies before
buffer reuse: native median 15.199 s (15.097–15.279) becomes 14.886 s
(14.814–15.034), about 2.1% less time. Flink controls are 13.452 s in both runs
(ranges 13.313–13.511 and 13.290–13.764). This uses release+mimalloc, JDK 17,
Flink 2.2.1, Core i7-12650H/Linux, 2 GiB heap, parallelism one, 64 keys,
1024-row mini-batches, two warmups and five alternating engine trials, both
transposes and row source/sink. The improvement leaves native slower than Flink.

With bounded output-buffer reuse as well, the same sustained diagnostic measures
14.524 s native (14.449–14.831) against 13.408 s Flink (13.286–13.463).
That is about 4.4% less native time than the matched 15.199 s baseline, but
still 8.3% slower than Flink. No process-local exchange handles are enabled.

At 2M rows, the final COUNT/SUM diagnostic measures 1.434 s native
(1.407–1.454) versus 1.440 s Flink (1.367–1.469), effectively tied within
variation. The earlier matched-resource native baseline was 1.469 s. This
short-run result does not erase the sustained wide-AVG deficit above.

Validation runs 48 serializer, coalescing, key-group routing/reassembly, recovery
and decimal SQL checks on each released Flink line (2.2.1 and 1.18.1). They
cover consecutive legacy/tagged frames, truncated-frame isolation, growing and
shrinking payloads beyond the reuse limit, and aligned/unaligned checkpoint
recovery with rescaling in both directions. All pass without skips.


## Compact timestamp payload copy semantics

Top-level TIMESTAMP/TIMESTAMP_LTZ payloads with precision at most three mirror Flink's row-shuffle
serializer: retain milliseconds and clear the nanosecond remainder within that millisecond.
This applies before the aligned single-channel fast path and before partitioning, including
recoverable exchange. It preserves downstream hash/ordering behavior such as timestamp COLLECT
followed by a string cast. Timestamp precisions above three retain their full components.

The planner derives the selected physical columns from the exchange's logical row type. The
existing C Data ownership protocol exports/imports the replacement batch; native code keeps
component millisecond buffers, validity and metadata and allocates a zero remainder buffer only
where needed. Unchanged columns and already-zero component columns retain their Arrow references.
The source batch remains immutable. Native slice/NULL/metadata/ownership tests and released
collection, key-byte and aligned-recovery checks validate the boundary. Legacy primitive Arrow
timestamp units use the existing timestamp conversion helpers. Nested representation gates
retain their existing contracts; this adds no support for deferred updating MAP/MULTISET state.


Existing checkpoints whose compact timestamp payload state already contains hidden
sub-millisecond remainders, including unaligned in-flight records, need a fresh-state upgrade
decision. The corrected shuffle changes
which physical values reach downstream state; this change does not migrate such historical
payloads. Compact values whose remainders were already zero and higher-precision timestamp
payloads retain their representation. New timestamp/interval collection states are validated
through repeated recovery after this copy boundary; old primitive collection snapshot bytes
remain unchanged.
