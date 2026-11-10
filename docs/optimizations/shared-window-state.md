# Shared-window state costs


High-cardinality regional HOP/CUMULATE firing updates memory accounting from the removed timer,
removed slice and replaced accumulator sizes. A final whole-state recount remains at the flush
boundary. Recounting every pending key after each fired key made the original wide hopping
aggregate quadratic: its first native warmup took 70.8s versus 4.2s stock. A CPU capture attributed
13,242 of 14,956 samples to the full-state recount. Those unfavorable and instrumented trials
are retained. The subsequent revisions and final validation are recorded below.

The release fixture compares tracked bytes with the independent full recount after each firing
for accounted HOP and CUMULATE state. Removing slices subtracts their accumulator/key footprint;
removing timers subtracts their key footprint; replacing a cumulative accumulator applies the
new size and removes the old one before the budget check.

Heap firing batches reservation reductions at each window end, retaining checks when state grows
and the persistent per-key checks. Shared-slice merging also reuses one set of state arrays to
restore the source accumulator and merge into the target, preserving namespace/aggregate order.
The accounting revision alone remained slower in the wide hopping job (4.90s native versus 4.18s
stock); the remaining CPU capture and its instrumented trials are retained. With both changes, the wide hopping job averages 4.57s native versus 4.17s stock, so further
optimization remains necessary. The revision passed 721 native tests, 992 release fixtures and
324 operator checks on each supported Flink line.

Shared merges read ordinary built-in SUM, COUNT, MIN and MAX state without rebuilding the source:
the pinned DataFusion implementations preserve their state when exporting it. Consuming DISTINCT
state and other accumulator implementations retain reconstruction before the same arrays merge
into the target. The state-read revision passed 721 native tests, 992 release fixtures and 326 checks on each Flink
line, including mixed built-in and string DISTINCT shared-window SQL. In the same two-million-row,
4,096-key, 256-byte-key hopping job it averages 4.061s versus 4.232s stock and 4.293s previous
fallback: 1.04× stock and 1.06× previous. Two warmups and three measured trials rotate the engines;
all unfavorable earlier revisions remain in the trial artifact.

See the [calendar trial artifact](https://streamfusion.tech/benchmarks/results/calendar-windows-2026-10-14.json) for all revisions and raw trials.

## Persistent timer writes

Shared-window updates persist distinct resident timer keys together at the bundle boundary.
Firing persists its remaining timer set before clearing resident state. This replaces a new
RocksDB write batch per input row, including repeated keys, with bounded write batches over
unique touched timer keys. Canonical import retains its single-key helper.

The first regional RocksDB HOP aggregate profile averaged 1.802s native versus 0.595s stock
for one million rows and 64 keys. A diagnostic CPU capture attributes 180 of 318 native samples
to timer writes. The batched revision passes native, both-SDK operator and release-fixture checks. Its matched
recheck averages 0.914s native versus 0.651s stock: substantially improved, but still slower.
A follow-up CPU profile is investigating the remaining cost. All earlier results and instrumented captures remain in the artifact.

The next revision groups timer/key pairs with borrowed encoded keys and computes key-group
routing once per distinct pair through the existing batch encoder. The batched-write CPU capture
attributes 67 of 263 native samples to repeated routing. The corrected revision passes 722 native tests, 264 affected checks on each Flink line and
992 release fixtures. The matched one-million-row recheck averages 0.770s native versus
0.672s stock for HOP and 0.614s versus 0.569s for CUMULATE. Both remain slower; another CPU
capture is investigating the remaining read/write costs before shipping.

Shared persistent input writes timer and aggregate state together through the bounded write batch.
Firing multi-gets each key's needed slices in one read and retains the same merge order, without
expanding the resident set across keys. This revision passes 722 native tests, 264 affected checks
on each released Flink line and 992 release fixtures. The preceding combined-write comparison
still lost by 7–9%; the matched multi-get comparison averages 0.657s native versus 0.630s stock
for HOP, and 0.623s versus 0.560s for CUMULATE. Both still lose.

Window COUNT updates count selected valid rows directly through the existing scalar-count
accumulator, avoiding a selected Arrow array for that aggregate. Ordinary COUNT keeps the same
partial state, null semantics and checkpoint representation. The native suite passes 722 cases;
300 affected checks on each released Flink line and 992 release fixtures also pass.
The matched release recheck averages 0.783s native versus 0.817s stock for HOP
(1.04× stock), and 0.612s native versus 0.550s stock for CUMULATE (0.90× stock).
All three trials remain in the artifact, including a 5.684s previous-fallback HOP
outlier. The small HOP advantage needs broader evidence; the cumulative loss
still prevents treating the acceleration work as complete.

COUNT restoration and shared-slice merging use the same scalar partial directly, avoiding
one-element Arrow arrays while retaining the persisted Int64 representation. The source
COUNT state remains reusable. This revision passed 722 native tests, 451 checks on each
released Flink line (including 155 final calendar SQL checks), and 992 release fixtures.
The matched HOP job averages 0.690s native versus 0.651s stock, and CUMULATE
0.644s versus 0.590s stock. The temporary-array change retains a simpler scalar
merge but does not establish acceleration for these persistent workloads. All
trials, including the cumulative native 0.740s sample, remain in the artifact.

## Repeated visible window-time conversion

Each calendar callback reuses up to eight distinct local-to-epoch conversions within its
input batch. HOP and CUMULATE repeat boundaries in alternating order, so adjacent-value
reuse misses their repeated conversions. The bounded primitive-array cache retains no
state between calls and invokes released Flink for every cache miss. It preserves gaps,
overlaps, historical offsets and broad-year values; timer conversion remains independent.
The preceding adjacent-reuse implementation averages 1.148s native versus 1.241s stock
for TUMBLE, 3.207s versus 2.982s for HOP, and 2.201s versus 2.137s for CUMULATE
(two million rows, 64 keys, two warmups and five measured trials). The bounded-cache revision passes 40 callback and mixed-extrema checks on each
released Flink line and all 288 TVF release fixtures. Its matched five-trial means
are 1.152s native versus 1.232s stock for TUMBLE, 2.794s versus 2.991s for HOP,
and 1.974s versus 2.116s for CUMULATE: about 1.07× stock for all three. The earlier
losses remain in the artifact.

## Accepted persistent aggregate limitation

The maintainer accepted shipping the regional-calendar implementation on 2026-10-14 with
the final low-cardinality RocksDB COUNT measurements above: HOP is about 6% slower and
CUMULATE about 9% slower than stock Flink by the three-trial means. This is an accepted
coverage tradeoff, not a claim that these two paths accelerate the workload. Other measured
consumer and wide-key profiles outperform stock.

Further profiling of the exact shipped revision and optimization of the persistent aggregate
path are tracked in [#350](https://github.com/datafusion-contrib/StreamFusion/issues/350),
outside the accepted original ticket-batch shipping scope. All earlier losses, instrumented
captures and trial variability remain in the linked artifact.


## Exact ordinary COUNT panes and bulk state ordering

Ordinary fixed-grid HOP/CUMULATE COUNT can use the existing exact per-slice pre-aggregation
previously shared with timestamp extrema. Nullable contributions are counted once per slice/key;
scalar counts then merge into the still-open overlapping windows. This avoids building repeated
selection arrays/group vectors for each overlap. The accumulator and checkpoint representation
remain unchanged. Floating and order-sensitive aggregates retain their existing path.

Persistent batch reads sort bytewise keys before calling RocksDB's already-sorted MultiGet path
and restore the caller's original order, including misses and duplicates. Already sorted or
single-key inputs avoid the permutation allocation. Window-state writes also follow bytewise
key order while indexing each row's original state. Pins remain borrowed only through decode;
no state/checkpoint format or durability boundary changes.

That revision's attached-window COUNT→MAX comparison improved the 1M/4096-key in-memory HOP
native median from 0.812 to 0.402s, but its paired final five-million-row case remained slower
than stock (1.786 versus 1.357s). Its persistent cases also lost substantially.
The individual optimizations were not independently timed, so this is a combined revision
result. A 10-second CPU diagnostic attributed 4,338 of 11,097 inclusive samples to MultiGet
and 2,637 to writes; state I/O remained the bottleneck in that revision. Its assigned-bound
route remained opt-in until the bounded COUNT work below. See the [full trial matrix](../benchmarks/results/attached-windows-2026-10-14.json)
and [window operator page](../operators/window-aggregate.md#assigned-bound-one_phase-release-measurements)
for the measured scope, variability and retained losses. Native checks cover original-order
pinned reads with misses, duplicates, empty values, compaction and reopen; release fixtures
cover exact COUNT panes and repeated persistent read/modify/write with allocation reporting.

## Bounded persistent COUNT updates

Ordinary fixed-grid COUNT-only windows retain decoded updates across Arrow batches instead of
reading, decoding, writing and dropping every touched group on every batch. The pending map
includes its key-group routing keys in task off-heap accounting. At most 8 MiB of tracked payload
remains after an input batch; reaching that limit or being denied a larger reservation writes
the current bundle through and releases its reservation. A batch can temporarily exceed the
threshold while it is being folded, just as on the previous per-batch path.

Watermarks write only pending groups whose windows close, then use the existing persistent firing
and removal path. Future windows remain resident, avoiding repeated writes when a periodic
watermark advances within their lifetime. Checkpoints and canonical exports first write every
pending group. The serialized accumulator, key-group placement, snapshot format, firing order
and late-row cutoff remain unchanged. Regional calendar callbacks, shared regional slices and
mixed aggregate lists retain per-batch writes.

Persistent firing bounds each iterator at the next key-group prefix. Checking the prefix only
after RocksDB returns a live record permits an iterator to cross other groups while skipping
deletion markers. This matters especially when COUNT groups are written only as they close,
then immediately deleted. A rejected unbounded-scan revision took 53.390s in its first native
warmup. A separate five-second capture during its next warmup placed 412 of 432 task-thread
samples in iterator seek; that instrumented run was interrupted and is not a comparison result.
The release artifact retains the completed unfavorable warmup and capture counts.

Release tests cover reservation denial and bundle overflow, nullable COUNT, TUMBLE/HOP/CUMULATE,
watermarks before and at firing, native checkpoints, canonical backend migration, split key-group
restores and late input after recovery. SQL checks compare typed rows and RowKinds through both
aggregation phases and state backends, including two completed checkpoints before failures.

The matched five-million-row ONE_PHASE COUNT-to-MAX matrix on main `3679d312` uses
release/mimalloc, Flink 2.2.1, JDK 11, both row/Arrow transposes and production IPC. At
ten-second windows/two-second slides, all twelve TUMBLE/HOP/CUMULATE, memory/RocksDB and
64/4,096-key profiles accelerate: 1.41–2.88× stock by three-trial medians after two warmups.
The original HOP/RocksDB/4,096-key case is 8.680s stock versus 3.256s native; native trials
range from 3.064 to 3.286s. The [operator comparison](../operators/window-aggregate.md#bounded-count-and-default-attached-execution)
and [raw trials](../benchmarks/results/attached-window-count-2026-10-18.json) retain all samples.

Rebuilt confirmation on main `0a02ecb8` measures 8.833s stock, 8.690s previous default fallback,
27.072s previous opt-in native and 3.077s new native for that persistent profile. Native trials
are 27.011–27.204s before and 3.055–3.085s after: about 8.80× faster than the previous native
implementation and 2.87× its current paired stock control. The in-memory HOP native medians
remain 1.087s before/after; its benefit here is default admission of the already-fast path.

The matched release Criterion comparison calls production Rust state operations on the same
base, using the system-backed counting allocator rather than the whole-job mimalloc library.
Four update/non-closing-watermark pairs and final firing include every deferred write, with
input/database setup outside timing. Each version validates all 72 fixtures; six are timed
with ten samples, 100 ms warmup and a requested 300 ms measurement window (Criterion lengthens
the large cases to collect its minimum samples). Values below are mean point estimates:

| Shape | Rows/batch | Key domain | Key padding bytes | Previous (ms) | New (ms) |
|---|---:|---:|---:|---:|---:|
| TUMBLE | 1,024 | 8 | 8 | 0.2303 | 0.1522 |
| HOP | 1,024 | 8 | 8 | 0.3557 | 0.2043 |
| CUMULATE | 1,024 | 8 | 8 | 0.2867 | 0.1803 |
| TUMBLE | 8,192 | 8,192 | 256 | 47.568 | 19.022 |
| HOP | 8,192 | 8,192 | 256 | 317.492 | 329.632 |
| CUMULATE | 8,192 | 8,192 | 256 | 248.607 | 248.424 |

All fixtures have nullable keys/values. Large wide-key HOP/CUMULATE exceed the retained-payload
limit and still write through each batch, so no improvement is established there. The wide HOP
point estimate is 3.8% higher; its 95% mean intervals overlap (304.0–332.4ms before,
317.9–343.1ms after). The separate allocation probe retains the extra requests: 526,914,268
versus 535,257,368 bytes for that case. Low-domain HOP reduces allocation calls from 3,884 to
2,172 and requested bytes from 607,786 to 384,141. These are Rust allocation requests, not
copied bytes, peak memory or C/C++ allocation traffic. Raw samples and confidence intervals
remain in the artifact; isolated state timings do not replace the complete-job comparison.

Wider overlap is still outside default ONE_PHASE admission: thirty-second HOP/two-second slide
with 4,096 keys and memory state measures 1.612s stock versus 2.355s native in the final opt-in
comparison. That loss is retained and tracked in
[#394](https://github.com/datafusion-contrib/StreamFusion/issues/394). Regional calendars,
non-COUNT upstream layouts and explicitly disabled attached windows retain their fallback;
the separate calendar investigation remains [#350](https://github.com/datafusion-contrib/StreamFusion/issues/350).


## Attached-boundary preparation

For zones whose rules have a fixed offset, attached windows subtract the cached offset
from local boundary milliseconds instead of constructing `Instant`, `LocalDateTime` and
`ZonedDateTime` objects for each boundary of every row. Checked subtraction preserves
overflow failure. Regional zones retain the existing calendar/DST conversion.

Attached BIGINT values retain a full-range Arrow view when source and destination share
an allocator root, following the existing timestamp handoff pattern. NULL validity and
buffer references stay owned until synchronous native ingestion finishes. Different roots
retain the copy path. Repeated aggregate arguments each own their retained reference.

On the [operator-format fixture](../benchmarks/feature-admission-2026-10-19.md), 262,144 rows
with 1,024-row batches reduce native attached SUM-input time from 45.0 to 17.6 ms and
wide COUNT-input time from 44.8 to 16.5 ms, approximately 2.6–2.7×. Matched final stock
medians are 42.3 and 37.0 ms. Fixed-offset arithmetic alone accounts for most of the gain;
the second retention pass changes 18.5/17.0 ms to 17.6/16.5 ms in separate JVMs, a small
increment without an isolated causal speedup claim. Full-pipeline performance is not
inferred from these operator measurements.
