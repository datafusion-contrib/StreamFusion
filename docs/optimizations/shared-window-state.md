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

See the [calendar trial artifact](../benchmarks/results/calendar-windows-2026-10-14.json) for all revisions and raw trials.

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

The attached-window COUNT→MAX comparison improves the 1M/4096-key in-memory HOP native
median from 0.812 to 0.402s after these changes, but the paired final five-million-row case
remains slower than stock (1.786 versus 1.357s). Persistent cases still lose substantially.
The individual optimizations were not independently timed, so this is a combined revision
result. A 10-second CPU diagnostic attributed 4,338 of 11,097 inclusive samples to MultiGet
and 2,637 to writes; state I/O remains the bottleneck. The new assigned-bound route is opt-in
and defaults off. See the [full trial matrix](../benchmarks/results/attached-windows-2026-10-14.json)
and [window operator page](../operators/window-aggregate.md#assigned-bound-one_phase-release-measurements)
for the measured scope, variability and retained losses. Native checks cover original-order
pinned reads with misses, duplicates, empty values, compaction and reopen; release fixtures
cover exact COUNT panes and repeated persistent read/modify/write with allocation reporting.
