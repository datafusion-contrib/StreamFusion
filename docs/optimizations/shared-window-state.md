# Shared-window state costs


High-cardinality regional HOP/CUMULATE firing updates memory accounting from the removed timer,
removed slice and replaced accumulator sizes. A final whole-state recount remains at the flush
boundary. Recounting every pending key after each fired key made the original wide hopping
aggregate quadratic: its first native warmup took 70.8s versus 4.2s stock. A CPU capture attributed
13,242 of 14,956 samples to the full-state recount. Those unfavorable and instrumented trials
are retained. Revised correctness and performance checks are still in progress.

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
