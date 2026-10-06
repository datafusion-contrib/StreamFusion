# Indexed pending rowtime deduplication

Keep-first rowtime deduplication holds one strict minimum per key until its watermark. Previously,
every arrival concatenated the full pending batch with new rows and reduced every key again. Small
batches arriving while a watermark remained behind paid for the complete retained state each time.

The memory operator retains contiguous batch reduction for up to 1,024 winners and promotes
immediately above that threshold. The first indexed version regressed small-state, 256-row arrival
measurements by 40–100%, so the hybrid keeps the previous small-state implementation. Large states
encode incoming keys and probe an index pointing to winning rows in Arrow chunks. Payloads remain columnar. Only new or strictly earlier winners enter new chunks;
equal-time ties preserve the incumbent. Winning-arrival ordinals preserve the former output order,
including replacements moving behind unchanged incumbents. Watermarks and snapshots gather winners.
Snapshot bytes keep the existing format, and restored batches gain their index on first arrival.

Fully dead chunks release immediately. A chunk with at least half its rows replaced compacts only
its remaining rows and updates their index positions. This amortizes replacement churn without a
whole-state rewrite. Row thresholds trade some stale-buffer retention for fewer copies: replacing
one large string in a mostly-live chunk can leave its bytes retained. When budget accounting
rejects growth, the operator compacts every sparse chunk and retries the transactional reservation;
fully live chunks remain untouched. Live state that still does not fit reports the budget failure.
This pressure path avoids rejecting affordable winners because of dead wide payloads. Retained
buffers, key bytes, and index capacities count toward the task budget.
RocksDB's existing point-lookup path is unchanged.
Arrival accounting traverses retained buffers only when memory tracking is enabled; unaccounted
operators retain the previous small-state path's accounting cost. Budgeted operators still count
the full live state and retained metadata before accepting growth.

The reference is Arroyo's keyed incremental aggregator: keyed state is probed by incoming encoded
keys, while record data travels in Arrow batches. Arroyo has no dedicated Flink-style rowtime
keep-first operator in the inspected checkout. StreamFusion keeps Flink's strict replacement,
watermark, emitted-marker TTL, and checkpoint contracts.

## Measurement

`dedup_pending` measures only production arrival pushes with initial state outside timing. It
covers pending cardinalities 64 and 16,384, batches 16 and 256, nullable payload widths 8 and 264,
and fresh keys versus earlier/equal/later mixtures. Its untimed oracle checks payload alignment,
ties, winning rows, and final drain. `PendingDedupBenchmark` separately compares complete stock
Flink and native SQL jobs with both row/Arrow transposes and terminal-only watermarks. Complete
SQL measurements are separate; this native kernel measurement does not include Flink startup,
JNI, conversions, or sinks.

Final quiet-host release measurements used the default Rust system allocator, 10 samples,
0.3-second warmup and 0.5-second measurement per case. Baseline production was `96f98a32`;
fixtures were `8953dc84`. Large retained states improved **22.33–321.75×**. Small-state mean
ratios ranged **0.98–1.06×**; overlapping intervals and Criterion comparisons do not establish
a small-state regression. The rejected always-indexed candidate slowed small 256-row arrivals
40–100%; the shipping hybrid preserves the previous reduction below its threshold.

| Shape (keys / arrivals / suffix bytes) | Before mean µs (95% CI) | After mean µs (95% CI) | Before / after |
| --- | ---: | ---: | ---: |
| Fresh 16384 / 16 / 264 | 3052.62 (2968.26–3152.79) | 9.49 (8.98–9.99) | 321.75× |
| Fresh 16384 / 16 / 8 | 759.62 (743.99–780.45) | 7.85 (7.50–8.16) | 96.72× |
| Fresh 16384 / 256 / 264 | 1171.86 (1149.33–1202.45) | 38.84 (37.18–41.59) | 30.17× |
| Fresh 16384 / 256 / 8 | 810.57 (787.82–836.79) | 36.30 (35.12–37.53) | 22.33× |
| Fresh 64 / 16 / 264 | 6.58 (6.51–6.65) | 6.73 (6.50–6.99) | 0.98× |
| Fresh 64 / 16 / 8 | 5.37 (5.34–5.40) | 5.41 (5.35–5.49) | 0.99× |
| Fresh 64 / 256 / 264 | 18.03 (17.78–18.31) | 18.07 (17.68–18.63) | 1.00× |
| Fresh 64 / 256 / 8 | 15.65 (15.32–16.01) | 14.77 (14.52–15.16) | 1.06× |
| Mixed 16384 / 16 / 264 | 1082.85 (1066.86–1096.10) | 12.82 (11.73–13.98) | 84.48× |
| Mixed 16384 / 16 / 8 | 717.92 (705.01–730.96) | 10.18 (9.22–11.40) | 70.49× |
| Mixed 16384 / 256 / 264 | 1135.94 (1111.53–1159.57) | 28.50 (27.87–29.26) | 39.86× |
| Mixed 16384 / 256 / 8 | 743.42 (720.81–769.50) | 28.14 (25.61–32.61) | 26.42× |
| Mixed 64 / 16 / 264 | 6.12 (5.97–6.33) | 6.26 (6.19–6.33) | 0.98× |
| Mixed 64 / 16 / 8 | 5.33 (5.23–5.43) | 5.39 (5.31–5.49) | 0.99× |
| Mixed 64 / 256 / 264 | 12.45 (12.23–12.70) | 12.13 (11.90–12.46) | 1.03× |
| Mixed 64 / 256 / 8 | 10.54 (10.42–10.66) | 10.08 (9.99–10.18) | 1.05× |

Suffix widths exclude the generated row-number prefix; every seventh payload is null. Mixed
arrivals include new keys, earlier replacements, equal-time ties and later rows. Operators are
unaccounted in these fixtures, so they do not measure budget traversal or pressure compaction.
Watermark gathering, snapshots and persistent state are outside this arrival measurement.

[Raw measurements and source fingerprints](../benchmarks/results/pending-dedup-criterion.json)
retain mean/median estimates, 95% confidence intervals, all 10 iteration/time samples and source
SHA-256 hashes. Ratios divide mean estimates and are not confidence intervals for speedup.
Use `bin/export-criterion-comparison.py` to export the saved Criterion `audit-before` and `new`
measurements. Correctness validation passed the full native engine suite (656 passed, 2 ignored),
including indexed churn, strict ties, nullable nanosecond keys, snapshot restoration and budget
pressure releasing dead wide buffers.


## Complete SQL jobs

A separate quiet-host release pair used the Maven `bench` profile (mimalloc), JDK 17, Flink 2.2.1,
parallelism 1, 262,144 row-fed inputs, 16,384 keys and an 8-byte nullable payload suffix. Each
engine had two warmup jobs and five retained jobs, alternating stock/native order. Production
NativeDeduplicate and both row/Arrow transposes were required in the plan, with a nonzero
runtime substitution count for every native trial. The timed `INSERT` awaited complete job
execution: planning, startup, source records, conversions, JNI, task-budget accounting, terminal
watermark state drain and the rowwise blackhole sink. Environment creation and parity were untimed.

Native median fell **317.284→174.058 ms (1.82×)**. The candidate also beat its same-run stock
control, **284.318 ms versus 174.058 ms (1.63×)**. The stock median itself rose 17.7% between runs,
so this is an observed bounded-job comparison, not a sustained-throughput or startup-free claim.
All five candidate native jobs were faster than all five baseline native jobs.

| Revision / engine | Median ms | Min–max ms | IQR ms |
| --- | ---: | ---: | ---: |
| Before / stock | 241.494 | 200.686–346.829 | 66.685 |
| Before / native | 317.284 | 306.231–389.874 | 23.929 |
| After / stock | 284.318 | 216.724–383.406 | 65.055 |
| After / native | 174.058 | 158.117–227.419 | 22.211 |

The untimed stock/native kinded-output witness used 4,096 rows and 2,048 keys, above the hybrid
threshold's key domain, with equal-time ties and nullable payloads. Both runs passed it.
[Complete-job raw trials](../benchmarks/results/pending-dedup-full-job.json) preserve every
observation, including outliers, inclusive quartiles/IQR, the latest Java fixture SHA-256 and
earlier pairs explicitly marked provisional because external benchmarks overlapped them.
Only the authoritative quiet pair supplies the conclusions above.
