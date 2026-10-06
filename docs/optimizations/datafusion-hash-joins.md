# Delegate joins to DataFusion's hash join

**Applies to:** time-bounded joins (interval join, window join)

The time-bounded joins buffer and evict, but the match itself runs as a `HashJoinExec` over the
buffered batches (Arroyo's split), putting the O(n·m) work on a vectorized, maintained join — the
join benches run at **20–40 M elements/s**.

Reusing one `TaskContext` instead of rebuilding a `SessionContext` (and its whole function
registry) per pushed batch later cut the join hot loops roughly in half: interval join **115 → 63
µs**, window join **184 → 130 µs** per 4096-row batch.

The memory interval join keeps the original full-batch probe and eager cleanup for sides with
at most 1,024 retained rows. Above that threshold it indexes retained rows by encoded equi-key.
Incoming probes
retrieve only the opposite rows for their enabled keys, following Arroyo's `JoinWithExpiration`
key-time-table lookup before its DataFusion join. Locators are sorted by original batch/row order
before gathering, so duplicate incoming keys do not duplicate cached inputs and outer matched-row
IDs retain their original meaning.

The index stores no Arrow payload ownership and is derived rather than checkpointed. Arrival
retention promotes a side above the threshold and updates an active index immediately; cleanup,
watermark retirement, restoration, and timestamp
key precision changes rebuild it. Native memory accounting includes encoded keys, map capacity,
locator vectors, and timestamp minima. RocksDB continues to use its existing grouped state scans.

Cached minimum timestamps let eager cleanup skip a side or batch when every cached row is still
in the future. When expiration is possible, cached locators limit cleanup to probed keys and
batches, preserving first-probe matching, pending cleanup timers, and null padding order. These
changes remove repeated full-state key encoding from future-only selective probes; production
`interval_probe` Criterion fixtures measure narrow and wide state with varying touched keys.

Index byte accounting caches owned key and locator capacity totals, so checking the retained
metadata budget takes constant time. Deleting cached rows rebuilds derived locators over surviving
state and drops the index when at most 1,024 rows remain. The optimization bounds the payload
gather and join inputs to probed keys; it does not make every arrival or cleanup proportional only
to touched keys. Checkpoint serialization and RocksDB lookup costs are unchanged.

## Selective-probe measurements

Final quiet-host release measurements used the default Rust system allocator, 10 samples,
0.3-second warmup and 0.5-second measurement per case, against production baseline `96f98a32`
with fixtures `8953dc84`. Above the threshold, selective probes improved **2.84–9.20×**.
The 1,024-row path keeps the original full-state implementation: its mean ratios were
**0.97–1.15×**, with overlapping 95% intervals. An always-indexed candidate slowed the narrow
1,024-row/256-arrival case about 23%; the hybrid avoids that overhead.

| Shape (keys / arrivals / suffix bytes) | Before mean µs (95% CI) | After mean µs (95% CI) | Before / after |
| --- | ---: | ---: | ---: |
| 1024 / 16 / 264 | 59.25 (57.48–61.24) | 61.01 (59.77–62.31) | 0.97× |
| 1024 / 16 / 8 | 58.63 (56.74–60.60) | 57.24 (54.77–61.01) | 1.02× |
| 1024 / 256 / 264 | 133.40 (120.56–152.02) | 115.81 (111.23–121.20) | 1.15× |
| 1024 / 256 / 8 | 103.07 (98.46–108.64) | 100.60 (98.99–102.35) | 1.02× |
| 16384 / 16 / 264 | 828.86 (763.92–891.48) | 90.08 (87.32–92.87) | 9.20× |
| 16384 / 16 / 8 | 360.54 (353.71–367.96) | 85.38 (81.54–89.56) | 4.22× |
| 16384 / 256 / 264 | 598.20 (565.43–635.92) | 154.84 (150.47–159.58) | 3.86× |
| 16384 / 256 / 8 | 402.01 (395.29–408.75) | 141.51 (139.05–143.88) | 2.84× |

The timed operation is one production INNER join push against preloaded unique left keys.
Arrivals touch 16 or 256 retained keys, timestamps are 1000 and bounds are [-100,+100]. Setup,
result validation and state teardown are outside timing. Nullable payload suffixes are 8/264
bytes plus generated row prefixes. This exercises future-only probes, without eager deletion,
watermark cleanup or snapshots. The unaccounted fixture excludes budget-accounting costs.
It measures neither JNI nor row/Arrow transposes, Flink execution, sinks or the RocksDB path.

[Raw measurements and source fingerprints](../benchmarks/results/interval-probe-criterion.json)
retain all sample iteration/time arrays and 95% confidence intervals. Speedups are ratios of
means, not confidence intervals for those ratios. Native engine validation passed 656 tests
with 2 ignored, including active-index INNER/FULL eager matching, matched flags, restoration,
threshold demotion, timer cleanup and unmatched padding after the final drain.


## Complete selective interval SQL jobs

The separate quiet-host release pair used the Maven `bench` profile (mimalloc), JDK 17,
Flink 2.2.1, parallelism 1, 65,536 left rows, 16,384 keys, 4,096 right probes across 256 sparse
keys and 264-byte nullable payload suffixes. Two warmups preceded five measured jobs per engine,
with alternating stock/native order. The plan required NativeIntervalJoin and both row/Arrow
transposes; every native job required runtime substitutions. Complete `INSERT` execution kept
planning/startup, row-fed sources, conversions, JNI, real task-budget accounting, terminal
watermark cleanup and the rowwise blackhole sink in timing. Setup and parity remained outside.

This workload showed **no demonstrated full-job gain or stable regression**. Native median
rose 133.554→144.959 ms (+8.5%) while the stock control rose 152.195→166.387 ms (+9.3%).
Stock/native ratios stayed similar: **1.140× before and 1.148× after**. Native trial ranges
overlap substantially. Unlike Criterion's fixed preloaded retained state, concurrent SQL input
scheduling varies the amount of opposite state at each probe, and startup is included. The
**2.84–9.20× selective kernel improvement does not imply a comparable complete-job gain**.

| Revision / engine | Median ms | Min–max ms | IQR ms |
| --- | ---: | ---: | ---: |
| Before / stock | 152.195 | 141.149–224.992 | 78.992 |
| Before / native | 133.554 | 118.468–195.179 | 62.924 |
| After / stock | 166.387 | 148.023–262.600 | 47.047 |
| After / native | 144.959 | 131.984–219.999 | 42.768 |

The untimed stock/native witness used 4,096 left rows, 2,048 keys, 128 right probes and 8 sparse
matching keys, with nullable payloads. Both runs passed output parity.
[Complete-job raw trials](../benchmarks/results/interval-probe-full-job.json) retain every
observation and outlier, inclusive quartiles/IQR, the latest Java fixture SHA-256 and earlier
contended pairs explicitly marked provisional. Only the final authoritative pair supports
the full-job comparison above.
