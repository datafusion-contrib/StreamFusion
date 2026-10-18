# Streaming Iceberg Nexmark observations

2,000,000 deterministic events; parallelism 4; four-bucket input/output tables. All times include streaming completion and final drain/commit.

| Query | State / mini-batch | Variant | Flink median [range], s | StreamFusion median [range], s | Median ratio |
|---|---|---|---:|---:|---:|
| q0 | memory-off | exact | 1.140 [1.050–1.236] | 0.670 [0.585–0.691] | 1.70× |
| q1 | memory-off | exact | 1.113 [1.111–1.122] | 0.635 [0.589–0.698] | 1.75× |

## Exact-query aggregates

Geometric means use every exact-query cell in each mode; labelled expression variants remain separate above. Throughput is nominal corpus events divided by median end-to-end time, including startup, checkpoints and drain.

| State / mini-batch | Queries | Geomean ratio | Flink corpus events/s, range | StreamFusion corpus events/s, range |
|---|---:|---:|---:|---:|
| memory-off | 2 | 1.73× | 1,754,085–1,797,341 | 2,985,459–3,147,590 |

## README query selection

Each query contributes once per mode, using its existing labelled variant when present. Median ratios retain all repetitions. The secondary headline sample uses the first memory sample or the best of the first two disk samples.

| State / mini-batch | Queries | Geomean median ratio | Geomean headline-sample ratio |
|---|---:|---:|---:|
| memory-off | 2 | 1.73× | 1.85× |

## Headline sampling convention

Memory uses the first measured sample; disk uses the best of the first two. The repeated medians and complete ranges above remain the primary results.

| Query | State / mini-batch | Variant | Flink, s | StreamFusion, s | Ratio |
|---|---|---|---:|---:|---:|
| q0 | memory-off | exact | 1.236 | 0.691 | 1.79× |
| q1 | memory-off | exact | 1.122 | 0.589 | 1.91× |

Raw samples, warmups, parity checks, plans, committed table locations and native/fallback task/file witnesses are retained in this artifact directory. PROCTIME q12 validates per-bidder count conservation and includes waiting for the final processing-time window.

[Per-cell timing, checkpoint and sampled-memory metrics](metrics.csv) retain separate phase medians. Phase medians need not sum to the total median. Memory peaks are sampled during progress polling, not exhaustive job maxima.
