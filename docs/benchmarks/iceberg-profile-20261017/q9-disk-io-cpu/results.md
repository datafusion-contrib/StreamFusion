# Streaming Iceberg Nexmark observations

2,000,000 deterministic events; parallelism 4; four-bucket input/output tables. All times include streaming completion and final drain/commit.

| Query | State / mini-batch | Variant | StreamFusion with stock Iceberg IO median [range], s | StreamFusion median [range], s | Median ratio |
|---|---|---|---:|---:|---:|
| q9 | disk-off | exact | 28.736 [28.736–28.736] | 125.693 [125.693–125.693] | 0.23× |

## Exact-query aggregates

Geometric means use every exact-query cell in each mode; labelled expression variants remain separate above. Throughput is nominal corpus events divided by median end-to-end time, including startup, checkpoints and drain.

| State / mini-batch | Queries | Geomean ratio | StreamFusion with stock Iceberg IO corpus events/s, range | StreamFusion corpus events/s, range |
|---|---:|---:|---:|---:|
| disk-off | 1 | 0.23× | 69,598–69,598 | 15,912–15,912 |

## README query selection

Each query contributes once per mode, using its existing labelled variant when present. Median ratios retain all repetitions. The secondary headline sample uses the first memory sample or the best of the first two disk samples.

| State / mini-batch | Queries | Geomean median ratio | Geomean headline-sample ratio |
|---|---:|---:|---:|
| disk-off | 1 | 0.23× | 0.23× |

Raw samples, warmups, parity checks, plans, committed table locations and native/fallback task/file witnesses are retained in this artifact directory. PROCTIME q12 validates per-bidder count conservation and includes waiting for the final processing-time window.

[Per-cell timing, checkpoint and sampled-memory metrics](metrics.csv) retain separate phase medians. Phase medians need not sum to the total median. Memory peaks are sampled during progress polling, not exhaustive job maxima.
