# Streaming Iceberg Nexmark observations

2,000,000 deterministic events; parallelism 4; four-bucket input/output tables. All times include streaming completion and final drain/commit.

| Query | State / mini-batch | Variant | Flink median [range], s | StreamFusion median [range], s | Median ratio |
|---|---|---|---:|---:|---:|
| q0 | memory-off | exact | 1.099 [1.051–1.252] | 0.719 [0.611–0.723] | 1.53× |

## Exact-query aggregates

Geometric means use every exact-query cell in each mode; labelled expression variants remain separate above. Throughput is nominal corpus events divided by median end-to-end time, including startup, checkpoints and drain.

| State / mini-batch | Queries | Geomean ratio | Flink corpus events/s, range | StreamFusion corpus events/s, range |
|---|---:|---:|---:|---:|
| memory-off | 1 | 1.53× | 1,819,107–1,819,107 | 2,782,189–2,782,189 |

## README query selection

Each query contributes once per mode, using its existing labelled variant when present. Median ratios retain all repetitions. The secondary headline sample uses the first memory sample or the best of the first two disk samples.

| State / mini-batch | Queries | Geomean median ratio | Geomean headline-sample ratio |
|---|---:|---:|---:|
| memory-off | 1 | 1.53× | 1.74× |

## Headline sampling convention

Memory uses the first measured sample; disk uses the best of the first two. The repeated medians and complete ranges above remain the primary results.

| Query | State / mini-batch | Variant | Flink, s | StreamFusion, s | Ratio |
|---|---|---|---:|---:|---:|
| q0 | memory-off | exact | 1.252 | 0.719 | 1.74× |

Raw samples, warmups, parity checks, plans, committed table locations and native/fallback task/file witnesses are retained in this artifact directory. PROCTIME q12 validates per-bidder count conservation and includes waiting for the final processing-time window.

[Per-cell timing, checkpoint and sampled-memory metrics](metrics.csv) retain separate phase medians. Phase medians need not sum to the total median. Memory peaks are sampled during progress polling, not exhaustive job maxima.
