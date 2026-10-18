# Streaming Iceberg Nexmark observations

2,000,000 deterministic events; parallelism 4; four-partition Kafka JSON topic; earliest to captured latest offsets; four-bucket Iceberg output tables. Completion: finite Kafka offsets, streaming end-of-input and final Iceberg commit.

| Query | State / mini-batch | Variant | Flink median [range], s | StreamFusion median [range], s | Median ratio |
|---|---|---|---:|---:|---:|
| q0 | disk-off | exact | 2.255 [2.255–2.255] | 1.361 [1.361–1.361] | 1.66× |
| q19 | disk-off | exact | 30.723 [30.723–30.723] | 9.487 [9.487–9.487] | 3.24× |

## Exact-query aggregates

Geometric means use every exact-query cell in each mode; labelled expression variants remain separate above. Throughput is nominal corpus events divided by median end-to-end time, including startup, checkpoints and drain.

| State / mini-batch | Queries | Geomean ratio | Flink corpus events/s, range | StreamFusion corpus events/s, range |
|---|---:|---:|---:|---:|
| disk-off | 2 | 2.32× | 65,098–887,044 | 210,823–1,469,608 |

## README query selection

Each query contributes once per mode, using its existing labelled variant when present. Median ratios retain all repetitions. The secondary headline sample uses the first memory sample or the best of the first two disk samples.

| State / mini-batch | Queries | Geomean median ratio | Geomean headline-sample ratio |
|---|---:|---:|---:|
| disk-off | 2 | 2.32× | 2.32× |

Raw samples, warmups, parity checks, plans, committed table locations and native/fallback task/file witnesses are retained in this artifact directory. PROCTIME q12 validates per-bidder emitted counts do not exceed input counts; unfinished processing-time windows remain open.

[Per-cell timing, checkpoint and sampled-memory metrics](metrics.csv) retain separate phase medians. Phase medians need not sum to the total median. Memory peaks are sampled during progress polling, not exhaustive job maxima.
