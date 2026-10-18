# Streaming Iceberg Nexmark observations

2,000,000 deterministic events; parallelism 4; four-bucket input/output tables. All times include streaming completion and final drain/commit.

| Query | State / mini-batch | Variant | Flink median [range], s | StreamFusion median [range], s | Median ratio |
|---|---|---|---:|---:|---:|
| q4 | disk-on | exact | 46.452 [46.452–46.452] | 2.079 [2.079–2.079] | 22.34× |
| q4 | memory-off | exact | 6.741 [6.741–6.741] | 3.624 [3.624–3.624] | 1.86× |

## Exact-query aggregates

Geometric means use every exact-query cell in each mode; labelled expression variants remain separate above. Throughput is nominal corpus events divided by median end-to-end time, including startup, checkpoints and drain.

| State / mini-batch | Queries | Geomean ratio | Flink corpus events/s, range | StreamFusion corpus events/s, range |
|---|---:|---:|---:|---:|
| disk-on | 1 | 22.34× | 43,056–43,056 | 962,017–962,017 |
| memory-off | 1 | 1.86× | 296,698–296,698 | 551,873–551,873 |

Raw samples, warmups, parity checks, plans, committed table locations and native/fallback task/file witnesses are retained in this artifact directory. PROCTIME q12 validates per-bidder count conservation and includes waiting for the final processing-time window.
