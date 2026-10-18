# Streaming Iceberg Nexmark observations

2,000,000 deterministic events; parallelism 4; four-bucket input/output tables. All times include streaming completion and final drain/commit.

| Query | State / mini-batch | Variant | Flink median [range], s | StreamFusion median [range], s | Median ratio |
|---|---|---|---:|---:|---:|
| q3 | disk-off | exact | 0.644 [0.644–0.644] | 0.651 [0.651–0.651] | 0.99× |
| q3 | memory-off | exact | 0.737 [0.737–0.737] | 0.713 [0.713–0.713] | 1.03× |
| q3 | memory-on | exact | 0.604 [0.604–0.604] | 0.605 [0.605–0.605] | 1.00× |
| q4 | disk-off | exact | 72.896 [72.896–72.896] | 16.294 [16.294–16.294] | 4.47× |
| q4 | memory-off | exact | 3.431 [3.431–3.431] | 16.779 [16.779–16.779] | 0.20× |
| q4 | memory-on | exact | 10.680 [10.680–10.680] | 28.032 [28.032–28.032] | 0.38× |

## Exact-query aggregates

Geometric means use every exact-query cell in each mode; labelled expression variants remain separate above. Throughput is nominal corpus events divided by median end-to-end time, including startup, checkpoints and drain.

| State / mini-batch | Queries | Geomean ratio | Flink corpus events/s, range | StreamFusion corpus events/s, range |
|---|---:|---:|---:|---:|
| disk-off | 2 | 2.10× | 27,436–3,103,596 | 122,746–3,071,904 |
| memory-off | 2 | 0.46× | 582,950–2,714,470 | 119,200–2,803,333 |
| memory-on | 2 | 0.62× | 187,259–3,311,161 | 71,347–3,308,051 |

Raw samples, warmups, parity checks, plans, committed table locations and native/fallback task/file witnesses are retained in this artifact directory. PROCTIME q12 validates per-bidder count conservation and includes waiting for the final processing-time window.
