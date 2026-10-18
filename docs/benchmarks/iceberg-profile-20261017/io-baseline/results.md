# Streaming Iceberg Nexmark observations

2,000,000 deterministic events; parallelism 4; four-bucket input/output tables. All times include streaming completion and final drain/commit.

| Query | State / mini-batch | Variant | StreamFusion with stock Iceberg IO median [range], s | StreamFusion median [range], s | Median ratio |
|---|---|---|---:|---:|---:|
| q3 | disk-off | exact | 0.629 [0.621–0.752] | 0.611 [0.608–0.731] | 1.03× |
| q3 | memory-off | exact | 0.676 [0.525–0.808] | 0.618 [0.582–0.683] | 1.09× |
| q3 | memory-on | exact | 0.440 [0.430–0.481] | 0.422 [0.419–0.449] | 1.04× |
| q4 | disk-off | exact | 47.419 [19.557–50.285] | 17.557 [14.450–37.906] | 2.70× |
| q4 | memory-off | exact | 9.980 [3.026–14.397] | 21.511 [5.434–30.415] | 0.46× |
| q4 | memory-on | exact | 1.800 [1.595–22.173] | 3.521 [1.320–9.591] | 0.51× |

## Exact-query aggregates

Geometric means use every exact-query cell in each mode; labelled expression variants remain separate above. Throughput is nominal corpus events divided by median end-to-end time, including startup, checkpoints and drain.

| State / mini-batch | Queries | Geomean ratio | StreamFusion with stock Iceberg IO corpus events/s, range | StreamFusion corpus events/s, range |
|---|---:|---:|---:|---:|
| disk-off | 2 | 1.67× | 42,177–3,181,999 | 113,918–3,274,728 |
| memory-off | 2 | 0.71× | 200,400–2,960,139 | 92,975–3,235,522 |
| memory-on | 2 | 0.73× | 1,110,922–4,545,899 | 567,949–4,743,924 |

## Headline sampling convention

Memory uses the first measured sample; disk uses the best of the first two. The repeated medians and complete ranges above remain the primary results.

| Query | State / mini-batch | Variant | StreamFusion with stock Iceberg IO, s | StreamFusion, s | Ratio |
|---|---|---|---:|---:|---:|
| q3 | disk-off | exact | 0.621 | 0.611 | 1.02× |
| q3 | memory-off | exact | 0.808 | 0.683 | 1.18× |
| q3 | memory-on | exact | 0.481 | 0.419 | 1.15× |
| q4 | disk-off | exact | 19.557 | 14.450 | 1.35× |
| q4 | memory-off | exact | 14.397 | 30.415 | 0.47× |
| q4 | memory-on | exact | 22.173 | 3.521 | 6.30× |

Raw samples, warmups, parity checks, plans, committed table locations and native/fallback task/file witnesses are retained in this artifact directory. PROCTIME q12 validates per-bidder count conservation and includes waiting for the final processing-time window.
