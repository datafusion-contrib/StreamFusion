# Streaming Iceberg Nexmark observations

2,000,000 deterministic events; parallelism 4; four-bucket input/output tables. All times include streaming completion and final drain/commit.

| Query | State / mini-batch | Variant | StreamFusion with stock Iceberg IO median [range], s | StreamFusion median [range], s | Median ratio |
|---|---|---|---:|---:|---:|
| q0 | disk-off | exact | 1.120 [1.095–1.184] | 0.629 [0.603–0.633] | 1.78× |
| q0 | memory-off | exact | 1.185 [1.155–1.537] | 0.657 [0.642–0.684] | 1.80× |
| q3 | disk-off | exact | 0.583 [0.571–0.609] | 0.604 [0.567–0.674] | 0.96× |
| q3 | memory-off | exact | 0.499 [0.482–0.501] | 0.446 [0.433–0.468] | 1.12× |
| q4 | disk-off | exact | 19.692 [9.537–29.457] | 20.823 [16.762–28.009] | 0.95× |
| q4 | memory-off | exact | 9.572 [3.034–16.923] | 12.097 [2.185–20.627] | 0.79× |
| q9 | disk-off | exact | 19.834 [13.563–36.062] | 40.174 [24.850–45.826] | 0.49× |
| q9 | memory-off | exact | 37.880 [30.560–42.000] | 4.667 [4.559–26.471] | 8.12× |

## Exact-query aggregates

Geometric means use every exact-query cell in each mode; labelled expression variants remain separate above. Throughput is nominal corpus events divided by median end-to-end time, including startup, checkpoints and drain.

| State / mini-batch | Queries | Geomean ratio | StreamFusion with stock Iceberg IO corpus events/s, range | StreamFusion corpus events/s, range |
|---|---:|---:|---:|---:|
| disk-off | 4 | 0.95× | 100,837–3,433,234 | 49,783–3,310,518 |
| memory-off | 4 | 1.90× | 52,799–4,008,403 | 165,332–4,484,866 |

## README query selection

Each query contributes once per mode, using its existing labelled variant when present. Median ratios retain all repetitions. The secondary headline sample uses the first memory sample or the best of the first two disk samples.

| State / mini-batch | Queries | Geomean median ratio | Geomean headline-sample ratio |
|---|---:|---:|---:|
| disk-off | 4 | 0.95× | 0.86× |
| memory-off | 4 | 1.90× | 2.03× |

## Headline sampling convention

Memory uses the first measured sample; disk uses the best of the first two. The repeated medians and complete ranges above remain the primary results.

| Query | State / mini-batch | Variant | StreamFusion with stock Iceberg IO, s | StreamFusion, s | Ratio |
|---|---|---|---:|---:|---:|
| q0 | disk-off | exact | 1.120 | 0.629 | 1.78× |
| q0 | memory-off | exact | 1.537 | 0.684 | 2.25× |
| q3 | disk-off | exact | 0.571 | 0.567 | 1.01× |
| q3 | memory-off | exact | 0.482 | 0.468 | 1.03× |
| q4 | disk-off | exact | 9.537 | 16.762 | 0.57× |
| q4 | memory-off | exact | 9.572 | 12.097 | 0.79× |
| q9 | disk-off | exact | 13.563 | 24.850 | 0.55× |
| q9 | memory-off | exact | 42.000 | 4.559 | 9.21× |

Raw samples, warmups, parity checks, plans, committed table locations and native/fallback task/file witnesses are retained in this artifact directory. PROCTIME q12 validates per-bidder count conservation and includes waiting for the final processing-time window.

[Per-cell timing, checkpoint and sampled-memory metrics](metrics.csv) retain separate phase medians. Phase medians need not sum to the total median. Memory peaks are sampled during progress polling, not exhaustive job maxima.
