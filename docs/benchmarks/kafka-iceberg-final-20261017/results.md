# Streaming Iceberg Nexmark observations

2,000,000 deterministic events; parallelism 4; four-partition Kafka JSON topic; earliest to captured latest offsets; four-bucket Iceberg output tables. Completion: finite Kafka offsets, streaming end-of-input and final Iceberg commit.

| Query | State / mini-batch | Variant | Flink median [range], s | StreamFusion median [range], s | Median ratio |
|---|---|---|---:|---:|---:|
| q0 | disk-off | exact | 1.914 [1.898–2.092] | 4.709 [1.696–6.955] | 0.41× |
| q1 | disk-off | approximate decimal (incompatible) | 1.644 [1.627–1.699] | 1.134 [1.131–1.138] | 1.45× |
| q1 | disk-off | exact | 1.766 [1.702–2.004] | 1.229 [1.219–1.273] | 1.44× |
| q2 | disk-off | exact | 1.227 [1.213–1.235] | 1.131 [1.122–1.194] | 1.08× |
| q3 | disk-off | exact | 1.207 [1.193–1.209] | 1.209 [1.209–1.221] | 1.00× |
| q4 | disk-off | exact | 22.228 [6.460–30.320] | 6.005 [5.374–20.367] | 3.70× |
| q5 | disk-off | exact | 3.482 [3.417–7.135] | 4.997 [4.766–9.721] | 0.70× |
| q7 | disk-off | exact | 20.495 [5.486–21.104] | 3.357 [3.334–3.423] | 6.11× |
| q8 | disk-off | exact | 1.294 [1.293–1.321] | 1.377 [1.309–1.891] | 0.94× |
| q9 | disk-off | exact | 16.352 [8.693–38.225] | 11.933 [7.164–31.627] | 1.37× |
| q10 | disk-off | exact | 1.926 [1.858–1.939] | 1.439 [1.435–1.741] | 1.34× |
| q10 | disk-off | native datetime (incompatible) | 2.321 [1.925–2.482] | 1.361 [1.351–1.371] | 1.71× |
| q11 | disk-off | exact | 5.192 [5.184–5.239] | 1.081 [1.070–1.129] | 4.80× |
| q12 | disk-off | exact | 1.429 [1.415–1.948] | 1.175 [1.168–1.191] | 1.22× |
| q13 | disk-off | exact | 1.816 [1.687–1.845] | 1.394 [1.256–1.516] | 1.30× |
| q14 | disk-off | exact | 1.997 [1.805–6.946] | 1.562 [1.549–1.669] | 1.28× |
| q14 | disk-off | native datetime (incompatible) | 1.866 [1.835–1.987] | 1.685 [1.517–2.051] | 1.11× |
| q15 | disk-off | exact | 11.525 [11.490–11.590] | 10.558 [10.533–10.575] | 1.09× |
| q15 | disk-off | native datetime (incompatible) | 12.110 [11.323–13.577] | 13.355 [10.620–13.838] | 0.91× |
| q16 | disk-off | exact | 7.117 [7.061–7.268] | 7.759 [7.471–12.632] | 0.92× |
| q16 | disk-off | native datetime (incompatible) | 7.276 [7.246–7.563] | 7.085 [7.060–7.229] | 1.03× |
| q17 | disk-off | exact | 6.581 [6.177–9.151] | 6.667 [3.407–6.784] | 0.99× |
| q17 | disk-off | native datetime (incompatible) | 4.186 [4.075–4.190] | 3.263 [3.194–3.392] | 1.28× |
| q18 | disk-off | exact | 16.277 [5.988–41.599] | 4.270 [3.998–25.891] | 3.81× |
| q19 | disk-off | exact | 14.719 [12.357–23.550] | 26.405 [24.030–31.231] | 0.56× |
| q20 | disk-off | exact | 4.769 [4.106–9.262] | 4.445 [4.195–34.564] | 1.07× |
| q21 | disk-off | exact | 1.572 [1.557–1.655] | 1.501 [1.471–1.685] | 1.05× |
| q21 | disk-off | native regex/case (incompatible) | 1.599 [1.511–1.748] | 1.119 [1.118–1.124] | 1.43× |
| q22 | disk-off | exact | 1.825 [1.823–1.860] | 1.236 [1.216–1.240] | 1.48× |
| q23 | disk-off | exact | 28.388 [21.906–50.334] | 4.037 [4.002–4.073] | 7.03× |

## Exact-query aggregates

Geometric means use every exact-query cell in each mode; labelled expression variants remain separate above. Throughput is nominal corpus events divided by median end-to-end time, including startup, checkpoints and drain.

| State / mini-batch | Queries | Geomean ratio | Flink corpus events/s, range | StreamFusion corpus events/s, range |
|---|---:|---:|---:|---:|
| disk-off | 23 | 1.44× | 70,452–1,656,895 | 75,744–1,850,537 |

## README query selection

Each query contributes once per mode, using its existing labelled variant when present. Median ratios retain all repetitions. The secondary headline sample uses the first memory sample or the best of the first two disk samples.

| State / mini-batch | Queries | Geomean median ratio | Geomean headline-sample ratio |
|---|---:|---:|---:|
| disk-off | 23 | 1.47× | 1.24× |

## Headline sampling convention

Memory uses the first measured sample; disk uses the best of the first two. The repeated medians and complete ranges above remain the primary results.

| Query | State / mini-batch | Variant | Flink, s | StreamFusion, s | Ratio |
|---|---|---|---:|---:|---:|
| q0 | disk-off | exact | 1.914 | 4.709 | 0.41× |
| q1 | disk-off | approximate decimal (incompatible) | 1.627 | 1.131 | 1.44× |
| q1 | disk-off | exact | 1.766 | 1.219 | 1.45× |
| q2 | disk-off | exact | 1.213 | 1.122 | 1.08× |
| q3 | disk-off | exact | 1.207 | 1.209 | 1.00× |
| q4 | disk-off | exact | 6.460 | 5.374 | 1.20× |
| q5 | disk-off | exact | 3.417 | 4.766 | 0.72× |
| q7 | disk-off | exact | 5.486 | 3.357 | 1.63× |
| q8 | disk-off | exact | 1.293 | 1.309 | 0.99× |
| q9 | disk-off | exact | 8.693 | 7.164 | 1.21× |
| q10 | disk-off | exact | 1.858 | 1.435 | 1.30× |
| q10 | disk-off | native datetime (incompatible) | 2.321 | 1.351 | 1.72× |
| q11 | disk-off | exact | 5.184 | 1.070 | 4.85× |
| q12 | disk-off | exact | 1.415 | 1.168 | 1.21× |
| q13 | disk-off | exact | 1.687 | 1.394 | 1.21× |
| q14 | disk-off | exact | 1.805 | 1.562 | 1.16× |
| q14 | disk-off | native datetime (incompatible) | 1.866 | 1.517 | 1.23× |
| q15 | disk-off | exact | 11.490 | 10.533 | 1.09× |
| q15 | disk-off | native datetime (incompatible) | 12.110 | 13.355 | 0.91× |
| q16 | disk-off | exact | 7.061 | 7.471 | 0.95× |
| q16 | disk-off | native datetime (incompatible) | 7.246 | 7.060 | 1.03× |
| q17 | disk-off | exact | 6.581 | 6.667 | 0.99× |
| q17 | disk-off | native datetime (incompatible) | 4.075 | 3.194 | 1.28× |
| q18 | disk-off | exact | 5.988 | 3.998 | 1.50× |
| q19 | disk-off | exact | 12.357 | 26.405 | 0.47× |
| q20 | disk-off | exact | 4.106 | 4.195 | 0.98× |
| q21 | disk-off | exact | 1.557 | 1.501 | 1.04× |
| q21 | disk-off | native regex/case (incompatible) | 1.511 | 1.119 | 1.35× |
| q22 | disk-off | exact | 1.823 | 1.236 | 1.47× |
| q23 | disk-off | exact | 21.906 | 4.002 | 5.47× |

Raw samples, warmups, parity checks, plans, committed table locations and native/fallback task/file witnesses are retained in this artifact directory. PROCTIME q12 validates per-bidder emitted counts do not exceed input counts; unfinished processing-time windows remain open.

[Per-cell timing, checkpoint and sampled-memory metrics](metrics.csv) retain separate phase medians. Phase medians need not sum to the total median. Memory peaks are sampled during progress polling, not exhaustive job maxima.
