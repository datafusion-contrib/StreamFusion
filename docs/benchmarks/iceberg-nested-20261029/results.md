# Repeated results

Elapsed seconds: median [minimum–maximum] across three measured repetitions. Ratios are stock/current or previous/current; higher is faster. Startup is separate.

## Running streaming maintenance jobs

| Rows | Case | Stock, s | Native, s | Ratio |
|---:|---|---:|---:|---:|
| 400k | sort-memory | 1.457 [1.430–1.531] | 0.450 [0.415–0.477] | 3.24× |
| 400k | sort-spill | 1.283 [1.169–1.321] | 0.426 [0.413–0.429] | 3.01× |
| 400k | equality-composite-v3 | 0.616 [0.587–0.767] | 0.333 [0.301–0.395] | 1.85× |
| 2m | sort-memory | 6.906 [6.904–6.981] | 1.170 [1.100–1.176] | 5.90× |
| 2m | sort-spill | 5.745 [5.646–5.850] | 1.322 [1.279–1.326] | 4.35× |
| 2m | equality-composite-v3 | 1.766 [1.431–2.050] | 0.624 [0.608–1.025] | 2.83× |

## File workers

Previous/native ratios compare separate cohorts on the same host, not paired processes. Nested schemas were not previously admitted; their previous-native entry is absent. For unsorted nested files, the stock worker represents the previously available continuous fallback. Sorted nested controls add the production Flink sorter because the released maintenance worker does not sort.

| Rows | Case | Stock, s | Current native, s | Stock/current | Previous native, s | Previous/current |
|---:|---|---:|---:|---:|---:|---:|
| 400k | sort-memory | 0.975 [0.971–1.235] | 0.253 [0.242–0.298] | 3.85× | 0.252 [0.242–0.275] | 1.00× |
| 400k | sort-spill | 1.027 [1.025–1.054] | 0.229 [0.218–0.232] | 4.49× | 0.223 [0.219–0.229] | 0.98× |
| 400k | equality-int-v2 | 0.221 [0.218–0.235] | 0.081 [0.081–0.085] | 2.72× | 0.082 [0.081–0.085] | 1.00× |
| 400k | equality-string-v2 | 0.281 [0.277–0.281] | 0.076 [0.073–0.078] | 3.68× | 0.074 [0.072–0.082] | 0.97× |
| 400k | equality-composite-v2 | 0.254 [0.253–0.260] | 0.074 [0.073–0.076] | 3.43× | 0.205 [0.203–0.211] | 2.77× |
| 400k | equality-composite-v3 | 0.289 [0.286–0.295] | 0.106 [0.105–0.108] | 2.72× | 0.239 [0.239–0.243] | 2.25× |
| 400k | nested-append | 0.538 [0.533–0.573] | 0.234 [0.230–0.238] | 2.30× | — | — |
| 400k | nested-sort | 2.107 [1.832–2.573] | 0.342 [0.338–0.361] | 6.17× | — | — |
| 2m | sort-memory | 7.403 [7.386–7.888] | 0.846 [0.833–0.851] | 8.75× | 0.887 [0.861–0.935] | 1.05× |
| 2m | sort-spill | 5.883 [5.848–5.884] | 13.760 [2.970–24.903] | 0.43× | 1.080 [1.073–1.094] | 0.08× |
| 2m | equality-int-v2 | 0.864 [0.863–0.880] | 0.277 [0.272–0.281] | 3.11× | 0.269 [0.266–0.279] | 0.97× |
| 2m | equality-string-v2 | 1.003 [0.985–1.012] | 0.261 [0.250–0.266] | 3.84× | 0.264 [0.261–0.267] | 1.01× |
| 2m | equality-composite-v2 | 1.023 [1.015–1.044] | 0.266 [0.265–0.271] | 3.84× | 0.910 [0.903–0.918] | 3.42× |
| 2m | equality-composite-v3 | 1.203 [1.200–1.208] | 0.365 [0.357–0.370] | 3.30× | 0.984 [0.979–1.002] | 2.70× |
| 2m | nested-append | 2.559 [2.532–2.559] | 0.975 [0.963–0.996] | 2.62× | — | — |
| 2m | nested-sort | 10.717 [10.462–10.831] | 2.983 [2.703–3.124] | 3.59× | — | — |

## Spill variability and isolated diagnostics

A read-only repository search overlapped the full 2M file cohort on this host, so that cohort was not isolated from competing host IO. It has native spill samples of 2.970, 13.760 and 24.903 s; all are retained above, including the slower-than-stock median. The earlier 400k previous-native cohort also has 3.327–4.250 s spill samples, versus approximately 0.2 s in a later repeat. The initial previous 400k cohort also received a brief perf probe during equality work and is retained as diagnostic evidence, excluded from the previous/native headline table. The overlap is a confound; no causal attribution for the outliers is established. The streaming cohorts completed before that search.

Separate fresh-process 2M spill repeats add GC/safepoint logging to both engines. They reproduce approximately one-second native work for both versions; these repeats are not substituted for the primary slower measurements. Native sort changes in this PR add nested key extraction and metrics; flat external sorting was already supported. Continuous forced-spill commit latencies remain faster at both sizes.

| Repeat | Stock, s | Native, s | Stock/native |
|---|---:|---:|---:|
| 2m-current-spill-repeat | 5.477 [5.475–5.485] | 1.045 [0.973–1.053] | 5.24× |
| 2m-previous-spill-repeat | 5.547 [5.433–5.712] | 12.724 [2.989–18.652] | 0.44× |

## Startup plus first rewrite

One cold observation per engine/case, not a repeated distribution. Spill fields in these raw streaming warmup records were not observed.

| Rows | Case | Stock, s | Native, s |
|---:|---|---:|---:|
| 400k | sort-memory | 4.085 | 1.412 |
| 400k | sort-spill | 2.119 | 1.128 |
| 400k | equality-composite-v3 | 1.201 | 0.877 |
| 2m | sort-memory | 10.106 | 2.894 |
| 2m | sort-spill | 6.093 | 2.503 |
| 2m | equality-composite-v3 | 2.095 | 1.785 |
