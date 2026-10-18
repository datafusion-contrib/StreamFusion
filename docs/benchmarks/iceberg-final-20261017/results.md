# Streaming Iceberg Nexmark observations

2,000,000 deterministic events; parallelism 4; four-bucket input/output tables. All times include streaming completion and final drain/commit.

| Query | State / mini-batch | Variant | Flink median [range], s | StreamFusion median [range], s | Median ratio |
|---|---|---|---:|---:|---:|
| q0 | disk-off | exact | 1.279 [1.199–1.315] | 0.958 [0.907–1.135] | 1.34× |
| q0 | disk-on | exact | 1.852 [1.454–3.028] | 1.324 [1.291–2.484] | 1.40× |
| q0 | memory-off | exact | 1.091 [1.076–1.397] | 0.724 [0.625–0.762] | 1.51× |
| q0 | memory-on | exact | 1.030 [1.019–1.059] | 0.613 [0.602–0.656] | 1.68× |
| q1 | disk-off | approximate decimal (incompatible) | 1.084 [1.082–1.086] | 0.595 [0.588–0.636] | 1.82× |
| q1 | disk-off | exact | 1.086 [1.067–1.107] | 0.642 [0.604–0.694] | 1.69× |
| q1 | disk-on | approximate decimal (incompatible) | 1.076 [1.071–1.080] | 0.609 [0.602–0.652] | 1.77× |
| q1 | disk-on | exact | 1.127 [1.072–1.139] | 0.646 [0.613–0.651] | 1.75× |
| q1 | memory-off | approximate decimal (incompatible) | 1.050 [1.033–1.075] | 0.640 [0.606–0.803] | 1.64× |
| q1 | memory-off | exact | 1.110 [1.092–1.146] | 0.673 [0.609–0.725] | 1.65× |
| q1 | memory-on | approximate decimal (incompatible) | 1.063 [1.047–1.098] | 0.626 [0.580–0.683] | 1.70× |
| q1 | memory-on | exact | 1.072 [1.052–1.081] | 0.585 [0.572–0.618] | 1.83× |
| q2 | disk-off | exact | 0.639 [0.610–0.641] | 0.332 [0.321–0.338] | 1.93× |
| q2 | disk-on | exact | 0.640 [0.639–0.660] | 0.341 [0.317–0.359] | 1.88× |
| q2 | memory-off | exact | 0.636 [0.620–0.644] | 0.298 [0.285–0.386] | 2.13× |
| q2 | memory-on | exact | 0.639 [0.619–0.642] | 0.297 [0.290–0.377] | 2.15× |
| q3 | disk-off | exact | 0.403 [0.401–0.437] | 0.543 [0.519–0.970] | 0.74× |
| q3 | disk-on | exact | 0.937 [0.933–1.090] | 0.941 [0.920–0.966] | 1.00× |
| q3 | memory-off | exact | 0.364 [0.341–0.405] | 0.372 [0.364–0.387] | 0.98× |
| q3 | memory-on | exact | 0.362 [0.339–0.423] | 0.335 [0.327–0.367] | 1.08× |
| q4 | disk-off | exact | 56.835 [53.120–57.907] | 10.292 [10.035–30.818] | 5.52× |
| q4 | disk-on | exact | 21.890 [19.563–34.637] | 1.745 [1.658–9.553] | 12.54× |
| q4 | memory-off | exact | 12.057 [2.838–19.009] | 12.473 [1.689–16.422] | 0.97× |
| q4 | memory-on | exact | 1.905 [1.853–12.374] | 1.068 [1.031–25.217] | 1.78× |
| q5 | disk-off | exact | 15.425 [13.665–17.634] | 8.944 [8.194–10.424] | 1.72× |
| q5 | disk-on | exact | 15.403 [15.111–17.568] | 2.804 [2.696–9.273] | 5.49× |
| q5 | memory-off | exact | 1.003 [0.945–1.021] | 0.886 [0.823–0.893] | 1.13× |
| q5 | memory-on | exact | 0.980 [0.960–0.989] | 0.839 [0.832–0.889] | 1.17× |
| q7 | disk-off | exact | 49.295 [43.406–53.326] | 6.967 [6.936–12.627] | 7.08× |
| q7 | disk-on | exact | 32.050 [28.152–36.331] | 10.058 [2.798–26.759] | 3.19× |
| q7 | memory-off | exact | 1.957 [1.715–5.289] | 1.605 [1.507–9.849] | 1.22× |
| q7 | memory-on | exact | 10.366 [2.036–17.916] | 1.683 [1.606–11.272] | 6.16× |
| q8 | disk-off | exact | 5.661 [5.045–7.139] | 3.825 [0.866–3.996] | 1.48× |
| q8 | disk-on | exact | 1.236 [1.167–1.261] | 0.917 [0.858–0.968] | 1.35× |
| q8 | memory-off | exact | 0.549 [0.506–0.586] | 0.910 [0.907–0.910] | 0.60× |
| q8 | memory-on | exact | 0.483 [0.477–0.497] | 0.472 [0.468–0.479] | 1.02× |
| q9 | disk-off | exact | 221.113 [212.544–239.534] | 13.759 [13.689–14.015] | 16.07× |
| q9 | disk-on | exact | 199.597 [174.006–204.258] | 2.493 [2.408–2.622] | 80.06× |
| q9 | memory-off | exact | 31.274 [28.053–41.623] | 14.208 [7.214–31.780] | 2.20× |
| q9 | memory-on | exact | 3.611 [3.127–35.944] | 22.942 [10.768–46.235] | 0.16× |
| q10 | disk-off | exact | 1.232 [1.224–1.248] | 0.858 [0.857–0.996] | 1.43× |
| q10 | disk-off | native datetime (incompatible) | 1.224 [1.217–1.279] | 0.910 [0.873–0.917] | 1.34× |
| q10 | disk-on | exact | 1.237 [1.228–1.262] | 0.905 [0.863–0.961] | 1.37× |
| q10 | disk-on | native datetime (incompatible) | 1.264 [1.234–1.270] | 0.929 [0.863–0.963] | 1.36× |
| q10 | memory-off | exact | 1.258 [1.229–1.263] | 0.874 [0.868–0.912] | 1.44× |
| q10 | memory-off | native datetime (incompatible) | 1.209 [1.195–1.362] | 0.897 [0.881–1.264] | 1.35× |
| q10 | memory-on | exact | 1.246 [1.245–1.312] | 0.900 [0.888–0.967] | 1.38× |
| q10 | memory-on | native datetime (incompatible) | 1.239 [1.212–1.241] | 0.952 [0.885–0.952] | 1.30× |
| q11 | disk-off | exact | 13.520 [5.075–15.860] | 2.036 [1.938–2.182] | 6.64× |
| q11 | disk-on | exact | 5.118 [5.114–5.181] | 0.837 [0.797–2.217] | 6.11× |
| q11 | memory-off | exact | 1.020 [1.018–1.071] | 0.381 [0.380–0.388] | 2.68× |
| q11 | memory-on | exact | 0.909 [0.908–0.940] | 0.432 [0.385–0.471] | 2.10× |
| q12 | disk-off | exact | 9.866 [9.833–9.914] | 9.935 [9.928–9.958] | 0.99× |
| q12 | disk-on | exact | 9.923 [9.853–9.928] | 9.928 [9.870–9.978] | 1.00× |
| q12 | memory-off | exact | 9.970 [9.926–10.227] | 9.869 [9.531–9.961] | 1.01× |
| q12 | memory-on | exact | 9.975 [9.905–9.985] | 9.911 [9.841–9.934] | 1.01× |
| q13 | disk-off | exact | 0.929 [0.928–0.973] | 0.509 [0.508–0.546] | 1.82× |
| q13 | disk-on | exact | 0.947 [0.940–0.967] | 0.551 [0.529–0.590] | 1.72× |
| q13 | memory-off | exact | 1.060 [0.981–1.185] | 0.610 [0.531–0.622] | 1.74× |
| q13 | memory-on | exact | 0.971 [0.896–1.043] | 0.499 [0.481–0.510] | 1.94× |
| q14 | disk-off | exact | 1.225 [1.217–1.241] | 0.854 [0.848–0.869] | 1.44× |
| q14 | disk-off | native datetime (incompatible) | 1.229 [1.207–1.245] | 0.904 [0.892–0.927] | 1.36× |
| q14 | disk-on | exact | 1.215 [1.212–1.281] | 0.857 [0.844–0.907] | 1.42× |
| q14 | disk-on | native datetime (incompatible) | 1.220 [1.171–1.251] | 0.894 [0.882–0.927] | 1.36× |
| q14 | memory-off | exact | 1.195 [1.192–1.272] | 0.918 [0.880–0.936] | 1.30× |
| q14 | memory-off | native datetime (incompatible) | 1.197 [1.160–1.247] | 0.832 [0.827–0.866] | 1.44× |
| q14 | memory-on | exact | 1.208 [1.160–1.227] | 0.870 [0.869–0.877] | 1.39× |
| q14 | memory-on | native datetime (incompatible) | 1.227 [1.134–1.229] | 0.863 [0.863–0.873] | 1.42× |
| q15 | disk-off | exact | 12.729 [12.262–15.956] | 9.370 [9.131–10.748] | 1.36× |
| q15 | disk-off | native datetime (incompatible) | 16.630 [12.932–17.726] | 8.794 [8.731–12.024] | 1.89× |
| q15 | disk-on | exact | 1.225 [1.196–1.274] | 0.907 [0.853–0.998] | 1.35× |
| q15 | disk-on | native datetime (incompatible) | 1.210 [1.133–1.281] | 0.900 [0.841–1.049] | 1.34× |
| q15 | memory-off | exact | 5.273 [5.185–5.303] | 6.343 [6.341–6.485] | 0.83× |
| q15 | memory-off | native datetime (incompatible) | 5.390 [5.273–5.639] | 6.966 [6.411–7.029] | 0.77× |
| q15 | memory-on | exact | 1.518 [1.437–1.680] | 1.952 [1.823–2.811] | 0.78× |
| q15 | memory-on | native datetime (incompatible) | 1.138 [1.086–1.147] | 0.603 [0.574–0.611] | 1.89× |
| q16 | disk-off | exact | 17.118 [9.548–22.228] | 19.965 [16.839–26.801] | 0.86× |
| q16 | disk-off | native datetime (incompatible) | 30.948 [19.624–36.456] | 10.175 [8.872–10.719] | 3.04× |
| q16 | disk-on | exact | 2.944 [2.442–6.460] | 4.608 [2.624–15.251] | 0.64× |
| q16 | disk-on | native datetime (incompatible) | 2.323 [2.257–3.510] | 2.511 [2.313–6.690] | 0.93× |
| q16 | memory-off | exact | 6.073 [5.733–8.232] | 23.423 [5.797–25.064] | 0.26× |
| q16 | memory-off | native datetime (incompatible) | 8.319 [5.691–8.681] | 5.374 [5.324–23.649] | 1.55× |
| q16 | memory-on | exact | 1.769 [1.737–1.936] | 1.765 [1.531–10.995] | 1.00× |
| q16 | memory-on | native datetime (incompatible) | 1.685 [1.660–4.998] | 7.096 [1.419–26.017] | 0.24× |
| q17 | disk-off | exact | 9.795 [3.796–14.109] | 8.610 [2.826–8.793] | 1.14× |
| q17 | disk-off | native datetime (incompatible) | 6.118 [3.636–6.760] | 3.876 [2.844–7.351] | 1.58× |
| q17 | disk-on | exact | 1.199 [1.186–4.338] | 0.864 [0.808–1.345] | 1.39× |
| q17 | disk-on | native datetime (incompatible) | 1.186 [1.183–1.191] | 0.812 [0.790–0.828] | 1.46× |
| q17 | memory-off | exact | 3.217 [3.199–4.845] | 7.328 [2.655–8.137] | 0.44× |
| q17 | memory-off | native datetime (incompatible) | 3.134 [3.134–3.202] | 2.654 [2.643–2.703] | 1.18× |
| q17 | memory-on | exact | 1.202 [1.177–1.219] | 0.788 [0.732–0.790] | 1.53× |
| q17 | memory-on | native datetime (incompatible) | 2.280 [1.155–3.092] | 0.763 [0.744–2.512] | 2.99× |
| q18 | disk-off | exact | 73.283 [66.479–93.761] | 7.768 [7.420–10.199] | 9.43× |
| q18 | disk-on | exact | 66.443 [62.575–88.572] | 7.185 [6.820–7.330] | 9.25× |
| q18 | memory-off | exact | 4.281 [3.959–27.871] | 43.848 [41.005–44.939] | 0.10× |
| q18 | memory-on | exact | 25.733 [25.490–35.784] | 27.224 [3.285–38.167] | 0.95× |
| q19 | disk-off | exact | 47.861 [42.985–56.541] | 33.628 [17.566–37.410] | 1.42× |
| q19 | disk-on | exact | 30.632 [24.289–43.800] | 9.636 [1.722–15.104] | 3.18× |
| q19 | memory-off | exact | 41.935 [39.020–54.016] | 34.364 [20.489–36.432] | 1.22× |
| q19 | memory-on | exact | 46.350 [43.397–51.973] | 2.256 [2.141–10.874] | 20.55× |
| q20 | disk-off | exact | 146.303 [129.657–193.544] | 24.050 [7.275–39.096] | 6.08× |
| q20 | disk-on | exact | 134.126 [114.493–152.102] | 1.428 [1.413–1.690] | 93.94× |
| q20 | memory-off | exact | 1.762 [1.752–1.988] | 18.580 [8.785–21.154] | 0.09× |
| q20 | memory-on | exact | 2.235 [2.214–21.962] | 28.969 [8.803–31.090] | 0.08× |
| q21 | disk-off | exact | 1.066 [1.046–1.066] | 0.709 [0.702–0.731] | 1.50× |
| q21 | disk-off | native regex/case (incompatible) | 1.069 [1.039–1.072] | 0.311 [0.306–0.321] | 3.44× |
| q21 | disk-on | exact | 1.082 [1.062–1.118] | 0.728 [0.724–0.729] | 1.49× |
| q21 | disk-on | native regex/case (incompatible) | 1.086 [1.078–1.096] | 0.315 [0.308–0.329] | 3.45× |
| q21 | memory-off | exact | 1.037 [1.035–1.085] | 0.701 [0.681–0.705] | 1.48× |
| q21 | memory-off | native regex/case (incompatible) | 1.037 [1.027–1.048] | 0.271 [0.266–0.278] | 3.83× |
| q21 | memory-on | exact | 1.064 [1.063–1.279] | 0.705 [0.681–0.706] | 1.51× |
| q21 | memory-on | native regex/case (incompatible) | 1.061 [1.057–1.087] | 0.270 [0.267–0.290] | 3.93× |
| q22 | disk-off | exact | 1.258 [1.241–1.265] | 0.549 [0.537–0.551] | 2.29× |
| q22 | disk-on | exact | 1.293 [1.279–1.321] | 0.622 [0.575–0.627] | 2.08× |
| q22 | memory-off | exact | 1.254 [1.242–1.280] | 0.543 [0.497–0.571] | 2.31× |
| q22 | memory-on | exact | 1.244 [1.231–1.244] | 0.509 [0.506–0.644] | 2.44× |
| q23 | disk-off | exact | 53.534 [50.634–56.357] | 8.447 [8.248–47.357] | 6.34× |
| q23 | disk-on | exact | 28.993 [26.114–40.131] | 30.541 [3.521–42.205] | 0.95× |
| q23 | memory-off | exact | 27.072 [4.499–33.257] | 2.330 [2.149–9.539] | 11.62× |
| q23 | memory-on | exact | 4.568 [4.279–41.618] | 2.179 [2.170–15.725] | 2.10× |

## Exact-query aggregates

Geometric means use every exact-query cell in each mode; labelled expression variants remain separate above. Throughput is nominal corpus events divided by median end-to-end time, including startup, checkpoints and drain.

| State / mini-batch | Queries | Geomean ratio | Flink corpus events/s, range | StreamFusion corpus events/s, range |
|---|---:|---:|---:|---:|
| disk-off | 23 | 2.34× | 9,045–4,958,935 | 59,475–6,028,987 |
| disk-on | 23 | 2.81× | 10,020–3,126,090 | 65,487–5,865,106 |
| memory-off | 23 | 1.06× | 47,693–5,494,553 | 45,612–6,707,653 |
| memory-on | 23 | 1.38× | 43,150–5,526,211 | 69,040–6,736,079 |

## README query selection

Each query contributes once per mode, using its existing labelled variant when present. Median ratios retain all repetitions. The secondary headline sample uses the first memory sample or the best of the first two disk samples.

| State / mini-batch | Queries | Geomean median ratio | Geomean headline-sample ratio |
|---|---:|---:|---:|
| disk-off | 23 | 2.63× | 2.55× |
| disk-on | 23 | 2.97× | 2.96× |
| memory-off | 23 | 1.24× | 1.14× |
| memory-on | 23 | 1.44× | 1.30× |

## Headline sampling convention

Memory uses the first measured sample; disk uses the best of the first two. The repeated medians and complete ranges above remain the primary results.

| Query | State / mini-batch | Variant | Flink, s | StreamFusion, s | Ratio |
|---|---|---|---:|---:|---:|
| q0 | disk-off | exact | 1.279 | 0.958 | 1.34× |
| q0 | disk-on | exact | 1.454 | 1.291 | 1.13× |
| q0 | memory-off | exact | 1.397 | 0.762 | 1.83× |
| q0 | memory-on | exact | 1.030 | 0.613 | 1.68× |
| q1 | disk-off | approximate decimal (incompatible) | 1.084 | 0.588 | 1.85× |
| q1 | disk-off | exact | 1.067 | 0.604 | 1.77× |
| q1 | disk-on | approximate decimal (incompatible) | 1.071 | 0.602 | 1.78× |
| q1 | disk-on | exact | 1.127 | 0.613 | 1.84× |
| q1 | memory-off | approximate decimal (incompatible) | 1.075 | 0.803 | 1.34× |
| q1 | memory-off | exact | 1.146 | 0.609 | 1.88× |
| q1 | memory-on | approximate decimal (incompatible) | 1.063 | 0.580 | 1.83× |
| q1 | memory-on | exact | 1.081 | 0.585 | 1.85× |
| q2 | disk-off | exact | 0.610 | 0.321 | 1.90× |
| q2 | disk-on | exact | 0.639 | 0.317 | 2.02× |
| q2 | memory-off | exact | 0.644 | 0.386 | 1.67× |
| q2 | memory-on | exact | 0.639 | 0.377 | 1.69× |
| q3 | disk-off | exact | 0.401 | 0.519 | 0.77× |
| q3 | disk-on | exact | 0.933 | 0.941 | 0.99× |
| q3 | memory-off | exact | 0.405 | 0.387 | 1.05× |
| q3 | memory-on | exact | 0.423 | 0.367 | 1.15× |
| q4 | disk-off | exact | 53.120 | 10.292 | 5.16× |
| q4 | disk-on | exact | 21.890 | 1.745 | 12.54× |
| q4 | memory-off | exact | 19.009 | 12.473 | 1.52× |
| q4 | memory-on | exact | 1.905 | 1.068 | 1.78× |
| q5 | disk-off | exact | 13.665 | 8.944 | 1.53× |
| q5 | disk-on | exact | 15.111 | 2.696 | 5.61× |
| q5 | memory-off | exact | 1.021 | 0.893 | 1.14× |
| q5 | memory-on | exact | 0.980 | 0.889 | 1.10× |
| q7 | disk-off | exact | 49.295 | 6.936 | 7.11× |
| q7 | disk-on | exact | 28.152 | 10.058 | 2.80× |
| q7 | memory-off | exact | 1.957 | 1.507 | 1.30× |
| q7 | memory-on | exact | 2.036 | 1.606 | 1.27× |
| q8 | disk-off | exact | 5.661 | 3.825 | 1.48× |
| q8 | disk-on | exact | 1.236 | 0.858 | 1.44× |
| q8 | memory-off | exact | 0.506 | 0.910 | 0.56× |
| q8 | memory-on | exact | 0.497 | 0.479 | 1.04× |
| q9 | disk-off | exact | 221.113 | 13.689 | 16.15× |
| q9 | disk-on | exact | 199.597 | 2.408 | 82.91× |
| q9 | memory-off | exact | 41.623 | 31.780 | 1.31× |
| q9 | memory-on | exact | 3.127 | 10.768 | 0.29× |
| q10 | disk-off | exact | 1.232 | 0.857 | 1.44× |
| q10 | disk-off | native datetime (incompatible) | 1.217 | 0.873 | 1.39× |
| q10 | disk-on | exact | 1.228 | 0.863 | 1.42× |
| q10 | disk-on | native datetime (incompatible) | 1.234 | 0.863 | 1.43× |
| q10 | memory-off | exact | 1.263 | 0.874 | 1.45× |
| q10 | memory-off | native datetime (incompatible) | 1.209 | 0.897 | 1.35× |
| q10 | memory-on | exact | 1.245 | 0.900 | 1.38× |
| q10 | memory-on | native datetime (incompatible) | 1.212 | 0.952 | 1.27× |
| q11 | disk-off | exact | 5.075 | 1.938 | 2.62× |
| q11 | disk-on | exact | 5.118 | 0.797 | 6.42× |
| q11 | memory-off | exact | 1.071 | 0.381 | 2.81× |
| q11 | memory-on | exact | 0.909 | 0.471 | 1.93× |
| q12 | disk-off | exact | 9.833 | 9.935 | 0.99× |
| q12 | disk-on | exact | 9.923 | 9.870 | 1.01× |
| q12 | memory-off | exact | 9.926 | 9.961 | 1.00× |
| q12 | memory-on | exact | 9.985 | 9.841 | 1.01× |
| q13 | disk-off | exact | 0.929 | 0.508 | 1.83× |
| q13 | disk-on | exact | 0.947 | 0.529 | 1.79× |
| q13 | memory-off | exact | 0.981 | 0.610 | 1.61× |
| q13 | memory-on | exact | 1.043 | 0.481 | 2.17× |
| q14 | disk-off | exact | 1.225 | 0.854 | 1.44× |
| q14 | disk-off | native datetime (incompatible) | 1.207 | 0.904 | 1.33× |
| q14 | disk-on | exact | 1.212 | 0.844 | 1.44× |
| q14 | disk-on | native datetime (incompatible) | 1.220 | 0.882 | 1.38× |
| q14 | memory-off | exact | 1.195 | 0.918 | 1.30× |
| q14 | memory-off | native datetime (incompatible) | 1.247 | 0.827 | 1.51× |
| q14 | memory-on | exact | 1.160 | 0.869 | 1.34× |
| q14 | memory-on | native datetime (incompatible) | 1.229 | 0.863 | 1.42× |
| q15 | disk-off | exact | 12.729 | 9.370 | 1.36× |
| q15 | disk-off | native datetime (incompatible) | 12.932 | 8.731 | 1.48× |
| q15 | disk-on | exact | 1.196 | 0.853 | 1.40× |
| q15 | disk-on | native datetime (incompatible) | 1.133 | 0.841 | 1.35× |
| q15 | memory-off | exact | 5.303 | 6.341 | 0.84× |
| q15 | memory-off | native datetime (incompatible) | 5.273 | 6.411 | 0.82× |
| q15 | memory-on | exact | 1.680 | 1.952 | 0.86× |
| q15 | memory-on | native datetime (incompatible) | 1.147 | 0.574 | 2.00× |
| q16 | disk-off | exact | 9.548 | 16.839 | 0.57× |
| q16 | disk-off | native datetime (incompatible) | 30.948 | 8.872 | 3.49× |
| q16 | disk-on | exact | 2.442 | 4.608 | 0.53× |
| q16 | disk-on | native datetime (incompatible) | 2.257 | 2.313 | 0.98× |
| q16 | memory-off | exact | 8.232 | 25.064 | 0.33× |
| q16 | memory-off | native datetime (incompatible) | 5.691 | 5.374 | 1.06× |
| q16 | memory-on | exact | 1.936 | 1.765 | 1.10× |
| q16 | memory-on | native datetime (incompatible) | 4.998 | 26.017 | 0.19× |
| q17 | disk-off | exact | 9.795 | 8.610 | 1.14× |
| q17 | disk-off | native datetime (incompatible) | 3.636 | 2.844 | 1.28× |
| q17 | disk-on | exact | 1.199 | 0.864 | 1.39× |
| q17 | disk-on | native datetime (incompatible) | 1.186 | 0.790 | 1.50× |
| q17 | memory-off | exact | 4.845 | 7.328 | 0.66× |
| q17 | memory-off | native datetime (incompatible) | 3.134 | 2.643 | 1.19× |
| q17 | memory-on | exact | 1.177 | 0.790 | 1.49× |
| q17 | memory-on | native datetime (incompatible) | 1.155 | 0.744 | 1.55× |
| q18 | disk-off | exact | 73.283 | 7.420 | 9.88× |
| q18 | disk-on | exact | 66.443 | 6.820 | 9.74× |
| q18 | memory-off | exact | 3.959 | 43.848 | 0.09× |
| q18 | memory-on | exact | 25.733 | 27.224 | 0.95× |
| q19 | disk-off | exact | 47.861 | 17.566 | 2.72× |
| q19 | disk-on | exact | 30.632 | 9.636 | 3.18× |
| q19 | memory-off | exact | 54.016 | 36.432 | 1.48× |
| q19 | memory-on | exact | 43.397 | 10.874 | 3.99× |
| q20 | disk-off | exact | 146.303 | 24.050 | 6.08× |
| q20 | disk-on | exact | 114.493 | 1.413 | 81.01× |
| q20 | memory-off | exact | 1.762 | 18.580 | 0.09× |
| q20 | memory-on | exact | 2.235 | 8.803 | 0.25× |
| q21 | disk-off | exact | 1.046 | 0.709 | 1.48× |
| q21 | disk-off | native regex/case (incompatible) | 1.069 | 0.306 | 3.49× |
| q21 | disk-on | exact | 1.062 | 0.728 | 1.46× |
| q21 | disk-on | native regex/case (incompatible) | 1.078 | 0.308 | 3.50× |
| q21 | memory-off | exact | 1.085 | 0.705 | 1.54× |
| q21 | memory-off | native regex/case (incompatible) | 1.027 | 0.278 | 3.69× |
| q21 | memory-on | exact | 1.279 | 0.705 | 1.81× |
| q21 | memory-on | native regex/case (incompatible) | 1.087 | 0.270 | 4.02× |
| q22 | disk-off | exact | 1.241 | 0.537 | 2.31× |
| q22 | disk-on | exact | 1.293 | 0.575 | 2.25× |
| q22 | memory-off | exact | 1.280 | 0.571 | 2.24× |
| q22 | memory-on | exact | 1.244 | 0.509 | 2.44× |
| q23 | disk-off | exact | 50.634 | 8.248 | 6.14× |
| q23 | disk-on | exact | 26.114 | 30.541 | 0.86× |
| q23 | memory-off | exact | 27.072 | 9.539 | 2.84× |
| q23 | memory-on | exact | 4.279 | 2.179 | 1.96× |

Raw samples, warmups, parity checks, plans, committed table locations and native/fallback task/file witnesses are retained in this artifact directory. PROCTIME q12 validates per-bidder count conservation and includes waiting for the final processing-time window.

[Per-cell timing, checkpoint and sampled-memory metrics](metrics.csv) retain separate phase medians. Phase medians need not sum to the total median. Memory peaks are sampled during progress polling, not exhaustive job maxima.
