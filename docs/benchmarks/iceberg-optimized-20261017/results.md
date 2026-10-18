# Streaming Iceberg Nexmark observations

2,000,000 deterministic events; parallelism 4; four-bucket input/output tables. All times include streaming completion and final drain/commit.

| Query | State / mini-batch | Variant | Flink median [range], s | StreamFusion median [range], s | Median ratio |
|---|---|---|---:|---:|---:|
| q0 | disk-off | exact | 1.071 [1.058–1.116] | 0.563 [0.541–0.619] | 1.90× |
| q0 | disk-on | exact | 1.336 [1.293–2.045] | 0.956 [0.802–1.025] | 1.40× |
| q0 | memory-off | exact | 1.128 [1.094–1.411] | 0.634 [0.553–0.744] | 1.78× |
| q0 | memory-on | exact | 1.104 [1.012–1.166] | 0.532 [0.526–0.631] | 2.08× |
| q1 | disk-off | approximate decimal (incompatible) | 1.090 [1.071–1.130] | 0.533 [0.532–0.707] | 2.05× |
| q1 | disk-off | exact | 1.045 [1.042–1.093] | 0.535 [0.534–0.585] | 1.95× |
| q1 | disk-on | approximate decimal (incompatible) | 1.096 [1.086–1.097] | 0.545 [0.535–0.626] | 2.01× |
| q1 | disk-on | exact | 1.102 [1.081–1.140] | 0.559 [0.548–0.572] | 1.97× |
| q1 | memory-off | approximate decimal (incompatible) | 1.120 [1.105–1.152] | 0.535 [0.527–1.675] | 2.09× |
| q1 | memory-off | exact | 1.356 [1.238–1.488] | 0.639 [0.570–1.268] | 2.12× |
| q1 | memory-on | approximate decimal (incompatible) | 1.098 [1.069–1.170] | 0.557 [0.518–1.968] | 1.97× |
| q1 | memory-on | exact | 1.109 [1.086–1.148] | 0.605 [0.514–1.203] | 1.83× |
| q2 | disk-off | exact | 0.627 [0.622–0.645] | 0.317 [0.309–0.325] | 1.98× |
| q2 | disk-on | exact | 0.638 [0.629–0.638] | 0.319 [0.311–0.340] | 2.00× |
| q2 | memory-off | exact | 0.616 [0.608–0.637] | 0.284 [0.276–0.286] | 2.17× |
| q2 | memory-on | exact | 0.651 [0.619–0.679] | 0.289 [0.284–0.324] | 2.25× |
| q3 | disk-off | exact | 0.378 [0.371–0.382] | 0.539 [0.527–0.621] | 0.70× |
| q3 | disk-on | exact | 0.400 [0.375–0.412] | 0.391 [0.386–0.401] | 1.02× |
| q3 | memory-off | exact | 0.379 [0.330–0.396] | 0.363 [0.354–0.376] | 1.04× |
| q3 | memory-on | exact | 0.337 [0.336–0.351] | 0.349 [0.345–0.369] | 0.96× |
| q4 | disk-off | exact | 55.603 [54.085–60.491] | 15.677 [9.761–26.734] | 3.55× |
| q4 | disk-on | exact | 33.358 [19.885–42.811] | 2.633 [1.548–10.752] | 12.67× |
| q4 | memory-off | exact | 5.814 [2.792–15.487] | 8.717 [2.266–16.746] | 0.67× |
| q4 | memory-on | exact | 1.856 [1.850–3.895] | 1.040 [0.966–1.066] | 1.78× |
| q5 | disk-off | exact | 15.229 [14.581–16.477] | 3.878 [3.547–8.551] | 3.93× |
| q5 | disk-on | exact | 16.311 [15.335–17.962] | 7.089 [2.629–7.300] | 2.30× |
| q5 | memory-off | exact | 1.003 [1.000–1.047] | 0.896 [0.858–0.926] | 1.12× |
| q5 | memory-on | exact | 0.995 [0.975–1.037] | 0.873 [0.846–0.875] | 1.14× |
| q7 | disk-off | exact | 49.217 [48.550–51.356] | 7.240 [7.171–7.329] | 6.80× |
| q7 | disk-on | exact | 36.744 [31.175–39.519] | 25.906 [3.332–28.906] | 1.42× |
| q7 | memory-off | exact | 1.951 [1.651–16.756] | 9.708 [1.598–13.995] | 0.20× |
| q7 | memory-on | exact | 2.024 [1.958–14.738] | 10.679 [1.580–13.673] | 0.19× |
| q8 | disk-off | exact | 1.244 [1.208–1.250] | 0.884 [0.879–0.981] | 1.41× |
| q8 | disk-on | exact | 4.091 [1.169–4.486] | 2.447 [0.908–2.615] | 1.67× |
| q8 | memory-off | exact | 0.545 [0.495–0.601] | 0.464 [0.455–0.469] | 1.17× |
| q8 | memory-on | exact | 0.515 [0.485–0.526] | 0.447 [0.446–0.459] | 1.15× |
| q9 | disk-off | exact | 236.634 [203.461–237.221] | 13.833 [13.554–17.375] | 17.11× |
| q9 | disk-on | exact | 195.659 [180.668–206.501] | 2.672 [2.582–9.091] | 73.24× |
| q9 | memory-off | exact | 29.720 [7.679–29.950] | 4.150 [4.144–8.427] | 7.16× |
| q9 | memory-on | exact | 31.505 [3.232–47.710] | 2.265 [2.074–10.531] | 13.91× |
| q10 | disk-off | exact | 1.249 [1.246–1.258] | 0.822 [0.821–0.860] | 1.52× |
| q10 | disk-off | native datetime (incompatible) | 1.264 [1.262–1.280] | 0.844 [0.793–0.867] | 1.50× |
| q10 | disk-on | exact | 1.267 [1.260–1.275] | 0.810 [0.803–0.848] | 1.56× |
| q10 | disk-on | native datetime (incompatible) | 1.251 [1.240–1.274] | 0.835 [0.804–0.886] | 1.50× |
| q10 | memory-off | exact | 1.233 [1.230–1.248] | 0.798 [0.773–0.800] | 1.55× |
| q10 | memory-off | native datetime (incompatible) | 1.208 [1.195–1.208] | 0.780 [0.777–0.803] | 1.55× |
| q10 | memory-on | exact | 1.243 [1.201–1.296] | 0.803 [0.768–0.831] | 1.55× |
| q10 | memory-on | native datetime (incompatible) | 1.217 [1.199–1.232] | 0.823 [0.819–0.823] | 1.48× |
| q11 | disk-off | exact | 5.208 [5.066–13.545] | 0.944 [0.803–1.849] | 5.52× |
| q11 | disk-on | exact | 5.205 [5.037–17.165] | 0.829 [0.772–0.888] | 6.28× |
| q11 | memory-off | exact | 0.919 [0.865–0.958] | 0.422 [0.371–0.494] | 2.18× |
| q11 | memory-on | exact | 0.873 [0.855–0.902] | 0.370 [0.364–0.459] | 2.36× |
| q12 | disk-off | exact | 10.180 [9.655–10.341] | 9.938 [9.157–10.172] | 1.02× |
| q12 | disk-on | exact | 9.870 [9.858–9.991] | 9.917 [9.873–9.983] | 1.00× |
| q12 | memory-off | exact | 9.920 [9.919–9.924] | 9.928 [9.871–9.972] | 1.00× |
| q12 | memory-on | exact | 9.946 [9.926–9.982] | 9.909 [9.860–9.918] | 1.00× |
| q13 | disk-off | exact | 0.926 [0.911–0.949] | 0.453 [0.438–0.457] | 2.04× |
| q13 | disk-on | exact | 0.947 [0.931–0.958] | 0.477 [0.469–0.490] | 1.99× |
| q13 | memory-off | exact | 0.958 [0.950–0.960] | 0.436 [0.407–0.444] | 2.19× |
| q13 | memory-on | exact | 0.907 [0.894–0.926] | 0.430 [0.428–0.437] | 2.11× |
| q14 | disk-off | exact | 1.214 [1.189–1.233] | 0.831 [0.830–0.836] | 1.46× |
| q14 | disk-off | native datetime (incompatible) | 1.469 [1.204–1.956] | 1.165 [0.798–1.365] | 1.26× |
| q14 | disk-on | exact | 1.601 [1.492–2.341] | 1.215 [1.117–1.250] | 1.32× |
| q14 | disk-on | native datetime (incompatible) | 2.267 [2.232–2.377] | 1.301 [1.006–1.352] | 1.74× |
| q14 | memory-off | exact | 1.199 [1.181–1.260] | 0.806 [0.806–0.881] | 1.49× |
| q14 | memory-off | native datetime (incompatible) | 1.161 [1.156–1.211] | 0.805 [0.801–0.871] | 1.44× |
| q14 | memory-on | exact | 1.166 [1.148–1.183] | 0.810 [0.761–0.830] | 1.44× |
| q14 | memory-on | native datetime (incompatible) | 1.177 [1.152–1.209] | 0.782 [0.771–0.807] | 1.51× |
| q15 | disk-off | exact | 15.450 [13.017–17.817] | 9.425 [8.459–9.782] | 1.64× |
| q15 | disk-off | native datetime (incompatible) | 14.120 [13.866–16.725] | 9.048 [8.783–9.487] | 1.56× |
| q15 | disk-on | exact | 1.731 [1.639–1.764] | 1.534 [1.500–1.820] | 1.13× |
| q15 | disk-on | native datetime (incompatible) | 2.491 [1.657–2.989] | 2.228 [1.620–2.361] | 1.12× |
| q15 | memory-off | exact | 5.312 [5.300–5.355] | 6.033 [5.988–6.080] | 0.88× |
| q15 | memory-off | native datetime (incompatible) | 5.310 [5.303–5.365] | 5.937 [5.936–6.005] | 0.89× |
| q15 | memory-on | exact | 1.105 [1.090–1.128] | 0.586 [0.565–0.587] | 1.89× |
| q15 | memory-on | native datetime (incompatible) | 1.079 [1.061–1.084] | 0.577 [0.570–0.590] | 1.87× |
| q16 | disk-off | exact | 13.546 [11.092–17.997] | 20.314 [14.706–21.602] | 0.67× |
| q16 | disk-off | native datetime (incompatible) | 27.589 [23.609–32.324] | 8.572 [8.563–12.840] | 3.22× |
| q16 | disk-on | exact | 5.834 [3.576–7.929] | 5.973 [2.310–11.008] | 0.98× |
| q16 | disk-on | native datetime (incompatible) | 2.141 [2.133–2.298] | 2.453 [2.324–2.745] | 0.87× |
| q16 | memory-off | exact | 9.454 [5.856–11.947] | 9.939 [5.459–11.938] | 0.95× |
| q16 | memory-off | native datetime (incompatible) | 5.760 [5.727–6.164] | 17.687 [5.007–25.286] | 0.33× |
| q16 | memory-on | exact | 5.087 [1.633–8.475] | 7.166 [1.366–17.288] | 0.71× |
| q16 | memory-on | native datetime (incompatible) | 1.748 [1.541–1.788] | 1.390 [1.232–5.705] | 1.26× |
| q17 | disk-off | exact | 6.378 [3.691–12.614] | 4.453 [2.760–7.335] | 1.43× |
| q17 | disk-off | native datetime (incompatible) | 5.155 [3.665–6.070] | 2.752 [2.744–4.446] | 1.87× |
| q17 | disk-on | exact | 4.642 [4.642–6.031] | 6.068 [5.407–6.668] | 0.77× |
| q17 | disk-on | native datetime (incompatible) | 1.184 [1.171–1.238] | 0.798 [0.780–0.799] | 1.48× |
| q17 | memory-off | exact | 5.588 [4.340–7.392] | 4.139 [4.033–5.023] | 1.35× |
| q17 | memory-off | native datetime (incompatible) | 3.174 [3.133–3.218] | 2.577 [2.556–2.593] | 1.23× |
| q17 | memory-on | exact | 1.118 [1.105–1.156] | 0.727 [0.662–0.742] | 1.54× |
| q17 | memory-on | native datetime (incompatible) | 1.125 [1.100–1.143] | 0.743 [0.717–0.758] | 1.51× |
| q18 | disk-off | exact | 69.089 [62.667–72.039] | 7.535 [7.170–39.316] | 9.17× |
| q18 | disk-on | exact | 77.094 [60.226–106.304] | 18.981 [6.740–40.888] | 4.06× |
| q18 | memory-off | exact | 4.223 [3.651–38.692] | 29.996 [4.101–54.018] | 0.14× |
| q18 | memory-on | exact | 36.165 [4.605–47.333] | 11.855 [3.410–53.672] | 3.05× |
| q19 | disk-off | exact | 40.701 [33.649–53.356] | 27.048 [16.340–38.571] | 1.50× |
| q19 | disk-on | exact | 49.364 [24.027–50.785] | 1.763 [1.758–18.586] | 28.00× |
| q19 | memory-off | exact | 48.191 [40.809–57.153] | 29.439 [24.471–32.358] | 1.64× |
| q19 | memory-on | exact | 22.225 [22.003–43.398] | 3.345 [2.307–10.465] | 6.64× |
| q20 | disk-off | exact | 155.799 [141.871–157.787] | 14.405 [6.700–23.291] | 10.82× |
| q20 | disk-on | exact | 157.319 [145.721–169.450] | 1.704 [1.591–1.798] | 92.30× |
| q20 | memory-off | exact | 1.799 [1.768–1.973] | 17.729 [10.670–30.198] | 0.10× |
| q20 | memory-on | exact | 2.231 [2.159–9.741] | 20.483 [18.679–23.013] | 0.11× |
| q21 | disk-off | exact | 1.053 [1.053–1.062] | 0.721 [0.707–0.724] | 1.46× |
| q21 | disk-off | native regex/case (incompatible) | 1.041 [1.039–1.046] | 0.306 [0.294–0.320] | 3.40× |
| q21 | disk-on | exact | 1.063 [1.058–1.072] | 0.717 [0.696–0.739] | 1.48× |
| q21 | disk-on | native regex/case (incompatible) | 1.073 [1.064–1.109] | 0.311 [0.306–0.311] | 3.45× |
| q21 | memory-off | exact | 1.037 [1.030–1.078] | 0.692 [0.682–0.703] | 1.50× |
| q21 | memory-off | native regex/case (incompatible) | 1.047 [1.041–1.056] | 0.280 [0.260–0.280] | 3.74× |
| q21 | memory-on | exact | 1.054 [1.052–1.106] | 0.696 [0.669–0.711] | 1.51× |
| q21 | memory-on | native regex/case (incompatible) | 1.054 [1.031–1.057] | 0.269 [0.261–0.282] | 3.91× |
| q22 | disk-off | exact | 1.243 [1.242–1.257] | 0.490 [0.478–0.519] | 2.54× |
| q22 | disk-on | exact | 1.244 [1.231–1.286] | 0.519 [0.472–0.574] | 2.40× |
| q22 | memory-off | exact | 1.223 [1.212–1.323] | 0.486 [0.475–0.487] | 2.52× |
| q22 | memory-on | exact | 1.254 [1.248–1.292] | 0.467 [0.462–0.472] | 2.68× |
| q23 | disk-off | exact | 48.056 [32.116–54.612] | 8.059 [7.719–34.267] | 5.96× |
| q23 | disk-on | exact | 20.247 [18.421–41.059] | 39.928 [3.346–39.946] | 0.51× |
| q23 | memory-off | exact | 31.782 [17.420–34.385] | 3.106 [1.974–19.632] | 10.23× |
| q23 | memory-on | exact | 28.373 [27.217–38.246] | 1.803 [1.637–1.820] | 15.73× |

## Exact-query aggregates

Geometric means use every exact-query cell in each mode; labelled expression variants remain separate above. Throughput is nominal corpus events divided by median end-to-end time, including startup, checkpoints and drain.

| State / mini-batch | Queries | Geomean ratio | Flink corpus events/s, range | StreamFusion corpus events/s, range |
|---|---:|---:|---:|---:|
| disk-off | 23 | 2.51× | 8,452–5,293,231 | 73,942–6,318,071 |
| disk-on | 23 | 2.74× | 10,222–4,994,558 | 50,090–6,265,440 |
| memory-off | 23 | 1.23× | 41,502–5,271,635 | 66,676–7,046,495 |
| memory-on | 23 | 1.69× | 55,302–5,932,680 | 97,641–6,921,563 |

## README query selection

Each query contributes once per mode, using its existing labelled variant when present. Median ratios retain all repetitions. The secondary headline sample uses the first memory sample or the best of the first two disk samples.

| State / mini-batch | Queries | Geomean median ratio | Geomean headline-sample ratio |
|---|---:|---:|---:|
| disk-off | 23 | 2.80× | 2.82× |
| disk-on | 23 | 2.94× | 3.46× |
| memory-off | 23 | 1.22× | 1.24× |
| memory-on | 23 | 1.81× | 2.01× |

## Headline sampling convention

Memory uses the first measured sample; disk uses the best of the first two. The repeated medians and complete ranges above remain the primary results.

| Query | State / mini-batch | Variant | Flink, s | StreamFusion, s | Ratio |
|---|---|---|---:|---:|---:|
| q0 | disk-off | exact | 1.058 | 0.541 | 1.96× |
| q0 | disk-on | exact | 1.293 | 0.802 | 1.61× |
| q0 | memory-off | exact | 1.411 | 0.744 | 1.90× |
| q0 | memory-on | exact | 1.104 | 0.631 | 1.75× |
| q1 | disk-off | approximate decimal (incompatible) | 1.071 | 0.532 | 2.01× |
| q1 | disk-off | exact | 1.042 | 0.535 | 1.95× |
| q1 | disk-on | approximate decimal (incompatible) | 1.096 | 0.535 | 2.05× |
| q1 | disk-on | exact | 1.102 | 0.559 | 1.97× |
| q1 | memory-off | approximate decimal (incompatible) | 1.152 | 0.535 | 2.15× |
| q1 | memory-off | exact | 1.488 | 0.570 | 2.61× |
| q1 | memory-on | approximate decimal (incompatible) | 1.098 | 1.968 | 0.56× |
| q1 | memory-on | exact | 1.086 | 0.514 | 2.11× |
| q2 | disk-off | exact | 0.622 | 0.309 | 2.01× |
| q2 | disk-on | exact | 0.638 | 0.311 | 2.05× |
| q2 | memory-off | exact | 0.616 | 0.286 | 2.15× |
| q2 | memory-on | exact | 0.679 | 0.289 | 2.35× |
| q3 | disk-off | exact | 0.371 | 0.539 | 0.69× |
| q3 | disk-on | exact | 0.400 | 0.386 | 1.04× |
| q3 | memory-off | exact | 0.396 | 0.354 | 1.12× |
| q3 | memory-on | exact | 0.336 | 0.369 | 0.91× |
| q4 | disk-off | exact | 54.085 | 9.761 | 5.54× |
| q4 | disk-on | exact | 19.885 | 2.633 | 7.55× |
| q4 | memory-off | exact | 15.487 | 16.746 | 0.92× |
| q4 | memory-on | exact | 3.895 | 1.040 | 3.74× |
| q5 | disk-off | exact | 14.581 | 3.547 | 4.11× |
| q5 | disk-on | exact | 15.335 | 7.089 | 2.16× |
| q5 | memory-off | exact | 1.047 | 0.896 | 1.17× |
| q5 | memory-on | exact | 0.975 | 0.875 | 1.11× |
| q7 | disk-off | exact | 48.550 | 7.171 | 6.77× |
| q7 | disk-on | exact | 31.175 | 3.332 | 9.36× |
| q7 | memory-off | exact | 1.951 | 9.708 | 0.20× |
| q7 | memory-on | exact | 14.738 | 13.673 | 1.08× |
| q8 | disk-off | exact | 1.208 | 0.884 | 1.37× |
| q8 | disk-on | exact | 1.169 | 0.908 | 1.29× |
| q8 | memory-off | exact | 0.495 | 0.469 | 1.06× |
| q8 | memory-on | exact | 0.515 | 0.446 | 1.15× |
| q9 | disk-off | exact | 203.461 | 13.833 | 14.71× |
| q9 | disk-on | exact | 180.668 | 2.582 | 69.97× |
| q9 | memory-off | exact | 29.950 | 8.427 | 3.55× |
| q9 | memory-on | exact | 31.505 | 2.265 | 13.91× |
| q10 | disk-off | exact | 1.246 | 0.821 | 1.52× |
| q10 | disk-off | native datetime (incompatible) | 1.262 | 0.793 | 1.59× |
| q10 | disk-on | exact | 1.260 | 0.803 | 1.57× |
| q10 | disk-on | native datetime (incompatible) | 1.240 | 0.804 | 1.54× |
| q10 | memory-off | exact | 1.248 | 0.800 | 1.56× |
| q10 | memory-off | native datetime (incompatible) | 1.208 | 0.780 | 1.55× |
| q10 | memory-on | exact | 1.243 | 0.768 | 1.62× |
| q10 | memory-on | native datetime (incompatible) | 1.199 | 0.823 | 1.46× |
| q11 | disk-off | exact | 5.066 | 0.803 | 6.31× |
| q11 | disk-on | exact | 5.205 | 0.772 | 6.74× |
| q11 | memory-off | exact | 0.958 | 0.422 | 2.27× |
| q11 | memory-on | exact | 0.902 | 0.370 | 2.44× |
| q12 | disk-off | exact | 9.655 | 9.938 | 0.97× |
| q12 | disk-on | exact | 9.858 | 9.917 | 0.99× |
| q12 | memory-off | exact | 9.924 | 9.972 | 1.00× |
| q12 | memory-on | exact | 9.946 | 9.909 | 1.00× |
| q13 | disk-off | exact | 0.911 | 0.453 | 2.01× |
| q13 | disk-on | exact | 0.931 | 0.477 | 1.95× |
| q13 | memory-off | exact | 0.950 | 0.444 | 2.14× |
| q13 | memory-on | exact | 0.907 | 0.430 | 2.11× |
| q14 | disk-off | exact | 1.189 | 0.830 | 1.43× |
| q14 | disk-off | native datetime (incompatible) | 1.204 | 0.798 | 1.51× |
| q14 | disk-on | exact | 1.492 | 1.215 | 1.23× |
| q14 | disk-on | native datetime (incompatible) | 2.232 | 1.006 | 2.22× |
| q14 | memory-off | exact | 1.181 | 0.806 | 1.46× |
| q14 | memory-off | native datetime (incompatible) | 1.156 | 0.801 | 1.44× |
| q14 | memory-on | exact | 1.148 | 0.761 | 1.51× |
| q14 | memory-on | native datetime (incompatible) | 1.152 | 0.771 | 1.49× |
| q15 | disk-off | exact | 13.017 | 8.459 | 1.54× |
| q15 | disk-off | native datetime (incompatible) | 14.120 | 9.048 | 1.56× |
| q15 | disk-on | exact | 1.731 | 1.534 | 1.13× |
| q15 | disk-on | native datetime (incompatible) | 1.657 | 1.620 | 1.02× |
| q15 | memory-off | exact | 5.312 | 6.080 | 0.87× |
| q15 | memory-off | native datetime (incompatible) | 5.310 | 5.936 | 0.89× |
| q15 | memory-on | exact | 1.105 | 0.565 | 1.96× |
| q15 | memory-on | native datetime (incompatible) | 1.079 | 0.590 | 1.83× |
| q16 | disk-off | exact | 11.092 | 14.706 | 0.75× |
| q16 | disk-off | native datetime (incompatible) | 23.609 | 8.572 | 2.75× |
| q16 | disk-on | exact | 5.834 | 5.973 | 0.98× |
| q16 | disk-on | native datetime (incompatible) | 2.133 | 2.324 | 0.92× |
| q16 | memory-off | exact | 5.856 | 5.459 | 1.07× |
| q16 | memory-off | native datetime (incompatible) | 5.760 | 17.687 | 0.33× |
| q16 | memory-on | exact | 1.633 | 1.366 | 1.20× |
| q16 | memory-on | native datetime (incompatible) | 1.541 | 1.390 | 1.11× |
| q17 | disk-off | exact | 6.378 | 4.453 | 1.43× |
| q17 | disk-off | native datetime (incompatible) | 3.665 | 2.744 | 1.34× |
| q17 | disk-on | exact | 4.642 | 5.407 | 0.86× |
| q17 | disk-on | native datetime (incompatible) | 1.171 | 0.780 | 1.50× |
| q17 | memory-off | exact | 4.340 | 4.033 | 1.08× |
| q17 | memory-off | native datetime (incompatible) | 3.174 | 2.556 | 1.24× |
| q17 | memory-on | exact | 1.156 | 0.742 | 1.56× |
| q17 | memory-on | native datetime (incompatible) | 1.143 | 0.758 | 1.51× |
| q18 | disk-off | exact | 62.667 | 7.535 | 8.32× |
| q18 | disk-on | exact | 60.226 | 18.981 | 3.17× |
| q18 | memory-off | exact | 3.651 | 4.101 | 0.89× |
| q18 | memory-on | exact | 36.165 | 3.410 | 10.61× |
| q19 | disk-off | exact | 33.649 | 27.048 | 1.24× |
| q19 | disk-on | exact | 49.364 | 1.758 | 28.08× |
| q19 | memory-off | exact | 40.809 | 24.471 | 1.67× |
| q19 | memory-on | exact | 22.003 | 3.345 | 6.58× |
| q20 | disk-off | exact | 141.871 | 6.700 | 21.18× |
| q20 | disk-on | exact | 145.721 | 1.704 | 85.49× |
| q20 | memory-off | exact | 1.973 | 10.670 | 0.18× |
| q20 | memory-on | exact | 2.159 | 18.679 | 0.12× |
| q21 | disk-off | exact | 1.053 | 0.707 | 1.49× |
| q21 | disk-off | native regex/case (incompatible) | 1.041 | 0.306 | 3.40× |
| q21 | disk-on | exact | 1.058 | 0.717 | 1.47× |
| q21 | disk-on | native regex/case (incompatible) | 1.073 | 0.311 | 3.45× |
| q21 | memory-off | exact | 1.078 | 0.703 | 1.53× |
| q21 | memory-off | native regex/case (incompatible) | 1.041 | 0.280 | 3.72× |
| q21 | memory-on | exact | 1.054 | 0.711 | 1.48× |
| q21 | memory-on | native regex/case (incompatible) | 1.057 | 0.269 | 3.92× |
| q22 | disk-off | exact | 1.243 | 0.478 | 2.60× |
| q22 | disk-on | exact | 1.244 | 0.472 | 2.63× |
| q22 | memory-off | exact | 1.212 | 0.486 | 2.49× |
| q22 | memory-on | exact | 1.248 | 0.472 | 2.64× |
| q23 | disk-off | exact | 32.116 | 7.719 | 4.16× |
| q23 | disk-on | exact | 20.247 | 3.346 | 6.05× |
| q23 | memory-off | exact | 34.385 | 19.632 | 1.75× |
| q23 | memory-on | exact | 38.246 | 1.803 | 21.21× |

Raw samples, warmups, parity checks, plans, committed table locations and native/fallback task/file witnesses are retained in this artifact directory. PROCTIME q12 validates per-bidder count conservation and includes waiting for the final processing-time window.

[Per-cell timing, checkpoint and sampled-memory metrics](metrics.csv) retain separate phase medians. Phase medians need not sum to the total median. Memory peaks are sampled during progress polling, not exhaustive job maxima.
