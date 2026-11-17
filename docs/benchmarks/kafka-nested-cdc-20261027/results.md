# Kafka source repeated measurements

Median [min–max] elapsed seconds over every measured trial; ratios divide medians.

| Workload | Records | Stock | Previous | Native | Stock/native | Previous/native |
|---|---:|---:|---:|---:|---:|---:|
| nested-canal | 1,000,000 | 3.868 [3.806–4.235] | 2.222 [2.201–2.570] | 1.799 [1.780–1.810] | 2.15× | 1.24× |
| nested-canal | 10,000,000 | 32.052 [31.794–32.442] | 15.952 [15.799–17.097] | 11.763 [11.458–12.099] | 2.72× | 1.36× |
| nested-maxwell | 1,000,000 | 2.345 [2.277–2.638] | 1.548 [1.489–1.645] | 1.295 [1.274–1.309] | 1.81× | 1.20× |
| nested-maxwell | 10,000,000 | 16.091 [15.817–16.742] | 8.416 [8.191–9.002] | 6.454 [6.404–6.637] | 2.49× | 1.30× |
