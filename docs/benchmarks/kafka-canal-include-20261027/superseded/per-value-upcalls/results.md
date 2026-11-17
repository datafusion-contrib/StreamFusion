# Kafka source repeated measurements

Median [min–max] elapsed seconds over every measured trial; ratios divide medians.

| Workload | Records | Stock | Previous | Native | Stock/native | Previous/native |
|---|---:|---:|---:|---:|---:|---:|
| canal-all-ten-tables | 1,000,000 | 1.515 [1.395–2.487] | 1.330 [1.177–1.421] | 1.281 [1.166–1.335] | 1.18× | 1.04× |
| canal-all-ten-tables | 10,000,000 | 7.888 [6.970–9.329] | 6.378 [6.111–8.419] | 6.395 [6.104–6.495] | 1.23× | 1.00× |
| canal-one-of-ten-tables | 1,000,000 | 1.321 [1.209–1.404] | 1.199 [1.188–1.307] | 1.001 [0.959–1.130] | 1.32× | 1.20× |
| canal-one-of-ten-tables | 10,000,000 | 5.458 [5.093–6.926] | 5.300 [4.762–6.154] | 3.713 [3.487–4.379] | 1.47× | 1.43× |
| canal-sharded-tables | 1,000,000 | 1.571 [1.438–2.738] | 1.450 [1.199–1.769] | 1.437 [1.377–1.490] | 1.09× | 1.01× |
| canal-sharded-tables | 10,000,000 | 5.656 [5.068–6.785] | 5.571 [4.964–6.599] | 7.900 [7.456–8.431] | 0.72× | 0.71× |
