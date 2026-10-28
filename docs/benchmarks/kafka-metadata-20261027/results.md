# Kafka source repeated measurements

Median [min–max] elapsed seconds over every measured trial; ratios divide medians.

| Workload | Records | Stock | Previous | Native | Stock/native | Previous/native |
|---|---:|---:|---:|---:|---:|---:|
| json-metadata | 1,000,000 | 1.619 [1.522–1.890] | 1.332 [1.224–1.390] | 1.016 [0.988–1.149] | 1.59× | 1.31× |
| json-metadata | 10,000,000 | 6.553 [6.307–7.056] | 5.703 [5.331–5.800] | 3.390 [3.275–3.610] | 1.93× | 1.68× |
