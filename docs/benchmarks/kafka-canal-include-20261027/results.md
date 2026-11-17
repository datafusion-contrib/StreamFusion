# Kafka source repeated measurements

Median [min–max] elapsed seconds over every measured trial; ratios divide medians.

| Workload | Records | Stock | Previous | Native | Stock/native | Previous/native |
|---|---:|---:|---:|---:|---:|---:|
| canal-all-ten-tables | 1,000,000 | 1.640 [1.395–2.139] | 1.293 [1.188–1.395] | 1.168 [1.067–1.185] | 1.40× | 1.11× |
| canal-all-ten-tables | 10,000,000 | 6.831 [6.574–7.635] | 5.787 [5.518–6.054] | 5.051 [4.950–5.205] | 1.35× | 1.15× |
| canal-one-of-ten-tables | 1,000,000 | 1.214 [1.198–1.433] | 1.124 [1.065–1.191] | 0.973 [0.965–0.983] | 1.25× | 1.16× |
| canal-one-of-ten-tables | 10,000,000 | 5.164 [4.992–6.820] | 4.886 [4.583–4.910] | 3.329 [3.264–4.783] | 1.55× | 1.47× |
| canal-sharded-tables | 1,000,000 | 1.342 [1.230–1.558] | 1.124 [1.099–1.449] | 1.006 [0.980–1.098] | 1.33× | 1.12× |
| canal-sharded-tables | 10,000,000 | 5.274 [4.871–5.451] | 5.053 [4.779–5.391] | 4.018 [3.929–4.144] | 1.31× | 1.26× |
