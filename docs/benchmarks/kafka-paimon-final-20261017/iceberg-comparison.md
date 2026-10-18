# Same-host Kafka sink comparisons

| Query | Paimon stock / native median, s | Paimon speedup | Iceberg stock / native median, s | Iceberg speedup |
|---|---:|---:|---:|---:|
| q0 | 1.893 / 1.270 | 1.49× | 1.914 / 4.709 | 0.41× |
| q1 | 1.928 / 1.326 | 1.45× | 1.766 / 1.229 | 1.44× |
| q2 | 1.394 / 1.158 | 1.20× | 1.227 / 1.131 | 1.08× |
| q3 | 1.665 / 1.504 | 1.11× | 1.207 / 1.209 | 1.00× |
| q4 | 11.905 / 5.376 | 2.21× | 22.228 / 6.005 | 3.70× |
| q5 | 3.583 / 4.618 | 0.78× | 3.482 / 4.997 | 0.70× |
| q7 | 6.382 / 3.963 | 1.61× | 20.495 / 3.357 | 6.11× |
| q8 | 2.774 / 2.097 | 1.32× | 1.294 / 1.377 | 0.94× |
| q9 | 27.723 / 6.954 | 3.99× | 16.352 / 11.933 | 1.37× |
| q10 | 1.948 / 1.435 | 1.36× | 1.926 / 1.439 | 1.34× |
| q11 | 5.345 / 1.057 | 5.06× | 5.192 / 1.081 | 4.80× |
| q12 | 1.578 / 1.182 | 1.33× | 1.429 / 1.175 | 1.22× |
| q13 | 1.600 / 1.222 | 1.31× | 1.816 / 1.394 | 1.30× |
| q14 | 1.743 / 1.476 | 1.18× | 1.997 / 1.562 | 1.28× |
| q15 | 9.680 / 5.839 | 1.66× | 11.525 / 10.558 | 1.09× |
| q16 | 6.930 / 5.090 | 1.36× | 7.117 / 7.759 | 0.92× |
| q17 | 4.003 / 2.112 | 1.90× | 6.581 / 6.667 | 0.99× |
| q18 | 18.688 / 11.560 | 1.62× | 16.277 / 4.270 | 3.81× |
| q19 | 17.964 / 9.392 | 1.91× | 14.719 / 26.405 | 0.56× |
| q20 | 4.359 / 9.493 | 0.46× | 4.769 / 4.445 | 1.07× |
| q21 | 1.595 / 1.453 | 1.10× | 1.572 / 1.501 | 1.05× |
| q22 | 1.765 / 1.182 | 1.49× | 1.825 / 1.236 | 1.48× |
| q23 | 10.881 / 17.014 | 0.64× | 28.388 / 4.037 | 7.03× |

| Exact-query selection | Paimon geomean | Iceberg geomean |
|---|---:|---:|
| all | 1.43× | 1.44× |
| append | 1.24× | 1.45× |
| primary-key | 1.97× | 1.40× |

Both use 2M unchanged Kafka JSON events, four input partitions, job parallelism four, RocksDB, mini-batching off, a 4 GiB JVM heap and 512 MiB managed memory across four slots. Each cell has one warmup and three alternating measured pairs. Both keep all samples and validate committed output.

The formats retain their own sink semantics. Paimon uses four fixed buckets, default Zstd level 1, 256 MiB append / 128 MiB primary-key file targets and in-job compaction. Iceberg uses four bucket-transform partitions, Zstd level 3, a 512 MiB target and Java delete files for upserts. Writer parallelism is audited separately. Optional native library sets differ; production engine/query source code is unchanged. These separate runs are a same-host diagnostic comparison, not a concurrent connector-only A/B.

A speedup difference can come from either stock or native timings. Inspect both columns and the retained trial ranges; neither ratios nor elapsed time alone identify source, sink or state cost.
