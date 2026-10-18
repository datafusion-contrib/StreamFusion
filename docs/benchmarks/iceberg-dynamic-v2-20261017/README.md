# Iceberg format-v2 dynamic writer diagnostic

Release + mimalloc, 2M generated Arrow rows, one warmup and three measured pairs, alternating engine order. Both engines use a 4 GiB Java heap, parallelism four, four buckets per target, Zstd 3, one-second checkpoints and immediate metadata updates. Timing includes Java routing, native/stock data encoding, Java deletes, a non-draining canonical savepoint and terminal draining canonical savepoint. This writer-only diagnostic is separate from the final Nexmark matrix. Stock readback validates every ID/payload; keyed writes repeat 100k keys.

| Mode | Stock median [range], s | Native median [range], s | Ratio |
|---|---:|---:|---:|
| forward-hash | 2.249 [2.179–2.301] | 2.313 [2.303–2.322] | 0.97× |
| keyed | 3.206 [2.951–3.641] | 3.371 [3.057–3.505] | 0.95× |

All 16 trials passed committed-output, native-data-footer and released-handle/reservation assertions. Format-v3 keyed trials require actual Java Puffin deletion vectors; native v3 reading remains excluded. Raw observations, graphs, final table metadata, checkpoint sizes, environment and compressed logs are retained alongside this report.
