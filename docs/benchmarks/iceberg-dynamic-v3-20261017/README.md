# Iceberg format-v3 dynamic writer diagnostic

Release + mimalloc, 2M generated Arrow rows, one warmup and three measured pairs, alternating engine order. Both engines use a 4 GiB Java heap, parallelism four, four buckets per target, Zstd 3, one-second checkpoints and immediate metadata updates. Timing includes Java routing, native/stock data encoding, Java deletes, a non-draining canonical savepoint and terminal draining canonical savepoint. This writer-only diagnostic is separate from the final Nexmark matrix. Stock readback validates every ID/payload; keyed writes repeat 100k keys.

| Mode | Stock median [range], s | Native median [range], s | Ratio |
|---|---:|---:|---:|
| forward-hash | 2.315 [2.170–2.486] | 2.258 [2.203–2.305] | 1.03× |
| keyed | 2.689 [2.528–3.072] | 2.745 [2.539–3.076] | 0.98× |

All 16 trials passed committed-output, native-data-footer and released-handle/reservation assertions. Format-v3 keyed trials require actual Java Puffin deletion vectors; native v3 reading remains excluded. Raw observations, graphs, final table metadata, checkpoint sizes, environment and compressed logs are retained alongside this report.
