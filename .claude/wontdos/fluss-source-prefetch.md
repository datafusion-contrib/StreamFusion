# One-request Fluss source prefetch

A bounded prototype overlapped the next fetch with current Arrow batch collection,
keeping separate fetched and collected offsets and retaining checkpoint/restore
correctness. It was rejected because matched release-mode runs did not demonstrate
an improvement beyond variability, while it added queue, cancellation and split-owner
state. The shipping source keeps one outstanding fetch and its existing checkpoint
contract. Reconsider if profiling on longer-lived, higher-latency workloads demonstrates
an exposed fetch stall.

Controls: released Fluss 1.0.0, 2M events, four buckets, parallelism four, memory state,
mini-batching off, 2 GiB test JVM, one warmup and three measured stock/native pairs.
Direct frame-aware receive, streaming LZ4 and connection sharing are held enabled.
All exact deterministic output and native-plan checks pass. Complete job seconds,
including teardown:

| Query / prefetch | Stock Flink | StreamFusion |
| --- | --- | --- |
| q0 / false | 4.972316 / 4.824557 / 9.796006 | 2.488826 / 2.457210 / 2.427856 |
| q14 / false | 5.283465 / 5.078214 / 5.103457 | 3.047837 / 2.926985 / 2.908032 |
| q0 / true | 4.919775 / 5.382697 / 4.804491 | 2.530623 / 2.426442 / 2.395931 |
| q14 / true | 5.037875 / 5.014304 / 5.039994 | 3.012810 / 2.870290 / 2.919401 |

Native q0 median improves about 1.3%; q14 about 0.3%. No prototype code or diagnostic
prefetch switch is retained. This decision does not reject source prefetch for every
possible deployment; it rejects its added complexity for the measured hot path.
