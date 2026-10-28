# Rejected design: one connector row per batch

These measurements timed an earlier version of the StarRocks sink that handed the stream-load SDK
one pre-joined row per Arrow batch. It was replaced before commit by per-row hand-off. The SDK
then counted batches as rows, checked `sink.buffer-flush.max-rows`, `sink.chunk-limit` and its
flush thresholds once per batch, and needed batch-sized Java strings, which exceed Java's array
limit for wide rows; see
[divergence 49](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/49-starrocks-text-load-bodies.md).
The numbers are kept as the measured cost of that alternative, not as evidence for the current
code. [`results.json`](results.json) identifies the measured sources and libraries by SHA-256.

The design was not rejected for speed. Its complete jobs were faster than the per-row design's,
because the SDK's per-row bookkeeping then ran once per batch: compare the
[long-lived-fixture cohort](../long-lived-fixture/README.md), which ran the per-row design on the
same container later that day.

The host, workload, controls and timing boundary are those of the [parent report](../README.md).
These runs used base `15e7031d` and the container that had been running since the morning, not
the parent's freshly started one. Complete-job medians and full ranges in seconds, with two
warmups and six measured trials per cell:

| Rows | Format | Stock | Previous | Native | Stock / native |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1,000,000 | CSV | 3.394 [3.280–3.550] | 3.531 [3.491–3.664] | 2.798 [2.755–3.140] | 1.21× |
| 1,000,000 | JSON | 4.075 [3.978–4.347] | 4.460 [4.157–4.644] | 3.463 [3.329–3.876] | 1.18× |
| 5,000,000 | CSV | 11.024 [9.907–11.754] | 10.719 [10.096–11.932] | 8.563 [8.295–9.769] | 1.29× |
| 5,000,000 | JSON | 14.601 [14.038–15.273] | 14.334 [13.396–16.403] | 12.156 [11.805–12.386] | 1.20× |

Native and stock ranges separate in every cell. Over the extra 4M rows, the sustained rates are
1.32× stock for CSV and 1.21× for JSON.

The rows-per-batch encoding gate measured 3.27–3.65× over the stock serializer
([`criterion/`](criterion/starrocks_encode.txt), [`encode-java.csv`](encode-java.csv)). Its
production Java entry spent 2.2–2.7 times the Rust encode time, which includes decoding each
batch-sized body into a Java string that the SDK encodes back to UTF-8.

[`cohort-1/`](cohort-1/connector.csv) holds this design's first complete-job run, with one warmup
and three trials. A slow native 1M-row CSV trial and a 5M-row JSON round that slowed every mode
leave its ranges overlapping in two cells.
