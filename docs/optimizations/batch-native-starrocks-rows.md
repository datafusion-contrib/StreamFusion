# Batch-native StarRocks rows

**Applies to:** the StarRocks SQL sink on the Flink 1.18 line ([coverage](../connectors/starrocks.md))

## Encode a batch's rows in Rust

Stock Flink serializes rows one at a time. The connector's transformer turns each `RowData` into
an `Object[]` of boxed and formatted values. Its CSV serializer then joins that array into a
`String`, and its JSON serializer copies it into a `HashMap` for fastjson. StreamFusion encodes a
whole Arrow batch in Rust from typed column views and reproduces those bytes exactly. The per-row
boxing, date-formatting objects, maps and fastjson dispatch disappear.

The encoder is task-local. Format options, logical types and the connector's JSON key order are
parsed once when the writer opens, and each JSON key is pre-escaped together with its colon. Each
batch is exported through the C Data Interface once. Rust then fills a reused buffer, so a slab
takes three allocations totalling 896 bytes, whatever its row count.

Under the small-functionality gate, the connector's serializer is timed in Java over prebuilt
`RowData`, and the native encoder inside Rust over the same rows as prebuilt Arrow batches:

| Format | Row kinds | Rows | Stock Java ns/row | Native Rust ns/row | Speedup |
| --- | --- | ---: | ---: | ---: | ---: |
| CSV | insert | 8,192 | 906 | 291 | 3.12× |
| CSV | changelog | 8,192 | 719 | 226 | 3.19× |
| JSON | insert | 8,192 | 1,195 | 372 | 3.21× |
| JSON | changelog | 8,192 | 940 | 289 | 3.25× |

The [complete report](../benchmarks/starrocks-2026-10-26/README.md) adds 1,024-row batches
(3.12–3.33×) and conversion-inclusive Java timings: through the production Java entry, including
Arrow export, JNI and the per-row hand-off, encoding is 1.35–1.56× faster than stock.

## Hand rows over one at a time from bounded slabs

The connector's stream-load SDK accepts only `String` rows. It converts each back to UTF-8, then
frames, counts and buffers it. Rust returns rows in slabs of about a megabyte with their end
offsets, and each row reaches the SDK as its own `String` through the connector's write path, as
the stock serializer's rows do. The SDK's chunk limit, flush thresholds, back-pressure and
metrics therefore behave as in stock Flink, and a task holds at most one slab of encoded rows.

Handing the SDK one string per batch was measured and rejected
([evidence](../benchmarks/starrocks-2026-10-26/rejected-batch-rows/README.md)). Its complete jobs
were faster, 1.18–1.29× over stock against 1.07–1.28× for per-row hand-off on the same container,
because the SDK's per-row bookkeeping then ran once per batch. It was rejected because the SDK
counted batches as rows, checked `sink.buffer-flush.max-rows`, `sink.chunk-limit` and its flush
thresholds once per batch, and needed batch-sized strings that exceed Java's array limit for wide
rows. Through the production Java entry, per-row strings measured 1.59× (CSV) and 1.46× (JSON) the
stock serializer's speed on 8,192-row batches, against 1.52× and 1.32× for batch strings, each
against stock in the same run on the same base.

## Give every row the SDK's table key

The SDK finds a table's buffer by a `database-table` key. Rows without one, like the stock
serializer's, make it rebuild that string, hash it and compare it with its region map on every
row. Native rows carry the key the SDK itself derives, computed once per task. Each lookup then
reuses one cached hash and an identity match, and the region and every per-row check stay the
same. In a JFR profile of a 5M-row CSV job, the lookup fell from 24.2% to 3.9% of the native sink
thread's samples, and the SDK write path from 42.1% to 12.4%
([profile summary](../benchmarks/starrocks-2026-10-26/before-rebase/profiles.json)).

## Complete jobs

From SQL submission to committed, queryable rows, with both perimeter conversions and the
server's ingestion inside the timer, the native sink's medians are 1.05–1.25× faster than stock
Flink and 1.13–1.24× faster than the previous behavior for 1M- and 5M-row CSV and JSON jobs. Every
range overlaps on this host, and native was the slowest mode in three of the 24 rounds. Over the
extra 4M rows, sustained CSV writing is 1.28× stock and 1.10× the previous behavior, while
sustained JSON writing stays within 0.99–1.07× of stock in every per-row cohort. Profiles show
the writers then waiting for StarRocks, on the same host, to ingest the loads; JSON loads cost
the server the most. These bounded jobs do not establish unbounded-stream or Nexmark throughput.
