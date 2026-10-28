# 49 — StarRocks loads keep the connector; Rust reproduces its text bodies

## Reference pattern

Arroyo's sinks own their transport and serialization natively. Its JSON and text encoders choose
their own spellings, such as Ryu's shortest floats and Arrow's decimal and timestamp formatting.
Comet keeps a native writer behind the host's commit protocol and accepts any output the host's
readers observe as equal.

## StreamFusion decision

The released StarRocks connector keeps everything except the load body: its transaction
stream-load SDK, labels, retries, headers, compression, checkpoint state and exactly-once commit.
Rust produces the body, and it must equal the connector's own CSV or JSON serializer byte for
byte. That covers fastjson's Ryu and escaping, the JVM's legacy `Double.toString`,
`SimpleDateFormat` over the hybrid Julian/Gregorian calendar, `BigDecimal.toString`,
`LocalDateTime.toString` and the JDK's malformed-UTF-8 replacement.

Released StarRocks 4.1 has no Arrow Stream Load. The Doris shape, where Arrow IPC reaches the
server, is therefore unavailable, and the server's text parser defines what is stored.
Equal-looking but differently spelled numbers, dates or escapes can store different values, or
be rejected. Byte identity is the only contract that makes every server-side parse, NULL
conversion and filter decision match.

Two spellings depend on the JVMs that run the job. Each writer task calls `Double.toString`,
which changed in JDK 19. The connector creates its `SimpleDateFormat` where the job is planned,
so that JVM's calendar, locale and time zone travel with the job, while each task builds the
`java.sql.Date` it formats in its own zone. The planner compares both writers on probe values
before admission and keeps the stock writer on any difference, rather than hard-coding one JDK's
behavior. Each writer task repeats the comparison with the planner's formatter. A task whose JDK
or zone would make the stock connector spell differently writes through the connector's own
serializer, so it stores exactly what the stock writer would.

That per-task fallback is the closest this contract gets to the project's rule of falling back
at planning time. The planner cannot inspect TaskManager JVMs, and failing such a task would
break a job that stock Flink runs, for example one submitted from a client in another time zone.

Rust encodes each Arrow batch into slabs of about a megabyte, with every row's end offset, and the
connector receives the rows one at a time through its own pre-serialized-row write path. The
stream-load SDK therefore still frames, accounts, buffers, back-pressures and flushes every row,
and reports the same row and byte metrics, while the JVM never holds a whole batch's rows. Handing
the SDK one pre-joined row per batch measured faster complete jobs, because the SDK's per-row
bookkeeping then ran once per batch. It was rejected because the SDK would count batches as rows,
check `sink.buffer-flush.max-rows`, `sink.chunk-limit` and its flush thresholds once per batch, and
need batch-sized Java strings, which exceed Java's array limit for wide rows. The
[benchmark report](../docs/benchmarks/starrocks-2026-10-26/README.md) retains both designs'
measurements.

The connector's pre-serialized-row constructor would derive stream-load headers from an
unvalidated table. The adapter therefore replaces that function's private stream-load manager with
one built from the planner's validated properties, and reads the stock sink's private options and
schema. These reflective accesses run only after the exact connector release is confirmed, and a
missing member declines the sink at planning time.

## Why deviate

The prime directive is identical results with stock Flink, and here the stock result is decided by
text the server parses. Keeping the connector also keeps every `sink.*` option, both writer APIs
and its recovery protocol verbatim, including its known defects, such as the unified writer's
restore failure on 1.2.16. See the [connector page](../docs/connectors/starrocks.md) for the
encoding table, admission and validation.
