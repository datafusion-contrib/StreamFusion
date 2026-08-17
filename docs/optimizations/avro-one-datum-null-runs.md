# Kafka-bounded Avro decode and deferred null runs

**Applies to:** the native bare-Avro Kafka decoder and Arrow Avro record materialization

## What it is

Kafka already supplies one value-message boundary, and Flink's bare-Avro deserializer reads exactly
one datum from that message while ignoring trailing bytes. StreamFusion exposes arrow-avro's
existing one-record primitive through a narrow vendored patch and decodes the datum directly. This
replaces the earlier full schema walk used only to find the datum's end and removes the synthetic
five-byte Confluent header plus payload copy.

The same patch defers physical child values for consecutive nullable records. Null validity bits are
recorded immediately, while placeholder values are appended to child arrays in one bulk run before
the next non-null value or batch flush. This is especially effective for sparse union-like records
such as Nexmark's person/auction/bid event, where two of the three nested records are inactive on
every row. Output remains ordinary Arrow arrays with child lengths aligned at flush.

Confluent Avro retains its schema-id framing and frame-measuring path because writer schemas may
change inside a polled batch; this optimization does not alter that recovery or resolution contract.

## Measured

The isolated 8,192-message Nexmark format-decode benchmark, release build, mini-batching outside the
timed path, measured bare Avro at **2.336M rows/s** in the original matched 20-second profile and
**4.712M rows/s** after both optimizations: a **2.02x native throughput improvement**. The normal
best-of-three run measured the final decoder at **4.768M rows/s (209.7 ns/row)** versus 1.775M rows/s
for Java RowData, making native Arrow **2.69x faster**.

The intermediate one-datum-only profile measured 2.749M rows/s (+17.7% over the matched 2.336M
profile baseline). Its next profile attributed 42.8% of native CPU inclusively to recursive null
materialization; deferred null runs produced the remaining gain. In the final profile the old datum
skipper is absent, `append_nulls` is 7.6% leaf CPU, and Arrow C Data import/export remains below 1%.
