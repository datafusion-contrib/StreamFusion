# Bind the stock temporal-sort cursor and pending rows

Arroyo's window execution splits input into timestamp buckets and completes them as watermarks
advance. StreamFusion's existing sorter retains that structure and stable order within a timestamp.
Stock-compatible recovery also needs Flink's last actually emitted timestamp. A received watermark
can exceed that cursor without emitting data; conflating the two would discard a later accepted row.
The adapter therefore transfers pending full rows, timer membership and the independent cursor.

Ordinary SQL starts from its original stock template. The binding captures the exact released
rowtime operator, empty-key serializer, schema and maximum parallelism, rather than inferring a
key-group space from the single running task. Transpose boundaries retain that same maximum.
Canonical export visits live memory or persistent rows directly. Native checkpoints retain their
raw path; there is no intermediate native snapshot in canonical export.

Imports validate the complete stock inventory and pending timers before installation, and reject
an already populated native target. Source entries remain intact on allocation failure, allowing
retry on a fresh target. The JNI import preserves the memory-limit exception after imported Arrow
owners have been dropped, following Comet's ownership pattern. Restored timer serializers are
registered once per backend; tests use fresh projection backends and the same adapter instance
when retrying within that projection.

The binding covers one ascending rowtime key at every precision supported by released Flink
watermark attributes (zero through three). Secondary comparisons, wider job schemas and the
remaining connector/version/failure contracts stay open under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.
