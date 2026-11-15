# Stock window rank and deduplication state binding

Flink 2.2.1 WindowAggOperator and its synchronous event-time rank/dedup processors define the
canonical boundary. Their actual serializers, processor time zone, original identities, resolved
maximum parallelism, Long window-end namespaces, event timer service and UNION watermark are
preserved. Private runtime lowering leaves the stock portable plan unchanged and needs no user
operator mapping or explicit UID.

The native ranker follows Arroyo's window function structure: partition retained Arrow batches
and release windows on time progress. Canonical transfer additionally projects directly into
Flink's retained sort-key lists or winner rows. This stock-schema projection differs from Arroyo's
checkpoint representation because an unmodified stock Flink process must restore the result.
The Arrow bridge follows Comet's moved FFI ownership pattern; it does not introduce a serialized
native snapshot before export.

Calendar and first/last tie configuration must precede restore-time row import, rather than run
only in open. Managed UNION watermarks accompany raw native checkpoints as well as canonical
savepoints. A restored bound owner clamps stale watermarks to that saved progress. Flink's
maximum-watermark sentinel behavior remains unchanged. Global owners preserve the actual stock
singleton maximum rather than infer the configured pipeline maximum.

Qualification and remaining gaps are on the window rank/dedup operator pages. All fifteen state
families and all ten acceptance criteria remain partial/open under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569); benchmarking is deferred.

Ordinary processing-time window rank/dedup keeps its existing native runtime graph. The released
stock translators reject these queries, so event-time template binding must not route processing
time through them. Six direct planner regression controls cover keyed/global rank, first and last
selection, and independently assert the released stock rejection. This preserves native processing
support without claiming a stock canonical counterpart.
