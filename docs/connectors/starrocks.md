# StarRocks

**Status:** partial — native Stream Load body encoding for the SQL sink. The StarRocks source,
the DataStream sink API and the declined cases below keep the released connector unchanged.

The optional `streamfusion-starrocks` module adapts the released
`com.starrocks:flink-connector-starrocks:1.2.16_flink-1.18` connector. StarRocks publishes no
Flink 2.x connector, so the module exists only on StreamFusion's
[Flink 1.18 line](../flink-compatibility.md). Install the connector JAR and the
`streamfusion-starrocks-flink1.18` JAR with the loader and core. The module carries its own small
native library (`libstreamfusion_starrocks`), which depends only on the shared bridge crate. Without
the connector on the classpath the adapter stays inactive.

Set `-Dstreamfusion.starrocks.enabled=false` to keep the stock writer while native SQL operators
still run upstream of it.

## Sink

The native part is the rows of the Stream Load body. Arrow batches from native operators, or from
the row transpose after a rowwise source, are encoded in Rust into exactly the rows the connector's
own CSV or JSON serializer would produce. Rust returns them in slabs of about a megabyte, with each
row's end offset, and every row reaches the connector through its own pre-serialized-row write
path. The stream-load SDK therefore still frames, buffers, back-pressures and flushes each row, and
its `sink.chunk-limit`, `sink.buffer-flush.*` thresholds and row and byte metrics behave as in the
row-at-a-time writer.

Everything else is the released connector, including:

- the transaction stream-load SDK, label generation and lingering-transaction abort;
- buffering, flush intervals, retries, compression and every `sink.properties.*` header,
  including generated `columns` lists, partial updates and merge conditions;
- checkpoint state and the exactly-once commit on checkpoint completion.

The planner mirrors the writer the table selects:

- `sink.use.new-sink-api=false` (the default) keeps the connector's transactional sink
  function. Its pre-serialized-row constructor would skip table validation and add a `columns`
  header for aligned CSV tables, so the adapter rebuilds the function's stream-load manager from
  the planner's validated properties. Every request then carries the stock headers.
- `sink.use.new-sink-api=true` uses the unified sink. Its writer receives each encoded row, and
  its writer state, committables and committer are the connector's own.

Besides the connector's metrics, each native writer task reports `nativeStarRocksEncodedBatches`,
`nativeStarRocksEncodedRows`, `nativeStarRocksEncodedBytes` (row bytes before the SDK's
delimiters) and `nativeStarRocksEncodingNanos` (Arrow export and native encoding). A task that
keeps the connector's serializer, as described below, logs a warning and reports none of them.

### Byte-identical encoding

The encoder reproduces the connector's spellings, which come from `StarRocksTableRowTransformer`
and the shaded fastjson 1.2.83 it uses:

| Flink type | CSV | JSON |
| --- | --- | --- |
| BOOLEAN | `1` / `0` | `1` / `0` |
| TINYINT … BIGINT | decimal | number |
| FLOAT, DOUBLE | the JVM's legacy `Float`/`Double.toString` | fastjson's Ryu (shortest round-trip digits); NaN and ±Infinity become `null` |
| DECIMAL | `BigDecimal.toString`, including scientific forms such as `0E-10` | number, same spelling |
| CHAR, VARCHAR | text, unescaped | fastjson string escaping |
| DATE | `SimpleDateFormat("yyyy-MM-dd")` over `java.sql.Date` | quoted, same spelling |
| TIMESTAMP, TIMESTAMP_LTZ | `LocalDateTime.toString()` (UTC wall time for LTZ) | quoted, same spelling |
| BINARY(n) | the bytes folded big-endian into a `long`, with Java's wrapping shifts | number |

SQL NULL is `\N` in CSV and an omitted key in JSON. JSON keys follow the iteration order of the
connector's row `HashMap`. Malformed UTF-8 is replaced exactly as the JDK decoder does, because
Flink falls back to that decoder.

Primary-key tables append the connector's `__op` column: `0` for inserts and updates, `1` for
deletes. An update-before is written as a delete only when `sink.ignore.update-before=false`.
Tables without a primary key drop deletes and update-befores, as the connector does.

Two spellings depend on the JVM, so the planner checks them before admitting a sink. For every
admitted FLOAT and DOUBLE (CSV) or DATE column, it compares the stock serializer with the native
encoder on probe values:

- `Double.toString` changed in JDK 19. The stock serializer calls it on each writer task.
- The connector creates its `SimpleDateFormat` in the JVM that plans the job, so that JVM's
  calendar, locale and time zone travel with the job. Each writer task builds the
  `java.sql.Date` it formats at midnight in the task's own zone. In particular,
  1582-10-05…14 print ten days later, zones that skipped a day print it differently, and a
  planner west of its TaskManagers prints every date one day early.

The DATE probes include the day on each side of every transition in both zones. A difference at
planning time keeps the stock writer. The planner can only check its own JVM, so each writer task
repeats the probe when it starts, with the formatter the planner created. A task whose JDK feature
release or time zone makes the stock connector spell differently writes that task's rows through
the connector's own serializer: it reads each Arrow batch back into rows and stores exactly what
the stock writer would. Encoding the native spelling there would silently change what the server
stores.

Timestamps print every fraction a value carries, following the project's
[timestamp value contract](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/39-timestamp-value-contract.md). Values within their
declared precision, as Flink normally produces them, spell exactly as stock.

### Admission

A StarRocks SQL sink runs natively only when all of the following hold. The fallback reason a
declined sink records is shown after each condition, prefixed `starrocks sink:` in planner
output.

- The connector is exactly 1.2.16 for Flink 1.18 (`only connector 1.2.16_flink-1.18 is verified`).
- The sink has no abilities such as overwrite or partitioning (`sink abilities`).
- The writer is transactional:
  - `sink.version=V1` declines (`sink.version=V1 keeps the connector's non-transactional writer`).
  - The connector's own `AUTO` detection must select V2 (`the connector selected sink version …`).
- No multi-table transactions
  (`sink.transaction.multi-table.enabled needs the connector's partition-aware writer`) and no
  merge commit (`merge commit (sink.properties.enable_merge_commit) is not natively verified`).
- Every column is BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, CHAR, VARCHAR,
  DATE, TIMESTAMP, TIMESTAMP_LTZ or BINARY. Other types decline with
  `column … of type … is not natively encoded`. Arrays, maps, rows, VARBINARY, TIME and the like
  keep the connector's nested serialization.
- NOT NULL enforcement uses Flink's default `ERROR` policy, and type-length enforcement is off;
  other policies keep Flink's `ConstraintEnforcer`.
- The JVM-dependent spellings agree
  (`DATE spelling differs from this JVM's SimpleDateFormat …` or
  `FLOAT spelling differs from this JVM's legacy Float/Double.toString (JDK 19+)`).
- The connector prepares the table over JDBC exactly as its own sink would. A failure declines
  with `the connector could not prepare the table: …`, and the stock writer then reports the
  same error.
- No text column targets a StarRocks JSON column, or a column the connector cannot recognize by
  its exact Flink name, such as `LARGEINT` (reported as `bigint unsigned`) or a differently cased
  name. The connector re-parses such text through fastjson when it starts with `{` or `[`
  (`text column … targets a StarRocks JSON column …` / `… unrecognized or differently cased
  column …`).
- A primary-key table has no Flink column named `__op`
  (`column __op collides with the connector's upsert flag`).

### Parallelism, routing and materialization

The adapter applies Flink 1.18's sink translation to Arrow batches:

- the NOT NULL check;
- the rejection of a changelog that a primary-key-less sink would receive at another
  parallelism;
- `table.exec.sink.keyed-shuffle` routing by primary key, through the native columnar exchange;
- upsert materialization through the shared
  [native sink materializer](../operators/sink-upsert-materialize.md), with the job's idle-state
  retention.

Flink compares the configured sink parallelism with its input's unresolved parallelism, which
stays at the default (-1) above a hash exchange. The adapter makes the same comparison, so the
same jobs are rejected and keyed.

### Failure behavior

- Exactly-once commits only what a completed checkpoint prepared, in both paths.
- Server-side rejections, such as rows StarRocks filters, fail the same load with the same
  status and message, because the bodies are identical.
- Under at-least-once, a NOT NULL violation fails its whole Arrow batch before the writer. The
  stock row-at-a-time writer may still flush the rows that preceded the violating row while it
  closes. Both jobs fail; only the partially written prefix can differ.
- Connector 1.2.16 cannot restore the unified sink's writer state once a label prefix has stored
  label snapshots. Recovery fails with a `ClassCastException`
  ([StarRocks/starrocks-connector-for-apache-flink#518](https://github.com/StarRocks/starrocks-connector-for-apache-flink/issues/518),
  fix proposed in
  [#519](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/519)). The native
  path mirrors the connector's writer choice and fails the same way.

## Validation

The suite script needs Docker. The fixture is the released `starrocks/allin1-ubuntu:4.1.6` image,
pinned by digest, with FE HTTP, MySQL, BE HTTP and BE thrift ports on `127.0.0.1`. The released
connector ITs start the same pinned image through their own test harness, on fixed ports, after
the script's fixture stops. `bin/starrocks-suite.sh` runs the following layers in order:

1. Rust unit tests for the encoder, plus `StarRocksEncoderRefereeTest`. The referee compares every
   native row, in order, byte for byte with the row the connector's own serializer produces, over:
   - edge and seeded random rows for every admitted type, in both formats and all row-kind
     policies;
   - every precision and scale of DECIMAL and TIMESTAMP, and every BINARY length;
   - every Basic Multilingual Plane character and surrogate pair, and malformed UTF-8;
   - configured separators, including multi-byte and sign-extended hex escapes;
   - JSON key orders for wide and hash-colliding schemas;
   - every Flink date from 0000-01-01 to 9999-12-31 in UTC, New York and Kolkata;
   - jobs planned in one time zone whose tasks run in another, which keep the connector's
     serializer or stay native exactly as the stock spelling requires;
   - rows across slab boundaries and single rows several times larger than a slab.
2. `StarRocksPlannerTest` for the declines that need no server.
3. `StarRocksSqlIT`: 57 stock-against-native cases on the real server, comparing the server's own
   spellings of both tables. It covers:
   - every type in both formats and both writer APIs, and all four table models;
   - changelogs with both update-before policies, and native aggregate upserts;
   - parallel and materialized writers, with Flink's parallelism rejection at equal and other
     sink parallelism;
   - recovery under both semantics and the #518 failure;
   - NOT NULL and server rejections;
   - partial updates, column mappings, declines and custom separators;
   - compressed multi-load writes, zone-dependent dates and a fully columnar pipeline;
   - dates planned in another zone than their tasks, through both writer APIs;
   - two writers in one job, empty input, and allocator and native-handle leak checks.
4. The unchanged `StarRocksSinkITTest` and `KafkaToStarRocksITTest` from the connector's v1.2.16
   tag, pinned to commit `0657e0dbf24951ecad1bb9ee1800bfa86e7991a5`. The Kafka test replays
   Debezium change logs from Kafka into primary-key tables, including a key change that becomes a
   delete. Both are compiled against the released jar, with a hash manifest recording that no
   file or replayed change log was adapted, and run under StreamFusion's upstream-suite agent.
   Execution contracts cover 84 invocations across 101 tests (17 are upstream's own V1 skips).
   The 58 transactional writer invocations require positive native encoder work. Six V1
   invocations decline the sink but still run their upstream Calc natively, so they require that
   Calc work; the other 20 require full fallback with the exact reason.
