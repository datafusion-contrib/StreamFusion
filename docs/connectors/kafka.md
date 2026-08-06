# Kafka

**Status:** Native on both the source and sink sides — including the exactly-once transactional
commit hand-off — across the full value-format family Flink itself supports for Kafka (JSON, CSV,
Avro, Avro Confluent, Protobuf, raw, and the CDC JSON envelope dialects). A specific, enumerated set
of table shapes, key configurations, properties, and security schemes falls back to stock Flink;
those conditions are listed precisely below rather than summarized as "partial" support. Per-format
decode/encode detail (byte-level option handling, type coverage, parity corner cases) lives on each
format's own page, linked throughout.

Unlike the [Parquet connector](parquet.md) — where the native side only produces Parquet bytes and
Flink's own filesystem plugins, credentials, and exactly-once commit own everything else — Kafka is
native end to end on both source and sink, including the exactly-once producer identity that
Flink's committer resumes.

## Source

The source requires both the `streamfusion-kafka` connector extension and the matching
`streamfusion-*` format JAR to be installed; either missing is a planner fallback to Flink's own
connector, never a linkage failure. See [Deployment](../deployment.md) for the JAR list.

### Value formats

A `kafka` table's value format decodes natively for insert-only streams in
[JSON](formats/json.md), [CSV](formats/csv.md), [raw](formats/raw.md), bare [Avro](formats/avro.md),
[Avro Confluent](formats/avro-confluent.md), and [Protobuf](formats/protobuf.md), and for full
changelog streams in the four [CDC JSON](formats/cdc-json.md) envelope dialects
(`debezium-json`, `canal-json`, `maxwell-json`, `ogg-json`) and `debezium-avro-confluent`. Any other
value format falls back. Format-specific decode wrinkles (protobuf presence/enum handling,
`ignore-parse-errors` granularity, and so on) are covered on each format's page, not here.

### Keyed tables

Exactly one keyed shape decodes on the fused native source path: `key.format = 'raw'` with a
**single** key field, over an insert-only JSON/CSV/raw value format. It reproduces Flink's merge
semantics exactly — a raw key produces one key row per record, a `NULL` Kafka key keeps the record
with a `NULL` key column, `EXCEPT_KEY`/`ALL` projections resolve as Flink's factory does (value
fields win the `ALL` overlap), and a JSON value fanning a top-level array into N rows shares the
record's key across all of them. Projection pushdown is off for keyed tables.

Falling back: any `key.format` other than `raw`; multiple `key.fields` (raw is single-column); an
`ALL` projection combined with `key.fields-prefix` (Flink's own rejection); a non-UTF-8
`key.raw.charset`; a keyed CDC value format; and keyed Avro or Protobuf value formats (their schema
providers derive from the gated row type).

### Metadata columns

A metadata column (`ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'` and its kin) falls back on
every decoded path, insert-only and CDC alike — the connector, not the value decode, fills it, so a
value-only decode would silently emit `NULL`. A metadata column that's declared but never
referenced also declines, since Flink keeps it in the scan's output regardless of projection.
Computed columns are unaffected on insert-only paths — the planner projects them above the native
scan.

### Bounded scanning

Only `scan.bounded.mode` of unbounded or `latest-offset` runs natively; any other bounded mode
falls back.

### Startup offsets

All Flink startup modes remain supported. `earliest-offset`, `timestamp`, and `specific-offsets`
can use the fused native source. `latest-offset` and `group-offsets` retain Flink's source reader
and use the native decode operator: this preserves Flink's precise job-submission/savepoint
boundary for latest offsets and its committed-offset, auto-commit, and missing-offset exception
semantics. The message format is still decoded natively on that shallow path.

### Consumer properties and security

Consumer property translation is fail-closed: every supplied `properties.*` key is classified
against kafka-clients 4.2's `ConsumerConfig`, and anything unclassified is a fallback — vanilla
Flink instead forwards arbitrary keys and lets kafka-clients merely warn. Java-owned coordination
keys (`client.id.prefix`, discovery and commit-on-checkpoint options, deserializers,
group-membership/assignment machinery that never engages under manual assignment, reader-call
tuning like `max.poll.records`) are honored on the JVM side and deliberately not forwarded to the
native layer.

Security running natively: PLAINTEXT, SSL, SASL_PLAINTEXT, and SASL_SSL with the PLAIN,
SCRAM-SHA-256, or SCRAM-SHA-512 mechanisms — credentials from a Plain/Scram JAAS module, PEM trust
and key material, and (when SSL is on with no configured truststore) a probe of the platform CA
bundle to match the JVM's default trust. The native DSO links OpenSSL statically, so no system
OpenSSL install is required.

Falling back: client plugins (`interceptor.classes`, metric reporters, `config.providers`,
`security.providers`); `sasl.login.*`/`sasl.oauthbearer.*` (OAUTHBEARER needs the Java client);
**SASL/GSSAPI (Kerberos)** entirely — every `sasl.kerberos.*` key, a `Krb5LoginModule`, an explicit
`GSSAPI` mechanism, or a SASL protocol with no mechanism set (the Java default is GSSAPI) — because
cyrus-sasl is deliberately excluded from the portable native build; JVM-specific SSL machinery
(protocol/algorithm selection, engine factories, inline PEM strings, JKS/PKCS#12 stores needing
conversion); SASL credentials missing from `sasl.jaas.config`; `metadata.recovery.*`; an
unrecognized JAAS login module; an unmappable `auto.offset.reset` (e.g. `by_duration:...`); and any
unknown key. See [Configuration](../configuration.md) for the `streamfusion.kafka.prefetch-mb`
off-heap budget this source draws on, and [Native Kafka source](../optimizations/native-rdkafka-source.md)
for why the source is built this way.

### Watermarks

A pushed `WATERMARK` clause regenerates inside the native source for the reproducible shapes:
bounded out-of-orderness (`rt` or `rt - INTERVAL const`) over a physical rowtime column, or
`TO_TIMESTAMP_LTZ(bigintCol, 3)` (the epoch-millis computed-rowtime idiom), with periodic emit and
`scan.watermark.idle-timeout`/`table.exec.source.idle-timeout` honored. Decode happens in-poll, so
the split reader stamps each per-partition batch's max rowtime as its record timestamp and Flink's
own per-split machinery (one generator per split, min combination, idleness) reproduces the pushed
strategy exactly — the same shared path the Fluss source uses.

Falling back: an `on-event` emit strategy, watermark alignment, `SOURCE_WATERMARK()`, or any other
rowtime expression; a watermarked CDC table (CDC decodes in an operator downstream of Flink's
source, which can't regenerate watermarks); and a watermarked table the native source can't consume
at all (the decode-operator path regenerates no watermarks either).

## Sink

### Delivery modes

A fixed-topic `kafka`/`upsert-kafka` table runs its data plane natively in one of two shapes,
selected by the sink's own delivery guarantee:

- **Exactly-once, incremental transaction naming** — the whole data plane is native. Rust
  serializes and produces each Arrow batch inside a librdkafka transaction, flushes it at the
  checkpoint barrier, and surfaces the transaction's `(producer id, epoch)` as a real Flink
  `KafkaCommittable`; Flink's own committer, checkpoint-completion commit, recovery re-commit, and
  probing abort on restore remain the host's contract unchanged. The producer identity is read
  authoritatively from the transaction coordinator (`DescribeTransactions`, KIP-664), so this path
  requires brokers **≥ 3.0** — on an older cluster the writer fails at startup with the broker's
  admin error, and the table must use the Java sink instead.
- **`none`/at-least-once delivery** — serialization is native; the resulting key/value bytes feed
  Flink's unmodified `KafkaSink`, whose producer, parallelism, and metrics contracts apply verbatim.

### Table shapes and formats

| Table shape | Supported formats | Changelog admitted |
|---|---|---|
| plain `kafka`, insert-only value | [JSON](formats/json.md), [CSV](formats/csv.md), [Protobuf](formats/protobuf.md), [raw](formats/raw.md), [Avro](formats/avro.md), [Avro Confluent](formats/avro-confluent.md) | insert-only only |
| plain `kafka`, CDC value format | `debezium-avro-confluent`, the four [CDC JSON](formats/cdc-json.md) envelopes (`debezium-json`, `canal-json`, `maxwell-json`, `ogg-json`) | full changelog — the CDC format itself requests UPDATE_BEFORE from the planner |
| `upsert-kafka` | JSON, CSV, Avro, Avro Confluent, Protobuf, raw for key and value (resolved independently, so key and value formats can differ) | upsert changelog: INSERT/UPDATE_AFTER carry a serialized value, UPDATE_BEFORE/DELETE carry a Kafka tombstone |

A plain `kafka` table may carry a `PRIMARY KEY` alongside a CDC value format (Flink permits a PK
there) and, as in Flink, produces no key output without an explicit `key.format`. `upsert-kafka`
keeps Flink's own rejection of CDC value formats — that's parity with Flink, not a fallback.

### Producer properties

Producer properties are normalized against kafka-clients' own defaults and translated to
librdkafka **one classified key at a time**; a property outside the classification is a planner
fallback, never silently dropped or ignored.

### Fallback causes

Shape and ability fallbacks, independent of wire format:

- a changelog produced by a host-side operator (including the currently disabled native
  non-windowed `GROUP BY` paths) — the all-or-nothing island gate keeps its downstream Kafka sink
  on Flink too; a natively decoded CDC source can still feed the supported CDC/upsert sink shapes;
- an **upsert-materialized sink** — when Flink decides the upsert changelog can arrive out of
  order it bakes a stateful `SinkUpsertMaterializer` into its own sink translation, which a
  substituted native sink would silently drop;
- a **keyed ordinary `kafka` table** (only `upsert-kafka` carries a key at the sink);
- **`sink.parallelism` on a changelog input** — stock Flink keys that edge by primary key (or
  rejects the plan without one) so a key's changes stay ordered across the parallelism change,
  and the native sink would only rebalance whole batches; insert-only inputs keep
  `sink.parallelism` native, since there's no per-key ordering to preserve;
  a non-default partitioner, sink-side buffer flushing, writable metadata, or any other sink
  ability beyond plain production;
- an **explicit key/value projection**, key prefix, or `EXCEPT_KEY` value projection;
- a connector library built without a given format's encode arm — probed at plan time, so a
  missing optional format module is a fallback rather than a runtime dispatch failure.

Connectivity and transactional fallbacks:

- missing `properties.bootstrap.servers`, or exactly-once configured without a transactional ID
  prefix;
- exactly-once with a transaction naming strategy other than `INCREMENTING` (`POOLING` is a
  planned follow-up);
- exactly-once with a producer property the native translator can't guarantee parity for: an
  unclassified/unknown key, `transactional.id` (owned by Flink's own naming strategy),
  `enable.idempotence=false`, custom serializers/partitioner/interceptors, non-default adaptive
  partitioning or `partitioner.ignore.keys=true`, `batch.size=0`, JKS or other non-PEM security
  material, SASL/GSSAPI (Kerberos), or a SASL mechanism outside PLAIN/SCRAM-SHA-256/SCRAM-SHA-512.

Per-format value fallbacks (an unsupported value/key format, option values Flink's own factory
rejects, and format-specific type-family limits) are documented on each format's page linked above.
