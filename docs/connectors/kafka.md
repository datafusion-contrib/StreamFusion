# Kafka

**Status:** Partial. StreamFusion accelerates Kafka value/key serialization and deserialization;
Flink's Kafka enumerator, partition split reader/client, and sink own every broker interaction.

This boundary is intentional. It gives an accelerated job the same Kafka client, defaults,
properties, security plugins, partition discovery, offset/checkpoint behavior, producer batching,
metrics, and transaction implementation as the equivalent Flink job. StreamFusion does not
translate kafka-clients settings to librdkafka and does not create native Kafka consumers or
producers.

The `streamfusion-kafka` connector extension and the matching `streamfusion-*` format extension
must both be installed. A missing extension is a planner fallback, never a linkage failure. See
[Deployment](../deployment.md).

Nested ROW transport metadata is excluded from format input. The annotation plan removes only
marked RowKind and binary-row sidecars, recursively through containers, while retaining the
logical field names, parent nulls and existing Arrow data buffers. A user field with a similar
name remains a SQL field. The same logical view feeds JSON, CSV, Avro and Protobuf encoders;
it does not change broker ownership or the format-specific admission limits.

## Flink release lines

Each build line pins the released Kafka connector and the client used with it:

| | Default build | `flink-1.18` build |
| --- | --- | --- |
| Flink | 2.2.1 | 1.18.1 |
| Kafka connector | `flink-connector-kafka:5.0.0-2.2` | `flink-connector-kafka:3.2.0-1.18` |
| Kafka client | 4.2.0 | 3.4.0 |

The direct Kafka client dependency is needed for source compilation. Keep it aligned with
its connector: overriding the transitive client also changes the broker-facing implementation.
StreamFusion retains Flink's connector for broker interactions, so its native format paths do
not supply connector or client fixes absent from the selected releases.

The `streamfusion-kafka` module tests exercise the 1.18 pairing under `-Pflink-1.18`.
The unchanged upstream Kafka 3.2 suite exercises `KafkaChangelogTableITCase`,
`KafkaTableITCase` and `UpsertKafkaTableITCase` against real brokers on that line.
`DynamicKafkaTableITCase` belongs to the newer connector suite. Upstream Kafka invocations do
not yet have individual native-route contracts: passing these integration tests establishes
compatibility, not native execution for every invocation. See the
[upstream suite's validation evidence](../upstream-flink-suite.md) for the distinction between
suite results and native coverage.

## Source: Flink consumption, native decode

Flink continues to own topic enumeration, split assignment, offsets, checkpoints, authentication,
and the Kafka client. StreamFusion wraps Flink's source and delegates enumeration and split-state
serialization to it. At the task, Flink's partition split reader groups raw key/value bytes into
batches that StreamFusion decodes directly to Arrow in Rust, avoiding Flink's
format-to-`RowData` materialization without losing the Kafka split identity.

For JSON, the split reader copies one poll into a reusable direct `[keys][values]` byte slab and
passes only its address plus row lengths to the task-local native decoder. It does not construct,
export, or re-import Arrow binary input vectors. The decoded output still crosses the Arrow C Data
Interface, while the parser buffers, schema lookup plan, and recursive appenders remain attached to
the decoder handle across polls.

Native value decoding covers insert-only [JSON](formats/json.md), [CSV](formats/csv.md),
[raw](formats/raw.md), bare [Avro](formats/avro.md), [Avro Confluent](formats/avro-confluent.md),
and [Protobuf](formats/protobuf.md), plus the supported [CDC JSON](formats/cdc-json.md) envelopes
and `debezium-avro-confluent`. Format-specific type and option gaps are listed on those pages.

All Flink Kafka startup modes, consumer properties, authentication schemes, interceptors,
partition discovery, group-offset behavior, and checkpoint offset commits remain available
because Flink constructs and runs the source. The native source wrapper admits unbounded input and
`scan.bounded.mode = 'latest-offset'`; other bounded modes use Flink's stock source path. No Kafka
property translation gate exists.

JSON, bare Avro, Avro Confluent, and Protobuf can push a physical-column projection into native
decode. CSV and raw decode their complete positional record. A keyed table currently also decodes
the full value record.

### Source admission and fallbacks

- A metadata column falls back because connector metadata is not present in the message value.
- A source bounded by anything other than `latest-offset` stays on Flink's stock source path.
- Pushed periodic bounded-out-of-orderness watermarks (`rowtime` or `rowtime - INTERVAL constant`
  with non-negative day-time or year-month intervals, including chained subtractions)
  are regenerated from each partition's decoded Arrow batches, including the common
  `TO_TIMESTAMP_LTZ(epoch_millis, 3)` rowtime form. Flink runs one generator per Kafka split and
  combines them with its normal minimum/idleness logic. On-event emission, watermark alignment,
  connector-defined source watermarks, CDC changelog tables, and other expressions stay on Flink.
  YEAR, MONTH and YEAR TO MONTH use Flink's calendar subtraction. The decoder computes
  `MAX(rowtime - interval)` before forwarding the batch and carries this candidate separately
  from its maximum event timestamp, so month-end clamping cannot lose the maximum candidate.
  The source serializes the shared native expression plan and creates one evaluator per split
  reader. Projection remapping and source sharing retain the expression's typed literals and
  column references; native handles remain local to the reader and are closed with it.
- Keyed native decoding currently requires a single raw key field over a supported insert-only
  value format. Other key formats/shapes stay on Flink.
- CDC values combined with `key.format` stay on Flink.
- Unsupported format options or logical types stay on Flink as documented by the format page.

## Sink: native encode, Flink production

The native serialization operator imports one Arrow batch and emits the final heap `byte[]` key
and value records. Those pre-serialized records feed Flink's unmodified `KafkaSink` for every
delivery guarantee: `none`, `at-least-once`, and `exactly-once`.

The operator creates one native encoder plan at `open`: format options, logical types, field names,
and key/value projections cross JNI and are parsed once rather than once per Arrow batch.

Consequently Flink owns producer construction, partitioning, batching, compression, callbacks,
metrics, transaction naming, checkpoint preparation, commit, abort, and restore. Producer
`properties.*` are passed to kafka-clients unchanged. Exactly-once still requires the normal
`sink.transactional-id-prefix`; both transaction naming strategies supported by the installed
Flink connector remain Flink behavior.

Native encoding covers plain insert-only Kafka values, supported CDC envelopes, and
`upsert-kafka` key/value/tombstone output using JSON, CSV, raw, Avro, Avro Confluent, or Protobuf.
Key and value formats are resolved independently. Native sink substitution requires one fixed
`topic`; topic patterns and multi-topic routing stay on Flink.

### Sink fallbacks

- A keyed ordinary `kafka` sink; use `upsert-kafka` for key/value output.
- A topic pattern, topic list, or other non-fixed-topic routing.
- Explicit key/value projection, key prefix, or `EXCEPT_KEY` projection.
- Non-default sink partitioners, sink-side buffer flushing, or writable metadata.
- A changelog/parallelism shape whose host translation inserts ordering or materialization that
  the substituted native boundary cannot preserve.
- A nullable query field assigned to a `NOT NULL` target, or a bounded `CHAR`/`VARCHAR` or
  `BINARY`/`VARBINARY` target while `table.exec.sink.type-length-enforcer` is enabled. The stock
  sink path preserves Flink's configured fail/drop and trim/pad/error behavior.
- Unsupported format options, types, or a missing format artifact.

## Copy cost

The source necessarily receives Kafka payloads as JVM byte arrays and copies them once into its
reusable direct batch slab before JNI decode, and
the sink materializes one final JVM byte array per encoded key/value because kafka-clients consumes
that representation. For structured JSON/Avro/Protobuf/CSV workloads, parsing, validation, type
conversion, and serialization are generally the larger costs; raw or very small records are the
important exception where copying and per-record object overhead can dominate. This trade keeps
Kafka semantics literally identical to Flink and can be benchmarked again if the codec work later
makes the byte-array boundary the measured bottleneck.

If a native decode fails after other partition batches from the same poll have decoded,
the source closes those un-emitted Arrow batches immediately before propagating the error.
This releases staged off-heap memory without waiting for the abandoned-batch Cleaner.

## Portable compiled-plan source admission

The Flink 2.2 compiled-plan loader can ask the optional Kafka planner extension to apply its
existing source whitelist to a stock table-source node. Admitted scans use the same native decode
source and sharing behavior as ordinary planning; unsupported configurations keep the stock scan.
The portable artifact remains stock JSON. The source handoff subset below has separate real-broker
qualification; sink transactions and the wider failure/type/version matrix remain unfinished under
[canonical handoff](https://github.com/datafusion-contrib/StreamFusion/issues/569).

## Canonical source handoffs (partial)

The native Kafka source keeps Flink's reader split and enumerator state serializers. Canonical
handoff therefore retains those stock bytes rather than translating them through a native snapshot.
The ordinary SQL planner also binds the original source and host sink/helper identities: inserting
a stateless Arrow boundary must not change the IDs of their durable state. Optional connector
extensions must explicitly declare stock source-state preservation before participating in that
binding; other native source implementations receive no inferred compatibility guarantee.

Qualification passes fourteen real-broker scenarios on Flink 2.2.1 with released Kafka connector
5.0.0-2.2: four continued-input, four no-input return, two native-checkpoint recovery and four
source-rescale cases. Ordinary SQL and portable compiled plans cover both initial engine orders;
native-checkpoint recovery starts on StreamFusion. Source parallelism changes 1→3→2 with fixed
maximum parallelism. A three-partition JSON source produces three nullable BIGINT fields into a
custom committing sink. Each stage checks exact external committed rows, actual native source
output, portable metadata and stock worker classpaths without StreamFusion code. Metadata checks
decode the saved partition offsets through the released source serializer and require all three
splits, nonempty coordinator state and stable canonical operator identities. A separate 839-check
planner/identity regression cohort also passes; it does not add connector delivery scenarios.
The existing native checkpoints retain their native graph identities; canonical handoff artifacts
use the stock identities. Eight existing grouped-aggregate INT-key rescale jobs also pass with
the updated isolated runner. Twenty-two shared Flink 1.18 processing/connector checks pass; these
do not establish canonical compatibility on 1.18.

These fourteen scenarios do not qualify native Kafka sink transactions, writer rescaling, topic
patterns, security/schema/format variations, broker/coordinator failures or cross-version migration.
Source rescaling keeps the custom sink singleton and disables chaining for that test. Partition
discovery and both-native source/sink pipelines have separate qualification below.
Benchmarking remains deferred for #569.

### Partition discovery across engine switches (partial)

The combined source corpus now passes all 22 real-broker cases on the committed production tree:
the original fourteen scenarios plus eight partition-discovery round trips. Discovery covers
ordinary SQL and portable compiled plans, both initial engine orders and partitions added either
between stopped jobs or while restored readers are already running. Reader parallelism changes
1→3→2 while the topic grows from one to three to five partitions. In live cases the restored job
must consume one probe from an old partition before any partition is added, so initial source
startup cannot substitute for periodic discovery.

The schema remains three nullable BIGINT fields in JSON on Flink 2.2.1, connector 5.0.0-2.2 and
the Confluent Kafka 7.6.1 test broker. A custom committing sink stays at one task. Exact saved
offsets use independent expectations: `0=5`, then `0=6,1=2,2=1`, then
`0=7,1=2,2=1,3=2,4=1`. These require actual consumption on newly discovered partitions and
preservation of prior positions. Each canonical artifact also retains every discovered split,
nonempty coordinator state and stock identities, with no StreamFusion serializer dependency.
Committed rows must match all thirteen input records after the repeated switches; native source
output and stock worker classpaths are checked at each advancing stage. Enumeration and split
serialization still delegate to the released connector without a conversion utility or a native
snapshot intermediate.

Run all 22 cases with:

```sh
SF_KAFKA_INTEGRATION=true mvn -Pcanonical-kafka -pl streamfusion-runtime -am \
  -Dtest=CanonicalKafkaSourceJobTest,CanonicalKafkaPartitionDiscoveryJobTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

The new class alone runs eight; its one-case smoke is only a debugging subset. The corpus does not establish discovery through broker/coordinator
outages, topic patterns/deletion, broader schemas/security/versions or simultaneous native Kafka
reader/writer rescaling. Those remain in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).
Benchmarking remains deferred.

The functional lane uses a real optional JSON artifact and an isolated stock classpath. With Docker
available and the native core, Kafka and JSON libraries built, run from the repository root:

```sh
SF_KAFKA_INTEGRATION=true mvn -Pcanonical-kafka -pl streamfusion-runtime -am test \
  -Dtest=CanonicalKafkaSourceJobTest -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.extraJvmArgs="-Djava.library.path=${PWD}/native/target/debug"
```

For a targeted checkpoint fixture run, add `-Dsf.canonical.kafka.mode=checkpoint`; omit it for the
complete matrix. This is a correctness lane, independent of the benchmark environment flag.


## Canonical sink handoffs (partial)

Native Kafka serialization is synchronous and owns no durable state. Its unchanged Flink Kafka
writer and committer retain their released state codecs and transaction ownership. Canonical
identity binding treats the encoder as an explicitly declared stateless representation stage and
retains the original source/writer/committer identities. Optional sink extensions must opt into
that contract; unsupported implementations receive no inferred compatibility claim. A helper that
actually owns state still fails canonical export rather than silently losing it.

Portable stock plans now use the same sink admission checks as ordinary SQL, including type/format,
constraint enforcement, sink abilities, changelog mode, materialization and repartitioning rules.
The submission artifact stays stock JSON; native encoding is installed only in the runtime graph.
The compiled-plan adapter supplies the original input schema to admission without executing a
synthetic input. Runtime records continue over the actual restored source edge.

The consolidated 36-case real-broker sink matrix passes: eight continued-input, eight no-input,
four native-job checkpoint recovery, eight writer-rescale cases with generated hashes and chaining
disabled, and eight writer-rescale cases with automatic UID generation and normal chaining. Both
SQL/compiled submissions, initial engine orders where applicable, and incrementing/pooling names
are covered. The source stays singleton while writer parallelism changes 1→3→2 at fixed maximum
parallelism. The query has three nullable BIGINT fields, a custom source and an exactly-once Kafka
sink. Source offsets, transaction prefixes/ownership/precommitted transactions, native encoded
row counts (including partial barrier batches), clean stock classpaths and exact `read_committed`
records are checked after each switch. Native execution is witnessed by the actual submitted job
graph and row counters. The separate 875-check cohort includes fourteen Kafka source jobs and
sixty-six sink admission/graph controls; its cases overlap earlier cohorts. Twenty-two shared
Flink 1.18 checks establish processing regressions only.

Changing parallelism with generated hashes can change chaining and therefore stock identities.
Both unchanged stock-to-stock SQL/compiled controls reproduce the same unmatched source ID.
Disable chaining consistently for that policy, or retain stable UIDs. The qualified automatic-UID
ordinary SQL cases submit once per worker. Extra planner previews consume execution-node IDs and
can change the subsequent ordinary-SQL UID namespace differently between engines; portability for
that planning history remains unfinished in #569. A portable compiled plan retains its original
UID namespace. No failed restore enables unmatched-state acceptance or drops source/writer state.

The separate both-native matrix below also exercises independently starting source/sink tasks.
Its decoder schema can allocate Arrow buffers before another task publishes the memory budget.
Startup adopts those live buffers into the shared budget before publication, preserving their
release balance; an insufficient initial cap still fails normally.

Broker/coordinator failures, failures around external commits, further formats/schemas/security,
upsert/CDC delivery, source partition discovery and wider both-native source/sink combinations remain open
under [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Canonical interchange
is qualified only on the released Flink/connector versions stated above. Benchmarking is deferred.

Use the source handoff command above with `-Dtest=CanonicalKafkaSinkJobTest` for the sink lane.
`-Dsf.canonical.kafka.sink.mode=checkpoint` selects checkpoint recovery; omit it for all cases.


## Canonical native source and sink pipelines (partial)

A separate 36-case matrix passes with a three-partition Kafka source replacing the custom source
in the sink matrix above. It repeats both submission forms, initial engine orders, transaction
naming strategies, no-input returns, native-job checkpoint recovery and both writer-rescale
policies. The schema is three nullable BIGINT fields in JSON on released Flink 2.2.1 and Kafka
connector 5.0.0-2.2. Reader parallelism stays one; writer parallelism changes 1→3→2.

Every claimed native leg checks the actual submitted job graph and output counters for native
Kafka decoding and encoding, with no RowData/Arrow transpose operators. Saved split offsets and
nonempty coordinator state, writer transaction ownership and precommits, stock identities and
exact `read_committed` effects are checked after the switches. Stock workers cannot load
StreamFusion classes. This also qualifies the startup memory-accounting fix with real independent
source/writer tasks. The ordinary-SQL planner-preview UID limitation above remains open.

Run the sink command with `-Dtest=CanonicalKafkaPipelineJobTest` for this complete matrix. Generated
hash rescaling disables chaining consistently; automatic-UID rescaling keeps normal chaining.
Joint reader/writer rescaling has the additional qualification below. Combining it with partition
discovery, schemas/formats/security, upsert/CDC delivery and failures around broker/coordinator/
transaction commits needs further qualification under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).
Benchmarking remains deferred.

## Joint Kafka reader and writer rescaling (partial)

A further eight real-broker cases pass while both source readers and exactly-once writers change
parallelism 1→3→2 across repeated canonical engine switches. They cover ordinary SQL and portable
compiled plans, both initial engine orders, and incrementing/pooling transaction ownership with
automatic UIDs and normal chaining. Source maximum parallelism remains 128. The schema stays
three nullable BIGINT fields in JSON, with three existing input partitions, on released Flink
2.2.1, Kafka connector 5.0.0-2.2 and the Confluent Kafka 7.6.1 test broker.

Workers also record an independent `read_committed` scan before requesting the savepoint.
A failed precommit assertion reports that boundary, the engine order and the complete saved
writer inventory; the precommit and external exactly-once assertions remain required.

Every savepoint checks actual reader and writer parallelism, exact offsets for all three splits,
nonempty coordinator state, writer prefixes/ownership/precommitted transactions and stable stock
state identities. External `read_committed` results must contain exactly the expected five, nine
and thirteen records at successive cuts. Advancing native legs must execute both decoding and
encoding; their submitted plans contain neither transpose. Stock workers exclude StreamFusion
classes, and canonical metadata contains stock dependencies.

The final 44-case corpus passes all eight joint-rescale cases and reruns the original 36 native
source/sink pipeline controls on the same test sources and native libraries. The earlier isolated
eight-case run overlaps this corpus. Production connector code and state formats are unchanged.

```sh
SF_KAFKA_INTEGRATION=true mvn -Pcanonical-kafka -pl streamfusion-runtime -am \
  -Dtest=CanonicalKafkaReaderWriterRescaleJobTest,CanonicalKafkaPipelineJobTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

This qualifies joint rescaling with a fixed input partition set. Partition discovery combined
with joint rescaling, wider schemas/security/versions and additional failure/delivery boundaries
remain open in [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).
Benchmarking remains deferred.

## Transaction ownership at handoff

Keep the original transactional prefix across an engine switch, and stop the previous owner before
starting its replacement. Other running applications must use distinct prefixes. Configure the
transaction timeout to exceed the maximum checkpoint and restart duration, as required by
[Flink's Kafka delivery contract](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/datastream/kafka/#fault-tolerance).
Canonical state conversion preserves the connector's protocol and cannot repair transactions
aborted by a competing producer or expired by the broker.

The released Kafka connector reports a fenced commit as a known failed committable. Flink can
therefore complete a stop/savepoint even though the corresponding records were not committed.
A successful savepoint pointer alone does not establish delivery under conflicting ownership.
Do not treat such an artifact as a successful handoff, reset the transactional prefix, or roll
back to an older savepoint after new effects have already committed.

## Canonical task-manager recovery (partial)

Sixteen real-broker scenarios pass after terminating the only TaskManager before another
savepoint: eight use the custom row source and eight use native Kafka decoding and encoding.
Both SQL/compiled submissions, initial engine orders and transaction naming strategies are
covered at singleton parallelism, with the three nullable BIGINT fields and released versions
above. The worker waits for actual output at the broker before termination, requires a FAILED
job and no new savepoint pointer, and checks that only the previous prefix was committed.
Restoring its last valid canonical savepoint then commits the remaining input exactly once,
followed by another engine switch and exact external-output checks. The row-source fixture
retains its transpose boundary; the Kafka pipeline verifies that neither transpose executes.

The separate sixteen-case conflicting-owner lane checks the fencing boundary above. It requires
actual failed-committable metrics, advanced saved source positions and missing committed effects;
it rejects that savepoint. Its retry uses the earlier valid savepoint only after an independent
check proves that no new effects committed. These are negative ownership controls, not a delivery
guarantee with competing owners. Current-attempt input counters prevent aborted broker records
from satisfying retry readiness. Native execution, stock classpath exclusion and exact saved
reader/writer state checks remain mandatory.

TaskManager termination runs normal shutdown code. Abrupt process death has the separate subset
below. Broker/coordinator outages, failures during savepoint upload/finalization and additional
external-commit boundaries require separate qualification. The wider connector/type/version
matrix remains open under #569.
Run `CanonicalKafkaSinkFailureJobTest,CanonicalKafkaPipelineFailureJobTest` for task-manager
recovery and `CanonicalKafkaSinkFencingJobTest,CanonicalKafkaPipelineFencingJobTest` for the
ownership controls, using the functional Kafka command above. Benchmarking remains deferred.

## Canonical batch boundaries and abrupt-process recovery (partial)

A separate 70-check cohort passes 24 controlled canonical batch-boundary scenarios, sixteen
abrupt-process recovery scenarios, eight existing grouped-state rescale jobs and 22 admission
checks. The new Kafka scenarios cover both SQL/compiled submissions, initial engine orders and
transaction naming strategies, using singleton source/writer tasks, three nullable BIGINT fields,
JSON and the released versions above. These counts overlap other regression cohorts.

The 24 boundary scenarios use a custom row source and the actual RowData-to-Arrow boundary before
native Kafka encoding. Eight cover partial batches, eight include no-input engine switches and
eight reach a full five-row batch. The fixture explicitly disables the default 50 ms latency
flush timer. Before requesting the savepoint, it checks actual transpose input and encoded-row
counters, including the exact pending-row count. After the barrier, every pre-barrier row must
have been encoded, saved reader/writer state must match, and committed Kafka records must be exact.
Source readiness alone does not prove that a partial batch remains pending.

The sixteen process cases halt the worker JVM after new records reach the broker and before
another savepoint. They bypass producer close, finally blocks and shutdown hooks. Eight use the
custom row source, requiring exactly three encoded rows and one pending row before death; eight
use native Kafka decoding/encoding, requiring four encoded rows and no transpose. The last valid
canonical savepoint is retried only after checking that the interrupted attempt committed no new
effects. Recovery must clean up its actual lingering transactions, commit the remaining input
exactly once, and preserve exact output through another engine switch. Stock processes remain
free of StreamFusion classes.

Run `CanonicalKafkaSinkBarrierJobTest,CanonicalKafkaSinkProcessFailureJobTest,CanonicalKafkaPipelineProcessFailureJobTest`
with the functional Kafka command for the full forty-case matrix. Adding
`-Dsf.canonical.kafka.fixture.smoke=true` selects five cases; it does not qualify the full matrix.
The 70-check regression selection also includes `CanonicalGroupArithmeticIntJobTest` and
`CanonicalKafkaSinkAdmissionTest`. The separate 94-check ordinary sink/pipeline/admission cohort
passes against the preceding worker sources and the same native libraries; it does not silently
requalify every historical operator cohort against this newer fixture.

Client failure after completed savepoints has the separate subset below. Broker/coordinator
outages, capture/upload/finalization failures and the wider schema/security/discovery/version
matrix remain open under [#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).
The production latency timer and ordinary native checkpoint paths are unchanged. Benchmarking
remains deferred.

## Client failure after canonical completion (partial)

Sixteen additional real-broker cases pass when the client JVM dies after a canonical savepoint
and the job finish, before reporting the savepoint location. Eight use the custom row source and
eight use native Kafka decoding/encoding. Both submission forms, initial engine orders and
transaction naming strategies retain the singleton tasks, schema and released versions above.
The row-source cases also retain controlled partial batches before the canonical barrier.

The tests require the original five-row prefix, then nine committed records at the completed
cut. They find exactly one completed artifact in that stage's isolated output directory and use
it for normal restore on the other engine, with no new input. That restore must process zero new
records, preserve the saved positions and transaction ownership, and leave exactly nine external
effects. Another switch and four new records must produce exactly thirteen effects. The original
completed artifact is checked alongside the subsequent canonical artifacts for stable stock IDs,
source/coordinator state, writer precommits and serializer dependencies. Stock workers exclude
StreamFusion code throughout.

Recovery uses the completed canonical cut after the nine effects committed. Restarting from the
older five-row savepoint is not a safe response to losing the client's result. This lane checks
client death after successful completion; it does not qualify every failure during commit or
metadata publication, or an automatic search for the correct artifact in a deployment.

The full 86-check cohort includes these sixteen cases and reruns the preceding 70 boundary,
process-recovery, grouped-state and admission checks against the updated fixtures and the same
verified native libraries. Run `CanonicalKafkaSinkCompletionFailureJobTest,CanonicalKafkaPipelineCompletionFailureJobTest`
for all sixteen completion cases, or add `-Dsf.canonical.kafka.fixture.smoke=true` for two.
Broader delivery/failure guarantees remain open under #569. Benchmarking remains deferred.
