use crate::*;

/// The production native Kafka consumer for one Flink subtask: a single rdkafka `BaseConsumer` that
/// multiplexes all of the subtask's assigned partitions (Flink-parity — one consumer, not one per
/// split). Each `poll` buckets the drained payloads by partition and decodes them to typed Arrow
/// INLINE on the fetcher thread: a separate decode thread was benchmarked slower on every format —
/// Flink already pipelines the fetcher thread against the task thread, so the extra hop only added
/// channel/wakeup overhead. Manual `assign()`+seek, never `subscribe()`/rebalance.
#[cfg(feature = "kafka")]
pub(crate) struct KafkaSplitReader {
    consumer: rdkafka::consumer::BaseConsumer,
    /// The consumer's message queue, drained via the callback API (see `poll`).
    consumer_queue: *mut rdkafka::bindings::rd_kafka_queue_t,
    body_schema: SchemaRef,
    /// The same format-dispatched decoder the shallow path uses; only who produces the body batch
    /// (rdkafka vs Flink) differs. Owned here and driven only from the fetcher thread.
    decoder: MessageDecoder,
    /// Next offset to consume per assigned partition — the split's checkpoint position.
    next_offsets: HashMap<(String, i32), i64>,
    /// Topics whose broker metadata has been primed (see `reassign`).
    warmed_topics: std::collections::HashSet<String>,
    /// Index of the rowtime column in the decoded batch, or -1 when the table declares no watermark.
    /// When set, each pending batch carries the column's max (epoch millis) so the JVM source emits it
    /// as the batch's record timestamp, feeding Flink's per-split source watermarks.
    rowtime_index: i32,
    /// Decoded batches ready for the JVM to drain one split at a time, in arrival (offset) order so a
    /// split's offset never goes backwards when several of its batches are drained in one cycle. Fields:
    /// (topic, partition, next offset, max rowtime millis or i64::MIN, batch).
    pending: std::collections::VecDeque<(String, i32, i64, i64, RecordBatch)>,
}

#[cfg(feature = "kafka")]
impl Drop for KafkaSplitReader {
    fn drop(&mut self) {
        if !self.consumer_queue.is_null() {
            unsafe { rdkafka::bindings::rd_kafka_queue_destroy(self.consumer_queue) };
        }
    }
}

#[cfg(feature = "kafka")]
impl KafkaSplitReader {
    /// `format` selects the decoder, the same dispatch the shallow decode path uses: JSON (0) decodes
    /// against `output_schema`; bare Avro (4) / Confluent Avro (1) build a schema store mapping
    /// `schema_id` → `avro_schema` (the writer schema JSON), optionally projecting to `reader_avro_schema`
    /// (the narrowed output) via Avro resolution; protobuf (5) decodes against `proto_descriptor` (an
    /// encoded `FileDescriptorSet`) / `proto_message_name`.
    fn open(
        config: &[(String, String)],
        format: i32,
        output_schema: SchemaRef,
        avro_schema: &str,
        reader_avro_schema: &str,
        schema_id: i32,
        proto_descriptor: Vec<u8>,
        proto_message_name: String,
        rowtime_index: i32,
        format_options: &str,
    ) -> KafkaSplitReader {
        use rdkafka::config::ClientConfig;

        let mut client = ClientConfig::new();
        for (key, value) in config {
            client.set(key, value);
        }
        let consumer: rdkafka::consumer::BaseConsumer =
            client.create().expect("failed to create kafka consumer");
        // The consumer's queue, for draining. (assign/seek still go through the BaseConsumer.)
        let consumer_queue = unsafe {
            use rdkafka::consumer::Consumer;
            rdkafka::bindings::rd_kafka_queue_get_consumer(consumer.client().native_ptr())
        };

        // Protobuf is built straight from the descriptor (not in MessageDecoder::new, like the shallow
        // path's createProtobufDecoder); every other format dispatches through MessageDecoder::new.
        let decoder = if format == 5 {
            // Prune the descriptor to the output schema's fields so ptars builds only those columns
            // (projection pushed into the source); a no-op when the schema is the full message.
            let pruned = prune_descriptor_set(&proto_descriptor, &proto_message_name, &output_schema);
            MessageDecoder {
                decoder: FormatDecoder::Protobuf(ProtobufDecoder::new(&pruned, &proto_message_name)),
                skip_errors: false,
            }
        } else {
            MessageDecoder::new(
                format,
                output_schema,
                avro_schema,
                reader_avro_schema,
                schema_id,
                false,
                format_options,
            )
        };

        KafkaSplitReader {
            consumer,
            consumer_queue,
            body_schema: Arc::new(Schema::new(vec![Field::new("body", DataType::Binary, true)])),
            decoder,
            next_offsets: HashMap::default(),
            warmed_topics: std::collections::HashSet::default(),
            rowtime_index,
            pending: std::collections::VecDeque::new(),
        }
    }

    /// Adds splits (idempotent) and re-assigns the whole set: each newly added partition seeks to its
    /// given start offset, each existing one stays at its tracked next offset. assign() with explicit
    /// offsets both assigns and seeks, so no subscribe/rebalance is involved.
    ///
    /// A negative start offset is one of Flink's `KafkaPartitionSplit` markers, which the enumerator
    /// leaves for the reader to resolve: -2 EARLIEST -> beginning, -1 LATEST -> end, -3 COMMITTED ->
    /// the group's stored offset. A concrete (>= 0) offset seeks to exactly there.
    fn assign_splits(&mut self, topics: &[String], partitions: &[i64], offsets: &[i64]) {
        for i in 0..topics.len() {
            self.next_offsets
                .entry((topics[i].clone(), partitions[i] as i32))
                .or_insert(offsets[i]);
        }
        self.reassign();
    }

    /// Removes the given splits (which reached their stopping offset) from the assignment so the
    /// consumer no longer fetches or blocks on them — mirroring the connector's `unassignPartitions`.
    /// Without this a finished partition makes `poll` block for the timeout at the bounded tail.
    fn unassign_splits(&mut self, topics: &[String], partitions: &[i64]) {
        for i in 0..topics.len() {
            self.next_offsets.remove(&(topics[i].clone(), partitions[i] as i32));
        }
        self.reassign();
    }

    /// (Re)assigns the consumer to exactly the currently-tracked partitions, each seeked to its tracked
    /// offset (or start marker). assign() with explicit offsets replaces the whole assignment.
    fn reassign(&mut self) {
        use rdkafka::consumer::Consumer;
        use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

        if self.next_offsets.is_empty() {
            self.consumer.unassign().expect("failed to unassign");
            return;
        }
        // Prime broker metadata for topics this consumer hasn't resolved yet, BEFORE assigning:
        // an assign on a cold connection parks each partition in leader-query until librdkafka's
        // periodic metadata refresh resolves it — measured as ~0.5s of dead time before the first
        // fetch. An explicit blocking metadata fetch resolves leaders now (the same warm-up the
        // Java client gets from its initial metadata round). Failure is ignored: assign still
        // works through the refresh cycle, just slower.
        for topic in
            self.next_offsets.keys().map(|(topic, _)| topic.clone()).collect::<Vec<_>>()
        {
            if self.warmed_topics.insert(topic.clone()) {
                let _ = self
                    .consumer
                    .fetch_metadata(Some(&topic), std::time::Duration::from_secs(10));
            }
        }
        let mut tpl = TopicPartitionList::new();
        for ((topic, partition), &offset) in &self.next_offsets {
            let position = match offset {
                -2 => Offset::Beginning,
                -1 => Offset::End,
                -3 => Offset::Stored,
                concrete if concrete >= 0 => Offset::Offset(concrete),
                _ => Offset::Beginning,
            };
            tpl.add_partition_offset(topic, *partition, position)
                .expect("failed to add partition offset");
        }
        self.consumer.assign(&tpl).expect("failed to assign partitions");
    }

    /// Polls up to `max_records` messages, buckets them by partition, and decodes one typed Arrow batch
    /// per partition into `pending`, advancing each split's next offset. Returns the number of
    /// per-partition batches now pending (0 on a poll timeout).
    fn poll(&mut self, max_records: usize, timeout: std::time::Duration) -> usize {
        use arrow::array::BinaryBuilder;
        use rdkafka::bindings as rdsys;
        use rdkafka::consumer::Consumer;

        // Fetcher thread: drain the consumer queue with the CALLBACK API — one queue-mutex acquisition
        // moves the whole queued backlog local (rd_kafka_consume_batch_queue re-locks per message,
        // contending with the broker thread's enqueue), each payload is copied into a per-partition
        // binary builder from the callback, and librdkafka frees each op after its callback returns.
        // `max_records` is enforced with rd_kafka_yield (a thread-local stop flag): the dispatch loop
        // stops and prepends the untaken remainder back onto the queue head.
        struct PollContext {
            rk: *mut rdsys::rd_kafka_t,
            max_records: usize,
            seen: usize,
            buffered: usize,
            /// Per-partition buckets: a subtask holds a handful of partitions and a fetch response
            /// delivers a partition's records contiguously, so a last-bucket cache + linear scan
            /// beats a per-message hash lookup.
            buckets: Vec<(i32, String, BinaryBuilder, i64)>,
            last_bucket: usize,
        }
        unsafe extern "C" fn bucket_message(
            message: *mut rdsys::rd_kafka_message_t,
            opaque: *mut std::os::raw::c_void,
        ) {
            let context = &mut *(opaque as *mut PollContext);
            context.seen += 1;
            let message = &*message;
            if message.err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR
                && !message.payload.is_null()
            {
                let payload = std::slice::from_raw_parts(message.payload as *const u8, message.len);
                let index = if context
                    .buckets
                    .get(context.last_bucket)
                    .is_some_and(|bucket| bucket.0 == message.partition)
                {
                    context.last_bucket
                } else if let Some(found) =
                    context.buckets.iter().position(|bucket| bucket.0 == message.partition)
                {
                    found
                } else {
                    // Topic resolved once per partition (not per message); pre-size so the binary
                    // buffers don't reallocate as the batch fills.
                    let topic =
                        std::ffi::CStr::from_ptr(rdsys::rd_kafka_topic_name(message.rkt))
                            .to_string_lossy()
                            .into_owned();
                    // Pre-size for the poll cap (bounded — the cap can be huge when a caller wants
                    // an unchunked drain; the builder grows amortized past this).
                    let presize = context.max_records.min(65536);
                    context.buckets.push((
                        message.partition,
                        topic,
                        BinaryBuilder::with_capacity(presize, presize * 64),
                        0,
                    ));
                    context.buckets.len() - 1
                };
                context.last_bucket = index;
                let bucket = &mut context.buckets[index];
                bucket.2.append_value(payload);
                bucket.3 = message.offset + 1;
                context.buffered += 1;
            }
            // Errors are queue events (e.g. transient connectivity); they are counted against the cap
            // but otherwise skipped. librdkafka destroys every op after this returns.
            if context.seen >= context.max_records {
                rdsys::rd_kafka_yield(context.rk);
            }
        }
        let mut context = PollContext {
            rk: self.consumer.client().native_ptr(),
            max_records,
            seen: 0,
            buffered: 0,
            buckets: Vec::new(),
            last_bucket: 0,
        };
        unsafe {
            rdsys::rd_kafka_consume_callback_queue(
                self.consumer_queue,
                timeout.as_millis() as std::os::raw::c_int,
                Some(bucket_message),
                &mut context as *mut PollContext as *mut std::os::raw::c_void,
            )
        };
        // Decode inline: one typed batch per partition, straight into `pending` (the JVM drains all
        // of them right after this returns, so nothing is ever left behind on a bounded finish).
        self.pending.clear();
        for (partition, topic, mut builder, next_offset) in context.buckets {
            let body = RecordBatch::try_new(self.body_schema.clone(), vec![Arc::new(builder.finish())])
                .expect("failed to build kafka body batch");
            self.next_offsets.insert((topic.clone(), partition), next_offset);
            let batch = self.decoder.decode(&body);
            let max_rowtime = if self.rowtime_index >= 0 {
                max_rowtime_millis(&batch, self.rowtime_index as usize)
            } else {
                i64::MIN
            };
            self.pending.push_back((topic, partition, next_offset, max_rowtime, batch));
        }
        self.pending.len()
    }
}

/// Max of a rowtime column in epoch millis, or `i64::MIN` when every value is null — the JVM side
/// treats `i64::MIN` as "no timestamp" (Flink's `NO_TIMESTAMP` sentinel). Two column shapes, matching
/// the two watermark forms the planner admits: a nanosecond timestamp (floor-divided to millis, so
/// pre-epoch values round down like Flink's `TimestampData.getMillisecond`), and a bigint already
/// holding epoch millis (a `TO_TIMESTAMP_LTZ(col, 3)` computed rowtime reads the column verbatim).
#[cfg(any(feature = "kafka", feature = "fluss", test))]
pub(crate) fn max_rowtime_millis(batch: &RecordBatch, index: usize) -> i64 {
    use arrow::array::TimestampNanosecondArray;
    let column = batch.column(index);
    match column.data_type() {
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            let array = column
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("nanosecond timestamp downcast");
            arrow::compute::max(array).map(|ns| ns.div_euclid(1_000_000)).unwrap_or(i64::MIN)
        }
        DataType::Int64 => {
            let array =
                column.as_any().downcast_ref::<Int64Array>().expect("bigint rowtime downcast");
            arrow::compute::max(array).unwrap_or(i64::MIN)
        }
        other => panic!("unsupported rowtime column type {other}"),
    }
}

/// Whether this build carries the native Kafka source (the `kafka` cargo feature). Compiled into
/// every build so the planner can probe before routing a table to a source the library can't run.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_kafkaFeatureBuilt<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jni::sys::jboolean {
    cfg!(feature = "kafka") as jni::sys::jboolean
}

/// Opens a native Kafka split reader for one subtask and returns an opaque handle, released with
/// `closeKafkaConsumer`. `configKeys`/`configValues` are the translated librdkafka config (applied
/// verbatim). `format` selects the decoder (the same codes the shallow decode path uses): 0 JSON
/// (decoded against the schema in the C structs), 1 Confluent / 4 bare Avro (decoded against
/// `avroSchema` registered at `schemaId`, optionally projected to `readerAvroSchema`), 5 protobuf
/// (decoded against `descriptor`/`messageName`). `rowtimeIndex` is the decoded batch's rowtime column
/// for per-split source watermarks, or -1 when the table declares none. Splits are added later via
/// `assignKafkaSplits`.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_openKafkaConsumer<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config_keys: JObjectArray<'local>,
    config_values: JObjectArray<'local>,
    format: jint,
    schema_array_address: jlong,
    schema_address: jlong,
    avro_schema: JString<'local>,
    reader_avro_schema: JString<'local>,
    schema_id: jint,
    descriptor: JByteArray<'local>,
    message_name: JString<'local>,
    rowtime_index: jint,
    format_options: JString<'local>,
) -> jlong {
    let keys = read_string_array(&mut env, &config_keys);
    let values = read_string_array(&mut env, &config_values);
    let config: Vec<(String, String)> = keys.into_iter().zip(values).collect();
    let schema = import_record_batch(schema_array_address, schema_address).schema();
    let avro_schema: String =
        env.get_string(&avro_schema).map(Into::into).unwrap_or_default();
    // Empty unless the planner pushed a projection into a bare-Avro decode (the narrowed reader schema).
    let reader_avro_schema: String =
        env.get_string(&reader_avro_schema).map(Into::into).unwrap_or_default();
    // The protobuf FileDescriptorSet + message name; empty for non-protobuf formats (JByteArray is null).
    let proto_descriptor: Vec<u8> =
        if descriptor.is_null() { Vec::new() } else { env.convert_byte_array(&descriptor).unwrap_or_default() };
    let proto_message_name: String =
        env.get_string(&message_name).map(Into::into).unwrap_or_default();
    let format_options: String =
        env.get_string(&format_options).map(Into::into).unwrap_or_default();
    let reader = KafkaSplitReader::open(
        &config,
        format,
        schema,
        &avro_schema,
        &reader_avro_schema,
        schema_id,
        proto_descriptor,
        proto_message_name,
        rowtime_index,
        &format_options,
    );
    into_handle(reader)
}

/// Adds splits to the reader and re-assigns: `topics`/`partitions`/`startOffsets` are index-aligned;
/// new partitions seek to their start offset, existing ones keep their tracked position.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_assignKafkaSplits<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topics: JObjectArray<'local>,
    partitions: JLongArray<'local>,
    start_offsets: JLongArray<'local>,
) {
    let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
    let topics = read_string_array(&mut env, &topics);
    let partitions = read_longs(&env, &partitions);
    let offsets = read_longs(&env, &start_offsets);
    reader.assign_splits(&topics, &partitions, &offsets);
}

/// Removes finished splits (reached their bounded stopping offset) from the assignment so the consumer
/// stops fetching/blocking on them. Index-aligned `topics`/`partitions`.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_unassignKafkaSplits<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topics: JObjectArray<'local>,
    partitions: JLongArray<'local>,
) {
    let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
    let topics = read_string_array(&mut env, &topics);
    let partitions = read_longs(&env, &partitions);
    reader.unassign_splits(&topics, &partitions);
}

/// Polls one cycle, decoding one Arrow batch per partition that had messages. Returns the number of
/// per-partition batches now pending; the JVM drains each with `drainKafkaSplit`.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pollKafkaBatch<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_records: jint,
    timeout_ms: jlong,
) -> jint {
    let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
    reader.poll(
        max_records as usize,
        std::time::Duration::from_millis(timeout_ms as u64),
    ) as jint
}

/// Drains one pending per-partition batch: exports the decoded typed Arrow into the consumer C structs,
/// writes `[partition, nextOffset, maxRowtimeMillis]` into `splitMeta` (the last is `i64::MIN` when the
/// table has no watermark or the batch's rowtimes are all null), and the topic into `outTopic[0]`, so
/// the JVM can form the split id, advance that split's checkpoint offset, and timestamp the batch for
/// per-split watermarks. Returns the decoded row count; call it `pollKafkaBatch`'s return-value times.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_drainKafkaSplit<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    split_meta: JLongArray<'local>,
    out_topic: JObjectArray<'local>,
    out_array_address: jlong,
    out_schema_address: jlong,
) -> jint {
    let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
    let (topic, partition, next_offset, max_rowtime, batch) =
        reader.pending.pop_front().expect("drainKafkaSplit called with no pending batch");
    let rows = batch.num_rows() as jint;
    env.set_long_array_region(&split_meta, 0, &[partition as i64, next_offset, max_rowtime])
        .expect("failed to write split meta");
    let topic_jstr = env.new_string(&topic).expect("failed to make topic string");
    env.set_object_array_element(&out_topic, 0, &topic_jstr)
        .expect("failed to write topic");
    export_record_batch(batch, out_array_address, out_schema_address);
    rows
}

/// Releases a native Kafka split reader, dropping the rdkafka consumer (which closes its connections).
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeKafkaConsumer<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<KafkaSplitReader>(handle));
    }
}

/// Benchmark-only: drive the **production** split reader (poll + inline decode) over a
/// whole topic and count the decoded rows **entirely in Rust** — the decoded Arrow batches are consumed
/// in Rust and never exported to the JVM, exactly as they would feed a downstream native operator in a
/// fused pipeline. This is the honest "fastest way to get Arrow batches in Rust" measurement: it
/// excludes the per-batch JVM export that the FLIP-27 DataStream wrapper forces. Returns the row count.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_benchmarkNativeConsume<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config_keys: JObjectArray<'local>,
    config_values: JObjectArray<'local>,
    topic: JString<'local>,
    format: jint,
    schema_array_address: jlong,
    schema_address: jlong,
    avro_schema: JString<'local>,
    schema_id: jint,
    max_messages: jlong,
) -> jlong {
    let keys = read_string_array(&mut env, &config_keys);
    let values = read_string_array(&mut env, &config_values);
    let config: Vec<(String, String)> = keys.into_iter().zip(values).collect();
    let topic: String = env.get_string(&topic).expect("failed to read topic").into();
    let schema = import_record_batch(schema_array_address, schema_address).schema();
    let avro_schema: String = env.get_string(&avro_schema).map(Into::into).unwrap_or_default();

    let mut reader =
        KafkaSplitReader::open(&config, format, schema, &avro_schema, "", schema_id, Vec::new(), String::new(), -1, "");
    reader.assign_splits(&[topic], &[0], &[-2]); // partition 0, earliest

    let timeout = std::time::Duration::from_millis(250);
    let mut rows: i64 = 0;
    let mut idle = 0;
    // The topic holds exactly `max_messages`; loop until we've decoded them all. A generous idle guard
    // (≈10s of empty polls) only trips if the broker truly stops delivering, avoiding a hang.
    // Poll cap from SF env via JVM? Keep it simple: an experiment knob compiled in — the production
    // reader is driven with the same generous cap the SQL source uses.
    let poll_cap: usize = std::env::var("SF_KAFKA_POLL_CAP")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(65536);
    while rows < max_messages && idle < 40 {
        let count = reader.poll(poll_cap, timeout);
        if count == 0 {
            idle += 1;
            continue;
        }
        idle = 0;
        for (_topic, _partition, _next_offset, _max_rowtime, batch) in reader.pending.drain(..) {
            rows += batch.num_rows() as i64; // consumed in Rust; no JVM export
        }
    }
    rows
}

/// Benchmark-only: measure librdkafka's raw delivery rate — batch-consume the whole topic and count
/// messages with NO decode and no decode thread, isolating the consumer from everything downstream.
/// Compared against the Java client's raw poll to answer "is librdkafka delivery actually slower here".
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_benchmarkConsumeOnly<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config_keys: JObjectArray<'local>,
    config_values: JObjectArray<'local>,
    topic: JString<'local>,
    max_messages: jlong,
) -> jlong {
    use rdkafka::bindings as rdsys;
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

    let keys = read_string_array(&mut env, &config_keys);
    let values = read_string_array(&mut env, &config_values);
    let topic: String = env.get_string(&topic).expect("failed to read topic").into();
    let mut client = ClientConfig::new();
    for (key, value) in keys.iter().zip(&values) {
        client.set(key, value);
    }
    let consumer: BaseConsumer = client.create().expect("failed to create kafka consumer");
    // Assign every partition at the beginning — librdkafka fetches them all (one FetchRequest per
    // broker) and merges them onto the single consumer queue this loop drains.
    let metadata = consumer
        .fetch_metadata(Some(&topic), std::time::Duration::from_secs(10))
        .expect("fetch metadata");
    let partitions = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .expect("topic in metadata")
        .partitions();
    let mut tpl = TopicPartitionList::new();
    for partition in partitions {
        tpl.add_partition_offset(&topic, partition.id(), Offset::Beginning).expect("add partition");
    }
    consumer.assign(&tpl).expect("assign");
    let queue = unsafe { rdsys::rd_kafka_queue_get_consumer(consumer.client().native_ptr()) };

    // Drain with the callback API instead of `rd_kafka_consume_batch_queue`: the batch call locks
    // and unlocks the queue mutex PER MESSAGE (contending with the broker thread's enqueue), while
    // the callback path bulk-moves the whole queued backlog under ONE lock and dispatches lock-free
    // (librdkafka destroys each op after the callback returns).
    struct CountCtx {
        count: i64,
    }
    unsafe extern "C" fn count_message(
        message: *mut rdsys::rd_kafka_message_t,
        opaque: *mut std::os::raw::c_void,
    ) {
        let context = &mut *(opaque as *mut CountCtx);
        let message = &*message;
        if message.err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR
            && !message.payload.is_null()
        {
            context.count += 1; // no decode — raw delivery only
        }
    }
    let mut context = CountCtx { count: 0 };
    let mut idle = 0;
    while context.count < max_messages && idle < 40 {
        let served = unsafe {
            rdsys::rd_kafka_consume_callback_queue(
                queue,
                250,
                Some(count_message),
                &mut context as *mut CountCtx as *mut std::os::raw::c_void,
            )
        };
        if served <= 0 {
            idle += 1;
        } else {
            idle = 0;
        }
    }
    unsafe { rdsys::rd_kafka_queue_destroy(queue) };
    context.count
}

/// Benchmark-only: a hand-rolled consume+decode loop with none of the split-reader machinery (no
/// per-partition bucketing, no offset tracking, no pending queue) — the ideal-case floor the
/// production reader is validated against. The decode-thread experiment this leg once judged is
/// settled: inline decode won on every format, and the production reader now decodes inline too.
/// Returns the decoded row count.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_benchmarkNativeConsumeSerial<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config_keys: JObjectArray<'local>,
    config_values: JObjectArray<'local>,
    topic: JString<'local>,
    format: jint,
    schema_array_address: jlong,
    schema_address: jlong,
    avro_schema: JString<'local>,
    schema_id: jint,
    max_messages: jlong,
) -> jlong {
    use arrow::array::BinaryBuilder;
    use rdkafka::bindings as rdsys;
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

    let keys = read_string_array(&mut env, &config_keys);
    let values = read_string_array(&mut env, &config_values);
    let topic: String = env.get_string(&topic).expect("failed to read topic").into();
    let schema = import_record_batch(schema_array_address, schema_address).schema();
    let avro_schema: String = env.get_string(&avro_schema).map(Into::into).unwrap_or_default();

    let mut client = ClientConfig::new();
    for (key, value) in keys.iter().zip(&values) {
        client.set(key, value);
    }
    let consumer: BaseConsumer = client.create().expect("failed to create kafka consumer");
    let metadata = consumer
        .fetch_metadata(Some(&topic), std::time::Duration::from_secs(10))
        .expect("fetch metadata");
    let mut tpl = TopicPartitionList::new();
    for partition in metadata.topics().iter().find(|t| t.name() == topic).expect("topic").partitions() {
        tpl.add_partition_offset(&topic, partition.id(), Offset::Beginning).expect("add partition");
    }
    consumer.assign(&tpl).expect("assign");
    let queue = unsafe { rdsys::rd_kafka_queue_get_consumer(consumer.client().native_ptr()) };

    // The same decoder the pipelined path builds, just driven inline.
    let decoder = MessageDecoder::new(format, schema, &avro_schema, "", schema_id, false, "");
    let body_schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Binary, true)]));

    // Callback drain (one queue lock per poll, not per message — see benchmarkConsumeOnly); each
    // payload is copied into the Arrow binary builder from the callback, librdkafka frees the op after.
    struct SerialCtx {
        builder: BinaryBuilder,
        appended: usize,
    }
    unsafe extern "C" fn append_payload(
        message: *mut rdsys::rd_kafka_message_t,
        opaque: *mut std::os::raw::c_void,
    ) {
        let context = &mut *(opaque as *mut SerialCtx);
        let message = &*message;
        if message.err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR
            && !message.payload.is_null()
        {
            let payload = std::slice::from_raw_parts(message.payload as *const u8, message.len);
            context.builder.append_value(payload);
            context.appended += 1;
        }
    }
    let mut rows: i64 = 0;
    let mut idle = 0;
    while rows < max_messages && idle < 40 {
        let mut context =
            SerialCtx { builder: BinaryBuilder::with_capacity(8192, 8192 * 64), appended: 0 };
        let served = unsafe {
            rdsys::rd_kafka_consume_callback_queue(
                queue,
                250,
                Some(append_payload),
                &mut context as *mut SerialCtx as *mut std::os::raw::c_void,
            )
        };
        if served <= 0 || context.appended == 0 {
            idle += if served <= 0 { 1 } else { 0 };
            continue;
        }
        idle = 0;
        // Decode INLINE — the next batch is not fetched until this returns.
        let body =
            RecordBatch::try_new(body_schema.clone(), vec![Arc::new(context.builder.finish())])
                .expect("failed to build kafka body batch");
        rows += decoder.decode(&body).num_rows() as i64;
    }
    unsafe { rdsys::rd_kafka_queue_destroy(queue) };
    rows
}

/// Benchmark-only: consume an entire topic with a native (rdkafka) consumer and decode it to typed
/// Arrow, all in Rust — message payloads go straight from librdkafka into an Arrow binary builder (one
/// copy, no JVM heap byte[] and no per-record JNI crossing), then through the same `JsonDecoder` the
/// shallow path uses. Returns the decoded row count; the JVM times this single call to compare native
/// consume+decode against the shallow path. This is the fast path's measurement, not the production
/// FLIP-27 source (remaining source tails: https://github.com/datafusion-contrib/StreamFusion/issues/16).
#[cfg(feature = "kafka-bench")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_benchmarkKafkaConsume<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    brokers: JString<'local>,
    topic: JString<'local>,
    schema_array_address: jlong,
    schema_address: jlong,
    max_messages: jlong,
) -> jlong {
    use arrow::array::BinaryBuilder;
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::message::Message;

    let brokers: String = env.get_string(&brokers).expect("failed to read brokers").into();
    let topic: String = env.get_string(&topic).expect("failed to read topic").into();
    let decoder = JsonDecoder::new(import_record_batch(schema_array_address, schema_address).schema());

    // A fresh group reading from the beginning each run; offsets are not committed (the consumer is
    // throwaway). This mirrors the manual, non-committing consumption the production source would do.
    // Unique group per call so each timed run re-reads the whole topic from the beginning (a fixed
    // group would leave the warm-up run's position at the end and the timed run would read nothing).
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let group = format!("streamfusion-bench-{}-{}", std::process::id(), nonce);
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", &group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("failed to create kafka consumer");
    consumer.subscribe(&[&topic]).expect("failed to subscribe");

    let body_field = Field::new("body", DataType::Binary, true);
    let body_schema = Arc::new(Schema::new(vec![body_field]));
    let mut builder = BinaryBuilder::new();
    let mut buffered = 0usize;
    let mut seen: i64 = 0;
    let mut rows: i64 = 0;
    let mut decode = |builder: &mut BinaryBuilder| -> i64 {
        let batch = RecordBatch::try_new(body_schema.clone(), vec![Arc::new(builder.finish())])
            .expect("failed to build kafka body batch");
        decoder.decode(&batch).num_rows() as i64
    };

    while seen < max_messages {
        match consumer.poll(std::time::Duration::from_secs(5)) {
            Some(Ok(message)) => {
                builder.append_value(message.payload().unwrap_or(&[]));
                buffered += 1;
                seen += 1;
                if buffered >= 8192 {
                    rows += decode(&mut builder);
                    buffered = 0;
                }
            }
            Some(Err(error)) => panic!("kafka consume error: {error}"),
            None => break, // poll timeout: the produced messages are exhausted
        }
    }
    if buffered > 0 {
        rows += decode(&mut builder);
    }
    rows
}
