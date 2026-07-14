use crate::*;

/// The production native Kafka consumer for one Flink subtask: a single rdkafka `BaseConsumer` that
/// multiplexes all of the subtask's assigned partitions (Flink-parity — one consumer, not one per
/// split). Each `poll` buckets the drained payloads by partition directly into Arrow binary body
/// batches and, when a format decoder is attached, decodes each bucket to its typed batch in the same
/// call while the bytes are cache-hot — the decode dispatches through a C-ABI entry the format DSO
/// handed over (never symbol linkage), so the connector stays format-neutral. Without an attached
/// decoder the bodies cross to the JVM and the split reader decodes them there. Manual
/// `assign()`+seek, never `subscribe()`/rebalance.
#[cfg(feature = "kafka")]
pub(crate) struct KafkaSplitReader {
    consumer: rdkafka::consumer::BaseConsumer,
    /// The consumer's message queue, drained via the callback API (see `poll`).
    consumer_queue: *mut rdkafka::bindings::rd_kafka_queue_t,
    body_schema: SchemaRef,
    /// The attached format decode: the format DSO's version-1 driver vtable and its opaque decoder
    /// handle, obtained through the driver-init handshake (see `format_abi`).
    decode: Option<(DecodeBodyBatch, i64)>,
    /// Next offset to consume per assigned partition — the split's checkpoint position.
    next_offsets: HashMap<(String, i32), i64>,
    /// Concrete bounded stopping offsets. The poll callback drops records at or beyond this boundary.
    stopping_offsets: HashMap<(String, i32), i64>,
    /// Topics whose broker metadata has been primed (see `reassign`).
    warmed_topics: std::collections::HashSet<String>,
    /// Body (or decoded, when a decoder is attached) batches ready for the JVM to drain one split at a
    /// time, in arrival (offset) order so a split's offset never goes backwards when several of its
    /// batches are drained in one cycle. Fields: (topic, partition, next offset, batch).
    pending: std::collections::VecDeque<(String, i32, i64, RecordBatch)>,
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
    fn open(config: &[(String, String)]) -> KafkaSplitReader {
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

        KafkaSplitReader {
            consumer,
            consumer_queue,
            body_schema: Arc::new(Schema::new(vec![Field::new("body", DataType::Binary, true)])),
            decode: None,
            next_offsets: HashMap::default(),
            stopping_offsets: HashMap::default(),
            warmed_topics: std::collections::HashSet::default(),
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
    fn assign_splits(
        &mut self,
        topics: &[String],
        partitions: &[i64],
        offsets: &[i64],
        stopping_offsets: &[i64],
    ) {
        assert_eq!(topics.len(), partitions.len());
        assert_eq!(topics.len(), offsets.len());
        assert_eq!(topics.len(), stopping_offsets.len());
        for i in 0..topics.len() {
            let key = (topics[i].clone(), partitions[i] as i32);
            self.next_offsets
                .entry(key.clone())
                .or_insert(offsets[i]);
            if stopping_offsets[i] != i64::MIN {
                self.stopping_offsets.insert(key, stopping_offsets[i]);
            }
        }
        self.reassign();
    }

    /// Removes the given splits (which reached their stopping offset) from the assignment so the
    /// consumer no longer fetches or blocks on them — mirroring the connector's `unassignPartitions`.
    /// Without this a finished partition makes `poll` block for the timeout at the bounded tail.
    fn unassign_splits(&mut self, topics: &[String], partitions: &[i64]) {
        for i in 0..topics.len() {
            self.next_offsets.remove(&(topics[i].clone(), partitions[i] as i32));
            self.stopping_offsets
                .remove(&(topics[i].clone(), partitions[i] as i32));
        }
        self.reassign();
    }

    /// Commits a completed Flink checkpoint for Kafka-side monitoring. Flink state remains the
    /// recovery authority; the synchronous mode lets the Java reader distinguish a real broker ack
    /// from a failed commit without sharing the native handle across threads.
    fn commit_offsets(
        &self,
        topics: &[String],
        partitions: &[i64],
        offsets: &[i64],
    ) -> Result<(), String> {
        use rdkafka::consumer::{CommitMode, Consumer};
        use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

        let mut tpl = TopicPartitionList::with_capacity(topics.len());
        for i in 0..topics.len() {
            tpl.add_partition_offset(
                &topics[i],
                partitions[i] as i32,
                Offset::Offset(offsets[i]),
            )
            .map_err(|error| format!("failed to build Kafka offset commit: {error}"))?;
        }
        self.consumer
            .commit(&tpl, CommitMode::Sync)
            .map_err(|error| format!("failed to commit Kafka offsets: {error}"))
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
    fn poll(&mut self, max_records: usize, timeout: std::time::Duration) -> Result<usize, String> {
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
            buckets: Vec<(
                *mut rdsys::rd_kafka_topic_t,
                i32,
                String,
                BinaryBuilder,
                i64,
                Option<i64>,
            )>,
            last_bucket: usize,
            stopping_offsets: *const HashMap<(String, i32), i64>,
            error: Option<String>,
        }
        unsafe extern "C" fn bucket_message(
            message: *mut rdsys::rd_kafka_message_t,
            opaque: *mut std::os::raw::c_void,
        ) {
            let context = &mut *(opaque as *mut PollContext);
            context.seen += 1;
            let message = &*message;
            if message.err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                let index = if context
                    .buckets
                    .get(context.last_bucket)
                    .is_some_and(|bucket| bucket.0 == message.rkt && bucket.1 == message.partition)
                {
                    context.last_bucket
                } else if let Some(found) =
                    context
                        .buckets
                        .iter()
                        .position(|bucket| bucket.0 == message.rkt && bucket.1 == message.partition)
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
                    let stop = (*context.stopping_offsets)
                        .get(&(topic.clone(), message.partition))
                        .copied();
                    context.buckets.push((
                        message.rkt,
                        message.partition,
                        topic,
                        BinaryBuilder::with_capacity(presize, presize * 64),
                        0,
                        stop,
                    ));
                    context.buckets.len() - 1
                };
                context.last_bucket = index;
                let bucket = &mut context.buckets[index];
                if bucket.5.is_none_or(|stop| message.offset < stop) {
                    if message.payload.is_null() {
                        bucket.3.append_null();
                    } else {
                        let payload =
                            std::slice::from_raw_parts(message.payload as *const u8, message.len);
                        bucket.3.append_value(payload);
                    }
                    bucket.4 = message.offset + 1;
                    context.buffered += 1;
                }
            } else if message.err
                != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__PARTITION_EOF
                && context.error.is_none()
            {
                let name = std::ffi::CStr::from_ptr(rdsys::rd_kafka_err2name(message.err))
                    .to_string_lossy();
                let description = std::ffi::CStr::from_ptr(rdsys::rd_kafka_err2str(message.err))
                    .to_string_lossy();
                context.error = Some(format!("Kafka consumer error {name}: {description}"));
            }
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
            stopping_offsets: &self.stopping_offsets,
            error: None,
        };
        unsafe {
            rdsys::rd_kafka_consume_callback_queue(
                self.consumer_queue,
                timeout.as_millis() as std::os::raw::c_int,
                Some(bucket_message),
                &mut context as *mut PollContext as *mut std::os::raw::c_void,
            )
        };
        if let Some(error) = context.error {
            return Err(error);
        }
        // One batch per partition, straight into `pending` (the JVM drains all of them right after this
        // returns, so nothing is ever left behind on a bounded finish). With a decoder attached, each
        // body batch is decoded to its typed batch here, while its bytes are still cache-hot from the
        // callback's copies — deferring the decode to a later pass re-streamed the payload bytes cold
        // and, with the JVM round trip, measured at roughly half the fused throughput.
        self.pending.clear();
        let positions = self
            .consumer
            .position()
            .map_err(|error| format!("failed to retrieve Kafka consumer positions: {error}"))?
            .elements()
            .into_iter()
            .filter_map(|position| match position.offset() {
                rdkafka::Offset::Offset(offset) => Some((
                    (position.topic().to_owned(), position.partition()),
                    offset,
                )),
                _ => None,
            })
            .collect::<HashMap<_, _>>();
        let mut reported = HashSet::default();
        for (_rkt, partition, topic, mut builder, payload_next_offset, stop) in context.buckets {
            let key = (topic.clone(), partition);
            let position = positions.get(&key).copied().unwrap_or(payload_next_offset);
            let next_offset = stop.map_or(position, |stop| position.min(stop));
            let body = RecordBatch::try_new(self.body_schema.clone(), vec![Arc::new(builder.finish())])
                .expect("failed to build kafka body batch");
            let batch = match (self.decode, body.num_rows()) {
                (_, 0) | (None, _) => body,
                (Some((entry, decoder)), _) => Self::decode_bucket(entry, decoder, body),
            };
            self.next_offsets.insert((topic.clone(), partition), next_offset);
            self.pending.push_back((topic, partition, next_offset, batch));
            reported.insert(key);
        }
        // Empty partitions, null-only tails, and read_committed control records can advance Kafka's
        // position without producing a payload bucket. Emit an empty body batch so the JVM advances the
        // split state and can report a bounded split finished.
        for (key, position) in positions {
            if reported.contains(&key) {
                continue;
            }
            let previous = self.next_offsets.get(&key).copied();
            let next_offset = self
                .stopping_offsets
                .get(&key)
                .map_or(position, |stop| position.min(*stop));
            let reached_stop = self
                .stopping_offsets
                .get(&key)
                .is_some_and(|stop| position >= *stop);
            if previous == Some(next_offset) && !reached_stop {
                continue;
            }
            self.next_offsets.insert(key.clone(), next_offset);
            let body = RecordBatch::new_empty(self.body_schema.clone());
            self.pending
                .push_back((key.0, key.1, next_offset, body));
        }
        Ok(self.pending.len())
    }

    /// Runs the attached format's C-ABI decode on one body batch. In and out cross as Arrow C Data on
    /// this stack frame; ownership follows each structure's release callback into its producing DSO.
    fn decode_bucket(entry: DecodeBodyBatch, decoder: i64, body: RecordBatch) -> RecordBatch {
        use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
        let mut in_array = FFI_ArrowArray::empty();
        let mut in_schema = FFI_ArrowSchema::empty();
        export_record_batch(
            body,
            &mut in_array as *mut FFI_ArrowArray as jlong,
            &mut in_schema as *mut FFI_ArrowSchema as jlong,
        );
        let mut out_array = FFI_ArrowArray::empty();
        let mut out_schema = FFI_ArrowSchema::empty();
        let rc = entry(
            decoder,
            &mut in_array as *mut FFI_ArrowArray as i64,
            &mut in_schema as *mut FFI_ArrowSchema as i64,
            &mut out_array as *mut FFI_ArrowArray as i64,
            &mut out_schema as *mut FFI_ArrowSchema as i64,
        );
        assert_eq!(rc, 0, "attached format decode failed (rc {rc})");
        import_record_batch(
            &mut out_array as *mut FFI_ArrowArray as jlong,
            &mut out_schema as *mut FFI_ArrowSchema as jlong,
        )
    }
}

/// Contains native failures at the JNI boundary and turns them into the checked exception the
/// FLIP-27 split-reader contract expects. This follows the same boundary discipline as Comet: no
/// Rust panic may unwind through a JVM native frame.
#[cfg(feature = "kafka")]
fn kafka_jni<T, F>(env: &mut JNIEnv, default: T, f: F) -> T
where
    F: FnOnce(&mut JNIEnv) -> Result<T, String>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(env))) {
        Ok(Ok(value)) => value,
        Ok(Err(message)) => {
            let _ = env.throw_new("java/io/IOException", message);
            default
        }
        Err(payload) => {
            let message = if let Some(message) = payload.downcast_ref::<&str>() {
                (*message).to_string()
            } else if let Some(message) = payload.downcast_ref::<String>() {
                message.clone()
            } else {
                "unknown panic".to_string()
            };
            let _ = env.throw_new(
                "java/io/IOException",
                format!("native Kafka reader panic: {message}"),
            );
            default
        }
    }
}

/// Whether this extension library carries the native Kafka source.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_featureBuilt<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jni::sys::jboolean {
    cfg!(feature = "kafka") as jni::sys::jboolean
}

/// Opens a native Kafka split reader for one subtask and returns an opaque handle, released with
/// `closeKafkaConsumer`. `configKeys`/`configValues` are the translated librdkafka config (applied
/// verbatim). It produces raw Kafka value bodies as Arrow binary batches; the following format extension
/// owns decoding. Splits are added later via `assignKafkaSplits`.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_openKafkaConsumer<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config_keys: JObjectArray<'local>,
    config_values: JObjectArray<'local>,
) -> jlong {
    let keys = read_string_array(&mut env, &config_keys);
    let values = read_string_array(&mut env, &config_values);
    let config: Vec<(String, String)> = keys.into_iter().zip(values).collect();
    let reader = KafkaSplitReader::open(&config);
    into_handle(reader)
}

/// Attaches a format library's decode to this consumer through the driver-init handshake:
/// `initAddress` is the format's exported `streamfusion_format_driver_init`, called with the ABI
/// version this connector speaks; the format fills the vtable or refuses. Returns whether the attach
/// happened — a refusal (a format artifact from another release) leaves the caller on the
/// JVM-mediated decode. Subsequent polls of an attached consumer emit typed batches instead of
/// binary bodies. The decoder's lifecycle stays with its Java owner, which must outlive this
/// consumer.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_attachKafkaDecoder<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    init_address: jlong,
    decoder_handle: jlong,
) -> jboolean {
    let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
    let init: FormatDriverInit = unsafe { std::mem::transmute(init_address as usize) };
    let mut driver = FormatDriver { decode_body_batch: unsupported_decode };
    if init(FORMAT_DRIVER_VERSION_1, &mut driver) != 0 {
        return 0;
    }
    reader.decode = Some((driver.decode_body_batch, decoder_handle));
    1
}

/// Placeholder the driver struct is initialized with before the handshake fills it; never invoked
/// (a failed init leaves the consumer unattached).
#[cfg(feature = "kafka")]
extern "C" fn unsupported_decode(_: i64, _: i64, _: i64, _: i64, _: i64) -> i32 {
    1
}

/// Adds splits to the reader and re-assigns: `topics`/`partitions`/`startOffsets` are index-aligned;
/// new partitions seek to their start offset, existing ones keep their tracked position. Concrete
/// `stoppingOffsets` are enforced inside the poll callback; `i64::MIN` means unbounded.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_assignKafkaSplits<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topics: JObjectArray<'local>,
    partitions: JLongArray<'local>,
    start_offsets: JLongArray<'local>,
    stopping_offsets: JLongArray<'local>,
) {
    let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
    let topics = read_string_array(&mut env, &topics);
    let partitions = read_longs(&env, &partitions);
    let offsets = read_longs(&env, &start_offsets);
    let stopping_offsets = read_longs(&env, &stopping_offsets);
    reader.assign_splits(&topics, &partitions, &offsets, &stopping_offsets);
}

/// Removes finished splits (reached their bounded stopping offset) from the assignment so the consumer
/// stops fetching/blocking on them. Index-aligned `topics`/`partitions`.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_unassignKafkaSplits<'local>(
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

/// Commits checkpoint positions from a split-fetcher task, serializing the operation with native
/// poll/assign/close access to this handle.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_commitKafkaOffsets<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topics: JObjectArray<'local>,
    partitions: JLongArray<'local>,
    offsets: JLongArray<'local>,
) {
    kafka_jni(&mut env, (), |env| {
        let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
        let topics = read_string_array(env, &topics);
        let partitions = read_longs(env, &partitions);
        let offsets = read_longs(env, &offsets);
        if topics.len() != partitions.len() || topics.len() != offsets.len() {
            return Err("Kafka offset commit arrays have different lengths".to_string());
        }
        reader.commit_offsets(&topics, &partitions, &offsets)
    });
}

/// Polls one cycle, producing one Arrow binary-body batch per partition that had messages. Returns the number of
/// per-partition batches now pending; the JVM drains each with `drainKafkaSplit`.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_pollKafkaBatch<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_records: jint,
    timeout_ms: jlong,
) -> jint {
    kafka_jni(&mut env, 0, |_env| {
        let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
        Ok(reader.poll(
            max_records as usize,
            std::time::Duration::from_millis(timeout_ms as u64),
        )? as jint)
    })
}

/// Drains one pending per-partition body batch, writes `[partition, nextOffset]` into `splitMeta`, and
/// the topic into `outTopic[0]`, so the JVM can form the split id and advance its checkpoint offset.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_drainKafkaSplit<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    split_meta: JLongArray<'local>,
    out_topic: JObjectArray<'local>,
    out_array_address: jlong,
    out_schema_address: jlong,
) -> jint {
    let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
    let (topic, partition, next_offset, batch) =
        reader.pending.pop_front().expect("drainKafkaSplit called with no pending batch");
    let rows = batch.num_rows() as jint;
    env.set_long_array_region(&split_meta, 0, &[partition as i64, next_offset])
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
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_closeKafkaConsumer<'local>(
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
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_benchmarkNativeConsume<'local>(
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
    let _ = (format, schema_array_address, schema_address, avro_schema, schema_id);
    let mut reader = KafkaSplitReader::open(&config);
    reader.assign_splits(&[topic], &[0], &[-2], &[i64::MIN]); // partition 0, earliest

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
        let count = reader.poll(poll_cap, timeout).expect("failed to poll Kafka");
        if count == 0 {
            idle += 1;
            continue;
        }
        idle = 0;
        for (_topic, _partition, _next_offset, batch) in reader.pending.drain(..) {
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
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_benchmarkConsumeOnly<'local>(
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

/// Benchmark-only: a hand-rolled raw-consume loop with none of the split-reader machinery (no
/// per-partition bucketing, no offset tracking, no pending queue). Kept for comparisons with the Java
/// client; format decode is now deliberately owned by a separate format DSO.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_benchmarkNativeConsumeSerial<'local>(
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
    use rdkafka::bindings as rdsys;
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

    let keys = read_string_array(&mut env, &config_keys);
    let values = read_string_array(&mut env, &config_values);
    let topic: String = env.get_string(&topic).expect("failed to read topic").into();
    let _ = (format, schema_array_address, schema_address, avro_schema, schema_id);

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

    // Callback drain (one queue lock per poll, not per message — see benchmarkConsumeOnly). No payload
    // copy occurs here: this benchmark measures the connector DSO's raw delivery floor.
    struct SerialCtx {
        appended: i64,
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
            context.appended += 1;
        }
    }
    let mut rows: i64 = 0;
    let mut idle = 0;
    while rows < max_messages && idle < 40 {
        let mut context = SerialCtx { appended: 0 };
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
        rows += context.appended;
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
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_benchmarkKafkaConsume<'local>(
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
