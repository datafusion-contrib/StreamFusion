use crate::*;

#[cfg(feature = "fluss")]
const NO_PARTITION: i64 = i64::MIN;
#[cfg(feature = "fluss")]
const NO_STOPPING_OFFSET: i64 = i64::MIN;

/// Whether this native library was built with the native Fluss reader feature.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flussFeatureBuilt<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jni::sys::jboolean {
    cfg!(feature = "fluss") as jni::sys::jboolean
}

#[cfg(feature = "fluss")]
struct FlussSplitReader {
    _connection: fluss_rs::client::FlussConnection,
    scanner: fluss_rs::client::RecordBatchLogScanner,
    split_ids: HashMap<fluss_rs::metadata::TableBucket, String>,
    stopping_offsets: HashMap<fluss_rs::metadata::TableBucket, i64>,
    pending: std::collections::VecDeque<(String, i64, RecordBatch)>,
}

#[cfg(feature = "fluss")]
impl FlussSplitReader {
    fn open(
        config: &[(String, String)],
        database_name: &str,
        table_name: &str,
        projected_fields: &[usize],
    ) -> Result<Self, String> {
        let config = fluss_config(config)?;
        let table_path =
            fluss_rs::metadata::TablePath::new(database_name.to_string(), table_name.to_string());
        let (connection, scanner) = runtime().block_on(async {
            let connection = fluss_rs::client::FlussConnection::new(config)
                .await
                .map_err(|e| format!("failed to create Fluss connection: {e}"))?;
            let scanner = {
                let table = connection
                    .get_table(&table_path)
                    .await
                    .map_err(|e| format!("failed to get Fluss table: {e}"))?;
                let scan = table.new_scan();
                let scan = if projected_fields.is_empty() {
                    scan
                } else {
                    scan.project(projected_fields)
                        .map_err(|e| format!("failed to project Fluss scan: {e}"))?
                };
                scan.create_record_batch_log_scanner()
                    .map_err(|e| format!("failed to create Fluss RecordBatch log scanner: {e}"))?
            };
            Ok::<_, String>((connection, scanner))
        })?;
        Ok(Self {
            _connection: connection,
            scanner,
            split_ids: HashMap::default(),
            stopping_offsets: HashMap::default(),
            pending: std::collections::VecDeque::new(),
        })
    }

    fn assign_splits(
        &mut self,
        split_ids: &[String],
        table_ids: &[i64],
        partition_ids: &[i64],
        buckets: &[i64],
        start_offsets: &[i64],
        stopping_offsets: &[i64],
    ) -> Result<(), String> {
        if split_ids.len() != table_ids.len()
            || split_ids.len() != partition_ids.len()
            || split_ids.len() != buckets.len()
            || split_ids.len() != start_offsets.len()
            || split_ids.len() != stopping_offsets.len()
        {
            return Err("mismatched Fluss split assignment array lengths".to_string());
        }
        for i in 0..split_ids.len() {
            let table_bucket = table_bucket(table_ids[i], partition_ids[i], buckets[i]);
            self.split_ids
                .insert(table_bucket.clone(), split_ids[i].clone());
            if stopping_offsets[i] != NO_STOPPING_OFFSET {
                self.stopping_offsets
                    .insert(table_bucket.clone(), stopping_offsets[i]);
            }
            if partition_ids[i] == NO_PARTITION {
                runtime()
                    .block_on(self.scanner.subscribe(buckets[i] as i32, start_offsets[i]))
                    .map_err(|e| format!("failed to subscribe Fluss bucket: {e}"))?;
            } else {
                runtime()
                    .block_on(self.scanner.subscribe_partition(
                        partition_ids[i],
                        buckets[i] as i32,
                        start_offsets[i],
                    ))
                    .map_err(|e| format!("failed to subscribe Fluss partition bucket: {e}"))?;
            }
        }
        Ok(())
    }

    fn unassign_splits(
        &mut self,
        table_ids: &[i64],
        partition_ids: &[i64],
        buckets: &[i64],
    ) -> Result<(), String> {
        if table_ids.len() != partition_ids.len() || table_ids.len() != buckets.len() {
            return Err("mismatched Fluss split unassignment array lengths".to_string());
        }
        for i in 0..table_ids.len() {
            let table_bucket = table_bucket(table_ids[i], partition_ids[i], buckets[i]);
            self.unsubscribe_bucket(&table_bucket);
            self.split_ids.remove(&table_bucket);
            self.stopping_offsets.remove(&table_bucket);
        }
        Ok(())
    }

    fn poll(&mut self, timeout: std::time::Duration) -> Result<usize, String> {
        let batches = match runtime().block_on(self.scanner.poll(timeout)) {
            Ok(batches) => batches,
            Err(fluss_rs::error::Error::FlussAPIError { api_error })
                if fluss_rs::error::FlussError::for_code(api_error.code)
                    == fluss_rs::error::FlussError::PartitionNotExists =>
            {
                self.remove_missing_partition(api_error.message.as_str())?;
                return Ok(self.pending.len());
            }
            Err(e) => return Err(format!("failed to poll Fluss batches: {e}")),
        };
        for batch in batches {
            self.enqueue(batch);
        }
        Ok(self.pending.len())
    }

    fn remove_missing_partition(&mut self, message: &str) -> Result<(), String> {
        let partition_id = parse_missing_partition_id(message).ok_or_else(|| {
            format!("failed to poll Fluss batches: partition disappeared but Fluss did not return a partition id: {message}")
        })?;
        let removed_buckets: Vec<_> = self
            .split_ids
            .keys()
            .filter(|table_bucket| table_bucket.partition_id() == Some(partition_id))
            .cloned()
            .collect();
        if removed_buckets.is_empty() {
            return Err(format!(
                "failed to poll Fluss batches: partition {partition_id} disappeared but no assigned split matched it: {message}"
            ));
        }

        let removed_split_ids: HashSet<_> = removed_buckets
            .iter()
            .filter_map(|table_bucket| self.split_ids.get(table_bucket).cloned())
            .collect();
        for table_bucket in removed_buckets {
            self.unsubscribe_bucket(&table_bucket);
            self.split_ids.remove(&table_bucket);
            self.stopping_offsets.remove(&table_bucket);
        }
        self.pending
            .retain(|(split_id, _, _)| !removed_split_ids.contains(split_id));
        Ok(())
    }

    fn enqueue(&mut self, scan_batch: fluss_rs::record::ScanBatch) {
        let table_bucket = scan_batch.bucket().clone();
        let Some(split_id) = self.split_ids.get(&table_bucket).cloned() else {
            return;
        };

        let base_offset = scan_batch.base_offset();
        let mut batch = scan_batch.into_batch();
        let mut next_offset = base_offset + batch.num_rows() as i64;

        if let Some(stopping_offset) = self.stopping_offsets.get(&table_bucket).copied() {
            if base_offset >= stopping_offset {
                self.unsubscribe_bucket(&table_bucket);
                return;
            }
            if next_offset > stopping_offset {
                let keep_rows = (stopping_offset - base_offset).max(0) as usize;
                if keep_rows == 0 {
                    self.unsubscribe_bucket(&table_bucket);
                    return;
                }
                batch = batch.slice(0, keep_rows);
                next_offset = stopping_offset;
            }
            if next_offset >= stopping_offset {
                self.stopping_offsets.remove(&table_bucket);
                self.unsubscribe_bucket(&table_bucket);
            }
        }

        if batch.num_rows() > 0 {
            self.pending.push_back((split_id, next_offset, batch));
        }
    }

    fn unsubscribe_bucket(&self, table_bucket: &fluss_rs::metadata::TableBucket) {
        if let Some(partition_id) = table_bucket.partition_id() {
            let _ = runtime().block_on(
                self.scanner
                    .unsubscribe_partition(partition_id, table_bucket.bucket_id()),
            );
        } else {
            let _ = runtime().block_on(self.scanner.unsubscribe(table_bucket.bucket_id()));
        }
    }
}

#[cfg(feature = "fluss")]
fn table_bucket(table_id: i64, partition_id: i64, bucket: i64) -> fluss_rs::metadata::TableBucket {
    if partition_id == NO_PARTITION {
        fluss_rs::metadata::TableBucket::new(table_id, bucket as i32)
    } else {
        fluss_rs::metadata::TableBucket::new_with_partition(
            table_id,
            Some(partition_id),
            bucket as i32,
        )
    }
}

#[cfg(feature = "fluss")]
fn parse_missing_partition_id(message: &str) -> Option<i64> {
    let marker = "partition id '";
    let start = message.find(marker)? + marker.len();
    let end = message[start..].find('\'')?;
    message[start..start + end].parse().ok()
}

#[cfg(feature = "fluss")]
fn fluss_config(values: &[(String, String)]) -> Result<fluss_rs::config::Config, String> {
    let mut config = fluss_rs::config::Config::default();
    for (key, value) in values {
        match key.as_str() {
            "bootstrap_servers" => config.bootstrap_servers = value.clone(),
            "writer_request_max_size" => config.writer_request_max_size = parse(key, value)?,
            "writer_acks" => config.writer_acks = value.clone(),
            "writer_retries" => config.writer_retries = parse(key, value)?,
            "writer_batch_size" => config.writer_batch_size = parse(key, value)?,
            "writer_dynamic_batch_size_enabled" => {
                config.writer_dynamic_batch_size_enabled = parse(key, value)?
            }
            "writer_dynamic_batch_size_min" => {
                config.writer_dynamic_batch_size_min = parse(key, value)?
            }
            "writer_bucket_no_key_assigner" => {
                config.writer_bucket_no_key_assigner = value.parse().map_err(|e| {
                    format!("failed to parse writer_bucket_no_key_assigner={value}: {e}")
                })?
            }
            "scanner_remote_log_prefetch_num" => {
                config.scanner_remote_log_prefetch_num = parse(key, value)?
            }
            "remote_file_download_thread_num" => {
                config.remote_file_download_thread_num = parse(key, value)?
            }
            "scanner_log_max_poll_records" => {
                config.scanner_log_max_poll_records = parse(key, value)?
            }
            "scanner_log_fetch_max_bytes" => {
                config.scanner_log_fetch_max_bytes = parse(key, value)?
            }
            "scanner_log_fetch_min_bytes" => {
                config.scanner_log_fetch_min_bytes = parse(key, value)?
            }
            "scanner_log_fetch_wait_max_time_ms" => {
                config.scanner_log_fetch_wait_max_time_ms = parse(key, value)?
            }
            "scanner_log_fetch_max_bytes_for_bucket" => {
                config.scanner_log_fetch_max_bytes_for_bucket = parse(key, value)?
            }
            "writer_batch_timeout_ms" => config.writer_batch_timeout_ms = parse(key, value)?,
            "writer_enable_idempotence" => config.writer_enable_idempotence = parse(key, value)?,
            "writer_max_inflight_requests_per_bucket" => {
                config.writer_max_inflight_requests_per_bucket = parse(key, value)?
            }
            "writer_buffer_memory_size" => config.writer_buffer_memory_size = parse(key, value)?,
            "writer_buffer_wait_timeout_ms" => {
                config.writer_buffer_wait_timeout_ms = parse(key, value)?
            }
            "connect_timeout_ms" => config.connect_timeout_ms = parse(key, value)?,
            "security_protocol" => config.security_protocol = value.clone(),
            "security_sasl_mechanism" => config.security_sasl_mechanism = value.clone(),
            "security_sasl_username" => config.security_sasl_username = value.clone(),
            "security_sasl_password" => config.security_sasl_password = value.clone(),
            "lookup_queue_size" => config.lookup_queue_size = parse(key, value)?,
            "lookup_max_batch_size" => config.lookup_max_batch_size = parse(key, value)?,
            "lookup_batch_timeout_ms" => config.lookup_batch_timeout_ms = parse(key, value)?,
            "lookup_max_inflight_requests" => {
                config.lookup_max_inflight_requests = parse(key, value)?
            }
            "lookup_max_retries" => config.lookup_max_retries = parse(key, value)?,
            other => return Err(format!("unsupported fluss-rs config key {other}")),
        }
    }
    Ok(config)
}

#[cfg(feature = "fluss")]
fn parse<T>(key: &str, value: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|e| format!("failed to parse Fluss config {key}={value}: {e}"))
}

#[cfg(feature = "fluss")]
fn fluss_jni<T, F>(env: &mut JNIEnv, default: T, f: F) -> T
where
    F: FnOnce(&mut JNIEnv) -> Result<T, String>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(env))) {
        Ok(Ok(value)) => value,
        Ok(Err(message)) => {
            throw_fluss_exception(env, &message);
            default
        }
        Err(payload) => {
            throw_fluss_exception(
                env,
                &format!("native Fluss reader panic: {}", panic_message(payload)),
            );
            default
        }
    }
}

#[cfg(feature = "fluss")]
fn throw_fluss_exception(env: &mut JNIEnv, message: &str) {
    let _ = env.throw_new("java/io/IOException", message);
}

#[cfg(feature = "fluss")]
fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    }
}

#[cfg(feature = "fluss")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_openFlussReader<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config_keys: JObjectArray<'local>,
    config_values: JObjectArray<'local>,
    database_name: JString<'local>,
    table_name: JString<'local>,
    projected_fields: JIntArray<'local>,
) -> jlong {
    fluss_jni(&mut env, 0, |env| {
        let keys = read_string_array(env, &config_keys);
        let values = read_string_array(env, &config_values);
        let config: Vec<(String, String)> = keys.into_iter().zip(values).collect();
        let database_name: String = env
            .get_string(&database_name)
            .map(Into::into)
            .map_err(|e| format!("failed to read Fluss database name: {e}"))?;
        let table_name: String = env
            .get_string(&table_name)
            .map(Into::into)
            .map_err(|e| format!("failed to read Fluss table name: {e}"))?;
        let projected_fields = read_columns(env, &projected_fields);
        Ok(into_handle(FlussSplitReader::open(
            &config,
            &database_name,
            &table_name,
            &projected_fields,
        )?))
    })
}

#[cfg(feature = "fluss")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_assignFlussSplits<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    split_ids: JObjectArray<'local>,
    table_ids: JLongArray<'local>,
    partition_ids: JLongArray<'local>,
    buckets: JLongArray<'local>,
    start_offsets: JLongArray<'local>,
    stopping_offsets: JLongArray<'local>,
) {
    fluss_jni(&mut env, (), |env| {
        let reader = unsafe { &mut *(handle as *mut FlussSplitReader) };
        let split_ids = read_string_array(env, &split_ids);
        let table_ids = read_longs(env, &table_ids);
        let partition_ids = read_longs(env, &partition_ids);
        let buckets = read_longs(env, &buckets);
        let start_offsets = read_longs(env, &start_offsets);
        let stopping_offsets = read_longs(env, &stopping_offsets);
        reader.assign_splits(
            &split_ids,
            &table_ids,
            &partition_ids,
            &buckets,
            &start_offsets,
            &stopping_offsets,
        )
    });
}

#[cfg(feature = "fluss")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_unassignFlussSplits<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    table_ids: JLongArray<'local>,
    partition_ids: JLongArray<'local>,
    buckets: JLongArray<'local>,
) {
    fluss_jni(&mut env, (), |env| {
        let reader = unsafe { &mut *(handle as *mut FlussSplitReader) };
        let table_ids = read_longs(env, &table_ids);
        let partition_ids = read_longs(env, &partition_ids);
        let buckets = read_longs(env, &buckets);
        reader.unassign_splits(&table_ids, &partition_ids, &buckets)
    });
}

#[cfg(feature = "fluss")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pollFlussBatch<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    timeout_ms: jlong,
) -> jint {
    fluss_jni(&mut env, 0, |_env| {
        let reader = unsafe { &mut *(handle as *mut FlussSplitReader) };
        Ok(reader.poll(std::time::Duration::from_millis(timeout_ms as u64))? as jint)
    })
}

#[cfg(feature = "fluss")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_drainFlussSplit<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    split_meta: JLongArray<'local>,
    out_split_id: JObjectArray<'local>,
    out_array_address: jlong,
    out_schema_address: jlong,
) -> jint {
    fluss_jni(&mut env, 0, |env| {
        let reader = unsafe { &mut *(handle as *mut FlussSplitReader) };
        let (split_id, next_offset, batch) = reader
            .pending
            .pop_front()
            .ok_or_else(|| "drainFlussSplit called with no pending batch".to_string())?;
        let rows = batch.num_rows() as jint;
        env.set_long_array_region(&split_meta, 0, &[next_offset])
            .map_err(|e| format!("failed to write Fluss split meta: {e}"))?;
        let split_id = env
            .new_string(&split_id)
            .map_err(|e| format!("failed to allocate Fluss split id string: {e}"))?;
        env.set_object_array_element(&out_split_id, 0, &split_id)
            .map_err(|e| format!("failed to write Fluss split id: {e}"))?;
        export_record_batch(batch, out_array_address, out_schema_address);
        Ok(rows)
    })
}

#[cfg(feature = "fluss")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeFlussReader<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    fluss_jni(&mut env, (), |_env| {
        unsafe {
            drop(from_handle::<FlussSplitReader>(handle));
        }
        Ok(())
    });
}

#[cfg(all(test, feature = "fluss"))]
mod tests {
    use super::*;

    #[test]
    fn translated_keys_populate_fluss_rs_config_fields() {
        let config = fluss_config(&[
            ("bootstrap_servers".into(), "localhost:9123".into()),
            ("scanner_log_max_poll_records".into(), "2048".into()),
            ("scanner_remote_log_prefetch_num".into(), "8".into()),
            ("remote_file_download_thread_num".into(), "4".into()),
            ("scanner_log_fetch_max_bytes".into(), "16777216".into()),
            (
                "scanner_log_fetch_max_bytes_for_bucket".into(),
                "1048576".into(),
            ),
            ("scanner_log_fetch_min_bytes".into(), "1".into()),
            ("scanner_log_fetch_wait_max_time_ms".into(), "500".into()),
            ("writer_request_max_size".into(), "8388608".into()),
            ("writer_acks".into(), "1".into()),
            ("writer_retries".into(), "9".into()),
            ("writer_batch_size".into(), "2097152".into()),
            ("writer_dynamic_batch_size_enabled".into(), "false".into()),
            ("writer_bucket_no_key_assigner".into(), "round_robin".into()),
            ("writer_batch_timeout_ms".into(), "250".into()),
            ("writer_enable_idempotence".into(), "false".into()),
            ("writer_max_inflight_requests_per_bucket".into(), "3".into()),
            ("writer_buffer_memory_size".into(), "67108864".into()),
            ("writer_buffer_wait_timeout_ms".into(), "1000".into()),
            ("connect_timeout_ms".into(), "30000".into()),
            ("security_protocol".into(), "sasl".into()),
            ("security_sasl_mechanism".into(), "PLAIN".into()),
            ("security_sasl_username".into(), "alice".into()),
            ("security_sasl_password".into(), "secret".into()),
            ("lookup_queue_size".into(), "64".into()),
            ("lookup_max_batch_size".into(), "32".into()),
            ("lookup_batch_timeout_ms".into(), "25".into()),
            ("lookup_max_inflight_requests".into(), "16".into()),
            ("lookup_max_retries".into(), "7".into()),
        ])
        .expect("translated config");

        assert_eq!(config.bootstrap_servers, "localhost:9123");
        assert_eq!(config.scanner_log_max_poll_records, 2048);
        assert_eq!(config.scanner_remote_log_prefetch_num, 8);
        assert_eq!(config.remote_file_download_thread_num, 4);
        assert_eq!(config.scanner_log_fetch_max_bytes, 16_777_216);
        assert_eq!(config.scanner_log_fetch_max_bytes_for_bucket, 1_048_576);
        assert_eq!(config.scanner_log_fetch_min_bytes, 1);
        assert_eq!(config.scanner_log_fetch_wait_max_time_ms, 500);
        assert_eq!(config.writer_request_max_size, 8_388_608);
        assert_eq!(config.writer_acks, "1");
        assert_eq!(config.writer_retries, 9);
        assert_eq!(config.writer_batch_size, 2_097_152);
        assert!(!config.writer_dynamic_batch_size_enabled);
        assert_eq!(
            config.writer_bucket_no_key_assigner,
            fluss_rs::config::NoKeyAssigner::RoundRobin
        );
        assert_eq!(config.writer_batch_timeout_ms, 250);
        assert!(!config.writer_enable_idempotence);
        assert_eq!(config.writer_max_inflight_requests_per_bucket, 3);
        assert_eq!(config.writer_buffer_memory_size, 67_108_864);
        assert_eq!(config.writer_buffer_wait_timeout_ms, 1000);
        assert_eq!(config.connect_timeout_ms, 30_000);
        assert_eq!(config.security_protocol, "sasl");
        assert_eq!(config.security_sasl_mechanism, "PLAIN");
        assert_eq!(config.security_sasl_username, "alice");
        assert_eq!(config.security_sasl_password, "secret");
        assert_eq!(config.lookup_queue_size, 64);
        assert_eq!(config.lookup_max_batch_size, 32);
        assert_eq!(config.lookup_batch_timeout_ms, 25);
        assert_eq!(config.lookup_max_inflight_requests, 16);
        assert_eq!(config.lookup_max_retries, 7);
    }
}
