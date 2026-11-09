use super::*;

pub(super) const PROC_ROW_CLOCK: &str = "__sf_over_processing_clock";
const PROC_COUNT: &str = "__sf_over_processed_count";
const PROC_SMALLEST: &str = "__sf_over_smallest_ts";

#[derive(Clone)]
pub(super) struct ProcRowsState {
    pub(super) count: i64,
    pub(super) smallest: i64,
}

#[cfg(test)]
mod tests;

impl ProcRowsState {
    #[cfg(feature = "rocksdb-state")]
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![1];
        bytes.extend_from_slice(&self.count.to_le_bytes());
        bytes.extend_from_slice(&self.smallest.to_le_bytes());
        bytes
    }

    #[cfg(feature = "rocksdb-state")]
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, DataFusionError> {
        if bytes.len() != 17 || bytes[0] != 1 {
            return Err(DataFusionError::Execution(
                "invalid OVER processing ROWS metadata".into(),
            ));
        }
        let count = i64::from_le_bytes(bytes[1..9].try_into().unwrap());
        if count < 0 {
            return Err(DataFusionError::Execution(
                "negative OVER processed count".into(),
            ));
        }
        Ok(Self {
            count,
            smallest: i64::from_le_bytes(bytes[9..17].try_into().unwrap()),
        })
    }

    fn clock(row: &BufferedRow) -> i64 {
        // Earlier native checkpoints did not retain processing clocks. Their ordinary restore
        // remains available; missing original rows still prevent exact stock canonical export.
        row.processing_clock().unwrap_or(0)
    }

    pub(super) fn legacy(rows: &[BufferedRow], width: i64, now: i64) -> Self {
        Self {
            count: (rows.len() as i64).min(width),
            smallest: rows.first().map(Self::clock).unwrap_or(now),
        }
    }

    pub(super) fn advance(
        &mut self,
        rows: &[BufferedRow],
        active: &mut Vec<usize>,
        current: usize,
        width: i64,
    ) -> Option<usize> {
        let expired = if self.count == width {
            let position = active
                .iter()
                .position(|&index| Self::clock(&rows[index]) == self.smallest);
            let removed = position.map(|position| active.remove(position));
            if !active
                .iter()
                .any(|&index| Self::clock(&rows[index]) == self.smallest)
            {
                self.smallest = active
                    .iter()
                    .map(|&index| Self::clock(&rows[index]))
                    .min()
                    .unwrap_or(i64::MAX);
            }
            removed
        } else {
            self.count += 1;
            None
        };
        active.push(current);
        expired
    }
}

impl OverWindowAggregator {
    pub(super) fn fire_processing_rows_cleanup(
        &mut self,
        timestamp: i64,
    ) -> Result<(), DataFusionError> {
        if !self.processing_rows() {
            return Err(DataFusionError::Execution(
                "processing cleanup requires bounded processing ROWS".into(),
            ));
        }
        let due = self
            .cleanup_state
            .iter()
            .filter(|(_, deadline)| **deadline <= timestamp)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in due {
            self.clear_key(&key.0);
        }
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            self.flush_tombstones()?;
        }
        self.account()
    }

    pub(super) fn processing_rows(&self) -> bool {
        self.proctime && matches!(&self.inner, OverInner::Bounded(inner) if inner.rows_frame)
    }

    pub(super) fn proc_rows_bytes(&self) -> usize {
        self.proc_rows
            .keys()
            .map(|key| byte_key_bytes(&key.0) + std::mem::size_of::<ProcRowsState>())
            .sum()
    }

    pub(super) fn snapshot_proc_rows(&self) -> Vec<u8> {
        if self.proc_rows.is_empty() && self.cleanup_state.is_empty() {
            return Vec::new();
        }
        let types = self.cleanup_key_types();
        let converter = key_row_converter_from_types(&types);
        let keys = self
            .proc_rows
            .keys()
            .chain(self.cleanup_state.keys())
            .map(|key| key.0.as_ref())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mut fields = key_fields(&types);
        let mut columns = decode_byte_keys(Some(&converter), &keys, &types);
        fields.push(Field::new(CLEANUP_AT_COLUMN, DataType::Int64, true));
        fields.push(Field::new(PROC_COUNT, DataType::Int64, true));
        fields.push(Field::new(PROC_SMALLEST, DataType::Int64, true));
        columns.push(Arc::new(Int64Array::from(
            keys.iter()
                .map(|key| self.cleanup_state.get(*key).copied())
                .collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Int64Array::from(
            keys.iter()
                .map(|key| self.proc_rows.get(*key).map(|state| state.count))
                .collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Int64Array::from(
            keys.iter()
                .map(|key| self.proc_rows.get(*key).map(|state| state.smallest))
                .collect::<Vec<_>>(),
        )));
        write_ipc(
            &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .expect("processing ROWS metadata"),
        )
    }

    pub(super) fn restore_proc_rows_batch(&mut self, batch: &RecordBatch, keys: &arrow::row::Rows) {
        let arity = self.key_columns.len();
        if !self.processing_rows() || batch.num_columns() == arity + 1 {
            return;
        }
        assert_eq!(
            batch.num_columns(),
            arity + 3,
            "processing ROWS metadata schema"
        );
        assert_eq!(batch.schema().field(arity + 1).name(), PROC_COUNT);
        assert_eq!(batch.schema().field(arity + 2).name(), PROC_SMALLEST);
        let count = batch
            .column(arity + 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("processing ROWS count");
        let smallest = batch
            .column(arity + 2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("processing ROWS minimum");
        let width = match &self.inner {
            OverInner::Bounded(inner) => inner.offset.checked_add(1).unwrap(),
            _ => unreachable!(),
        };
        let before = self.proc_rows_bytes();
        for row in 0..batch.num_rows() {
            assert_eq!(
                count.is_null(row),
                smallest.is_null(row),
                "incomplete processing ROWS metadata"
            );
            if count.is_valid(row) {
                assert!(
                    (0..=width).contains(&count.value(row)),
                    "invalid processing ROWS count"
                );
                let key = ByteKey::from(keys.row(row).data());
                assert!(
                    !self.proc_rows.contains_key(&key),
                    "duplicate processing ROWS key"
                );
                self.proc_rows.insert(
                    key,
                    ProcRowsState {
                        count: count.value(row),
                        smallest: smallest.value(row),
                    },
                );
            }
        }
        self.retention_bytes = self.retention_bytes - before + self.proc_rows_bytes();
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_bindOverProcessingRowsCleanupTimers<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let operator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
        if !operator.processing_rows() {
            let _ = env.throw_new(
                "java/lang/IllegalStateException",
                "processing cleanup requires bounded processing ROWS",
            );
            return;
        }
        operator.processing_rows_external_timers = true;
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_nextOverProcessingRowsCleanup<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jlongArray {
    crate::bridge::jni_guard(env, move |env| {
        let operator = unsafe { &*(handle as *const OverWindowAggregator) };
        let deadlines = operator
            .cleanup_state
            .values()
            .min()
            .copied()
            .into_iter()
            .collect::<Vec<_>>();
        let output = env
            .new_long_array(deadlines.len() as i32)
            .expect("processing cleanup deadline");
        env.set_long_array_region(&output, 0, &deadlines)
            .expect("processing cleanup deadline value");
        output.into_raw()
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_fireOverProcessingRowsCleanup<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    timestamp: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let operator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
        if let Err(error) = operator.fire_processing_rows_cleanup(timestamp) {
            if matches!(error, DataFusionError::ResourcesExhausted(_)) {
                throw_memory_limit(&mut env, &error.to_string());
            } else {
                let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
            }
        }
    });
}
