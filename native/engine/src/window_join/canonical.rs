use super::*;

const CHUNK_ROWS: usize = 1024;

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("canonical window join: {message}"))
}

impl WindowJoiner {
    fn canonical_schema(&self, left: bool) -> &SchemaRef {
        if left {
            &self.left_data_schema
        } else {
            &self.right_data_schema
        }
    }

    fn visit_canonical_rows(
        &self,
        left: bool,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let schema = self.canonical_schema(left);
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            return store.visit_canonical_rows(
                left,
                schema,
                CHUNK_ROWS,
                self.memory.temporary_reservation(),
                accept,
            );
        }
        let batches = if left {
            &self.left_buffered
        } else {
            &self.right_buffered
        };
        for batch in batches {
            if batch.num_columns() < schema.fields().len() {
                return Err(invalid("retained input schema changed"));
            }
            for offset in (0..batch.num_rows()).step_by(CHUNK_ROWS) {
                let batch = batch.slice(offset, CHUNK_ROWS.min(batch.num_rows() - offset));
                // Native representation companions are not stock row fields.
                let columns = batch.columns()[..schema.fields().len()].to_vec();
                accept(RecordBatch::try_new(schema.clone(), columns)?)?;
            }
        }
        Ok(())
    }

    fn import_canonical_rows(
        &mut self,
        left: bool,
        batch: &RecordBatch,
    ) -> Result<(), DataFusionError> {
        if self.current_watermark != i64::MIN {
            return Err(invalid("import requires a fresh event frontier"));
        }
        let schema = self.canonical_schema(left).clone();
        if batch.num_rows() > CHUNK_ROWS
            || batch.num_columns() != schema.fields().len()
            || batch
                .schema()
                .fields()
                .iter()
                .zip(schema.fields())
                .any(|(a, b)| a.data_type() != b.data_type())
        {
            return Err(invalid("pending input schema or transfer chunk changed"));
        }
        let (start, end) = if left {
            (self.left_wstart, self.left_wend)
        } else {
            (self.right_wstart, self.right_wend)
        };
        let starts = rt_to_millis(batch.column(start));
        let ends = rt_to_millis(batch.column(end));
        if starts.null_count() != 0
            || ends.null_count() != 0
            || starts
                .values()
                .iter()
                .zip(ends.values())
                .any(|(start, end)| start >= end)
        {
            return Err(invalid("NULL or inverted window bounds"));
        }
        let mut work = self.memory.temporary_reservation();
        if let Some(work) = &mut work {
            work.try_grow(batch.get_array_memory_size())?;
        }
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            return self.push_store(
                RecordBatch::try_new(schema, batch.columns().to_vec())?,
                left,
            );
        }
        // Comet's CopyExec gathers into MutableArrayData before retaining imported JVM buffers.
        let columns = batch
            .columns()
            .iter()
            .map(|column| {
                let data = column.to_data();
                let mut owned = MutableArrayData::new(vec![&data], false, column.len());
                owned.extend(0, 0, column.len());
                crate::owned::own_array_data(owned.freeze(), false).map(make_array)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let owned = RecordBatch::try_new(schema.clone(), columns)?;
        drop(work);
        if left {
            self.left_buffered.push(owned);
        } else {
            self.right_buffered.push(owned);
        }
        if let Err(error) = self.account() {
            if left {
                self.left_buffered.pop();
            } else {
                self.right_buffered.pop();
            }
            self.account()?;
            return Err(error);
        }
        if left {
            self.left_schema = Some(schema);
        } else {
            self.right_schema = Some(schema);
        }
        Ok(())
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportWindowJoinCanonicalRows<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    left: jni::sys::jboolean,
    receiver: jni::objects::JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &*(handle as *const WindowJoiner) };
        if let Err(error) = joiner.visit_canonical_rows(left != 0, |batch| {
            crate::bridge::emit_record_batch(&mut env, &receiver, batch)
        }) {
            let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_importWindowJoinCanonicalRows<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    left: jni::sys::jboolean,
    array: jlong,
    schema: jlong,
) {
    crate::bridge::jni_guard(env, move |env| {
        let joiner = unsafe { &mut *(handle as *mut WindowJoiner) };
        let result = {
            let batch = import_record_batch(array, schema);
            joiner.import_canonical_rows(left != 0, &batch)
        };
        if let Err(error) = result {
            let _ = env.throw_new("java/lang/IllegalArgumentException", error.to_string());
        }
    });
}

#[cfg(test)]
mod tests;
