use super::*;
use jni::objects::JObject;

pub(super) const FRAME_INPUT_COLUMN: &str = "__sf_over_frame_input";
const INPUT_COLUMN: &str = "__sf_over_original_row";
const CLOCK_COLUMN: &str = "__sf_over_original_clock";

#[derive(Clone)]
pub(crate) struct FrameInput {
    pub(crate) row: RecordBatch,
    pub(crate) clock: i64,
}

impl FrameInput {
    pub(in crate::over_agg) fn owned(
        batch: &RecordBatch,
        index: usize,
        clock: i64,
    ) -> Result<Self, DataFusionError> {
        let columns = batch
            .columns()
            .iter()
            .map(|column| {
                let data = column.to_data();
                let mut owned = MutableArrayData::new(vec![&data], false, 1);
                owned.extend(0, index, index + 1);
                own_array_data(owned.freeze(), false).map(make_array)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            row: RecordBatch::try_new(batch.schema(), columns)?,
            clock,
        })
    }

    pub(super) fn from_projected(batch: &RecordBatch, index: usize) -> Option<Self> {
        let column = batch.schema().index_of(INPUT_COLUMN).ok()?;
        let input = batch
            .column(column)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("OVER input struct");
        let schema = Arc::new(Schema::new_with_metadata(
            input.fields().clone(),
            batch.schema().field(column).metadata().clone(),
        ));
        let original = RecordBatch::try_new(schema, input.columns().to_vec())
            .expect("OVER frame input schema");
        Some(
            Self::owned(
                &original,
                index,
                column_i64(batch, CLOCK_COLUMN).value(index),
            )
            .expect("owned OVER frame input"),
        )
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut bytes = self.clock.to_le_bytes().to_vec();
        bytes.extend(write_ipc(&self.row));
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, DataFusionError> {
        if bytes.len() <= 8 {
            return Err(DataFusionError::Execution(
                "truncated OVER frame input".into(),
            ));
        }
        let clock = i64::from_le_bytes(bytes[..8].try_into().unwrap());
        let mut reader = arrow::ipc::reader::StreamReader::try_new(&bytes[8..], None)?;
        let row = reader
            .next()
            .transpose()?
            .ok_or_else(|| DataFusionError::Execution("empty OVER frame input".into()))?;
        if row.num_rows() != 1 || reader.next().is_some() {
            return Err(DataFusionError::Execution(
                "OVER frame input must contain one row".into(),
            ));
        }
        Ok(Self { row, clock })
    }

    pub(crate) fn bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.row.get_array_memory_size()
    }

    fn export(&self, rt: i64, position: usize) -> Result<RecordBatch, DataFusionError> {
        let mut fields = self.row.schema().fields().to_vec();
        let mut columns = self.row.columns().to_vec();
        for (base, value) in [
            ("__over_frame_time", rt),
            ("__over_frame_clock", self.clock),
            ("__over_frame_position", position as i64),
        ] {
            let mut name = base.to_string();
            while fields.iter().any(|field| field.name() == &name) {
                name.push('_');
            }
            fields.push(Arc::new(Field::new(name, DataType::Int64, false)));
            columns.push(Arc::new(Int64Array::from(vec![value])));
        }
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(
                fields,
                self.row.schema().metadata().clone(),
            )),
            columns,
        )?)
    }
}

impl OverWindowAggregator {
    pub(crate) fn bind_canonical_frame_inputs(&mut self) -> Result<(), DataFusionError> {
        let OverInner::Bounded(inner) = &mut self.inner else {
            return Err(DataFusionError::Execution(
                "frame input retention requires a bounded OVER".into(),
            ));
        };
        if self.proctime && !inner.rows_frame {
            return Err(DataFusionError::Execution(
                "processing-time RANGE OVER is not admitted".into(),
            ));
        }
        if inner
            .keys
            .values()
            .flatten()
            .any(|row| row.input().is_none())
        {
            return Err(DataFusionError::Execution(
                "legacy OVER frames have no original input payload".into(),
            ));
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &mut self.store {
            if !store.frame_inputs_enabled()? && !store.scan_frames()?.is_empty() {
                return Err(DataFusionError::Execution(
                    "legacy OVER frames have no original input payload".into(),
                ));
            }
            store.enable_frame_inputs()?;
        }
        inner.capture_inputs = true;
        Ok(())
    }

    pub(super) fn project_frame_input(
        &self,
        source: &RecordBatch,
        fields: &mut Vec<Field>,
        columns: &mut Vec<ArrayRef>,
        rt: &ArrayRef,
    ) {
        if !matches!(&self.inner, OverInner::Bounded(inner) if inner.capture_inputs) {
            return;
        }
        let input = StructArray::from(source.clone());
        fields.push(
            Field::new(INPUT_COLUMN, input.data_type().clone(), false)
                .with_metadata(source.schema().metadata().clone()),
        );
        columns.push(Arc::new(input));
        fields.push(Field::new(CLOCK_COLUMN, DataType::Int64, false));
        columns.push(if self.proctime {
            Arc::new(Int64Array::from(vec![self.frame_clock; source.num_rows()]))
        } else {
            rt.clone()
        });
    }

    pub(crate) fn visit_canonical_frame_inputs(
        &self,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.ensure_canonical_ready()?;
        let OverInner::Bounded(inner) = &self.inner else {
            return Err(DataFusionError::Execution(
                "not a bounded OVER frame".into(),
            ));
        };
        if !inner.capture_inputs {
            return Err(DataFusionError::Execution(
                "OVER original frame inputs are not bound".into(),
            ));
        }
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.store {
            return store
                .visit_frame_inputs(|input, rt, position| accept(input.export(rt, position)?));
        }
        for rows in inner.keys.values() {
            for (position, row) in rows.iter().enumerate() {
                let input = row.input().ok_or_else(|| {
                    DataFusionError::Execution("missing original OVER frame input".into())
                })?;
                accept(input.export(row.rt, position)?)?;
            }
        }
        Ok(())
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_bindOverCanonicalFrameInputs<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |env| {
        if let Err(error) =
            unsafe { &mut *(handle as *mut OverWindowAggregator) }.bind_canonical_frame_inputs()
        {
            let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
        }
    });
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_exportOverCanonicalFrameInputs<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    receiver: JObject<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        if let Err(error) = unsafe { &*(handle as *const OverWindowAggregator) }
            .visit_canonical_frame_inputs(|batch| {
                crate::bridge::emit_record_batch(&mut env, &receiver, batch)
            })
        {
            let _ = env.throw_new("java/lang/IllegalStateException", error.to_string());
        }
    });
}

#[cfg(test)]
#[path = "frame_inputs_tests.rs"]
mod tests;
