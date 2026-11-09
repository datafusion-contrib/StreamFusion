use crate::state::canonical_context::check_cancelled;
use crate::*;

const CHUNK_ROWS: usize = 1024;
const CHUNK_BYTES: usize = 4 * 1024 * 1024;

pub(crate) struct KeyedLongWriter<F> {
    types: Vec<DataType>,
    converter: RowConverter,
    schema: SchemaRef,
    keys: Vec<Box<[u8]>>,
    kinds: Vec<i8>,
    times: Vec<i64>,
    bytes: usize,
    work: Option<MemoryReservation>,
    accept: F,
}

impl<F: FnMut(RecordBatch) -> Result<(), DataFusionError>> KeyedLongWriter<F> {
    pub(crate) fn new(
        types: Vec<DataType>,
        names: [&str; 2],
        work: Option<MemoryReservation>,
        accept: F,
    ) -> Self {
        let mut fields = key_fields(&types);
        fields.push(Field::new(names[0], DataType::Int8, false));
        fields.push(Field::new(names[1], DataType::Int64, false));
        Self {
            converter: key_row_converter_from_types(&types),
            schema: Arc::new(Schema::new(fields)),
            types,
            keys: Vec::new(),
            kinds: Vec::new(),
            times: Vec::new(),
            bytes: 0,
            work,
            accept,
        }
    }

    pub(crate) fn push(&mut self, key: &[u8], kind: i8, time: i64) -> Result<(), DataFusionError> {
        check_cancelled()?;
        let size = key.len() + 32 + self.types.len() * 16;
        if !self.keys.is_empty()
            && (self.keys.len() == CHUNK_ROWS || self.bytes + size > CHUNK_BYTES)
        {
            self.flush()?;
        }
        if let Some(work) = &mut self.work {
            work.try_resize((self.bytes + size).saturating_mul(4))?;
        }
        self.keys.push(key.into());
        self.kinds.push(kind);
        self.times.push(time);
        self.bytes += size;
        Ok(())
    }

    pub(crate) fn flush(&mut self) -> Result<(), DataFusionError> {
        if self.keys.is_empty() {
            return Ok(());
        }
        check_cancelled()?;
        let keys = std::mem::take(&mut self.keys);
        let references = keys.iter().map(|key| key.as_ref()).collect::<Vec<_>>();
        let mut columns = decode_byte_keys(Some(&self.converter), &references, &self.types);
        columns.push(Arc::new(arrow::array::Int8Array::from(std::mem::take(
            &mut self.kinds,
        ))));
        columns.push(Arc::new(Int64Array::from(std::mem::take(&mut self.times))));
        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        if let Some(work) = &mut self.work {
            work.try_resize(self.bytes + batch.get_array_memory_size())?;
        }
        (self.accept)(batch)?;
        drop(keys);
        self.bytes = 0;
        if let Some(work) = &mut self.work {
            work.resize(0);
        }
        Ok(())
    }
}
