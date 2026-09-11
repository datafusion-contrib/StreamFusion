use crate::*;

/// Composes a keyed table's two decodes the way Flink's key/value merge does. The input batch is
/// two binary columns `[key, body]`. Each record's VALUE decodes alone (its own skip semantics —
/// a JSON body may fan a top-level array into N rows or drop under `ignore-parse-errors`), and
/// the record's key bytes are gathered once per produced row, so every output row carries its
/// record's key — Flink's per-record cartesian with the raw key format's exactly-one key row. The
/// keys then decode through the parity-pinned `RawDecoder` (a null Kafka key stays a row with a
/// NULL key column — raw's special null-key rule) and scatter into the physical schema, value
/// columns written after the key column so an `ALL` projection's value fields win the overlap,
/// exactly `OutputProjectionCollector.emitRow`'s field order.
pub struct KeyedDecoder {
    pub value: Box<MessageDecoder>,
    pub key: RawDecoder,
    pub key_position: usize,
    pub value_positions: Vec<usize>,
    pub output: SchemaRef,
}

impl KeyedDecoder {
    pub fn decode(&self, records: &RecordBatch) -> RecordBatch {
        let keys = records.column(0);
        let bodies = records
            .project(&[1])
            .expect("keyed decode expects a two-column [key, body] batch");
        let mut kept = Vec::new();
        let mut sources = Vec::with_capacity(records.num_rows());
        for row in 0..records.num_rows() {
            let decoded = self.value.decode(&bodies.slice(row, 1));
            for _ in 0..decoded.num_rows() {
                sources.push(row as i32);
            }
            if decoded.num_rows() > 0 {
                kept.push(decoded);
            }
        }
        let value_schema: SchemaRef = Arc::new(Schema::new(
            self.value_positions
                .iter()
                .map(|position| self.output.field(*position).clone())
                .collect::<Vec<_>>(),
        ));
        let values = match kept.len() {
            0 => RecordBatch::new_empty(value_schema),
            1 => kept.into_iter().next().unwrap(),
            _ => concat_batches(&value_schema, &kept).expect("keyed value concat failed"),
        };
        let indices = Int32Array::from(sources);
        let gathered = take(keys, &indices, None).expect("failed to gather Kafka keys");
        let key_input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "key",
                gathered.data_type().clone(),
                true,
            )])),
            vec![gathered],
        )
        .expect("failed to build the key decode batch");
        let key_column = self.key.decode(&key_input).column(0).clone();
        let mut columns: Vec<Option<ArrayRef>> = vec![None; self.output.fields().len()];
        columns[self.key_position] = Some(key_column);
        for (index, position) in self.value_positions.iter().enumerate() {
            columns[*position] = Some(values.column(index).clone());
        }
        let columns = columns
            .into_iter()
            .map(|column| column.expect("keyed decode left a physical column unfilled"))
            .collect();
        RecordBatch::try_new(self.output.clone(), columns)
            .expect("failed to compose the keyed decode batch")
    }
}
