use crate::*;

/// Downcasts a projected OVER value column (`value{a}`) to its typed per-row reader.
pub(crate) fn over_value_column<'a>(column: &'a ArrayRef, value_type: &DataType) -> ValueColumn<'a> {
    if matches!(column.data_type(), DataType::Null) {
        return ValueColumn::NullOnly(column);
    }
    match value_type {
        DataType::Int64 => ValueColumn::I64(column.as_any().downcast_ref().expect("int64 value")),
        DataType::Int32 => ValueColumn::I32(column.as_any().downcast_ref().expect("int32 value")),
        DataType::Int16 => ValueColumn::I16(column.as_any().downcast_ref().expect("int16 value")),
        DataType::Int8 => ValueColumn::I8(column.as_any().downcast_ref().expect("int8 value")),
        DataType::Float64 => ValueColumn::F64(column.as_any().downcast_ref().expect("float64 value")),
        DataType::Float32 => ValueColumn::F32(column.as_any().downcast_ref().expect("float32 value")),
        other => panic!("unsupported OVER value type: {other:?}"),
    }
}

pub(crate) struct OverAggState {
    agg: RunningAgg,
    distinct: Option<DistinctSet>,
    value_type: DataType,
}

impl OverAggState {
    fn new(kind: i64, value_type: &DataType) -> Self {
        let distinct = kind >= 100;
        Self {
            agg: RunningAgg::new(if distinct { kind - 100 } else { kind }, value_type),
            distinct: distinct.then(|| DistinctSet::new(value_type)),
            value_type: value_type.clone(),
        }
    }

    fn fold(&mut self, value: Num) {
        if let Some(seen) = &mut self.distinct {
            if !seen.add_scalar(num_to_scalar(&self.value_type, Some(value))) {
                return;
            }
        }
        self.agg.fold(value);
    }

    fn emit(&self) -> ScalarValue {
        self.agg.emit()
    }

    fn result_type(&self) -> DataType {
        self.agg.result_type()
    }

    fn restore_value(&mut self, scalar: &ScalarValue) {
        self.agg.restore_value(scalar);
    }

    fn distinct_values(&self) -> Vec<ScalarValue> {
        self.distinct
            .as_ref()
            .map(|seen| seen.scalar_entries().into_iter().map(|(value, _)| value).collect())
            .unwrap_or_default()
    }
}

pub(crate) struct OverAggregator {
    kinds: Vec<i64>,
    /// One value type per aggregate (aggregates may read different value columns of different types).
    value_types: Vec<DataType>,
    // Keyed by arrow-row bytes, probed borrowed (see the group aggregate): a row whose partition
    // already exists — the steady state — allocates nothing; the key is copied on first touch only.
    keys: HashMap<ByteKey, Vec<OverAggState>>,
    key_converter: Option<RowConverter>,
    key_types: Vec<DataType>,
    // Managed-memory accounting (driven by the owning OVER operator): per-key state is fixed-size,
    // so the tracked bytes move only when a key is created.
    track: bool,
    bytes: usize,
}

impl OverAggregator {
    pub(crate) fn new(value_types: Vec<i64>, kinds: Vec<i64>) -> Self {
        OverAggregator {
            value_types: value_types.iter().map(|&code| value_data_type(code)).collect(),
            kinds,
            keys: HashMap::default(),
            key_converter: None,
            key_types: Vec::new(),
            track: false,
            bytes: 0,
        }
    }

    /// One key's fixed state footprint (the running aggregates plus the map entry).
    fn key_state_bytes(&self, key: &[u8]) -> usize {
        byte_key_bytes(key) + self.kinds.len() * std::mem::size_of::<OverAggState>()
    }

    fn recompute_bytes(&mut self) {
        self.bytes = self.keys.keys().map(|key| self.key_state_bytes(&key.0)).sum();
    }

    /// The running aggregate state for a key, created (copying the key bytes) on first touch.
    fn states(&mut self, key: &[u8]) -> &mut Vec<OverAggState> {
        if !self.keys.contains_key(key) {
            let fresh: Vec<OverAggState> = self
                .kinds
                .iter()
                .zip(&self.value_types)
                .map(|(&kind, vt)| OverAggState::new(kind, vt))
                .collect();
            self.keys.insert(ByteKey::from(key), fresh);
        }
        self.keys.get_mut(key).expect("state just ensured")
    }

    /// Folds the batch (`rt` i64, `value0..`, optional `key0..`) into the per-key running state in
    /// rowtime order and returns `[result0..resultN-1]` per input row, in input order. Each aggregate
    /// reads its own `value{a}` column.
    pub(crate) fn update(&mut self, batch: &RecordBatch) -> RecordBatch {
        let rt = column_i64(batch, "rt");
        let num_agg = self.kinds.len();
        let value_columns: Vec<ValueColumn> = (0..num_agg)
            .map(|a| {
                let column = batch.column_by_name(&format!("value{a}")).expect("missing value column");
                over_value_column(column, &self.value_types[a])
            })
            .collect();
        let key_arrays = key_arrays(batch);
        self.key_types = key_types(&key_arrays);
        let n = batch.num_rows();
        let keys_encoded = encode_keys(&mut self.key_converter, &key_arrays, n);

        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by_key(|&row| rt.value(row));
        let mut results: Vec<Vec<ScalarValue>> = vec![vec![ScalarValue::Null; n]; num_agg];
        let mut start = 0;
        while start < n {
            let mut end = start;
            while end < n && rt.value(order[end]) == rt.value(order[start]) {
                end += 1;
            }
            // Fold every row of this rt group into its key before reading any (RANGE: tied rows of a
            // key share the post-fold value); a null value is skipped, but the key's state is touched
            // so the row still emits the running value.
            for &row in &order[start..end] {
                let key = keys_encoded.row(row).data();
                let keys_before = self.keys.len();
                let states = self.states(key);
                for (a, state) in states.iter_mut().enumerate() {
                    if let Some(num) = value_columns[a].at(row) {
                        state.fold(num);
                    }
                }
                if self.track && self.keys.len() > keys_before {
                    self.bytes += self.key_state_bytes(key);
                }
            }
            for &row in &order[start..end] {
                let states =
                    self.keys.get(keys_encoded.row(row).data()).expect("key present");
                for (a, state) in states.iter().enumerate() {
                    results[a][row] = state.emit();
                }
            }
            start = end;
        }

        let mut fields = Vec::with_capacity(num_agg);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_agg);
        for a in 0..num_agg {
            let result_type = OverAggState::new(self.kinds[a], &self.value_types[a]).result_type();
            fields.push(Field::new(format!("result{a}"), result_type.clone(), true));
            columns.push(scalars_to_array(std::mem::take(&mut results[a]), &result_type));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build over result batch")
    }

    /// Serializes the per-key running state (`[key0.., state0..]`, one row per key, one scalar per
    /// aggregate — the running value is itself the checkpointed state), the optional retention
    /// stamp riding as a trailing per-key column.
    fn snapshot(&mut self, retention: RetentionStamps) -> Vec<u8> {
        let result_types: Vec<DataType> = self
            .kinds
            .iter()
            .zip(&self.value_types)
            .map(|(&k, vt)| OverAggState::new(k, vt).result_type())
            .collect();
        let mut keys: Vec<&[u8]> = Vec::new();
        let mut state_columns: Vec<Vec<ScalarValue>> = vec![Vec::new(); self.kinds.len()];
        let mut distinct_columns: Vec<Option<(Vec<i32>, Vec<ScalarValue>)>> = self
            .kinds
            .iter()
            .map(|kind| (kind >= &100).then(|| (vec![0], Vec::new())))
            .collect();
        let mut stamps: Vec<i64> = Vec::new();
        for (key, states) in self.keys.iter() {
            keys.push(&key.0);
            for (i, state) in states.iter().enumerate() {
                state_columns[i].push(state.emit());
                if let Some((offsets, values)) = &mut distinct_columns[i] {
                    values.extend(state.distinct_values());
                    offsets.push(values.len() as i32);
                }
            }
            if let Some((_, per_key)) = retention {
                stamps.push(per_key.get(key).copied().expect("over retention stamp"));
            }
        }
        let mut fields = key_fields(&self.key_types);
        let mut columns = decode_byte_keys(self.key_converter.as_ref(), &keys, &self.key_types);
        for (index, result_type) in result_types.iter().enumerate() {
            fields.push(Field::new(format!("state{index}"), result_type.clone(), true));
        }
        for (index, scalars) in state_columns.into_iter().enumerate() {
            columns.push(scalars_to_array(scalars, &result_types[index]));
        }
        for (index, distinct) in distinct_columns.into_iter().enumerate() {
            if let Some((offsets, values)) = distinct {
                let item = Arc::new(Field::new("item", self.value_types[index].clone(), true));
                fields.push(Field::new(
                    format!("distinct{index}"),
                    DataType::List(item.clone()),
                    false,
                ));
                columns.push(Arc::new(ListArray::new(
                    item,
                    OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    scalars_to_array(values, &self.value_types[index]),
                    None,
                )));
            }
        }
        if let Some((name, _)) = retention {
            fields.push(Field::new(name, DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(stamps)));
        }
        write_ipc(&RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build over snapshot batch"))
    }

    /// Backend-mode firing: seeds each listed key's running state from persisted scalars (the
    /// same emit()/restore_value round trip the raw snapshot uses), folds the batch, then
    /// exports the updated scalars per seed and drops the in-memory map — in backend mode the
    /// store's write buffer owns the state between firings.
    #[cfg(feature = "paimon-state")]
    fn update_hydrated(
        &mut self,
        batch: &RecordBatch,
        seeds: &[(usize, Option<Vec<ScalarValue>>)],
    ) -> (RecordBatch, Vec<Vec<ScalarValue>>) {
        {
            let key_arrays = key_arrays(batch);
            self.key_types = key_types(&key_arrays);
            let keys_encoded = encode_keys(&mut self.key_converter, &key_arrays, batch.num_rows());
            for (row, scalars) in seeds {
                if let Some(scalars) = scalars {
                    for (i, state) in
                        self.states(keys_encoded.row(*row).data()).iter_mut().enumerate()
                    {
                        state.restore_value(&scalars[i]);
                    }
                }
            }
        }
        let out = self.update(batch);
        let key_arrays = key_arrays(batch);
        let keys_encoded = encode_keys(&mut self.key_converter, &key_arrays, batch.num_rows());
        let published = seeds
            .iter()
            .map(|(row, _)| {
                self.keys
                    .get(keys_encoded.row(*row).data())
                    .expect("fired key folded")
                    .iter()
                    .map(|state| state.emit())
                    .collect()
            })
            .collect();
        self.keys.clear();
        (out, published)
    }

    fn restore(
        value_types: Vec<i64>,
        kinds: Vec<i64>,
        bytes: &[u8],
        stamps: &mut HashMap<ByteKey, i64>,
    ) -> Self {
        let mut aggregator = OverAggregator::new(value_types, kinds);
        let num_agg = aggregator.kinds.len();
        let distinct_count = aggregator.kinds.iter().filter(|&&kind| kind >= 100).count();
        for batch in read_ipc(bytes) {
            let retention = retention_stamps(&batch);
            let arity =
                batch.num_columns() - num_agg - distinct_count - retention.is_some() as usize;
            let key_arrays: Vec<&ArrayRef> = (0..arity).map(|j| batch.column(j)).collect();
            aggregator.key_types = key_types(&key_arrays);
            let keys_encoded =
                encode_keys(&mut aggregator.key_converter, &key_arrays, batch.num_rows());
            for row in 0..batch.num_rows() {
                let key = keys_encoded.row(row).data();
                for (i, state) in aggregator.states(key).iter_mut().enumerate() {
                    let scalar = ScalarValue::try_from_array(batch.column(arity + i), row)
                        .expect("over state scalar");
                    state.restore_value(&scalar);
                }
                let mut distinct_column = arity + num_agg;
                for i in 0..num_agg {
                    if aggregator.kinds[i] < 100 {
                        continue;
                    }
                    let list = batch
                        .column(distinct_column)
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .expect("over distinct list");
                    let values = list.value(row);
                    let state = &mut aggregator.states(key)[i];
                    let seen = state.distinct.as_mut().expect("distinct state");
                    for value in 0..values.len() {
                        seen.add_scalar(
                            ScalarValue::try_from_array(&values, value)
                                .expect("over distinct scalar"),
                        );
                    }
                    distinct_column += 1;
                }
                if let Some(column) = retention {
                    stamps.insert(ByteKey::from(key), column.value(row));
                }
            }
        }
        aggregator
    }
}

/// One buffered row of a bounded-frame OVER partition: its rowtime and the OVER value per aggregate
/// (None = null, which the aggregates skip). Held until it can no longer fall inside a future frame.
#[derive(Clone)]
pub(crate) struct BufferedRow {
    rt: i64,
    values: Vec<Option<Num>>,
}

/// Bounded-frame event-time OVER (`ROWS BETWEEN n PRECEDING AND CURRENT ROW`, or `RANGE BETWEEN
/// INTERVAL x PRECEDING AND CURRENT ROW`). Unlike the unbounded {@link OverAggregator}, which folds a
/// single persistent accumulator per key, a bounded frame drops rows off its trailing edge — so the
/// running value cannot be maintained incrementally for MIN/MAX (they would need a retractable
/// multiset). Instead this keeps a per-key sorted buffer of the rows still reachable by some future
/// frame and **recomputes** each emitted row's aggregate over its frame slice with a fresh
/// {@link RunningAgg}. The result is byte-identical to Flink's `*BoundedPrecedingFunction` (both
/// aggregate over the same frame) and sidesteps MIN/MAX retraction entirely. See divergences/11.
pub(crate) struct BoundedOverAggregator {
    kinds: Vec<i64>,
    /// One value type per aggregate (aggregates may read different value columns of different types).
    value_types: Vec<DataType>,
    /// true = ROWS (count of rows), false = RANGE (rowtime interval).
    rows_frame: bool,
    /// n preceding rows (ROWS) or the preceding interval in millis (RANGE).
    offset: i64,
    /// Per key (arrow-row bytes, probed borrowed), the buffered rows sorted ascending by rowtime
    /// (stable for ties).
    keys: HashMap<ByteKey, Vec<BufferedRow>>,
    key_converter: Option<RowConverter>,
    key_types: Vec<DataType>,
    // Managed-memory accounting: buffered rows are fixed-size, tracked on append and eviction.
    track: bool,
    bytes: usize,
}

impl BoundedOverAggregator {
    fn new(value_types: Vec<i64>, kinds: Vec<i64>, rows_frame: bool, offset: i64) -> Self {
        BoundedOverAggregator {
            value_types: value_types.iter().map(|&code| value_data_type(code)).collect(),
            kinds,
            rows_frame,
            offset,
            keys: HashMap::default(),
            key_converter: None,
            key_types: Vec::new(),
            track: false,
            bytes: 0,
        }
    }

    /// One buffered row's fixed footprint (its rowtime and per-aggregate values).
    fn row_bytes(&self) -> usize {
        std::mem::size_of::<BufferedRow>()
            + self.kinds.len() * std::mem::size_of::<Option<Num>>()
    }

    fn recompute_bytes(&mut self) {
        let row = self.row_bytes();
        self.bytes = self
            .keys
            .iter()
            .map(|(key, buffer)| byte_key_bytes(&key.0) + buffer.len() * row)
            .sum();
    }

    /// Folds the batch (`rt` i64, `value0..`, optional `key0..`) into the per-key buffer and returns
    /// `[result0..]` per input row, each computed by recomputing the aggregate over that row's frame.
    /// Each aggregate reads its own `value{a}` column.
    fn update(&mut self, batch: &RecordBatch) -> RecordBatch {
        let rt = column_i64(batch, "rt");
        let num_agg = self.kinds.len();
        let value_columns: Vec<ValueColumn> = (0..num_agg)
            .map(|a| {
                let column = batch.column_by_name(&format!("value{a}")).expect("missing value column");
                over_value_column(column, &self.value_types[a])
            })
            .collect();
        let key_arrays = key_arrays(batch);
        self.key_types = key_types(&key_arrays);
        let n = batch.num_rows();
        let keys_encoded = encode_keys(&mut self.key_converter, &key_arrays, n);

        // Append the new rows to their per-key buffers in rowtime order (stable for ties). Every new
        // row's rowtime is past the prior watermark, hence at or after all already-buffered rows, so
        // appending in this order keeps each buffer sorted. Record where each input row landed.
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by_key(|&row| rt.value(row));
        let mut buffer_index = vec![0usize; n];
        let mut max_rt = i64::MIN;
        for &row in &order {
            let row_rt = rt.value(row);
            max_rt = max_rt.max(row_rt);
            let values: Vec<Option<Num>> = value_columns.iter().map(|c| c.at(row)).collect();
            let key = keys_encoded.row(row).data();
            let (row_bytes, track) = (self.row_bytes(), self.track);
            let buffer = match self.keys.get_mut(key) {
                Some(buffer) => buffer,
                None => {
                    if track {
                        self.bytes += byte_key_bytes(key);
                    }
                    self.keys.entry(ByteKey::from(key)).or_default()
                }
            };
            buffer.push(BufferedRow { rt: row_rt, values });
            buffer_index[row] = buffer.len() - 1;
            if track {
                self.bytes += row_bytes;
            }
        }

        let mut results: Vec<Vec<ScalarValue>> = vec![vec![ScalarValue::Null; n]; num_agg];
        for row in 0..n {
            let buffer = &self.keys[keys_encoded.row(row).data()];
            let i = buffer_index[row];
            let cur_rt = buffer[i].rt;
            // ROWS counts physical rows up to and including this one; RANGE covers all rows within the
            // rowtime interval and shares one frame across rows of equal rowtime (ending at the last).
            let (lower, upper) = if self.rows_frame {
                (i.saturating_sub(self.offset as usize), i)
            } else {
                let lo = buffer.partition_point(|r| r.rt < cur_rt - self.offset);
                let hi = buffer.partition_point(|r| r.rt <= cur_rt) - 1;
                (lo, hi)
            };
            let mut aggs: Vec<OverAggState> = self
                .kinds
                .iter()
                .zip(&self.value_types)
                .map(|(&k, vt)| OverAggState::new(k, vt))
                .collect();
            for r in &buffer[lower..=upper] {
                for (a, agg) in aggs.iter_mut().enumerate() {
                    if let Some(v) = r.values[a] {
                        agg.fold(v);
                    }
                }
            }
            for (a, agg) in aggs.iter().enumerate() {
                results[a][row] = agg.emit();
            }
        }

        self.evict(max_rt);

        let mut fields = Vec::with_capacity(num_agg);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_agg);
        for a in 0..num_agg {
            let result_type = OverAggState::new(self.kinds[a], &self.value_types[a]).result_type();
            fields.push(Field::new(format!("result{a}"), result_type.clone(), true));
            columns.push(scalars_to_array(std::mem::take(&mut results[a]), &result_type));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build bounded over result batch")
    }

    /// Drops buffered rows that can no longer fall inside any future row's frame. A future row's
    /// rowtime exceeds `max_rt` (it has not completed yet), so for RANGE keep rows whose rowtime is at
    /// or after `max_rt - offset`; for ROWS keep the most recent `offset` rows (the deepest any future
    /// frame can reach back). Empty partitions are removed to bound memory.
    fn evict(&mut self, max_rt: i64) {
        let (rows_frame, offset) = (self.rows_frame, self.offset);
        let (track, row_bytes) = (self.track, self.row_bytes());
        let mut freed = 0usize;
        self.keys.retain(|key, buffer| {
            let dropped = if rows_frame {
                let keep = offset as usize;
                let dropped = buffer.len().saturating_sub(keep);
                if dropped > 0 {
                    buffer.drain(0..dropped);
                }
                dropped
            } else {
                let bound = max_rt - offset;
                let cut = buffer.partition_point(|r| r.rt < bound);
                if cut > 0 {
                    buffer.drain(0..cut);
                }
                cut
            };
            if track {
                freed += dropped * row_bytes;
                if buffer.is_empty() {
                    freed += byte_key_bytes(&key.0);
                }
            }
            !buffer.is_empty()
        });
        self.bytes = self.bytes.saturating_sub(freed);
    }

    /// Serializes the per-key buffer (`[key0.., rt, value0..]`, one row per buffered row, one value
    /// column per aggregate), the optional retention stamp repeated per row of its key.
    fn snapshot(&mut self, retention: RetentionStamps) -> Vec<u8> {
        let num_agg = self.kinds.len();
        let mut keys: Vec<&[u8]> = Vec::new();
        let mut rts: Vec<ScalarValue> = Vec::new();
        let mut value_columns: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_agg];
        let mut stamps: Vec<i64> = Vec::new();
        for (key, buffer) in self.keys.iter() {
            for row in buffer {
                keys.push(&key.0);
                rts.push(ScalarValue::Int64(Some(row.rt)));
                for a in 0..num_agg {
                    value_columns[a].push(num_to_scalar(&self.value_types[a], row.values[a]));
                }
                if let Some((_, per_key)) = retention {
                    stamps.push(per_key.get(key).copied().expect("bounded over retention stamp"));
                }
            }
        }
        let mut fields = key_fields(&self.key_types);
        let mut columns = decode_byte_keys(self.key_converter.as_ref(), &keys, &self.key_types);
        fields.push(Field::new("rt", DataType::Int64, false));
        columns.push(scalars_to_array(rts, &DataType::Int64));
        for (a, scalars) in value_columns.into_iter().enumerate() {
            fields.push(Field::new(format!("value{a}"), self.value_types[a].clone(), true));
            columns.push(scalars_to_array(scalars, &self.value_types[a]));
        }
        if let Some((name, _)) = retention {
            fields.push(Field::new(name, DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(stamps)));
        }
        write_ipc(&RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build bounded over snapshot batch"))
    }

    fn restore(
        value_types: Vec<i64>,
        kinds: Vec<i64>,
        rows_frame: bool,
        offset: i64,
        bytes: &[u8],
        stamps: &mut HashMap<ByteKey, i64>,
    ) -> Self {
        let mut aggregator = BoundedOverAggregator::new(value_types, kinds, rows_frame, offset);
        let num_agg = aggregator.kinds.len();
        for batch in read_ipc(bytes) {
            let retention = retention_stamps(&batch);
            // Trailing rt + one value column per agg (+ the optional retention stamp).
            let arity = batch.num_columns() - 1 - num_agg - retention.is_some() as usize;
            let key_arrays: Vec<&ArrayRef> = (0..arity).map(|j| batch.column(j)).collect();
            aggregator.key_types = key_types(&key_arrays);
            let keys_encoded =
                encode_keys(&mut aggregator.key_converter, &key_arrays, batch.num_rows());
            let rt = column_i64(&batch, "rt");
            for row in 0..batch.num_rows() {
                let key = ByteKey::from(keys_encoded.row(row).data());
                let values: Vec<Option<Num>> = (0..num_agg)
                    .map(|a| {
                        let column = batch.column_by_name(&format!("value{a}")).expect("value column");
                        num_from_scalar(&ScalarValue::try_from_array(column, row).expect("over value"))
                    })
                    .collect();
                if let Some(column) = retention {
                    stamps.insert(key.clone(), column.value(row));
                }
                aggregator.keys.entry(key).or_default().push(BufferedRow { rt: rt.value(row), values });
            }
        }
        aggregator
    }
}

/// Window-function code: a SQL `OVER` analytic function that is *not* a mergeable aggregate.
pub(crate) fn is_window_function_kind(kind: i64) -> bool {
    (10..=12).contains(&kind)
}

/// Per-key running state for one OVER window function. Unlike the aggregate path these are not
/// DataFusion accumulators (DataFusion's window evaluators expose no serializable state); we own the
/// small running state so it checkpoints, computing it incrementally in rowtime order like Flink's
/// own `OverAggregate` (see divergences/11).
pub(crate) enum WindowFnState {
    /// `ROW_NUMBER()` over `ROWS UNBOUNDED PRECEDING` — a per-partition counter (1-based).
    RowNumber(i64),
    /// `RANK()` — `count` rows seen, `rank` of the current order-value group, `last` order value.
    /// Tied order values share a rank; the next value's rank jumps to its row position (gaps).
    Rank { count: i64, rank: i64, last: Option<i64> },
    /// `DENSE_RANK()` — increments only when the order value changes, so ranks are gap-free.
    DenseRank { dense: i64, last: Option<i64> },
}

impl WindowFnState {
    fn new(kind: i64) -> Self {
        match kind {
            10 => WindowFnState::RowNumber(0),
            11 => WindowFnState::Rank { count: 0, rank: 0, last: None },
            12 => WindowFnState::DenseRank { dense: 0, last: None },
            other => panic!("unsupported window function kind: {other}"),
        }
    }

    /// Advances the state for the current row (whose ORDER BY value is `rt`) and returns its value.
    /// Rows are fed in ascending order, so tied order values arrive consecutively.
    fn next(&mut self, rt: i64) -> ScalarValue {
        match self {
            WindowFnState::RowNumber(n) => {
                *n += 1;
                ScalarValue::Int64(Some(*n))
            }
            WindowFnState::Rank { count, rank, last } => {
                *count += 1;
                if *last != Some(rt) {
                    *rank = *count;
                    *last = Some(rt);
                }
                ScalarValue::Int64(Some(*rank))
            }
            WindowFnState::DenseRank { dense, last } => {
                if *last != Some(rt) {
                    *dense += 1;
                    *last = Some(rt);
                }
                ScalarValue::Int64(Some(*dense))
            }
        }
    }

    fn result_type(&self) -> DataType {
        DataType::Int64
    }

    /// The checkpointable running state, as scalars (one or more per function).
    fn state(&self) -> Vec<ScalarValue> {
        let i = |v: i64| ScalarValue::Int64(Some(v));
        match self {
            WindowFnState::RowNumber(n) => vec![i(*n)],
            WindowFnState::Rank { count, rank, last } => {
                vec![i(*count), i(*rank), ScalarValue::Int64(*last)]
            }
            WindowFnState::DenseRank { dense, last } => vec![i(*dense), ScalarValue::Int64(*last)],
        }
    }

    fn state_types(&self) -> Vec<DataType> {
        match self {
            WindowFnState::RowNumber(_) => vec![DataType::Int64],
            WindowFnState::Rank { .. } => vec![DataType::Int64; 3],
            WindowFnState::DenseRank { .. } => vec![DataType::Int64; 2],
        }
    }

    fn restore_state(&mut self, state: &[ScalarValue]) {
        let int = |scalar: &ScalarValue| match scalar {
            ScalarValue::Int64(value) => *value,
            _ => None,
        };
        match self {
            WindowFnState::RowNumber(n) => *n = int(&state[0]).unwrap_or(0),
            WindowFnState::Rank { count, rank, last } => {
                *count = int(&state[0]).unwrap_or(0);
                *rank = int(&state[1]).unwrap_or(0);
                *last = int(&state[2]);
            }
            WindowFnState::DenseRank { dense, last } => {
                *dense = int(&state[0]).unwrap_or(0);
                *last = int(&state[1]);
            }
        }
    }
}

/// OVER window functions (ROW_NUMBER today; RANK/DENSE_RANK/FIRST_VALUE/LAST_VALUE to follow),
/// computed incrementally per partition key in rowtime order. The sibling of {@link OverAggregator}
/// for the non-aggregate `OVER` functions: same `[rt, key0..]` sub-batch in, one result column per
/// function out, but driven by per-key {@link WindowFnState} rather than DataFusion accumulators.
pub(crate) struct WindowFunctionOver {
    kinds: Vec<i64>,
    // Keyed by arrow-row bytes, probed borrowed (see OverAggregator).
    keys: HashMap<ByteKey, Vec<WindowFnState>>,
    key_converter: Option<RowConverter>,
    key_types: Vec<DataType>,
    // Managed-memory accounting (see OverAggregator): fixed per-key state, tracked on key creation.
    track: bool,
    bytes: usize,
}

impl WindowFunctionOver {
    pub(crate) fn new(kinds: Vec<i64>) -> Self {
        WindowFunctionOver {
            kinds,
            keys: HashMap::default(),
            key_converter: None,
            key_types: Vec::new(),
            track: false,
            bytes: 0,
        }
    }

    /// One key's fixed state footprint (the window-function states plus the map entry).
    fn key_state_bytes(&self, key: &[u8]) -> usize {
        byte_key_bytes(key) + self.kinds.len() * std::mem::size_of::<WindowFnState>()
    }

    fn recompute_bytes(&mut self) {
        self.bytes = self.keys.keys().map(|key| self.key_state_bytes(&key.0)).sum();
    }

    fn states(&mut self, key: &[u8]) -> &mut Vec<WindowFnState> {
        if !self.keys.contains_key(key) {
            let fresh: Vec<WindowFnState> =
                self.kinds.iter().map(|&k| WindowFnState::new(k)).collect();
            self.keys.insert(ByteKey::from(key), fresh);
        }
        self.keys.get_mut(key).expect("state just ensured")
    }

    /// Advances each function per row in rowtime order and returns `[result0..]` in input order.
    pub(crate) fn update(&mut self, batch: &RecordBatch) -> RecordBatch {
        let rt = column_i64(batch, "rt");
        let key_arrays = key_arrays(batch);
        self.key_types = key_types(&key_arrays);
        let n = batch.num_rows();
        let num = self.kinds.len();
        let keys_encoded = encode_keys(&mut self.key_converter, &key_arrays, n);
        // Stable sort by rowtime: rows of equal rowtime keep input (arrival) order, matching Flink's
        // ROWS-frame tie order.
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by_key(|&row| rt.value(row));
        let mut results: Vec<Vec<ScalarValue>> = vec![vec![ScalarValue::Null; n]; num];
        for &row in &order {
            let key = keys_encoded.row(row).data();
            let keys_before = self.keys.len();
            for (i, state) in self.states(key).iter_mut().enumerate() {
                results[i][row] = state.next(rt.value(row));
            }
            if self.track && self.keys.len() > keys_before {
                self.bytes += self.key_state_bytes(key);
            }
        }
        let mut fields = Vec::with_capacity(num);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(num);
        for (i, &kind) in self.kinds.iter().enumerate() {
            let result_type = WindowFnState::new(kind).result_type();
            fields.push(Field::new(format!("result{i}"), result_type.clone(), true));
            columns.push(scalars_to_array(std::mem::take(&mut results[i]), &result_type));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build window-function result batch")
    }

    /// Serializes the per-key running state (`[key0.., state…]`, one row per key), the optional
    /// retention stamp riding as a trailing per-key column.
    fn snapshot(&mut self, retention: RetentionStamps) -> Vec<u8> {
        let state_types: Vec<DataType> =
            self.kinds.iter().flat_map(|&k| WindowFnState::new(k).state_types()).collect();
        let mut keys: Vec<&[u8]> = Vec::new();
        let mut state_columns: Vec<Vec<ScalarValue>> = vec![Vec::new(); state_types.len()];
        let mut stamps: Vec<i64> = Vec::new();
        for (key, states) in self.keys.iter() {
            keys.push(&key.0);
            let mut column = 0;
            for state in states {
                for scalar in state.state() {
                    state_columns[column].push(scalar);
                    column += 1;
                }
            }
            if let Some((_, per_key)) = retention {
                stamps.push(per_key.get(key).copied().expect("window-function retention stamp"));
            }
        }
        let mut fields = key_fields(&self.key_types);
        let mut columns = decode_byte_keys(self.key_converter.as_ref(), &keys, &self.key_types);
        for (index, state_type) in state_types.iter().enumerate() {
            fields.push(Field::new(format!("state{index}"), state_type.clone(), true));
        }
        for (index, scalars) in state_columns.into_iter().enumerate() {
            columns.push(scalars_to_array(scalars, &state_types[index]));
        }
        if let Some((name, _)) = retention {
            fields.push(Field::new(name, DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(stamps)));
        }
        write_ipc(&RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build window-function snapshot batch"))
    }

    /// Backend-mode firing — see {@link OverAggregator::update_hydrated}; window-function state
    /// round-trips through the same state()/restore_state scalars as the raw snapshot.
    #[cfg(feature = "paimon-state")]
    fn update_hydrated(
        &mut self,
        batch: &RecordBatch,
        seeds: &[(usize, Option<Vec<ScalarValue>>)],
    ) -> (RecordBatch, Vec<Vec<ScalarValue>>) {
        let state_counts: Vec<usize> =
            self.kinds.iter().map(|&k| WindowFnState::new(k).state_types().len()).collect();
        {
            let key_arrays = key_arrays(batch);
            self.key_types = key_types(&key_arrays);
            let keys_encoded = encode_keys(&mut self.key_converter, &key_arrays, batch.num_rows());
            for (row, scalars) in seeds {
                if let Some(scalars) = scalars {
                    let mut column = 0;
                    for (i, state) in
                        self.states(keys_encoded.row(*row).data()).iter_mut().enumerate()
                    {
                        let count = state_counts[i];
                        state.restore_state(&scalars[column..column + count]);
                        column += count;
                    }
                }
            }
        }
        let out = self.update(batch);
        let key_arrays = key_arrays(batch);
        let keys_encoded = encode_keys(&mut self.key_converter, &key_arrays, batch.num_rows());
        let published = seeds
            .iter()
            .map(|(row, _)| {
                self.keys
                    .get(keys_encoded.row(*row).data())
                    .expect("fired key folded")
                    .iter()
                    .flat_map(|state| state.state())
                    .collect()
            })
            .collect();
        self.keys.clear();
        (out, published)
    }

    fn restore(kinds: Vec<i64>, bytes: &[u8], stamps: &mut HashMap<ByteKey, i64>) -> Self {
        let mut over = WindowFunctionOver::new(kinds);
        let state_counts: Vec<usize> =
            over.kinds.iter().map(|&k| WindowFnState::new(k).state_types().len()).collect();
        let state_total: usize = state_counts.iter().sum();
        for batch in read_ipc(bytes) {
            let retention = retention_stamps(&batch);
            let arity = batch.num_columns() - state_total - retention.is_some() as usize;
            let key_arrays: Vec<&ArrayRef> = (0..arity).map(|j| batch.column(j)).collect();
            over.key_types = key_types(&key_arrays);
            let keys_encoded = encode_keys(&mut over.key_converter, &key_arrays, batch.num_rows());
            for row in 0..batch.num_rows() {
                let key = keys_encoded.row(row).data();
                let mut column = arity;
                for (i, state) in over.states(key).iter_mut().enumerate() {
                    let count = state_counts[i];
                    let scalars: Vec<ScalarValue> = (column..column + count)
                        .map(|c| ScalarValue::try_from_array(batch.column(c), row).expect("state scalar"))
                        .collect();
                    state.restore_state(&scalars);
                    column += count;
                }
                if let Some(stamp_column) = retention {
                    stamps.insert(ByteKey::from(key), stamp_column.value(row));
                }
            }
        }
        over
    }
}

/// The optional per-key retention column a snapshot carries — the column name
/// ([`CLEANUP_AT_COLUMN`] for the deadline schemes, [`TTL_TS_COLUMN`] for the proctime per-value
/// TTL) with the stamp per key. `None` while retention is off, keeping those snapshots
/// byte-identical to the pre-retention format.
type RetentionStamps<'a> = Option<(&'static str, &'a HashMap<ByteKey, i64>)>;

/// How idle-state retention bounds an OVER shape — Flink runs three schemes, by shape (see
/// {@link OverWindowAggregator::retention_scheme}).
#[derive(PartialEq)]
enum RetentionScheme {
    /// One per-key processing-time cleanup deadline at 1.5x the retention, clearing the key's
    /// whole state when it fires (rowtime shapes and the proctime bounded-ROWS frame).
    Deadline,
    /// Per-value `StateTtlConfig` on the running accumulator (the proctime unbounded fold).
    ValueTtl,
    /// No retention at all (the bounded-RANGE rowtime frame — Flink's function takes none).
    None,
}

/// The trailing retention column of a snapshot batch, if it carries one — read by name so a
/// snapshot restores across a retention flip in either direction (adopt or shed).
pub(crate) fn retention_stamps(batch: &RecordBatch) -> Option<&Int64Array> {
    [CLEANUP_AT_COLUMN, TTL_TS_COLUMN]
        .iter()
        .find_map(|name| batch.column_by_name(name))
        .map(|column| column.as_any().downcast_ref().expect("retention stamp column"))
}

/// The inner per-key computation of a columnar OVER: mergeable aggregates (DataFusion accumulators)
/// or non-aggregate window functions ({@link WindowFunctionOver}). Both take a `[rt, value?, key0..]`
/// sub-batch and return one result column per output, in input row order.
pub(crate) enum OverInner {
    Aggregates(OverAggregator),
    Bounded(BoundedOverAggregator),
    WindowFunctions(WindowFunctionOver),
}

impl OverInner {
    fn new(value_types: Vec<i64>, kinds: Vec<i64>, frame_kind: i64, frame_offset: i64) -> Self {
        if kinds.iter().all(|&k| is_window_function_kind(k)) {
            OverInner::WindowFunctions(WindowFunctionOver::new(kinds))
        } else if frame_kind == 0 {
            OverInner::Aggregates(OverAggregator::new(value_types, kinds))
        } else {
            // frame_kind 1 = bounded ROWS, 2 = bounded RANGE.
            OverInner::Bounded(BoundedOverAggregator::new(
                value_types,
                kinds,
                frame_kind == 1,
                frame_offset,
            ))
        }
    }

    fn update(&mut self, batch: &RecordBatch) -> RecordBatch {
        match self {
            OverInner::Aggregates(inner) => inner.update(batch),
            OverInner::Bounded(inner) => inner.update(batch),
            OverInner::WindowFunctions(inner) => inner.update(batch),
        }
    }

    /// Backend-mode firing with store-resident per-key state; only the fixed-width fold shapes
    /// support the backend (see `paimon_over_state_types`).
    #[cfg(feature = "paimon-state")]
    fn update_hydrated(
        &mut self,
        batch: &RecordBatch,
        seeds: &[(usize, Option<Vec<ScalarValue>>)],
    ) -> (RecordBatch, Vec<Vec<ScalarValue>>) {
        match self {
            OverInner::Aggregates(inner) => inner.update_hydrated(batch, seeds),
            OverInner::WindowFunctions(inner) => inner.update_hydrated(batch, seeds),
            OverInner::Bounded(_) => unreachable!("bounded OVER frames stay on memory state"),
        }
    }

    fn snapshot(&mut self, retention: RetentionStamps) -> Vec<u8> {
        match self {
            OverInner::Aggregates(inner) => inner.snapshot(retention),
            OverInner::Bounded(inner) => inner.snapshot(retention),
            OverInner::WindowFunctions(inner) => inner.snapshot(retention),
        }
    }

    /// Drops one key's fold/frame state (Flink's `cleanupState`), settling the tracked bytes.
    fn clear_key(&mut self, key: &[u8]) {
        match self {
            OverInner::Aggregates(inner) => {
                if inner.keys.remove(key).is_some() && inner.track {
                    inner.bytes -= inner.key_state_bytes(key);
                }
            }
            OverInner::Bounded(inner) => {
                if let Some(buffer) = inner.keys.remove(key) {
                    if inner.track {
                        inner.bytes -= byte_key_bytes(key) + buffer.len() * inner.row_bytes();
                    }
                }
            }
            OverInner::WindowFunctions(inner) => {
                if inner.keys.remove(key).is_some() && inner.track {
                    inner.bytes -= inner.key_state_bytes(key);
                }
            }
        }
    }

    /// The keys currently holding fold/frame state (for retention-migration stamping at restore).
    fn state_keys(&self) -> Vec<ByteKey> {
        match self {
            OverInner::Aggregates(inner) => inner.keys.keys().cloned().collect(),
            OverInner::Bounded(inner) => inner.keys.keys().cloned().collect(),
            OverInner::WindowFunctions(inner) => inner.keys.keys().cloned().collect(),
        }
    }

    /// Turns on state tracking and computes the current footprint (the restore path scans once).
    fn start_tracking(&mut self) {
        match self {
            OverInner::Aggregates(inner) => {
                inner.track = true;
                inner.recompute_bytes();
            }
            OverInner::Bounded(inner) => {
                inner.track = true;
                inner.recompute_bytes();
            }
            OverInner::WindowFunctions(inner) => {
                inner.track = true;
                inner.recompute_bytes();
            }
        }
    }

    /// The tracked per-key state footprint (zero until tracking starts).
    fn state_bytes(&self) -> usize {
        match self {
            OverInner::Aggregates(inner) => inner.bytes,
            OverInner::Bounded(inner) => inner.bytes,
            OverInner::WindowFunctions(inner) => inner.bytes,
        }
    }

    fn restore(
        value_types: Vec<i64>,
        kinds: Vec<i64>,
        frame_kind: i64,
        frame_offset: i64,
        bytes: &[u8],
        stamps: &mut HashMap<ByteKey, i64>,
    ) -> Self {
        if kinds.iter().all(|&k| is_window_function_kind(k)) {
            OverInner::WindowFunctions(WindowFunctionOver::restore(kinds, bytes, stamps))
        } else if frame_kind == 0 {
            OverInner::Aggregates(OverAggregator::restore(value_types, kinds, bytes, stamps))
        } else {
            OverInner::Bounded(BoundedOverAggregator::restore(
                value_types,
                kinds,
                frame_kind == 1,
                frame_offset,
                bytes,
                stamps,
            ))
        }
    }

    /// Number of trailing snapshot columns that are state rather than the partition key.
    fn snapshot_state_columns(&self) -> usize {
        match self {
            OverInner::Aggregates(inner) => {
                inner.kinds.len() + inner.kinds.iter().filter(|&&kind| kind >= 100).count()
            }
            OverInner::Bounded(inner) => 1 + inner.kinds.len(), // rt plus one value per aggregate
            OverInner::WindowFunctions(inner) => inner
                .kinds
                .iter()
                .map(|&kind| WindowFnState::new(kind).state_types().len())
                .sum(),
        }
    }
}

/// The fold-state column types a Paimon-backed OVER persists per key, or `None` when the shape
/// stays on memory state: proctime ordering (emission is eager, off-watermark), bounded
/// ROWS/RANGE frames (a per-key row buffer, not a fixed-width fold), or a mix of window
/// functions and aggregates.
#[cfg(feature = "paimon-state")]
pub(crate) fn paimon_over_state_types(
    value_types: &[i64],
    kinds: &[i64],
    frame_kind: i64,
    proctime: bool,
) -> Option<Vec<DataType>> {
    if proctime || kinds.iter().any(|&kind| kind >= 100) {
        return None;
    }
    if kinds.iter().all(|&k| is_window_function_kind(k)) {
        return Some(kinds.iter().flat_map(|&k| WindowFnState::new(k).state_types()).collect());
    }
    if frame_kind != 0 || kinds.iter().any(|&k| is_window_function_kind(k)) {
        return None;
    }
    Some(
        kinds
            .iter()
            .zip(value_types)
            .map(|(&kind, &vt)| OverAggState::new(kind, &value_data_type(vt)).result_type())
            .collect(),
    )
}

/// Columnar OVER: buffers whole input batches, and on a watermark emits the rows it has completed
/// (rowtime <= watermark) with the running aggregate / window-function column(s) appended — the input
/// columns pass straight through, so the data stays Arrow end to end. The {@link OverInner} does the
/// per-key running fold; this layer adds the buffering, the complete/pending split, the rowtime→millis
/// conversion the inner expects, and the passthrough.
pub(crate) struct OverWindowAggregator {
    inner: OverInner,
    rt_column: usize,
    /// One input value-column index per aggregate (each aggregate reads its own), or empty for
    /// functions with no argument (e.g. ROW_NUMBER).
    value_columns: Vec<usize>,
    key_columns: Vec<usize>,
    buffered: Vec<RecordBatch>,
    input_schema: Option<SchemaRef>,
    /// Proctime OVER: order by arrival rather than a rowtime, emitting each batch's rows eagerly (no
    /// watermark). The ordering key is a monotonic arrival sequence the operator assigns, so the
    /// existing rowtime fold/frames apply unchanged; `next_seq` is the running counter.
    proctime: bool,
    next_seq: i64,
    watermark: i64,
    /// Idle-state min retention millis (`table.exec.state.ttl`). Flink retention-bounds OVER
    /// three ways by shape ({@link RetentionScheme}); the deadline schemes enable iff this is
    /// `> 1` (Flink's literal `minRetentionTime > 1`), the per-value TTL iff `> 0`.
    min_retention_ms: i64,
    /// The planner-derived max deadline horizon, `min * 3 / 2` (Flink `TableConfigUtils`), saturating.
    max_retention_ms: i64,
    /// Per-key cleanup deadline (Flink's cleanup-time ValueState plus its registered timer),
    /// enforced lazily at key touches plus the periodic sweep — firing emits nothing, so the
    /// substitution is invisible (divergences/28). Keyed like the inner fold state.
    cleanup_state: HashMap<ByteKey, i64>,
    /// Per-key last-write wall clock for the proctime unbounded shape's per-value TTL
    /// (`StateTtlConfig`, OnCreateAndWrite / NeverReturnExpired — stamped on every processed row).
    last_write_ms: HashMap<ByteKey, i64>,
    /// Rowtime deferral counts: buffered rows per key the watermark has not folded yet. Flink's
    /// fired cleanup timer leaves such a key intact and re-registers. Maintained only while the
    /// deadline scheme is cleaning (keying pushed batches costs nothing otherwise); entries exist
    /// only while the count is positive, and re-derive from the buffer at restore.
    pending_rows: HashMap<ByteKey, u32>,
    /// When the last full sweep ran; it reclaims keys never touched again, at most once per
    /// min-retention period.
    last_sweep_ms: i64,
    /// Tracked footprint of the retention maps (their keys move only on entry create/remove).
    retention_bytes: usize,
    /// Keys pushed batches by the PARTITION BY columns for the retention bookkeeping; encodes
    /// byte-identically to the inner's converter (same columns, same codec).
    key_converter: Option<RowConverter>,
    pub(crate) memory: OperatorMemory,
    /// Persistent-state mode: pending rows and per-key fold state live in the Paimon store
    /// (write buffers + disk tables); the in-memory `buffered` batches and the inner's key map
    /// stay empty between calls.
    #[cfg(feature = "paimon-state")]
    backend: Option<crate::state::PaimonOverStore>,
    key_timestamp_precisions: Vec<i32>,
}

impl OverWindowAggregator {
    pub(crate) fn new(
        value_types: Vec<i64>,
        kinds: Vec<i64>,
        rt_column: usize,
        value_columns: Vec<usize>,
        key_columns: Vec<usize>,
        frame_kind: i64,
        frame_offset: i64,
        proctime: bool,
    ) -> Self {
        let key_arity = key_columns.len();
        OverWindowAggregator {
            inner: OverInner::new(value_types, kinds, frame_kind, frame_offset),
            rt_column,
            value_columns,
            key_columns,
            buffered: Vec::new(),
            input_schema: None,
            proctime,
            next_seq: 0,
            watermark: i64::MIN,
            min_retention_ms: 0,
            max_retention_ms: 0,
            cleanup_state: HashMap::default(),
            last_write_ms: HashMap::default(),
            pending_rows: HashMap::default(),
            last_sweep_ms: 0,
            retention_bytes: 0,
            key_converter: None,
            memory: OperatorMemory::unaccounted(),
            #[cfg(feature = "paimon-state")]
            backend: None,
            key_timestamp_precisions: vec![-1; key_arity],
        }
    }

    pub(crate) fn with_key_timestamp_precisions(
        mut self,
        key_timestamp_precisions: Vec<i32>,
    ) -> Self {
        self.key_timestamp_precisions = key_timestamp_precisions;
        self
    }

    /// Sets the idle-state retention (`table.exec.state.ttl`) in millis. The max deadline horizon
    /// is derived natively as Flink's planner does — `minRetentionTime * 3 / 2`, saturating.
    pub(crate) fn with_state_retention(mut self, min_retention_ms: i64) -> Self {
        self.min_retention_ms = min_retention_ms.max(0);
        self.max_retention_ms = self.min_retention_ms.saturating_mul(3) / 2;
        self
    }

    /// Which of Flink's three retention schemes this OVER shape runs. The rowtime shapes and the
    /// proctime bounded-ROWS frame keep one per-key processing-time cleanup deadline
    /// (`KeyedProcessFunctionWithCleanupState`); the proctime unbounded fold puts a per-value
    /// `StateTtlConfig` on its accumulator; and the bounded-RANGE rowtime function takes no
    /// retention at all — its event-time frame eviction already bounds state, so
    /// `table.exec.state.ttl` changes nothing there, exactly as in Flink.
    fn retention_scheme(&self) -> RetentionScheme {
        match (&self.inner, self.proctime) {
            (OverInner::Bounded(inner), _) if !inner.rows_frame => RetentionScheme::None,
            (OverInner::Bounded(_), true) => RetentionScheme::Deadline,
            (_, true) => RetentionScheme::ValueTtl,
            (_, false) => RetentionScheme::Deadline,
        }
    }

    /// Whether the deadline scheme is cleaning — Flink's exact enablement quirk: strictly greater
    /// than ONE millisecond, not zero.
    fn deadline_cleaning(&self) -> bool {
        self.retention_scheme() == RetentionScheme::Deadline && self.min_retention_ms > 1
    }

    /// Whether the proctime-unbounded per-value TTL is on (enabled iff the retention is positive —
    /// the `StateTtlConfig` threshold, not the deadline schemes' `> 1`).
    fn value_ttl_on(&self) -> bool {
        self.retention_scheme() == RetentionScheme::ValueTtl && self.min_retention_ms > 0
    }

    /// The per-value TTL ruleset for a proctime-unbounded touch.
    fn value_ttl(&self, now_ms: i64) -> StateTtl {
        match self.retention_scheme() {
            RetentionScheme::ValueTtl => StateTtl::new(self.min_retention_ms, now_ms),
            _ => StateTtl::disabled(),
        }
    }

    /// Writes a deadline mutation through to the persistent deadlines table — a no-op on memory
    /// state, where the deadline map rides the raw snapshot instead.
    fn stage_backend_deadline(&mut self, key: &[u8], deadline: Option<i64>) {
        #[cfg(feature = "paimon-state")]
        if let Some(store) = &mut self.backend {
            match deadline {
                Some(cleanup_at) => store.deadlines_mut().stage(key, cleanup_at),
                None => store.deadlines_mut().stage_delete(key),
            }
        }
        #[cfg(not(feature = "paimon-state"))]
        let _ = (key, deadline);
    }

    /// Flink's `registerProcessingCleanupTimer`: the deadline moves to `now + maxRetention` only
    /// when the key has none, or the current one would land within a min-retention of now.
    fn register_cleanup(&mut self, key: &[u8], now_ms: i64) {
        let armed = now_ms.saturating_add(self.max_retention_ms);
        let moved = match self.cleanup_state.get_mut(key) {
            Some(deadline) => {
                if now_ms.saturating_add(self.min_retention_ms) > *deadline {
                    *deadline = armed;
                    true
                } else {
                    false
                }
            }
            None => {
                self.retention_bytes += byte_key_bytes(key);
                self.cleanup_state.insert(ByteKey::from(key), armed);
                true
            }
        };
        if moved {
            self.stage_backend_deadline(key, Some(armed));
        }
    }

    /// Flink's `cleanupState`: drops the key's fold/frame state and its retention stamp, silently
    /// (emitting nothing). On the persistent backend the fold row tombstones through the write
    /// buffer, so a restore cannot resurrect the cleared fold.
    fn clear_key(&mut self, key: &[u8]) {
        self.inner.clear_key(key);
        #[cfg(feature = "paimon-state")]
        if let Some(store) = &mut self.backend {
            store.remove_fold(key);
        }
        if self.cleanup_state.remove(key).is_some() {
            self.retention_bytes -= byte_key_bytes(key);
            self.stage_backend_deadline(key, None);
        }
        if self.last_write_ms.remove(key).is_some() {
            self.retention_bytes -= byte_key_bytes(key);
        }
    }

    /// Lazy stand-in for the fired cleanup timer at a key touch (divergences/28): a timer
    /// registered at T fires once processing time reaches T, so cleared state is observable at
    /// `now >= T`. The rowtime shapes defer while the key still has buffered rows the watermark
    /// has not folded — Flink's fired timer leaves such a key intact; the proctime bounded-ROWS
    /// shape has no deferral and clears its frame unconditionally (the frame restarts short,
    /// exactly as in Flink).
    fn expire_if_due(&mut self, key: &[u8], now_ms: i64) {
        match self.cleanup_state.get(key) {
            Some(&deadline) if now_ms >= deadline && !self.pending_rows.contains_key(key) => {
                self.clear_key(key);
            }
            _ => {}
        }
    }

    /// Reclaims every key idle past its horizon with no further touch — the lazy check never sees
    /// such a key again. Silent, at most once per min-retention period. A deferred key (buffered
    /// rows pending a watermark) re-arms to `now + maxRetention` instead — Flink's fired timer
    /// re-registering.
    fn maybe_sweep(&mut self, now_ms: i64) {
        if now_ms < self.last_sweep_ms.saturating_add(self.min_retention_ms) {
            return;
        }
        self.last_sweep_ms = now_ms;
        if self.deadline_cleaning() {
            let due: Vec<ByteKey> = self
                .cleanup_state
                .iter()
                .filter(|(_, &deadline)| now_ms >= deadline)
                .map(|(key, _)| key.clone())
                .collect();
            for key in due {
                if self.pending_rows.contains_key(&key) {
                    let armed = now_ms.saturating_add(self.max_retention_ms);
                    self.cleanup_state.insert(key.clone(), armed);
                    self.stage_backend_deadline(&key.0, Some(armed));
                } else {
                    self.clear_key(&key.0);
                }
            }
        } else {
            let ttl = self.value_ttl(now_ms);
            let idle: Vec<ByteKey> = self
                .last_write_ms
                .iter()
                .filter(|(_, &ts)| ttl.expired(ts))
                .map(|(key, _)| key.clone())
                .collect();
            for key in idle {
                self.clear_key(&key.0);
            }
        }
    }

    /// Rowtime arrival under the deadline scheme. Flink registers the cleanup timer on EVERY
    /// element before anything else: an idle key past its deadline is cleared first (its timer
    /// fired before this element arrived, so the next fold restarts fresh), then re-armed under
    /// the hysteresis, and the buffered row joins the deferral count.
    fn register_batch(&mut self, batch: &RecordBatch, now_ms: i64) {
        let key_arrays: Vec<&ArrayRef> = self.key_columns.iter().map(|&i| batch.column(i)).collect();
        let keys_encoded = encode_keys(&mut self.key_converter, &key_arrays, batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = keys_encoded.row(row).data();
            self.expire_if_due(key, now_ms);
            self.register_cleanup(key, now_ms);
            match self.pending_rows.get_mut(key) {
                Some(count) => *count += 1,
                None => {
                    self.retention_bytes += byte_key_bytes(key);
                    self.pending_rows.insert(ByteKey::from(key), 1);
                }
            }
        }
    }

    /// Flink's post-fire re-registration: after an event-time fire folds a key's completed rows,
    /// they leave the deferral count and the cleanup deadline re-arms under the same hysteresis.
    fn settle_fired(&mut self, complete: &RecordBatch, now_ms: i64) {
        let key_arrays: Vec<&ArrayRef> =
            self.key_columns.iter().map(|&i| complete.column(i)).collect();
        let keys_encoded = encode_keys(&mut self.key_converter, &key_arrays, complete.num_rows());
        for row in 0..complete.num_rows() {
            let key = keys_encoded.row(row).data();
            if let Some(count) = self.pending_rows.get_mut(key) {
                *count -= 1;
                if *count == 0 {
                    self.pending_rows.remove(key);
                    self.retention_bytes -= byte_key_bytes(key);
                }
            }
            self.register_cleanup(key, now_ms);
        }
    }

    /// Proctime enforcement at each arrival. Under the per-value TTL an expired accumulator reads
    /// as absent — the fold (and any window-function numbering) restarts from zero, visibly, as
    /// Flink's `NeverReturnExpired` does — and every processed row stamps a fresh last-write.
    /// Under the bounded-ROWS deadline the frame clears unconditionally at the deadline and the
    /// touch re-arms it.
    fn expire_and_stamp(&mut self, batch: &RecordBatch, now_ms: i64, ttl: StateTtl) {
        let key_arrays: Vec<&ArrayRef> = self.key_columns.iter().map(|&i| batch.column(i)).collect();
        let keys_encoded = encode_keys(&mut self.key_converter, &key_arrays, batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = keys_encoded.row(row).data();
            if ttl.enabled() {
                match self.last_write_ms.get_mut(key) {
                    Some(ts) => {
                        if ttl.expired(*ts) {
                            self.inner.clear_key(key);
                        }
                        *ts = now_ms;
                    }
                    None => {
                        self.retention_bytes += byte_key_bytes(key);
                        self.last_write_ms.insert(ByteKey::from(key), now_ms);
                    }
                }
            } else {
                self.expire_if_due(key, now_ms);
                self.register_cleanup(key, now_ms);
            }
        }
    }

    #[cfg(feature = "paimon-state")]
    pub(crate) fn with_backend(mut self, store: crate::state::PaimonOverStore) -> Self {
        self.backend = Some(store);
        self
    }

    /// Attaches the task off-heap budget for a backend that starts with nothing resident.
    #[cfg(feature = "paimon-state")]
    pub(crate) fn with_read_through_budget(
        mut self,
        budget_bytes: i64,
    ) -> Result<Self, DataFusionError> {
        self.memory.attach("over-aggregate", budget_bytes, 0)?;
        Ok(self)
    }

    #[cfg(feature = "paimon-state")]
    pub(crate) fn store_mut(&mut self) -> &mut crate::state::PaimonOverStore {
        self.backend.as_mut().expect("over paimon backend")
    }

    /// Restore-time hydration of the persistent retention state — the backend's
    /// `adopt_restored_stamps`: the resident deadline map from the `deadlines/` table, the
    /// deferral counts re-derived from the pending table's payload (the pending PK is the
    /// arrival sequence, so only the payload's PARTITION BY columns identify the key), and every
    /// fold key without a restored deadline stamped `restored_at + max` (the enable-flip
    /// migration; pending-only keys deliberately get no stamp, exactly as memory mode stamps
    /// only fold-state keys — deferral protects them regardless).
    #[cfg(feature = "paimon-state")]
    pub(crate) fn hydrate_backend_retention(
        &mut self,
        restored_at_ms: i64,
    ) -> Result<(), DataFusionError> {
        if !self.deadline_cleaning() {
            return Ok(());
        }
        let (deadlines, fold_keys, pending_batches) = {
            let store = self.backend.as_mut().expect("over paimon backend");
            (
                store.deadlines_mut().hydrate_all()?,
                store.scan_fold_keys()?,
                store.scan_pending_payload()?,
            )
        };
        for (key, deadline) in deadlines {
            self.retention_bytes += byte_key_bytes(&key.0);
            self.cleanup_state.insert(key, deadline);
        }
        let stamp = restored_at_ms.saturating_add(self.max_retention_ms);
        let missing: Vec<ByteKey> = fold_keys
            .into_iter()
            .filter(|key| !self.cleanup_state.contains_key(&*key.0))
            .collect();
        let store = self.backend.as_mut().expect("over paimon backend");
        for key in &missing {
            store.deadlines_mut().stage(&key.0, stamp);
        }
        for key in missing {
            self.retention_bytes += byte_key_bytes(&key.0);
            self.cleanup_state.insert(key, stamp);
        }
        let key_columns = self.key_columns.clone();
        let precisions = self.key_timestamp_precisions.clone();
        for batch in pending_batches {
            let mut encoder = BinaryRowBatchEncoder::new(&batch, &key_columns, &precisions);
            for row in 0..batch.num_rows() {
                let key = encoder.encode(row);
                match self.pending_rows.get_mut(key) {
                    Some(count) => *count += 1,
                    None => {
                        self.retention_bytes += byte_key_bytes(key);
                        self.pending_rows.insert(ByteKey::from(key), 1);
                    }
                }
            }
        }
        let store = self.backend.as_mut().expect("over paimon backend");
        let delta = store.footprint_delta();
        self.memory.record(self.retention_bytes as isize + delta);
        self.memory.account()
    }

    /// `register_batch` for the persistent path, keyed by the store's BinaryRow key (the folds
    /// table's PK) so a fired deadline addresses the same fold row: expire, re-arm, and count
    /// per row, exactly as the memory twin.
    #[cfg(feature = "paimon-state")]
    fn register_batch_backend(&mut self, batch: &RecordBatch, now_ms: i64) {
        let key_columns = self.key_columns.clone();
        let precisions = self.key_timestamp_precisions.clone();
        let mut encoder = BinaryRowBatchEncoder::new(batch, &key_columns, &precisions);
        for row in 0..batch.num_rows() {
            let key = ByteKey::from(encoder.encode(row));
            self.expire_if_due(&key.0, now_ms);
            self.register_cleanup(&key.0, now_ms);
            match self.pending_rows.get_mut(&*key.0) {
                Some(count) => *count += 1,
                None => {
                    self.retention_bytes += byte_key_bytes(&key.0);
                    self.pending_rows.insert(key, 1);
                }
            }
        }
    }

    /// `settle_fired` for the persistent path, over the same BinaryRow keys.
    #[cfg(feature = "paimon-state")]
    fn settle_fired_backend(&mut self, complete: &RecordBatch, now_ms: i64) {
        let key_columns = self.key_columns.clone();
        let precisions = self.key_timestamp_precisions.clone();
        let mut encoder = BinaryRowBatchEncoder::new(complete, &key_columns, &precisions);
        for row in 0..complete.num_rows() {
            let key = ByteKey::from(encoder.encode(row));
            if let Some(count) = self.pending_rows.get_mut(&*key.0) {
                *count -= 1;
                if *count == 0 {
                    self.pending_rows.remove(&*key.0);
                    self.retention_bytes -= byte_key_bytes(&key.0);
                }
            }
            self.register_cleanup(&key.0, now_ms);
        }
    }

    /// Bounds this operator's state (buffered batches plus the inner per-key fold state) by the
    /// operator's task off-heap budget (negative = unaccounted), accounting restored state
    /// immediately.
    fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        if budget_bytes < 0 {
            return Ok(self);
        }
        self.inner.start_tracking();
        let state = buffered_batches_bytes(&self.buffered)
            + self.inner.state_bytes()
            + self.retention_bytes;
        self.memory.attach("over-aggregate", budget_bytes, state)?;
        Ok(self)
    }

    /// Re-accounts after a state change: the buffered batches are recounted (cheap, per batch not
    /// per row) and the inner fold state and retention maps report their tracked bytes.
    fn account(&mut self) -> Result<(), DataFusionError> {
        if self.memory.tracking() {
            self.memory.set(
                buffered_batches_bytes(&self.buffered)
                    + self.inner.state_bytes()
                    + self.retention_bytes,
            );
            self.memory.account()?;
        }
        Ok(())
    }

    /// Buffers a rowtime batch (no output until a watermark). `now_ms` is the host's
    /// processing-time reading — the cleanup-deadline clock.
    pub(crate) fn push(&mut self, batch: RecordBatch, now_ms: i64) -> Result<(), DataFusionError> {
        self.input_schema = Some(batch.schema());
        let rowtimes = rt_to_millis(batch.column(self.rt_column));
        let on_time: BooleanArray = rowtimes
            .iter()
            .map(|value| Some(value.is_some_and(|value| value >= self.watermark)))
            .collect();
        let batch = filter_record_batch(&batch, &on_time)?;
        #[cfg(feature = "paimon-state")]
        if self.backend.is_some() {
            return self.push_backend(batch, now_ms);
        }
        if self.deadline_cleaning() {
            self.maybe_sweep(now_ms);
            self.register_batch(&batch, now_ms);
        }
        self.buffered.push(batch);
        self.account()
    }

    /// Persistent-state arrival path: every input row stages into the pending write buffer under
    /// a fresh arrival sequence, routed by its PARTITION BY key's group. Nothing folds here —
    /// emission and the per-key fold are watermark-driven (`flush`). The deadline bookkeeping
    /// runs exactly as on memory state, over the store's BinaryRow keys.
    #[cfg(feature = "paimon-state")]
    fn push_backend(&mut self, batch: RecordBatch, now_ms: i64) -> Result<(), DataFusionError> {
        let retention_before = self.retention_bytes;
        if self.deadline_cleaning() {
            self.maybe_sweep(now_ms);
            if batch.num_rows() > 0 {
                self.register_batch_backend(&batch, now_ms);
            }
        }
        if batch.num_rows() > 0 {
            let rts = rt_to_millis(batch.column(self.rt_column));
            let mut encoder = BinaryRowBatchEncoder::new(
                &batch,
                &self.key_columns,
                &self.key_timestamp_precisions,
            );
            let store = self.backend.as_mut().expect("over paimon backend");
            let kgs: Vec<i32> =
                (0..batch.num_rows()).map(|row| store.key_group(encoder.encode(row))).collect();
            let rt_values: Vec<i64> = (0..batch.num_rows()).map(|row| rts.value(row)).collect();
            store.stage_pending(&kgs, rt_values, batch.columns().to_vec())?;
        }
        let store = self.backend.as_mut().expect("over paimon backend");
        let delta = store.footprint_delta();
        self.memory
            .record(delta + self.retention_bytes as isize - retention_before as isize);
        self.memory.account()
    }

    /// Persistent-state firing path: the store's overlay range read returns every pending row the
    /// watermark completed, in arrival order; the per-key running state hydrates from the folds
    /// table for exactly the fired keys, folds, and writes back into the folds write buffer. A
    /// fired key past its deadline folds anyway — its pending rows deferred the cleanup, exactly
    /// as memory mode — and re-arms through the post-fire settle.
    #[cfg(feature = "paimon-state")]
    fn flush_backend(&mut self, watermark: i64, now_ms: i64) -> Result<RecordBatch, DataFusionError> {
        let retention_before = self.retention_bytes;
        if self.deadline_cleaning() {
            self.maybe_sweep(now_ms);
        }
        let ctx = self.memory.task_ctx();
        let fired = self.backend.as_mut().expect("over paimon backend").fire(watermark, ctx)?;
        let Some(fired) = fired else {
            let store = self.backend.as_mut().expect("over paimon backend");
            store.end_bundle();
            let delta = store.footprint_delta();
            self.memory
                .record(delta + self.retention_bytes as isize - retention_before as isize);
            self.memory.account()?;
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        };
        // The fired batch is store-schema (`kg`, `k`, `rt` millis, payload…): the payload is the
        // completed input rows in arrival order, the rt column is already the fold's ordering key.
        let payload_schema = match &self.input_schema {
            Some(schema) => schema.clone(),
            None => Arc::new(Schema::new(
                fired.schema().fields()[3..]
                    .iter()
                    .map(|f| f.as_ref().clone())
                    .collect::<Vec<_>>(),
            )),
        };
        let complete = RecordBatch::try_new(payload_schema, fired.columns()[3..].to_vec())
            .expect("over payload projection");
        let subbatch = self.keyed_subbatch(&complete, fired.column(2).clone());

        let mut encoder = BinaryRowBatchEncoder::new(
            &complete,
            &self.key_columns,
            &self.key_timestamp_precisions,
        );
        let mut seen: std::collections::HashSet<ByteKey> = std::collections::HashSet::new();
        let mut first_rows: Vec<(ByteKey, usize)> = Vec::new();
        for row in 0..complete.num_rows() {
            let key = encoder.encode(row);
            if !seen.contains(key) {
                let owned = ByteKey::from(key);
                seen.insert(owned.clone());
                first_rows.push((owned, row));
            }
        }
        let seeds: Vec<(usize, Option<Vec<ScalarValue>>)> = {
            let store = self.backend.as_mut().expect("over paimon backend");
            let unique_keys: Vec<ByteKey> = first_rows.iter().map(|(k, _)| k.clone()).collect();
            store.ensure_folds(&unique_keys)?;
            first_rows
                .iter()
                .map(|(key, row)| (*row, store.fold_scalars(&key.0).map(|s| s.to_vec())))
                .collect()
        };
        let (aggregates, published) = self.inner.update_hydrated(&subbatch, &seeds);
        let store = self.backend.as_mut().expect("over paimon backend");
        for ((key, _), scalars) in first_rows.iter().zip(published) {
            store.put_fold(&key.0, scalars);
        }
        if self.deadline_cleaning() {
            self.settle_fired_backend(&complete, now_ms);
        }
        let store = self.backend.as_mut().expect("over paimon backend");
        store.end_bundle();
        let delta = store.footprint_delta();
        self.memory.record(delta + self.retention_bytes as isize - retention_before as isize);
        self.memory.account()?;

        let mut fields: Vec<Field> =
            complete.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut columns: Vec<ArrayRef> = complete.columns().to_vec();
        for (i, field) in aggregates.schema().fields().iter().enumerate() {
            fields.push(field.as_ref().clone());
            columns.push(aggregates.column(i).clone());
        }
        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build over output batch"))
    }

    /// Proctime OVER: fold the whole batch in arrival order and emit every row immediately (proctime
    /// has no watermark to wait on). Each row is tagged with an increasing arrival sequence used as the
    /// ordering key, so the per-key fold and any frame behave exactly as in the rowtime path — the
    /// sequence is distinct and increasing, hence rows fold one at a time in arrival order. The
    /// proctime order column's (non-deterministic) value is never read.
    /// `now_ms` is the host's processing-time reading — the retention clock for both proctime
    /// schemes (the unbounded fold's per-value TTL and the bounded-ROWS frame's deadline).
    pub(crate) fn push_proctime(
        &mut self,
        batch: RecordBatch,
        now_ms: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        self.input_schema = Some(batch.schema());
        let ttl = self.value_ttl(now_ms);
        if ttl.enabled() || self.deadline_cleaning() {
            self.maybe_sweep(now_ms);
            self.expire_and_stamp(&batch, now_ms, ttl);
        }
        let n = batch.num_rows();
        let seq: Int64Array = (0..n as i64).map(|i| self.next_seq + i).collect();
        self.next_seq += n as i64;
        let aggregates = self.inner.update(&self.keyed_subbatch(&batch, Arc::new(seq)));
        self.account()?;
        let mut fields: Vec<Field> =
            batch.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        for (i, field) in aggregates.schema().fields().iter().enumerate() {
            fields.push(field.as_ref().clone());
            columns.push(aggregates.column(i).clone());
        }
        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build proctime over output batch"))
    }

    /// Emits the rows the watermark has completed (input columns + running aggregates) and keeps the
    /// rest buffered. Returns an empty batch when nothing is complete. `now_ms` is the host's
    /// processing-time reading — the cleanup-deadline clock.
    pub(crate) fn flush(
        &mut self,
        watermark: i64,
        now_ms: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        self.watermark = self.watermark.max(watermark);
        #[cfg(feature = "paimon-state")]
        if self.backend.is_some() {
            return self.flush_backend(watermark, now_ms);
        }
        if self.deadline_cleaning() {
            self.maybe_sweep(now_ms);
        }
        let schema = match &self.input_schema {
            Some(schema) => schema.clone(),
            None => return Ok(RecordBatch::new_empty(Arc::new(Schema::empty()))),
        };
        let all = concat_batches(&schema, &self.buffered).expect("failed to concat over buffer");
        let rt_millis = rt_to_millis(all.column(self.rt_column));
        let complete_mask: BooleanArray = rt_millis.iter().map(|v| Some(v.unwrap() <= watermark)).collect();
        let complete = filter_record_batch(&all, &complete_mask).expect("failed to filter complete");
        let pending_mask = arrow::compute::not(&complete_mask).expect("failed to negate mask");
        let pending = filter_record_batch(&all, &pending_mask).expect("failed to filter pending");
        self.buffered = if pending.num_rows() > 0 { vec![pending] } else { Vec::new() };
        if complete.num_rows() == 0 {
            self.account()?;
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }

        let rt = Arc::new(rt_to_millis(complete.column(self.rt_column)));
        // The inner fold grows here (completed rows enter the per-key state), so even a flush can
        // exceed the budget.
        let aggregates = self.inner.update(&self.keyed_subbatch(&complete, rt));
        if self.deadline_cleaning() {
            self.settle_fired(&complete, now_ms);
        }
        self.account()?;
        let mut fields: Vec<Field> =
            complete.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut columns: Vec<ArrayRef> = complete.columns().to_vec();
        for (i, field) in aggregates.schema().fields().iter().enumerate() {
            fields.push(field.as_ref().clone());
            columns.push(aggregates.column(i).clone());
        }
        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build over output batch"))
    }

    /// The `[rt(i64), value0.., key0..]` batch the inner per-key fold reads, projected from `source`.
    /// `rt` is the ordering key — epoch millis for a rowtime OVER, the arrival sequence for proctime.
    /// One `value{a}` column per aggregate, in aggregate order.
    fn keyed_subbatch(&self, source: &RecordBatch, rt: ArrayRef) -> RecordBatch {
        let complete = source;
        let mut fields = vec![Field::new("rt", DataType::Int64, false)];
        let mut columns: Vec<ArrayRef> = vec![rt];
        for (a, &value_column) in self.value_columns.iter().enumerate() {
            fields.push(Field::new(
                format!("value{a}"),
                complete.column(value_column).data_type().clone(),
                true,
            ));
            columns.push(complete.column(value_column).clone());
        }
        for (j, &key) in self.key_columns.iter().enumerate() {
            fields.push(Field::new(format!("key{j}"), complete.column(key).data_type().clone(), false));
            columns.push(complete.column(key).clone());
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build over sub-batch")
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&mut self) -> Vec<u8> {
        let accumulators = self.snapshot_accumulators();
        let buffer = self.snapshot_buffer();
        Self::snapshot_parts(self.next_seq, accumulators, buffer)
    }

    /// The per-key state batch, the retention stamp riding as a trailing column only while
    /// retention is on — a retention-off checkpoint stays byte-identical to the pre-TTL format.
    fn snapshot_accumulators(&mut self) -> Vec<u8> {
        if self.deadline_cleaning() {
            self.inner.snapshot(Some((CLEANUP_AT_COLUMN, &self.cleanup_state)))
        } else if self.value_ttl_on() {
            self.inner.snapshot(Some((TTL_TS_COLUMN, &self.last_write_ms)))
        } else {
            self.inner.snapshot(None)
        }
    }

    fn snapshot_buffer(&self) -> Vec<u8> {
        match (&self.input_schema, self.buffered.is_empty()) {
            (Some(schema), false) => {
                write_ipc(&concat_batches(schema, &self.buffered).expect("concat over buffer"))
            }
            _ => Vec::new(),
        }
    }

    fn snapshot_parts(next_seq: i64, accumulators: Vec<u8>, buffer: Vec<u8>) -> Vec<u8> {
        // Prefix the proctime arrival counter so the sequence continues across a checkpoint.
        let mut out = next_seq.to_le_bytes().to_vec();
        out.extend_from_slice(&(accumulators.len() as u32).to_le_bytes());
        out.extend_from_slice(&accumulators);
        out.extend_from_slice(&buffer);
        out
    }

    pub(crate) fn snapshot_partitions(
        &mut self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<u8>> {
        self.raw_snapshot_partitions(max_parallelism, timestamp_precisions)
    }

    fn raw_snapshot_partitions(
        &mut self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<u8>> {
        // The retention stamp is one more trailing state column; it partitions with its row.
        let state_columns = self.inner.snapshot_state_columns()
            + (self.deadline_cleaning() || self.value_ttl_on()) as usize;
        let accumulators = self.snapshot_accumulators();
        let accumulators = Self::partition_snapshot(
            &accumulators,
            state_columns,
            max_parallelism,
            timestamp_precisions,
        );
        let buffer = Self::partition_buffer_snapshot(
            &self.snapshot_buffer(),
            &self.key_columns,
            max_parallelism,
            timestamp_precisions,
        );
        let mut groups: Vec<i32> = accumulators.keys().chain(buffer.keys()).copied().collect();
        groups.sort_unstable();
        groups.dedup();
        let mut snapshots = BTreeMap::new();
        for key_group in groups {
            snapshots.insert(
                key_group,
                Self::snapshot_parts(
                    self.next_seq,
                    accumulators
                        .get(&key_group)
                        .map(Self::merge_snapshot_batches)
                        .unwrap_or_default(),
                    buffer
                        .get(&key_group)
                        .map(Self::merge_snapshot_batches)
                        .unwrap_or_default(),
                ),
            );
        }
        snapshots
    }

    fn partition_snapshot(
        bytes: &[u8],
        state_columns: usize,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<RecordBatch>> {
        let mut partitions = BTreeMap::new();
        for batch in read_ipc_if_present(bytes) {
            let key_count = batch.num_columns() - state_columns;
            let key_columns: Vec<usize> = (0..key_count).collect();
            let mut rows_by_group: BTreeMap<i32, Vec<u32>> = BTreeMap::new();
            for row in 0..batch.num_rows() {
                let key_group = flink_key_group(
                    binary_row_hash(&batch, &key_columns, row, timestamp_precisions),
                    max_parallelism,
                ) as i32;
                rows_by_group.entry(key_group).or_default().push(row as u32);
            }
            for (key_group, rows) in rows_by_group {
                let indices = UInt32Array::from(rows);
                let columns = batch
                    .columns()
                    .iter()
                    .map(|column| take(column, &indices, None).expect("partition over snapshot"))
                    .collect();
                partitions
                    .entry(key_group)
                    .or_insert_with(Vec::new)
                    .push(
                        RecordBatch::try_new(batch.schema(), columns)
                            .expect("partitioned over snapshot"),
                    );
            }
        }
        partitions
    }

    fn partition_buffer_snapshot(
        bytes: &[u8],
        key_columns: &[usize],
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<RecordBatch>> {
        let mut partitions = BTreeMap::new();
        for batch in read_ipc_if_present(bytes) {
            let mut rows_by_group: BTreeMap<i32, Vec<u32>> = BTreeMap::new();
            for row in 0..batch.num_rows() {
                let key_group = flink_key_group(
                    binary_row_hash(&batch, key_columns, row, timestamp_precisions),
                    max_parallelism,
                ) as i32;
                rows_by_group.entry(key_group).or_default().push(row as u32);
            }
            for (key_group, rows) in rows_by_group {
                let indices = UInt32Array::from(rows);
                let columns = batch
                    .columns()
                    .iter()
                    .map(|column| take(column, &indices, None).expect("partition over buffer"))
                    .collect();
                partitions
                    .entry(key_group)
                    .or_insert_with(Vec::new)
                    .push(
                        RecordBatch::try_new(batch.schema(), columns)
                            .expect("partitioned over buffer"),
                    );
            }
        }
        partitions
    }

    fn merge_snapshot_batches(batches: &Vec<RecordBatch>) -> Vec<u8> {
        write_ipc(
            &concat_batches(&batches[0].schema(), batches.iter()).expect("merge over raw partitions"),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore(
        value_types: Vec<i64>,
        kinds: Vec<i64>,
        rt_column: usize,
        value_columns: Vec<usize>,
        key_columns: Vec<usize>,
        frame_kind: i64,
        frame_offset: i64,
        proctime: bool,
        bytes: &[u8],
        min_retention_ms: i64,
        restored_at_ms: i64,
    ) -> Self {
        if bytes.is_empty() {
            return OverWindowAggregator::new(
                value_types,
                kinds,
                rt_column,
                value_columns,
                key_columns,
                frame_kind,
                frame_offset,
                proctime,
            )
            .with_state_retention(min_retention_ms);
        }
        let next_seq = i64::from_le_bytes(bytes[0..8].try_into().expect("next_seq"));
        let accumulators_len = u32::from_le_bytes(bytes[8..12].try_into().expect("len")) as usize;
        assert!(12 + accumulators_len <= bytes.len(), "truncated over snapshot");
        let mut stamps: HashMap<ByteKey, i64> = HashMap::default();
        let inner = if accumulators_len == 0 {
            OverInner::new(value_types.clone(), kinds.clone(), frame_kind, frame_offset)
        } else {
            OverInner::restore(
                value_types.clone(),
                kinds.clone(),
                frame_kind,
                frame_offset,
                &bytes[12..12 + accumulators_len],
                &mut stamps,
            )
        };
        let key_arity = key_columns.len();
        let mut aggregator = OverWindowAggregator {
            inner,
            rt_column,
            value_columns,
            key_columns,
            buffered: Vec::new(),
            input_schema: None,
            proctime,
            next_seq,
            watermark: i64::MIN,
            min_retention_ms: 0,
            max_retention_ms: 0,
            cleanup_state: HashMap::default(),
            last_write_ms: HashMap::default(),
            pending_rows: HashMap::default(),
            last_sweep_ms: 0,
            retention_bytes: 0,
            key_converter: None,
            memory: OperatorMemory::unaccounted(),
            #[cfg(feature = "paimon-state")]
            backend: None,
            key_timestamp_precisions: vec![-1; key_arity],
        }
        .with_state_retention(min_retention_ms);
        let buffer = &bytes[12 + accumulators_len..];
        if !buffer.is_empty() {
            let reader =
                arrow::ipc::reader::StreamReader::try_new(buffer, None).expect("over buffer reader");
            for batch in reader {
                let batch = batch.expect("read over buffer");
                aggregator.input_schema = Some(batch.schema());
                aggregator.buffered.push(batch);
            }
        }
        aggregator.adopt_restored_stamps(stamps, restored_at_ms);
        aggregator
    }

    /// Retention migration at restore. With retention on, restored stamps are adopted absolutely
    /// (expiry timing survives the restore) and a key from a pre-retention writer is stamped from
    /// the restore clock — a full max horizon for the deadline schemes (Flink's enable-TTL
    /// migration), a fresh last-write for the per-value TTL. With retention off any restored
    /// stamps are shed. Pending-row counts are never snapshotted; they re-derive from the
    /// restored buffer.
    fn adopt_restored_stamps(&mut self, stamps: HashMap<ByteKey, i64>, restored_at_ms: i64) {
        if self.deadline_cleaning() {
            self.cleanup_state = stamps;
            let stamp = restored_at_ms.saturating_add(self.max_retention_ms);
            for key in self.inner.state_keys() {
                self.cleanup_state.entry(key).or_insert(stamp);
            }
            self.derive_pending_rows();
            self.retention_bytes = self
                .cleanup_state
                .keys()
                .chain(self.pending_rows.keys())
                .map(|key| byte_key_bytes(&key.0))
                .sum();
        } else if self.value_ttl_on() {
            self.last_write_ms = stamps;
            for key in self.inner.state_keys() {
                self.last_write_ms.entry(key).or_insert(restored_at_ms);
            }
            self.retention_bytes =
                self.last_write_ms.keys().map(|key| byte_key_bytes(&key.0)).sum();
        }
    }

    /// Rebuilds the per-key deferral counts from the restored (not yet folded) buffer.
    fn derive_pending_rows(&mut self) {
        for batch in &self.buffered {
            let key_arrays: Vec<&ArrayRef> =
                self.key_columns.iter().map(|&i| batch.column(i)).collect();
            let keys_encoded = encode_keys(&mut self.key_converter, &key_arrays, batch.num_rows());
            for row in 0..batch.num_rows() {
                match self.pending_rows.get_mut(keys_encoded.row(row).data()) {
                    Some(count) => *count += 1,
                    None => {
                        self.pending_rows.insert(ByteKey::from(keys_encoded.row(row).data()), 1);
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore_partitions(
        value_types: Vec<i64>,
        kinds: Vec<i64>,
        rt_column: usize,
        value_columns: Vec<usize>,
        key_columns: Vec<usize>,
        frame_kind: i64,
        frame_offset: i64,
        proctime: bool,
        snapshots: &[Vec<u8>],
        min_retention_ms: i64,
        restored_at_ms: i64,
    ) -> Self {
        let mut next_seq = 0i64;
        let mut accumulator_batches = Vec::new();
        let mut buffer_batches = Vec::new();
        for bytes in snapshots {
            if bytes.len() < 12 {
                continue;
            }
            next_seq = next_seq.max(i64::from_le_bytes(bytes[0..8].try_into().expect("next_seq")));
            let accumulator_len =
                u32::from_le_bytes(bytes[8..12].try_into().expect("accumulator len")) as usize;
            assert!(12 + accumulator_len <= bytes.len(), "truncated over raw key-group snapshot");
            accumulator_batches.extend(read_ipc_if_present(&bytes[12..12 + accumulator_len]));
            buffer_batches.extend(read_ipc_if_present(&bytes[12 + accumulator_len..]));
        }
        let accumulators = (!accumulator_batches.is_empty())
            .then(|| Self::merge_snapshot_batches(&accumulator_batches))
            .unwrap_or_default();
        let buffer = (!buffer_batches.is_empty())
            .then(|| Self::merge_snapshot_batches(&buffer_batches))
            .unwrap_or_default();
        OverWindowAggregator::restore(
            value_types,
            kinds,
            rt_column,
            value_columns,
            key_columns,
            frame_kind,
            frame_offset,
            proctime,
            &Self::snapshot_parts(next_seq, accumulators, buffer),
            min_retention_ms,
            restored_at_ms,
        )
    }
}

state_bytes_getter!(Java_tech_streamfusion_Native_overAggregatorStateBytes, OverWindowAggregator);

/// Creates a columnar OVER aggregator (event-time RANGE unbounded preceding); it buffers input
/// batches and flushes completed rows with the running aggregates appended. The rt/value/key column
/// indices locate those columns within the buffered input batch. `state_ttl_millis` is the
/// idle-state retention — the scheme it drives depends on the shape (see the operator docs).
#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createOverAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    rt_column: jint,
    value_columns: JIntArray<'local>,
    key_columns: JIntArray<'local>,
    frame_kind: jint,
    frame_offset: jlong,
    proctime: jboolean,
    state_ttl_millis: jlong,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_types = read_int_array(&env, &value_types);
        let values = read_columns(&env, &value_columns);
        let keys = read_columns(&env, &key_columns);
        let aggregator = OverWindowAggregator::new(
            value_types,
            kinds,
            rt_column as usize,
            values,
            keys,
            frame_kind as i64,
            frame_offset,
            proctime != 0,
        )
        .with_state_retention(state_ttl_millis)
        .with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, aggregator)
    })
}

/// Buffers an input batch (no output); the rows are emitted later when a watermark completes them.
/// `now_millis` is the operator's processing-time reading — the cleanup-deadline clock.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushOverAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    now_millis: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
        // The pushed batch is retained in the buffer (not dropped), so no JVM release upcall runs
        // between a failed account and the throw (see updateTumblingAggregator).
        let result =
            aggregator.push(import_record_batch(in_array_address, in_schema_address), now_millis);
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

/// Proctime OVER: folds a batch in arrival order and exports its rows immediately (no watermark),
/// each with the running aggregate / window-function column(s) appended. `now_millis` is the
/// operator's processing-time reading — the retention clock.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushProctimeOverAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    now_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            aggregator.push_proctime(batch, now_millis)
        };
        match result {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Exports the rows the watermark has completed (input columns + running aggregates).
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushOverAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    now_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
        // The inner per-key fold grows on flush, so even a flush can exceed the budget.
        match aggregator.flush(watermark_millis, now_millis) {
            Ok(result) => export_record_batch(result, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Releases the OVER aggregator and its native state.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeOverAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe {
            drop(from_handle::<OverWindowAggregator>(handle));
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_snapshotOverAggregatorPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
        let precisions = read_i32_array(&env, &timestamp_precisions);
        keyed_state_partition_array(
            &mut env,
            aggregator.snapshot_partitions(max_parallelism as usize, &precisions),
            "over",
        )
    })
}

#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_restoreOverAggregatorPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    rt_column: jint,
    value_columns: JIntArray<'local>,
    key_columns: JIntArray<'local>,
    frame_kind: jint,
    frame_offset: jlong,
    proctime: jboolean,
    state_ttl_millis: jlong,
    now_millis: jlong,
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_types = read_int_array(&env, &value_types);
        let values = read_columns(&env, &value_columns);
        let keys = read_columns(&env, &key_columns);
        let count = env
            .get_array_length(&snapshots)
            .expect("read over raw partition count");
        let mut restored = Vec::with_capacity(count as usize);
        for index in 0..count {
            let bytes = JByteArray::from(
                env.get_object_array_element(&snapshots, index)
                    .expect("read over raw partition"),
            );
            restored.push(
                env.convert_byte_array(&bytes)
                    .expect("read over raw partition bytes"),
            );
        }
        let aggregator = OverWindowAggregator::restore_partitions(
            value_types,
            kinds,
            rt_column as usize,
            values,
            keys,
            frame_kind as i64,
            frame_offset,
            proctime != 0,
            &restored,
            state_ttl_millis,
            now_millis,
        )
        .with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, aggregator)
    })
}
