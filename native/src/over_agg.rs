use crate::*;

/// Downcasts a projected OVER value column (`value{a}`) to its typed per-row reader.
pub(crate) fn over_value_column<'a>(column: &'a ArrayRef, value_type: &DataType) -> ValueColumn<'a> {
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

pub(crate) struct OverAggregator {
    kinds: Vec<i64>,
    /// One value type per aggregate (aggregates may read different value columns of different types).
    value_types: Vec<DataType>,
    // Keyed by arrow-row bytes, probed borrowed (see the group aggregate): a row whose partition
    // already exists — the steady state — allocates nothing; the key is copied on first touch only.
    keys: HashMap<ByteKey, Vec<RunningAgg>>,
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
        byte_key_bytes(key) + self.kinds.len() * std::mem::size_of::<RunningAgg>()
    }

    fn recompute_bytes(&mut self) {
        self.bytes = self.keys.keys().map(|key| self.key_state_bytes(&key.0)).sum();
    }

    /// The running aggregate state for a key, created (copying the key bytes) on first touch.
    fn states(&mut self, key: &[u8]) -> &mut Vec<RunningAgg> {
        if !self.keys.contains_key(key) {
            let fresh: Vec<RunningAgg> = self
                .kinds
                .iter()
                .zip(&self.value_types)
                .map(|(&kind, vt)| RunningAgg::new(kind, vt))
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
            let result_type = RunningAgg::new(self.kinds[a], &self.value_types[a]).result_type();
            fields.push(Field::new(format!("result{a}"), result_type.clone(), true));
            columns.push(scalars_to_array(std::mem::take(&mut results[a]), &result_type));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build over result batch")
    }

    /// Serializes the per-key running state (`[key0.., state0..]`, one row per key, one scalar per
    /// aggregate — the running value is itself the checkpointed state).
    fn snapshot(&mut self) -> Vec<u8> {
        let result_types: Vec<DataType> = self
            .kinds
            .iter()
            .zip(&self.value_types)
            .map(|(&k, vt)| RunningAgg::new(k, vt).result_type())
            .collect();
        let mut keys: Vec<&[u8]> = Vec::new();
        let mut state_columns: Vec<Vec<ScalarValue>> = vec![Vec::new(); self.kinds.len()];
        for (key, states) in self.keys.iter() {
            keys.push(&key.0);
            for (i, state) in states.iter().enumerate() {
                state_columns[i].push(state.emit());
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
        write_ipc(&RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build over snapshot batch"))
    }

    fn restore(value_types: Vec<i64>, kinds: Vec<i64>, bytes: &[u8]) -> Self {
        let mut aggregator = OverAggregator::new(value_types, kinds);
        let num_agg = aggregator.kinds.len();
        for batch in read_ipc(bytes) {
            let arity = batch.num_columns() - num_agg;
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
            let mut aggs: Vec<RunningAgg> =
                self.kinds.iter().zip(&self.value_types).map(|(&k, vt)| RunningAgg::new(k, vt)).collect();
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
            let result_type = RunningAgg::new(self.kinds[a], &self.value_types[a]).result_type();
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
    /// column per aggregate).
    fn snapshot(&mut self) -> Vec<u8> {
        let num_agg = self.kinds.len();
        let mut keys: Vec<&[u8]> = Vec::new();
        let mut rts: Vec<ScalarValue> = Vec::new();
        let mut value_columns: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_agg];
        for (key, buffer) in self.keys.iter() {
            for row in buffer {
                keys.push(&key.0);
                rts.push(ScalarValue::Int64(Some(row.rt)));
                for a in 0..num_agg {
                    value_columns[a].push(num_to_scalar(&self.value_types[a], row.values[a]));
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
        write_ipc(&RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build bounded over snapshot batch"))
    }

    fn restore(value_types: Vec<i64>, kinds: Vec<i64>, rows_frame: bool, offset: i64, bytes: &[u8]) -> Self {
        let mut aggregator = BoundedOverAggregator::new(value_types, kinds, rows_frame, offset);
        let num_agg = aggregator.kinds.len();
        for batch in read_ipc(bytes) {
            let arity = batch.num_columns() - 1 - num_agg; // trailing rt + one value column per agg
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
                aggregator.keys.entry(key).or_default().push(BufferedRow { rt: rt.value(row), values });
            }
        }
        aggregator
    }
}

/// Window-function code: a SQL `OVER` analytic function that is *not* a mergeable aggregate.
pub(crate) fn is_window_function_kind(kind: i64) -> bool {
    kind >= 10
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

    /// Serializes the per-key running state (`[key0.., state…]`, one row per key).
    fn snapshot(&mut self) -> Vec<u8> {
        let state_types: Vec<DataType> =
            self.kinds.iter().flat_map(|&k| WindowFnState::new(k).state_types()).collect();
        let mut keys: Vec<&[u8]> = Vec::new();
        let mut state_columns: Vec<Vec<ScalarValue>> = vec![Vec::new(); state_types.len()];
        for (key, states) in self.keys.iter() {
            keys.push(&key.0);
            let mut column = 0;
            for state in states {
                for scalar in state.state() {
                    state_columns[column].push(scalar);
                    column += 1;
                }
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
        write_ipc(&RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build window-function snapshot batch"))
    }

    fn restore(kinds: Vec<i64>, bytes: &[u8]) -> Self {
        let mut over = WindowFunctionOver::new(kinds);
        let state_counts: Vec<usize> =
            over.kinds.iter().map(|&k| WindowFnState::new(k).state_types().len()).collect();
        let state_total: usize = state_counts.iter().sum();
        for batch in read_ipc(bytes) {
            let arity = batch.num_columns() - state_total;
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
            }
        }
        over
    }
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

    fn snapshot(&mut self) -> Vec<u8> {
        match self {
            OverInner::Aggregates(inner) => inner.snapshot(),
            OverInner::Bounded(inner) => inner.snapshot(),
            OverInner::WindowFunctions(inner) => inner.snapshot(),
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

    fn restore(value_types: Vec<i64>, kinds: Vec<i64>, frame_kind: i64, frame_offset: i64, bytes: &[u8]) -> Self {
        if kinds.iter().all(|&k| is_window_function_kind(k)) {
            OverInner::WindowFunctions(WindowFunctionOver::restore(kinds, bytes))
        } else if frame_kind == 0 {
            OverInner::Aggregates(OverAggregator::restore(value_types, kinds, bytes))
        } else {
            OverInner::Bounded(BoundedOverAggregator::restore(
                value_types,
                kinds,
                frame_kind == 1,
                frame_offset,
                bytes,
            ))
        }
    }

    /// Number of trailing snapshot columns that are state rather than the partition key.
    fn snapshot_state_columns(&self) -> usize {
        match self {
            OverInner::Aggregates(inner) => inner.kinds.len(),
            OverInner::Bounded(inner) => 1 + inner.kinds.len(), // rt plus one value per aggregate
            OverInner::WindowFunctions(inner) => inner
                .kinds
                .iter()
                .map(|&kind| WindowFnState::new(kind).state_types().len())
                .sum(),
        }
    }
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
    memory: OperatorMemory,
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
        OverWindowAggregator {
            inner: OverInner::new(value_types, kinds, frame_kind, frame_offset),
            rt_column,
            value_columns,
            key_columns,
            buffered: Vec::new(),
            input_schema: None,
            proctime,
            next_seq: 0,
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds this operator's state (buffered batches plus the inner per-key fold state) by the
    /// operator's managed-memory budget (negative = unaccounted), accounting restored state
    /// immediately.
    fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        if budget_bytes < 0 {
            return Ok(self);
        }
        self.inner.start_tracking();
        let state = buffered_batches_bytes(&self.buffered) + self.inner.state_bytes();
        self.memory.attach("over-aggregate", budget_bytes, state)?;
        Ok(self)
    }

    /// Re-accounts after a state change: the buffered batches are recounted (cheap, per batch not
    /// per row) and the inner fold state reports its tracked bytes.
    fn account(&mut self) -> Result<(), DataFusionError> {
        if self.memory.tracking() {
            self.memory.set(buffered_batches_bytes(&self.buffered) + self.inner.state_bytes());
            self.memory.account()?;
        }
        Ok(())
    }

    pub(crate) fn push(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        self.input_schema = Some(batch.schema());
        self.buffered.push(batch);
        self.account()
    }

    /// Proctime OVER: fold the whole batch in arrival order and emit every row immediately (proctime
    /// has no watermark to wait on). Each row is tagged with an increasing arrival sequence used as the
    /// ordering key, so the per-key fold and any frame behave exactly as in the rowtime path — the
    /// sequence is distinct and increasing, hence rows fold one at a time in arrival order. The
    /// proctime order column's (non-deterministic) value is never read.
    pub(crate) fn push_proctime(&mut self, batch: RecordBatch) -> Result<RecordBatch, DataFusionError> {
        self.input_schema = Some(batch.schema());
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
    /// rest buffered. Returns an empty batch when nothing is complete.
    pub(crate) fn flush(&mut self, watermark: i64) -> Result<RecordBatch, DataFusionError> {
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

    fn snapshot(&mut self) -> Vec<u8> {
        let accumulators = self.inner.snapshot();
        let buffer = self.snapshot_buffer();
        Self::snapshot_parts(self.next_seq, accumulators, buffer)
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
        let accumulators = self.inner.snapshot();
        let accumulators = Self::partition_snapshot(
            &accumulators,
            self.inner.snapshot_state_columns(),
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

    fn restore(
        value_types: Vec<i64>,
        kinds: Vec<i64>,
        rt_column: usize,
        value_columns: Vec<usize>,
        key_columns: Vec<usize>,
        frame_kind: i64,
        frame_offset: i64,
        proctime: bool,
        bytes: &[u8],
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
            );
        }
        let next_seq = i64::from_le_bytes(bytes[0..8].try_into().expect("next_seq"));
        let accumulators_len = u32::from_le_bytes(bytes[8..12].try_into().expect("len")) as usize;
        assert!(12 + accumulators_len <= bytes.len(), "truncated over snapshot");
        let inner = if accumulators_len == 0 {
            OverInner::new(value_types.clone(), kinds.clone(), frame_kind, frame_offset)
        } else {
            OverInner::restore(
                value_types.clone(),
                kinds.clone(),
                frame_kind,
                frame_offset,
                &bytes[12..12 + accumulators_len],
            )
        };
        let mut aggregator = OverWindowAggregator {
            inner,
            rt_column,
            value_columns,
            key_columns,
            buffered: Vec::new(),
            input_schema: None,
            proctime,
            next_seq,
            memory: OperatorMemory::unaccounted(),
        };
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
        aggregator
    }

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
        )
    }
}

state_bytes_getter!(Java_io_github_jordepic_streamfusion_Native_overAggregatorStateBytes, OverWindowAggregator);

/// Creates a columnar OVER aggregator (event-time RANGE unbounded preceding); it buffers input
/// batches and flushes completed rows with the running aggregates appended. The rt/value/key column
/// indices locate those columns within the buffered input batch.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createOverAggregator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    rt_column: jint,
    value_columns: JIntArray<'local>,
    key_columns: JIntArray<'local>,
    frame_kind: jint,
    frame_offset: jlong,
    proctime: jboolean,
    memory_budget_bytes: jlong,
) -> jlong {
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
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, aggregator)
}

/// Buffers an input batch (no output); the rows are emitted later when a watermark completes them.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushOverAggregator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
    // The pushed batch is retained in the buffer (not dropped), so no JVM release upcall runs
    // between a failed account and the throw (see updateTumblingAggregator).
    let result = aggregator.push(import_record_batch(in_array_address, in_schema_address));
    if let Err(e) = result {
        throw_memory_limit(&mut env, &e.to_string());
    }
}

/// Proctime OVER: folds a batch in arrival order and exports its rows immediately (no watermark),
/// each with the running aggregate / window-function column(s) appended.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushProctimeOverAggregator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        aggregator.push_proctime(batch)
    };
    match result {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Exports the rows the watermark has completed (input columns + running aggregates).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushOverAggregator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
    // The inner per-key fold grows on flush, so even a flush can exceed the budget.
    match aggregator.flush(watermark_millis) {
        Ok(result) => export_record_batch(result, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Releases the OVER aggregator and its native state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeOverAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<OverWindowAggregator>(handle));
    }
}

/// Serializes the OVER aggregator's running state and buffered rows for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotOverAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let aggregator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
    env.byte_array_from_slice(&aggregator.snapshot())
        .expect("failed to allocate over snapshot array")
        .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotOverAggregatorPartitions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    let aggregator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
    let precisions: Vec<i32> = read_int_array(&env, &timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    keyed_state_partition_array(
        &mut env,
        aggregator.snapshot_partitions(max_parallelism as usize, &precisions),
        "over",
    )
}

/// Rebuilds an OVER aggregator from a snapshot taken by a prior run and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreOverAggregator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    rt_column: jint,
    value_columns: JIntArray<'local>,
    key_columns: JIntArray<'local>,
    frame_kind: jint,
    frame_offset: jlong,
    proctime: jboolean,
    snapshot: JByteArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let values = read_columns(&env, &value_columns);
    let keys = read_columns(&env, &key_columns);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read over snapshot");
    let aggregator = OverWindowAggregator::restore(
        value_types,
        kinds,
        rt_column as usize,
        values,
        keys,
        frame_kind as i64,
        frame_offset,
        proctime != 0,
        &bytes,
    )
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, aggregator)
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreOverAggregatorPartitions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    rt_column: jint,
    value_columns: JIntArray<'local>,
    key_columns: JIntArray<'local>,
    frame_kind: jint,
    frame_offset: jlong,
    proctime: jboolean,
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
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
    )
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, aggregator)
}
