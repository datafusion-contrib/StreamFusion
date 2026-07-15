use crate::*;

/// Every aligned window a timestamp (millis) belongs to, as (start, end) millis pairs, appended to
/// `windows` (cleared first) so the caller can reuse one buffer. Tumbling yields one window; hopping
/// yields the `size / slide` overlapping windows; cumulative yields the nested windows
/// `[base, base + k*step)` whose end is past the timestamp, all sharing the bucket start. Shared by
/// the window aggregate and the windowing TVF so their assignment is byte-for-byte identical.
pub(crate) fn windows_for(
    timestamp: i64,
    window_millis: i64,
    slide_millis: i64,
    cumulative: bool,
    windows: &mut Vec<(i64, i64)>,
) {
    windows.clear();
    if cumulative {
        let base = timestamp - timestamp.rem_euclid(window_millis);
        let mut end = base + slide_millis;
        while end <= base + window_millis {
            if end > timestamp {
                windows.push((base, end));
            }
            end += slide_millis;
        }
    } else {
        let mut start = timestamp - timestamp.rem_euclid(slide_millis);
        while start + window_millis > timestamp {
            windows.push((start, start + window_millis));
            start -= slide_millis;
        }
    }
}

/// Stateless windowing table function (Flink's `WindowTableFunctionOperator`): assigns each input
/// row to its window(s) and emits the input columns — fanned out, one copy per window for
/// hopping/cumulative — with `window_start`, `window_end`, and `window_time` (= `window_end - 1ms`)
/// appended. The downstream window join/aggregate does the event-time buffering; this is a pure
/// per-row map, so watermarks pass straight through (the wrapper operator forwards them).
///
/// Timestamps are nanosecond / no time zone — the unit `ArrowConversion` pins both `TIMESTAMP` and
/// `TIMESTAMP_LTZ` to — while the window math runs in millis, identical to the window aggregate
/// (shared [`windows_for`]). A trailing `$row_kind$` changelog tag stays the last output column, so
/// the window columns land at the input-column count (the indices the join/aggregate expect).
/// Replaces the time column with a constant clock value (epoch millis) for every row, rendered in the
/// joiner's declared column type. Used by the proctime interval join to time each row by the
/// operator's processing-time clock instead of a rowtime column. The incoming batch carries the
/// proctime attribute as a millisecond timestamp (PROCTIME() is TIMESTAMP_LTZ), whereas the joiner's
/// schema (derived from the logical row type) declares the rowtime slot as a nanosecond timestamp, so
/// the stamped column is cast to the target type and its schema field retyped to match — keeping the
/// buffered batches concat-compatible with the joiner's schema.
pub(crate) fn stamp_time_column(
    batch: &RecordBatch,
    col: usize,
    now_millis: i64,
    target: &DataType,
) -> RecordBatch {
    let base: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![now_millis; batch.num_rows()]));
    let array = arrow::compute::cast(&base, target).expect("cast stamped interval time to target type");
    let mut fields: Vec<Field> =
        batch.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
    fields[col] = Field::new(fields[col].name(), target.clone(), fields[col].is_nullable());
    let mut columns = batch.columns().to_vec();
    columns[col] = array;
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("stamp interval time column")
}

pub(crate) fn assign_windows(
    input: &RecordBatch,
    time_col: usize,
    window_millis: i64,
    slide_millis: i64,
    cumulative: bool,
    proctime_now_millis: Option<i64>,
) -> RecordBatch {
    let schema = input.schema();
    let row_kind_idx = schema.fields().iter().position(|f| f.name() == ROW_KIND_COLUMN);
    let data_end = row_kind_idx.unwrap_or_else(|| schema.fields().len());

    // Event-time assigns each row by its rowtime column; proctime assigns every row to the window(s)
    // covering the operator's processing-time clock (passed in), ignoring the time column.
    let times = proctime_now_millis.is_none().then(|| {
        input
            .column(time_col)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("windowing TVF time column must be timestamp(ns)")
    });

    // One take index per output row (the input row it copies), plus that row's window bounds in nanos.
    let mut take_indices: Vec<u32> = Vec::with_capacity(input.num_rows());
    let mut starts: Vec<i64> = Vec::new();
    let mut ends: Vec<i64> = Vec::new();
    let mut windows: Vec<(i64, i64)> = Vec::new();
    for row in 0..input.num_rows() {
        let time_millis = match proctime_now_millis {
            Some(now) => now,
            None => times.unwrap().value(row) / 1_000_000,
        };
        windows_for(time_millis, window_millis, slide_millis, cumulative, &mut windows);
        for &(start, end) in &windows {
            take_indices.push(row as u32);
            starts.push(start * 1_000_000);
            ends.push(end * 1_000_000);
        }
    }
    let indices = UInt32Array::from(take_indices);
    let timestamp = DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None);

    let mut fields: Vec<Field> = Vec::with_capacity(data_end + 4);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(data_end + 4);
    for i in 0..data_end {
        fields.push(schema.field(i).as_ref().clone());
        columns.push(take(input.column(i), &indices, None).expect("failed to fan out column"));
    }
    let window_time: Vec<i64> = ends.iter().map(|end| end - 1_000_000).collect();
    for name in ["window_start", "window_end", "window_time"] {
        fields.push(Field::new(name, timestamp.clone(), false));
    }
    columns.push(Arc::new(TimestampNanosecondArray::from(starts)));
    columns.push(Arc::new(TimestampNanosecondArray::from(ends)));
    columns.push(Arc::new(TimestampNanosecondArray::from(window_time)));
    if let Some(idx) = row_kind_idx {
        fields.push(schema.field(idx).as_ref().clone());
        columns.push(take(input.column(idx), &indices, None).expect("failed to fan out row kind"));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("failed to build TVF batch")
}

/// One open aligned window: its start, plus the per-key accumulators folding in matching rows. The
/// owning map keys windows by their *end*, which is unique even for cumulative windows that share a
/// start, so the start is carried here.
pub(crate) struct AlignedWindow {
    start: i64,
    keys: HashMap<OwnedRow, Vec<Box<dyn Accumulator>>>,
}

struct WindowSnapshotCache {
    max_parallelism: usize,
    timestamp_precisions: Vec<i32>,
    snapshots: BTreeMap<i32, Vec<u8>>,
}

/// Event-time aligned-window aggregation that holds open windows across batches: tumbling and
/// hopping (fixed-size windows at a slide interval) and cumulative (nested windows sharing a start,
/// growing by a step up to a max size). Mirrors the upstream streaming engine's window operator:
/// windows live in an ordered map keyed by end, each an incremental accumulator that folds in
/// matching rows, and a window is finalized and dropped once a watermark passes its end.
pub(crate) struct TumblingAggregator {
    window_millis: i64,
    slide_millis: i64,
    cumulative: bool,
    aggregates: Vec<WindowAggregate>,
    windows: BTreeMap<i64, AlignedWindow>,
    // Groups are keyed by the arrow-row memcomparable encoding of the key columns, not a
    // `Vec<ScalarValue>` — the same trade the non-windowed GROUP BY made: the scalar key's per-row
    // alloc/hash/drop dominates the keyed update loop, byte keys do not. `key_converter` encodes
    // the key columns once per batch and decodes stored keys back into output columns.
    key_converter: Option<RowConverter>,
    key_types: Vec<DataType>,
    // The highest watermark flushed so far; a row whose window ends at or before it is late (its
    // window already closed) and is dropped, matching the host's per-row late-data handling.
    current_watermark: i64,
    snapshot_cache: Option<WindowSnapshotCache>,
    // Managed-memory accounting: open-window footprint tracked per touched group (not by
    // rescanning all state) and resized against the reservation after every state change.
    pub(crate) memory: OperatorMemory,
}

impl TumblingAggregator {
    pub(crate) fn new(
        window_millis: i64,
        slide_millis: i64,
        cumulative: bool,
        value_types: Vec<i64>,
        kinds: Vec<i64>,
    ) -> Self {
        TumblingAggregator {
            window_millis,
            slide_millis,
            cumulative,
            aggregates: build_aggregates(&kinds, &value_types),
            windows: BTreeMap::new(),
            key_converter: None,
            key_types: Vec::new(),
            current_watermark: i64::MIN,
            snapshot_cache: None,
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds this aggregator's state by a managed-memory budget the host reserved for the operator
    /// (a negative budget means unaccounted). Registers a reservation against a pool of that size and
    /// accounts any state already present (the restore path), so a restored snapshot that no longer
    /// fits fails here rather than as a container OOM.
    pub(crate) fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        self.memory.attach("window-aggregate", budget_bytes, self.computed_state_bytes())?;
        Ok(self)
    }

    /// [`with_memory_budget`](Self::with_memory_budget) against a caller-owned pool (shared in tests
    /// to observe the pool's balance from outside).
    pub(crate) fn with_memory_pool(mut self, pool: &Arc<dyn MemoryPool>) -> Result<Self, DataFusionError> {
        self.memory.attach_pool("window-aggregate", pool, self.computed_state_bytes())?;
        Ok(self)
    }

    /// The full-scan footprint of the open windows — the ground truth the incremental
    /// `state_bytes` tracks. Used once when a budget attaches to restored state (and by tests to
    /// assert the incremental tracking does not drift).
    pub(crate) fn computed_state_bytes(&self) -> usize {
        self.windows
            .values()
            .map(|window| {
                window
                    .keys
                    .iter()
                    .map(|(key, accumulators)| owned_row_bytes(key) + accumulators_bytes(accumulators))
                    .sum::<usize>()
            })
            .sum()
    }

    /// Removes a dropped group's footprint from the tracked state size (a flush closing its window).
    fn forget_group_bytes(&mut self, key: &OwnedRow, accumulators: &[Box<dyn Accumulator>]) {
        if self.memory.tracking() {
            self.memory.forget(owned_row_bytes(key) + accumulators_bytes(accumulators));
        }
    }

    /// Every window a timestamp belongs to, as (start, end) pairs, appended to `windows` (cleared
    /// first) so the caller can reuse one buffer across rows. Tumbling yields one window; hopping
    /// yields the `size / slide` overlapping windows; cumulative yields the nested windows
    /// `[base, base + k*step)` whose end is past the timestamp, all sharing the bucket start.
    fn windows_for(&self, timestamp: i64, windows: &mut Vec<(i64, i64)>) {
        windows_for(timestamp, self.window_millis, self.slide_millis, self.cumulative, windows);
    }

    /// The N accumulators (one per aggregate) for a (window, key), created on first touch. Windows
    /// are keyed by end; the start is stored on first creation. The group key is a composite of the
    /// (zero or more) grouping columns.
    fn accumulators(&mut self, start: i64, end: i64, key: OwnedRow) -> &mut Vec<Box<dyn Accumulator>> {
        let aggregates = &self.aggregates;
        self.windows
            .entry(end)
            .or_insert_with(|| AlignedWindow { start, keys: HashMap::default() })
            .keys
            .entry(key)
            .or_insert_with(|| aggregates.iter().map(WindowAggregate::create_accumulator).collect())
    }

    /// Window ends at or before the watermark, in ascending order (the map is keyed by end).
    fn closed_windows(&self, watermark: i64) -> Vec<i64> {
        self.windows.keys().copied().take_while(|end| *end <= watermark).collect()
    }

    pub(crate) fn update(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        self.snapshot_cache = None;
        let ts = column_i64(batch, "ts");
        // One value column per aggregate (value0, value1, …), so aggregates can read different
        // columns. Sliced by type-agnostic take, so each accumulator sees its column's own type.
        let values: Vec<&ArrayRef> = (0..self.aggregates.len())
            .map(|i| batch.column_by_name(&format!("value{i}")).expect("missing value column"))
            .collect();
        let key_arrays = key_arrays(batch);
        self.key_types = key_types(&key_arrays);
        let keys_encoded = encode_keys(&mut self.key_converter, &key_arrays, batch.num_rows());

        // Group the row positions for each (window, key); the value columns are sliced by type-
        // agnostic take, so the accumulators see each column's own type (int, double, ...). The
        // grouping key is a borrowed byte-row view into the batch's encoded keys — no per-row
        // allocation; a key is materialized once per touched group in `accumulate_grouped`.
        let mut grouped: ahash::HashMap<(i64, i64, Row<'_>), Vec<u32>> = ahash::HashMap::default();
        let mut windows = Vec::new();
        for row in 0..batch.num_rows() {
            let key = keys_encoded.row(row);
            self.windows_for(ts.value(row), &mut windows);
            // Drop windows already closed by the watermark — the row is late for them. The host's
            // per-row assigner drops such rows; the columnar assigner slices batches so a closing
            // watermark precedes any row it makes late, and this is where that row is discarded.
            windows.retain(|(_, end)| *end > self.current_watermark);
            for &(start, end) in windows.iter() {
                grouped.entry((start, end, key)).or_default().push(row as u32);
            }
        }
        self.accumulate_grouped(grouped, &values)
    }

    /// Window-attached local half: each row already carries its window as `window_start`/`window_end`
    /// columns (epoch millis) — the output of an upstream window aggregate, re-aggregated per window
    /// (Nexmark q5's hot-items MAX over per-auction counts). Unlike `update`, there is no rowtime to
    /// slice: the row folds into exactly the one window it names (dropping rows whose window the
    /// watermark has already closed). `flush_partial` then emits the partials keyed by window end.
    pub(crate) fn update_attached(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        self.snapshot_cache = None;
        let starts = column_i64(batch, "window_start");
        let ends = column_i64(batch, "window_end");
        let values: Vec<&ArrayRef> = (0..self.aggregates.len())
            .map(|i| batch.column_by_name(&format!("value{i}")).expect("missing value column"))
            .collect();
        let key_arrays = key_arrays(batch);
        self.key_types = key_types(&key_arrays);
        let keys_encoded = encode_keys(&mut self.key_converter, &key_arrays, batch.num_rows());

        // No late-data drop here (unlike `update`): the upstream window aggregate emits each window's
        // rows exactly at the watermark that closes it, so a re-aggregation over the same window bounds
        // legitimately receives rows whose end equals the current watermark. They fold into their window
        // and the immediately-following flush(watermark) emits it — the re-aggregation window closes with
        // the same watermark as its input.
        let mut grouped: ahash::HashMap<(i64, i64, Row<'_>), Vec<u32>> = ahash::HashMap::default();
        for row in 0..batch.num_rows() {
            let key = keys_encoded.row(row);
            grouped.entry((starts.value(row), ends.value(row), key)).or_default().push(row as u32);
        }
        self.accumulate_grouped(grouped, &values)
    }

    /// Folds the grouped row positions into their (window, key) accumulators. The value columns are
    /// sliced by type-agnostic `take`, so each accumulator sees its column's own type. When a memory
    /// budget is set, each touched group's footprint change is folded into the tracked state size —
    /// measuring only touched groups keeps accounting O(batch), not O(open state).
    fn accumulate_grouped(
        &mut self,
        grouped: ahash::HashMap<(i64, i64, Row<'_>), Vec<u32>>,
        values: &[&ArrayRef],
    ) -> Result<(), DataFusionError> {
        let track = self.memory.tracking();
        for ((start, end, key), rows) in grouped {
            let indices = UInt32Array::from(rows);
            let columns: Vec<ArrayRef> =
                values.iter().map(|v| take(v, &indices, None).expect("failed to take values")).collect();
            let key = key.owned();
            let mut delta = 0isize;
            if track {
                delta = match self.windows.get(&end).and_then(|w| w.keys.get(&key)) {
                    Some(accumulators) => -(accumulators_bytes(accumulators) as isize),
                    None => owned_row_bytes(&key) as isize,
                };
            }
            let accumulators = self.accumulators(start, end, key);
            for (i, accumulator) in accumulators.iter_mut().enumerate() {
                accumulator.update_batch(std::slice::from_ref(&columns[i])).expect("failed to update");
            }
            if track {
                delta += accumulators_bytes(accumulators) as isize;
                self.memory.record(delta);
            }
        }
        self.memory.account()
    }

    /// Finalizes and removes closed windows, emitting
    /// `[key, window_start, window_end, result0..resultN-1]`. The end is carried explicitly since
    /// cumulative windows sharing a start differ by it; each result column takes the aggregate's
    /// own output type.
    pub(crate) fn flush(&mut self, watermark: i64) -> RecordBatch {
        self.snapshot_cache = None;
        self.current_watermark = self.current_watermark.max(watermark);
        let n = self.aggregates.len();
        let mut keys: Vec<OwnedRow> = Vec::new();
        let mut starts = Vec::new();
        let mut ends = Vec::new();
        let mut results: Vec<Vec<ScalarValue>> = vec![Vec::new(); n];
        for end in self.closed_windows(watermark) {
            let window = self.windows.remove(&end).expect("window present");
            let start = window.start;
            let mut group: Vec<(OwnedRow, Vec<Box<dyn Accumulator>>)> =
                window.keys.into_iter().collect();
            group.sort_by(|(a, _), (b, _)| a.cmp(b));
            for (key, mut accumulators) in group {
                self.forget_group_bytes(&key, &accumulators);
                keys.push(key);
                starts.push(start);
                ends.push(end);
                for (i, accumulator) in accumulators.iter_mut().enumerate() {
                    results[i].push(accumulator.evaluate().expect("failed to finalize"));
                }
            }
        }
        self.memory.account_shrink();

        let mut fields = key_fields(&self.key_types);
        let mut columns = decode_keys(self.key_converter.as_ref(), &keys, &self.key_types);
        fields.push(Field::new("window_start", DataType::Int64, false));
        fields.push(Field::new("window_end", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(starts)));
        columns.push(Arc::new(Int64Array::from(ends)));
        for (i, scalars) in results.into_iter().enumerate() {
            // Nullable: a SUM whose window saw only NULL values (or whose decimal sum overflowed)
            // evaluates to NULL, matching the host.
            fields.push(Field::new(format!("result{i}"), self.aggregates[i].result_type(), true));
            columns.push(scalars_to_array(scalars, &self.aggregates[i].result_type()));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build result batch")
    }

    /// Local half of two-phase aggregation: emits each closed window's per-aggregate partial state
    /// as `[key, partial0..partialN-1, slice_end]`. Single-field partials (sum/min/max/count).
    pub(crate) fn flush_partial(&mut self, watermark: i64) -> RecordBatch {
        self.snapshot_cache = None;
        self.current_watermark = self.current_watermark.max(watermark);
        let n = self.aggregates.len();
        let mut keys: Vec<OwnedRow> = Vec::new();
        let mut slice_ends = Vec::new();
        let mut partials: Vec<Vec<ScalarValue>> = vec![Vec::new(); n];
        for end in self.closed_windows(watermark) {
            let window = self.windows.remove(&end).expect("window present");
            let mut group: Vec<(OwnedRow, Vec<Box<dyn Accumulator>>)> =
                window.keys.into_iter().collect();
            group.sort_by(|(a, _), (b, _)| a.cmp(b));
            for (key, mut accumulators) in group {
                self.forget_group_bytes(&key, &accumulators);
                keys.push(key);
                slice_ends.push(end);
                for (i, accumulator) in accumulators.iter_mut().enumerate() {
                    let state = accumulator.state().expect("state");
                    partials[i].push(state.into_iter().next().expect("single-field partial"));
                }
            }
        }
        self.memory.account_shrink();

        let mut fields = key_fields(&self.key_types);
        let mut columns = decode_keys(self.key_converter.as_ref(), &keys, &self.key_types);
        for (i, scalars) in partials.into_iter().enumerate() {
            // Nullable: a SUM partial is Flink's nullable-sum buffer — NULL for an all-NULL bundle
            // or an overflowed decimal bundle (the global's merge skips it, as the host's does).
            let partial_type = self.aggregates[i].state_fields()[0].data_type().clone();
            fields.push(Field::new(format!("partial{i}"), partial_type.clone(), true));
            columns.push(scalars_to_array(scalars, &partial_type));
        }
        fields.push(Field::new("slice_end", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(slice_ends)));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build partial batch")
    }

    /// Global half of two-phase aggregation: merges incoming partials
    /// `[key, partial0..partialN-1, slice_end]` into the window each slice belongs to.
    pub(crate) fn update_partial(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        self.snapshot_cache = None;
        let n = self.aggregates.len();
        let key_arrays = key_arrays(batch);
        self.key_types = key_types(&key_arrays);
        let keys_encoded = encode_keys(&mut self.key_converter, &key_arrays, batch.num_rows());
        let slice_ends = column_i64(batch, "slice_end");
        // Partials are read as whole columns and merged a row-slice at a time, so any partial type
        // (int64 sum/count, float64 sum, …) flows through without per-type handling here.
        let partials: Vec<&ArrayRef> =
            (0..n).map(|i| batch.column_by_name(&format!("partial{i}")).expect("partial")).collect();

        let track = self.memory.tracking();
        for row in 0..batch.num_rows() {
            let slice_end = slice_ends.value(row);
            let key = keys_encoded.row(row).owned();
            for (start, end) in self.partial_windows(slice_end) {
                let mut delta = 0isize;
                if track {
                    delta = match self.windows.get(&end).and_then(|w| w.keys.get(&key)) {
                        Some(accumulators) => -(accumulators_bytes(accumulators) as isize),
                        None => owned_row_bytes(&key) as isize,
                    };
                }
                let accumulators = self.accumulators(start, end, key.clone());
                for (i, accumulator) in accumulators.iter_mut().enumerate() {
                    accumulator
                        .merge_batch(&[partials[i].slice(row, 1)])
                        .expect("failed to merge partial");
                }
                if track {
                    delta += accumulators_bytes(accumulators) as isize;
                    self.memory.record(delta);
                }
            }
        }
        self.memory.account()
    }

    /// The `(start, end)` windows a slice ending at `slice_end` belongs to in the global merge.
    /// Tumbling/hopping: the `window_millis / slide_millis` fixed-size windows sharing this slice
    /// (one for tumbling, several for an overlapping hopping window). Cumulative: the nested windows
    /// of its bucket from this slice's end up to the bucket's max-size end, all sharing the bucket
    /// start — the slice (rows in `(slice_end - step, slice_end]`) contributes to every cumulative
    /// window that ends at or after it.
    fn partial_windows(&self, slice_end: i64) -> Vec<(i64, i64)> {
        if self.cumulative {
            let base = (slice_end - 1).div_euclid(self.window_millis) * self.window_millis;
            let mut windows = Vec::new();
            let mut end = slice_end;
            while end <= base + self.window_millis {
                windows.push((base, end));
                end += self.slide_millis;
            }
            windows
        } else {
            let num_windows = self.window_millis / self.slide_millis;
            (0..num_windows)
                .map(|j| {
                    let end = slice_end + j * self.slide_millis;
                    (end - self.window_millis, end)
                })
                .collect()
        }
    }

    /// Serializes every open window's accumulator state as an Arrow batch (one row per (window,
    /// key): window end, window start, key, then every accumulator's state fields in order), encoded
    /// with Arrow IPC. Carries arbitrary multi-aggregate, multi-field state through one path.
    pub(crate) fn snapshot(&mut self) -> Vec<u8> {
        write_ipc(&self.snapshot_batch())
    }

    fn snapshot_batch(&mut self) -> RecordBatch {
        let state_fields: Vec<Field> =
            self.aggregates.iter().flat_map(WindowAggregate::state_fields).collect();

        let mut ends: Vec<i64> = Vec::new();
        let mut starts: Vec<i64> = Vec::new();
        let mut keys: Vec<OwnedRow> = Vec::new();
        let mut state_columns: Vec<Vec<ScalarValue>> = vec![Vec::new(); state_fields.len()];
        for (end, window) in self.windows.iter_mut() {
            for (key, accumulators) in window.keys.iter_mut() {
                ends.push(*end);
                starts.push(window.start);
                keys.push(key.clone());
                let mut column = 0;
                for accumulator in accumulators.iter_mut() {
                    for scalar in accumulator.state().expect("state") {
                        state_columns[column].push(scalar);
                        column += 1;
                    }
                }
            }
        }

        let mut fields = vec![
            Field::new("window_end", DataType::Int64, false),
            Field::new("window_start", DataType::Int64, false),
        ];
        let mut columns: Vec<ArrayRef> =
            vec![Arc::new(Int64Array::from(ends)), Arc::new(Int64Array::from(starts))];
        fields.extend(key_fields(&self.key_types));
        columns.extend(decode_keys(self.key_converter.as_ref(), &keys, &self.key_types));
        fields.extend(state_fields.iter().cloned());
        for (index, scalars) in state_columns.into_iter().enumerate() {
            columns.push(if scalars.is_empty() {
                new_empty_array(state_fields[index].data_type())
            } else {
                ScalarValue::iter_to_array(scalars).expect("state array")
            });
        }

        // Carry the watermark in schema metadata so late-data dropping survives a restore.
        let metadata = std::collections::HashMap::from([(
            "current_watermark".to_string(),
            self.current_watermark.to_string(),
        )]);
        RecordBatch::try_new(Arc::new(Schema::new(fields).with_metadata(metadata)), columns)
            .expect("failed to build snapshot batch")
    }

    pub(crate) fn snapshot_partitions(
        &mut self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<u8>> {
        self.materialize_raw_keyed_snapshots(max_parallelism, timestamp_precisions);
        self.snapshot_cache
            .take()
            .expect("window raw snapshot cache")
            .snapshots
    }

    fn materialize_raw_keyed_snapshots(
        &mut self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) {
        if self.snapshot_cache.as_ref().is_some_and(|cache| {
            cache.max_parallelism == max_parallelism
                && cache.timestamp_precisions.as_slice() == timestamp_precisions
        }) {
            return;
        }
        let state_field_count: usize = self
            .aggregates
            .iter()
            .map(|aggregate| aggregate.state_fields().len())
            .sum();
        let batch = self.snapshot_batch();
        let key_count = batch.num_columns() - 2 - state_field_count;
        let key_columns: Vec<usize> = (2..2 + key_count).collect();
        let mut rows_by_group: BTreeMap<i32, Vec<u32>> = BTreeMap::new();
        for row in 0..batch.num_rows() {
            let key_group = flink_key_group(
                binary_row_hash(&batch, &key_columns, row, timestamp_precisions),
                max_parallelism,
            ) as i32;
            rows_by_group.entry(key_group).or_default().push(row as u32);
        }
        let mut snapshots = BTreeMap::new();
        for (key_group, rows) in rows_by_group {
            let indices = UInt32Array::from(rows);
            let columns = batch
                .columns()
                .iter()
                .map(|column| take(column, &indices, None).expect("partition window snapshot"))
                .collect();
            let partition = RecordBatch::try_new(batch.schema(), columns)
                .expect("partitioned window snapshot");
            snapshots.insert(key_group, write_ipc(&partition));
        }
        self.snapshot_cache = Some(WindowSnapshotCache {
            max_parallelism,
            timestamp_precisions: timestamp_precisions.to_vec(),
            snapshots,
        });
    }

    pub(crate) fn restore(
        window_millis: i64,
        slide_millis: i64,
        cumulative: bool,
        value_types: Vec<i64>,
        kinds: Vec<i64>,
        bytes: &[u8],
    ) -> Self {
        let mut aggregator =
            TumblingAggregator::new(window_millis, slide_millis, cumulative, value_types, kinds);
        let field_counts: Vec<usize> =
            aggregator.aggregates.iter().map(|a| a.state_fields().len()).collect();
        let state_field_total: usize = field_counts.iter().sum();
        let reader = arrow::ipc::reader::StreamReader::try_new(bytes, None)
            .expect("failed to open snapshot reader");
        for batch in reader {
            let batch = batch.expect("failed to read snapshot");
            if let Some(watermark) = batch.schema().metadata().get("current_watermark") {
                aggregator.current_watermark = watermark.parse().expect("watermark");
            }
            // Columns are [window_end, window_start, key0..key{arity-1}, state fields...].
            let arity = batch.num_columns() - 2 - state_field_total;
            let ends =
                batch.column(0).as_any().downcast_ref::<Int64Array>().expect("window_end int64");
            let starts =
                batch.column(1).as_any().downcast_ref::<Int64Array>().expect("window_start int64");
            let key_arrays: Vec<&ArrayRef> = (0..arity).map(|j| batch.column(2 + j)).collect();
            aggregator.key_types = key_types(&key_arrays);
            let keys_encoded =
                encode_keys(&mut aggregator.key_converter, &key_arrays, batch.num_rows());
            for row in 0..batch.num_rows() {
                let key = keys_encoded.row(row).owned();
                let mut column = 2 + arity;
                for (i, accumulator) in aggregator
                    .accumulators(starts.value(row), ends.value(row), key)
                    .iter_mut()
                    .enumerate()
                {
                    let count = field_counts[i];
                    let state: Vec<ArrayRef> =
                        (column..column + count).map(|c| batch.column(c).slice(row, 1)).collect();
                    accumulator.merge_batch(&state).expect("failed to restore window");
                    column += count;
                }
            }
        }
        aggregator
    }

    pub(crate) fn restore_partitions(
        window_millis: i64,
        slide_millis: i64,
        cumulative: bool,
        value_types: Vec<i64>,
        kinds: Vec<i64>,
        snapshots: &[Vec<u8>],
    ) -> Self {
        let mut batches = Vec::new();
        let mut watermark = i64::MIN;
        for bytes in snapshots {
            for batch in read_ipc_if_present(bytes) {
                if let Some(current) = batch.schema().metadata().get("current_watermark") {
                    watermark = watermark.max(current.parse().expect("watermark"));
                }
                batches.push(batch);
            }
        }
        if batches.is_empty() {
            return TumblingAggregator::new(window_millis, slide_millis, cumulative, value_types, kinds);
        }
        let schema = batches[0].schema();
        let combined = concat_batches(&schema, batches.iter()).expect("merge window raw partitions");
        let mut restored = TumblingAggregator::restore(
            window_millis,
            slide_millis,
            cumulative,
            value_types,
            kinds,
            &write_ipc(&combined),
        );
        restored.current_watermark = watermark;
        restored
    }
}

state_bytes_getter!(Java_io_github_jordepic_streamfusion_Native_tumblingAggregatorStateBytes, TumblingAggregator);

/// Runs a batch through the stateless windowing table function, exporting the fanned-out batch with
/// window_start/window_end/window_time appended. Stateless, so there is no handle to create or close.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_assignWindows<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
    time_col: jint,
    window_millis: jlong,
    slide_millis: jlong,
    cumulative: jboolean,
    proctime: jboolean,
    proctime_now_millis: jlong,
) {
    let batch = import_record_batch(in_array_address, in_schema_address);
    let result = assign_windows(
        &batch,
        time_col as usize,
        window_millis,
        slide_millis,
        cumulative != 0,
        (proctime != 0).then_some(proctime_now_millis),
    );
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Runs an event-time tumbling-window sum over a batch from the JVM: rows are bucketed by the start
/// of the `window_millis`-wide window their `ts` falls in, and `value` is summed per bucket. This
/// is the first aggregating operator and the core of the initial target envelope.
///
/// The window assignment and grouped aggregation run as a DataFusion plan on the shared runtime,
/// and the per-window result batch is exported back. Aggregation across batch boundaries, where the
/// operator must hold partial windows until a watermark closes them, is a later step.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_tumblingSum<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    window_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let ffi_array = unsafe {
        std::ptr::replace(in_array_address as *mut FFI_ArrowArray, FFI_ArrowArray::empty())
    };
    let ffi_schema = unsafe {
        std::ptr::replace(in_schema_address as *mut FFI_ArrowSchema, FFI_ArrowSchema::empty())
    };

    let mut data = unsafe { from_ffi(ffi_array, &ffi_schema) }.expect("failed to import Arrow batch");
    data.align_buffers();
    let batch = RecordBatch::from(StructArray::from(data));

    let window = format!("ts - (ts % {window_millis})");
    let query = format!(
        "SELECT {window} AS window_start, SUM(value) AS total \
         FROM events GROUP BY {window} ORDER BY window_start"
    );

    let result = runtime().block_on(async move {
        let ctx = SessionContext::new();
        ctx.register_batch("events", batch).expect("failed to register batch");
        let frame = ctx.sql(&query).await.expect("failed to plan aggregation");
        let mut stream = frame.execute_stream().await.expect("failed to execute plan");
        let schema = stream.schema();
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.expect("failed to pull batch"));
        }
        concat_batches(&schema, &batches).expect("failed to assemble result")
    });

    let out_data = StructArray::from(result).to_data();
    let out_array = FFI_ArrowArray::new(&out_data);
    let out_schema =
        FFI_ArrowSchema::try_from(out_data.data_type()).expect("failed to export Arrow schema");
    unsafe {
        std::ptr::write(out_array_address as *mut FFI_ArrowArray, out_array);
        std::ptr::write(out_schema_address as *mut FFI_ArrowSchema, out_schema);
    }
}

/// Creates a stateful tumbling-window aggregator and returns an opaque handle to it. The handle
/// owns native state that lives across calls; the JVM must release it with the matching close.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createTumblingAggregator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    window_millis: jlong,
    slide_millis: jlong,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let aggregator =
        TumblingAggregator::new(window_millis, slide_millis, false, value_types, kinds)
            .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, aggregator)
}

/// Creates a stateful cumulative-window aggregator (nested windows of `step` up to `max_size`) and
/// returns an opaque handle. It shares the aligned-window engine and every other call (update,
/// flush, snapshot, close) with the tumbling handle; only the window assignment differs.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createCumulativeAggregator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    max_size_millis: jlong,
    step_millis: jlong,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let aggregator = TumblingAggregator::new(max_size_millis, step_millis, true, value_types, kinds)
        .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, aggregator)
}

/// Folds a batch from the JVM into the aggregator's open windows. Produces no output; results are
/// emitted later when a watermark closes windows.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_updateTumblingAggregator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
    // The batch must drop before a throw: its release callback upcalls into the JVM (the C Data
    // producer side), which cannot run with this thread's exception pending.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        aggregator.update(&batch)
    };
    if let Err(e) = result {
        throw_memory_limit(&mut env, &e.to_string());
    }
}

/// Window-attached local half: folds a batch whose rows carry explicit `window_start`/`window_end`
/// columns (an upstream window aggregate's output being re-aggregated per window — Nexmark q5).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_updateAttachedTumblingAggregator<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        aggregator.update_attached(&batch)
    };
    if let Err(e) = result {
        throw_memory_limit(&mut env, &e.to_string());
    }
}

/// Emits the windows the given watermark has closed as a batch and drops them from state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushTumblingAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
    let result = aggregator.flush(watermark_millis);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Local two-phase half: merges a batch of partials `[key, partial, slice_end]` into the windows.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_updatePartialTumblingAggregator<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        aggregator.update_partial(&batch)
    };
    if let Err(e) = result {
        throw_memory_limit(&mut env, &e.to_string());
    }
}

/// Local two-phase half: emits the partial state of the windows the watermark has closed.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushPartialTumblingAggregator<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
    let result = aggregator.flush_partial(watermark_millis);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Releases the aggregator and its native state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeTumblingAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<TumblingAggregator>(handle));
    }
}

/// Serializes the aggregator's open windows so the JVM can store them in a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotTumblingAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
    env.byte_array_from_slice(&aggregator.snapshot())
        .expect("failed to allocate snapshot array")
        .into_raw()
}

/// Rebuilds an aggregator from a snapshot taken by a prior run and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreTumblingAggregator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    window_millis: jlong,
    slide_millis: jlong,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    snapshot: JByteArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read snapshot");
    let aggregator =
        TumblingAggregator::restore(window_millis, slide_millis, false, value_types, kinds, &bytes)
            .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, aggregator)
}

/// Rebuilds a cumulative-window aggregator from a snapshot taken by a prior run.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreCumulativeAggregator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    max_size_millis: jlong,
    step_millis: jlong,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    snapshot: JByteArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read snapshot");
    let aggregator =
        TumblingAggregator::restore(max_size_millis, step_millis, true, value_types, kinds, &bytes)
            .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, aggregator)
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotTumblingAggregatorPartitions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
    let precisions: Vec<i32> = read_int_array(&env, &timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    keyed_state_partition_array(
        &mut env,
        aggregator.snapshot_partitions(max_parallelism as usize, &precisions),
        "fixed-window",
    )
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreTumblingAggregatorPartitions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    window_millis: jlong,
    slide_millis: jlong,
    cumulative: jboolean,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let count = env
        .get_array_length(&snapshots)
        .expect("read window raw partition count");
    let mut restored = Vec::with_capacity(count as usize);
    for index in 0..count {
        let bytes = JByteArray::from(
            env.get_object_array_element(&snapshots, index)
                .expect("read window raw partition"),
        );
        restored.push(
            env.convert_byte_array(&bytes)
                .expect("read window raw partition bytes"),
        );
    }
    let aggregator = TumblingAggregator::restore_partitions(
        window_millis,
        slide_millis,
        cumulative != 0,
        value_types,
        kinds,
        &restored,
    )
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, aggregator)
}
