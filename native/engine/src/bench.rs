use super::*;

/// The production Flink scalar UDFs, including scalar adaptation and result allocation.
pub fn flink_scalar_function(op: i64, arity: usize) -> datafusion::logical_expr::ScalarUDF {
    crate::flink_functions::function(op, arity).expect("registered scalar benchmark")
}

/// Binary ELT adaptation and allocation, through the same production UDF used by Calc.
pub fn binary_elt_function(arity: usize, width: i32) -> datafusion::logical_expr::ScalarUDF {
    crate::flink_functions::binary_elt::function(arity, width)
}

/// A filter predicate compiled once (on the first `run`) and reused, as the operator uses it.
pub struct Filter(FilterExpression);

impl Filter {
    pub fn new(
        kinds: Vec<i64>,
        payload: Vec<i64>,
        child_counts: Vec<i64>,
        longs: Vec<i64>,
        doubles: Vec<f64>,
        strings: Vec<Option<String>>,
    ) -> Self {
        Filter(FilterExpression {
            kinds,
            payload,
            child_counts,
            longs,
            doubles,
            strings,
            compiled: None,
        })
    }

    pub fn run(&mut self, batch: RecordBatch) -> RecordBatch {
        self.0.filter(batch)
    }
}

/// A tumbling-window aggregator driven by `update`/`flush`, as the stateful operator drives it.
pub struct Tumbling(TumblingAggregator);

impl Tumbling {
    pub fn new(window_millis: i64, value_type: i64, kinds: Vec<i64>) -> Self {
        let value_types = vec![value_type; kinds.len()];
        Tumbling(TumblingAggregator::new(
            window_millis,
            window_millis,
            false,
            value_types,
            kinds,
        ))
    }

    /// The accounted variant: state is tracked against `budget_bytes`, measuring the
    /// memory-accounting overhead against `new`.
    pub fn with_budget(
        window_millis: i64,
        value_type: i64,
        kinds: Vec<i64>,
        budget_bytes: i64,
    ) -> Self {
        let value_types = vec![value_type; kinds.len()];
        Tumbling(
            TumblingAggregator::new(window_millis, window_millis, false, value_types, kinds)
                .with_memory_budget(budget_bytes)
                .expect("empty state fits any budget"),
        )
    }

    pub fn update(&mut self, batch: &RecordBatch) {
        self.0.update(batch).expect("budget exceeded");
    }

    pub fn flush(&mut self, watermark: i64) -> RecordBatch {
        self.0.flush(watermark).expect("memory-backed flush")
    }
}

/// A session-window aggregator driven by `update`/`flush`.
pub struct Session(SessionAggregator);

impl Session {
    pub fn new(gap_millis: i64, value_type: i64, kinds: Vec<i64>) -> Self {
        let value_types = vec![value_type; kinds.len()];
        Session(SessionAggregator::new(gap_millis, value_types, kinds))
    }

    pub fn update(&mut self, batch: &RecordBatch) {
        self.0.update(batch).expect("budget exceeded");
    }

    pub fn flush(&mut self, watermark: i64) -> RecordBatch {
        self.0.flush(watermark).expect("memory-backed flush")
    }
}

/// A columnar OVER operator driven by push/flush, as the stateful operator drives it.
pub struct Over(OverWindowAggregator);

impl Over {
    pub fn new(
        value_type: i64,
        kinds: Vec<i64>,
        rt_column: usize,
        value_column: Option<usize>,
        key_columns: Vec<usize>,
    ) -> Self {
        let value_types = vec![value_type; kinds.len()];
        let value_columns = match value_column {
            Some(column) => vec![column; kinds.len()],
            None => Vec::new(),
        };
        Over(OverWindowAggregator::new(
            value_types,
            kinds,
            rt_column,
            value_columns,
            key_columns,
            0,
            0,
            false,
        ))
    }

    pub fn push(&mut self, batch: RecordBatch) {
        self.0.push(batch, 0).expect("budget exceeded");
    }

    pub fn flush(&mut self, watermark: i64) -> RecordBatch {
        self.0.flush(watermark, 0).expect("budget exceeded")
    }
}

/// The bounded-frame variant of {@link Over} (`ROWS`/`RANGE … PRECEDING`), which buffers rows per
/// key and recomputes each row's frame.
impl Over {
    pub fn bounded(
        value_type: i64,
        kinds: Vec<i64>,
        rt_column: usize,
        value_column: usize,
        key_columns: Vec<usize>,
        rows_frame: bool,
        frame_offset: i64,
    ) -> Self {
        let value_types = vec![value_type; kinds.len()];
        let value_columns = vec![value_column; kinds.len()];
        let frame_kind = if rows_frame { 1 } else { 2 };
        Over(OverWindowAggregator::new(
            value_types,
            kinds,
            rt_column,
            value_columns,
            key_columns,
            frame_kind,
            frame_offset,
            false,
        ))
    }
}

/// A retracting Top-N ranker (changelog input, full per-partition buffers), as the operator drives it.
pub struct RetractTopN(RetractableTopNRanker);

impl RetractTopN {
    /// `sort_columns` are (index, ascending) pairs (nulls-last).
    pub fn new(
        partition_columns: Vec<usize>,
        sort_columns: Vec<(usize, bool)>,
        limit: i64,
    ) -> Self {
        let sort = sort_columns
            .into_iter()
            .map(|(index, ascending)| SortColumn {
                index,
                ascending,
                nulls_first: false,
            })
            .collect();
        RetractTopN(RetractableTopNRanker::new(
            partition_columns,
            sort,
            0,
            limit,
            false,
        ))
    }

    pub fn new_mini_batch(
        partition_columns: Vec<usize>,
        sort_columns: Vec<(usize, bool)>,
        limit: i64,
    ) -> Self {
        let mut ranker = Self::new(partition_columns, sort_columns, limit);
        ranker.0 = ranker.0.with_net_diff(true);
        ranker
    }

    pub fn push(&mut self, batch: &RecordBatch) -> RecordBatch {
        self.0.push(batch, 0).expect("budget exceeded")
    }

    pub fn flush(&mut self) -> RecordBatch {
        self.0.flush_net_diff()
    }
}

/// INNER updating join with join-key uniqueness on both sides and optional logical bundling.
pub struct UniqueUpdatingJoin(UpdatingJoiner);

impl UniqueUpdatingJoin {
    pub fn new(schema: SchemaRef, mini_batch: bool) -> Self {
        UniqueUpdatingJoin(
            UpdatingJoiner::new(
                vec![0],
                vec![0],
                JoinKind::Inner,
                schema.clone(),
                schema,
                None,
            )
            .with_mini_batch(mini_batch),
        )
    }

    pub fn push(&mut self, batch: &RecordBatch, left: bool) -> RecordBatch {
        self.0.push(batch, left, 0).expect("budget exceeded")
    }

    pub fn flush(&mut self) -> RecordBatch {
        self.0.flush_mini_batch().expect("budget exceeded")
    }

    pub fn snapshot_partitions(&self, max_parallelism: usize) -> usize {
        self.0
            .snapshot_partitions(max_parallelism)
            .values()
            .map(Vec::len)
            .sum()
    }
}

/// Append-only Top-N with explicit logical mini-batch flushes.
pub struct AppendTopN(TopNRanker);

impl AppendTopN {
    pub fn new(
        partition_columns: Vec<usize>,
        sort_columns: Vec<(usize, bool)>,
        limit: i64,
        output_rank_number: bool,
        net_diff: bool,
    ) -> Self {
        let sort = sort_columns
            .into_iter()
            .map(|(index, ascending)| SortColumn {
                index,
                ascending,
                nulls_first: false,
            })
            .collect();
        AppendTopN(TopNRanker::new(
            partition_columns,
            sort,
            limit,
            output_rank_number,
            net_diff,
        ))
    }

    pub fn push(&mut self, batch: &RecordBatch) -> RecordBatch {
        self.0.push(batch, 0).expect("budget exceeded")
    }

    pub fn flush(&mut self) -> RecordBatch {
        self.0.flush_net_diff()
    }
}

/// The watermark-buffered event-time keep-first deduplicator, as the operator drives it.
pub struct KeepFirstDedup(KeepFirstDeduplicator);

impl KeepFirstDedup {
    pub fn new(partition_columns: Vec<usize>, rt_column: usize) -> Self {
        KeepFirstDedup(KeepFirstDeduplicator::new(partition_columns, rt_column))
    }

    pub fn push(&mut self, batch: &RecordBatch) {
        self.0.push(batch, 0).expect("budget exceeded");
    }

    pub fn flush(&mut self, watermark: i64) -> RecordBatch {
        self.0.flush(watermark, 0).expect("budget exceeded")
    }
}

/// Eager keep-last deduplication with optional logical-bundle changelog finalization.
pub struct KeepLastDedup(KeepLastDeduplicator);

impl KeepLastDedup {
    pub fn new(partition_columns: Vec<usize>, mini_batch: bool) -> Self {
        KeepLastDedup(
            KeepLastDeduplicator::new(partition_columns, 0, true, false, false)
                .with_mini_batch(mini_batch),
        )
    }

    pub fn push(&mut self, batch: &RecordBatch) -> RecordBatch {
        self.0.push(batch, 0).expect("budget exceeded")
    }

    pub fn flush(&mut self) -> RecordBatch {
        self.0.flush_mini_batch().expect("budget exceeded")
    }
}

/// The columnar exchange's by-key split (one call per batch, as the shuffle drives it).
pub fn split_by_key(
    batch: &RecordBatch,
    key_columns: &[usize],
    max_parallelism: usize,
) -> Vec<(usize, RecordBatch)> {
    partition_batch(
        batch,
        key_columns,
        &vec![-1; key_columns.len()],
        max_parallelism,
        max_parallelism.min(4),
    )
}

/// A source-edge JSON decoder (one document per input row -> a typed columnar batch).
/// A non-windowed GROUP BY aggregator (update emits the changelog), as the operator drives it.
pub struct GroupBy(GroupAggregator);

impl GroupBy {
    pub fn new(
        kinds: Vec<i64>,
        value_types: Vec<i64>,
        value_columns: Vec<i64>,
        key_columns: Vec<usize>,
    ) -> Self {
        GroupBy(GroupAggregator::new(
            kinds,
            value_types,
            value_columns,
            key_columns,
            true,
        ))
    }

    pub fn update(&mut self, batch: &RecordBatch) -> RecordBatch {
        self.0.update(batch, 0).expect("budget exceeded")
    }

    pub fn mini_batch(
        kinds: Vec<i64>,
        value_types: Vec<i64>,
        value_columns: Vec<i64>,
        key_columns: Vec<usize>,
    ) -> Self {
        GroupBy(
            GroupAggregator::new(kinds, value_types, value_columns, key_columns, true)
                .with_mini_batch(),
        )
    }

    pub fn flush(&mut self) -> RecordBatch {
        self.0.flush_mini_batch().expect("budget exceeded")
    }
}

/// Changelog normalization with either immediate or explicit logical-bundle output.
pub struct Normalize(ChangelogNormalizer);

impl Normalize {
    pub fn new(key_columns: Vec<usize>, generate_update_before: bool, mini_batch: bool) -> Self {
        Normalize(
            ChangelogNormalizer::new(key_columns, generate_update_before)
                .with_mini_batch(mini_batch),
        )
    }

    pub fn push(&mut self, batch: &RecordBatch) -> RecordBatch {
        self.0.push(batch, 0).expect("budget exceeded")
    }

    pub fn flush(&mut self) -> RecordBatch {
        self.0.flush_mini_batch().expect("budget exceeded")
    }
}

/// The transient local half of a two-phase GROUP BY, driven by logical bundle boundaries.
pub struct LocalGroupBy(LocalGroupAggregator);

impl LocalGroupBy {
    pub fn new(
        kinds: Vec<i64>,
        value_types: Vec<i64>,
        value_columns: Vec<i64>,
        key_columns: Vec<usize>,
    ) -> Self {
        let aggregate_count = kinds.len();
        LocalGroupBy(LocalGroupAggregator::new(
            kinds,
            value_types,
            value_columns,
            vec![-1; aggregate_count],
            key_columns,
            Vec::new(),
        ))
    }

    pub fn sum(value_column: i64, key_columns: Vec<usize>) -> Self {
        Self::new(vec![0], vec![0], vec![value_column], key_columns)
    }

    pub fn filtered(
        kinds: Vec<i64>,
        value_types: Vec<i64>,
        value_columns: Vec<i64>,
        filter_columns: Vec<i64>,
        key_columns: Vec<usize>,
        distinct_view_sources: Vec<i64>,
    ) -> Self {
        LocalGroupBy(LocalGroupAggregator::new(
            kinds,
            value_types,
            value_columns,
            filter_columns,
            key_columns,
            distinct_view_sources,
        ))
    }

    pub fn update(&mut self, batch: &RecordBatch) {
        self.0.update(batch).expect("budget exceeded");
    }

    pub fn flush(&mut self) -> RecordBatch {
        self.0.try_flush().expect("local aggregate benchmark flush")
    }
}

/// An event-time interval joiner (push emits matches immediately), as the operator drives it.
pub struct IntervalJoin(IntervalJoiner);

impl IntervalJoin {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        left_time: usize,
        right_time: usize,
        lower: i64,
        upper: i64,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
    ) -> Self {
        IntervalJoin(IntervalJoiner::new(
            left_keys,
            right_keys,
            left_time,
            right_time,
            lower,
            upper,
            None,
            JoinKind::Inner,
            left_schema,
            right_schema,
        ))
    }

    pub fn push_left(&mut self, batch: RecordBatch) -> RecordBatch {
        self.0.push_left(batch, None).expect("budget exceeded")
    }

    pub fn push_right(&mut self, batch: RecordBatch) -> RecordBatch {
        self.0.push_right(batch, None).expect("budget exceeded")
    }
}

/// An event-time window joiner (buffer on push, join on flush), as the operator drives it.
pub struct WindowJoin(WindowJoiner);

impl WindowJoin {
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        left_window_start: usize,
        left_window_end: usize,
        right_window_start: usize,
        right_window_end: usize,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
    ) -> Self {
        WindowJoin(WindowJoiner::new(
            left_keys,
            right_keys,
            left_window_start,
            left_window_end,
            right_window_start,
            right_window_end,
            None,
            JoinKind::Inner,
            left_schema,
            right_schema,
        ))
    }

    pub fn push_left(&mut self, batch: RecordBatch) {
        self.0.push_left(batch).expect("budget exceeded");
    }

    pub fn push_right(&mut self, batch: RecordBatch) {
        self.0.push_right(batch).expect("budget exceeded");
    }

    pub fn flush(&mut self, watermark: i64) -> RecordBatch {
        self.0.flush(watermark).expect("budget exceeded")
    }
}

/// Production IPC serialization, including the stream header and owned output buffer.
pub fn encode_ipc(batch: &RecordBatch) -> Vec<u8> {
    crate::ipc::write_ipc(batch)
}

pub fn decode_ipc(bytes: &[u8]) -> Vec<RecordBatch> {
    crate::ipc::read_ipc(bytes)
}

pub fn expand_grouping_sets(batch: &RecordBatch) -> RecordBatch {
    crate::flatten::expand(batch, 2, 3, 2, false, &[0, 1, -1, -1, 1, -1], &[0, 1])
}

pub fn unnest(batch: &RecordBatch, left: bool, ordinality: bool) -> RecordBatch {
    crate::flatten::unnest_array(batch, 1, ordinality, left, false)
}

pub struct TemporalSort(crate::sorter::TemporalSorter);

impl TemporalSort {
    pub fn new(time_column: usize) -> Self {
        Self(crate::sorter::TemporalSorter::new(time_column))
    }
    pub fn push(&mut self, batch: RecordBatch) {
        self.0.push(batch).unwrap();
    }
    pub fn flush(&mut self, watermark: i64) -> RecordBatch {
        self.0.flush(watermark).unwrap()
    }
}

pub struct ArrivalFirstN(crate::first_n::FirstN);

impl ArrivalFirstN {
    pub fn new(partitions: Vec<usize>, limit: i32) -> Self {
        Self(
            crate::first_n::FirstN::new(
                partitions.clone(),
                vec![0; partitions.len()],
                limit,
                true,
                0,
                MemoryStateStore::default(),
                -1,
            )
            .unwrap(),
        )
    }
    pub fn push(&mut self, batch: &RecordBatch) -> RecordBatch {
        self.0.push(batch, 0).unwrap()
    }
}

pub struct TemporalJoin(crate::temporal_join::TemporalJoiner);

impl TemporalJoin {
    pub fn new(schema: SchemaRef) -> Self {
        Self(crate::temporal_join::TemporalJoiner::new(
            vec![0],
            vec![0],
            1,
            1,
            JoinKind::LeftOuter,
            schema.clone(),
            schema,
            None,
        ))
    }
    pub fn push(&mut self, batch: &RecordBatch, left: bool) {
        if left {
            self.0.push_left(batch, 0)
        } else {
            self.0.push_right(batch, 0)
        }
        .unwrap();
    }
    pub fn flush(&mut self, watermark: i64) -> RecordBatch {
        self.0.advance(watermark, 0).unwrap()
    }
    pub fn snapshot(&self) -> Vec<u8> {
        self.0.snapshot()
    }
}

#[cfg(feature = "rocksdb-state")]
pub struct TemporalJoinState(crate::temporal_join::TemporalJoiner);

#[cfg(feature = "rocksdb-state")]
impl TemporalJoinState {
    pub fn new(directory: &str, schema: SchemaRef, options: &str) -> Self {
        let store = crate::state::RocksTemporalJoinStore::create(
            PersistentSort::config(directory, options),
            schema.clone(),
            schema.clone(),
            &[0],
        )
        .unwrap();
        Self(
            TemporalJoin::new(schema)
                .0
                .with_key_timestamp_precisions(vec![-1])
                .with_store(store),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        directory: &str,
        schema: SchemaRef,
        options: &str,
        source: &str,
        generation: i64,
        aligned: bool,
    ) -> Self {
        let store = crate::state::RocksTemporalJoinStore::open_merged(
            PersistentSort::config(directory, options),
            schema.clone(),
            schema.clone(),
            &[0],
            &[(source.into(), generation)],
            0..=0,
            aligned,
        )
        .unwrap();
        Self(
            TemporalJoin::new(schema)
                .0
                .with_key_timestamp_precisions(vec![-1])
                .with_store(store),
        )
    }

    pub fn push(&mut self, batch: &RecordBatch, left: bool) {
        if left {
            self.0.push_left(batch, 0)
        } else {
            self.0.push_right(batch, 0)
        }
        .unwrap();
    }

    pub fn flush(&mut self, watermark: i64) -> RecordBatch {
        self.0.advance(watermark, 0).unwrap()
    }

    pub fn checkpoint(&mut self, directory: &str) -> i64 {
        self.0
            .store_mut()
            .checkpoint(directory)
            .unwrap()
            .snapshot_id
    }
}

pub struct Projection(crate::calc::CalcExpression);

impl Projection {
    pub fn columns(columns: &[usize]) -> Self {
        Self(crate::calc::CalcExpression {
            kinds: vec![0; columns.len()],
            payload: columns.iter().map(|&column| column as i64).collect(),
            child_counts: vec![0; columns.len()],
            longs: vec![],
            doubles: vec![],
            strings: vec![],
            projection_roots: (0..columns.len()).collect(),
            condition_root: -1,
            output_names: columns.iter().map(|column| format!("v{column}")).collect(),
            compiled: None,
        })
    }
    pub fn run(&mut self, batch: RecordBatch) -> RecordBatch {
        self.0.evaluate(batch)
    }
}

/// Used by the benchmark catalog to catch a newly registered scalar without a fixture.
pub fn registered_scalar_codes() -> Vec<i64> {
    (0..256)
        .filter(|&op| crate::flink_functions::function(op, 3).is_some())
        .collect()
}

pub struct KeyCodec(arrow::row::RowConverter, Vec<DataType>);
impl KeyCodec {
    pub fn new(input: &[ArrayRef]) -> Self {
        let types: Vec<_> = input
            .iter()
            .map(|array| array.data_type().clone())
            .collect();
        Self(crate::keys::key_row_converter_from_types(&types), types)
    }
    pub fn encode(&self, input: &[ArrayRef], rows: usize) -> arrow::row::Rows {
        crate::keys::encode_group_keys(&self.0, input, rows)
    }
    pub fn decode(&self, rows: &arrow::row::Rows) -> Vec<ArrayRef> {
        let keys: Vec<_> = rows.iter().map(|row| row.data()).collect();
        crate::keys::decode_byte_keys(Some(&self.0), &keys, &self.1)
    }
}
pub fn flink_key_hashes(batch: &RecordBatch, columns: &[usize], precisions: &[i32]) -> Vec<i32> {
    let mut encoder = crate::flink_key::BinaryRowBatchEncoder::new(batch, columns, precisions);
    (0..batch.num_rows()).map(|row| encoder.hash(row)).collect()
}
impl AppendTopN {
    pub fn snapshot(&self, groups: usize) -> Vec<Vec<u8>> {
        self.0.snapshot_partitions(groups).into_values().collect()
    }
    pub fn restore(snapshots: &[Vec<u8>]) -> Self {
        Self(TopNRanker::restore_partitions(
            vec![0],
            vec![0],
            vec![SortColumn {
                index: 1,
                ascending: true,
                nulls_first: false,
            }],
            4,
            false,
            false,
            snapshots,
            0,
        ))
    }
}
impl GroupBy {
    pub fn snapshot(&mut self) -> Vec<Vec<u8>> {
        self.0.snapshot_partitions(1, &[-1]).into_values().collect()
    }
    pub fn restore(snapshot: &[Vec<u8>]) -> Self {
        Self(GroupAggregator::restore_partitions(
            vec![0],
            vec![0],
            vec![1],
            vec![0],
            true,
            snapshot,
            0,
        ))
    }
}
pub struct WindowRank(crate::topn::WindowRanker);
impl WindowRank {
    pub fn new() -> Self {
        Self(crate::topn::WindowRanker::new(
            0,
            1,
            vec![2],
            vec![SortColumn {
                index: 3,
                ascending: true,
                nulls_first: false,
            }],
            4,
            true,
        ))
    }
    pub fn run(&mut self, batch: &RecordBatch) -> RecordBatch {
        self.0.push(batch).unwrap();
        self.0.flush(i64::MAX).unwrap()
    }
}
impl Default for WindowRank {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameterized kernels selected outside the numbered scalar registry.
pub fn parameterized_scalar(name: &str, ty: DataType) -> datafusion::logical_expr::ScalarUDF {
    use crate::flink_functions as f;
    use datafusion::logical_expr::ScalarUDF;
    match name {
        "decimal_cast" => ScalarUDF::new_from_impl(f::decimal::DecimalCast::new(18, 2)),
        "decimal_round" => ScalarUDF::new_from_impl(f::decimal::DecimalRound::new(18, 2)),
        "decimal_truncate" => ScalarUDF::new_from_impl(f::decimal::DecimalRound::truncate(18, 2)),
        "decimal_add" => ScalarUDF::new_from_impl(f::decimal::DecimalBinary::new(
            f::decimal::DecimalOp::Add,
            18,
            2,
        )),
        "decimal_subtract" => ScalarUDF::new_from_impl(f::decimal::DecimalBinary::new(
            f::decimal::DecimalOp::Subtract,
            18,
            2,
        )),
        "decimal_multiply" => ScalarUDF::new_from_impl(f::decimal::DecimalBinary::new(
            f::decimal::DecimalOp::Multiply,
            18,
            2,
        )),
        "decimal_to_double" => f::decimal_float::function(false),
        "decimal_to_float" => f::decimal_float::function(true),
        "integer_divide" => f::integer_divide::function(&[ty.clone(), ty]).unwrap(),
        "integer_parse" => f::integer_string::parse_function(ty, false),
        "integer_try_parse" => f::integer_string::parse_function(ty, true),
        "integer_format" => f::integer_string::format_function(264, true),
        "from_unixtime" => f::from_unixtime::function(0, "yyyy-MM-dd HH:mm:ss").unwrap(),
        "array_item" => f::array_item::function(ty),
        "map_lookup_literal" => f::map_lookup::function(ty, ScalarValue::Int64(Some(1))),
        "map_lookup_dynamic" => f::map_lookup::dynamic_function(ty, false),
        "random" => f::random::function(false, false, false),
        "random_seeded" => f::random::function(false, true, true),
        "random_integer" => f::random::function(true, false, false),
        "random_integer_seeded" => f::random::function(true, true, true),
        "current_timestamp" => f::clock::function(0, "UTC".into()),
        "current_date" => f::clock::function(2, "UTC".into()),
        "current_time" => f::clock::function(3, "UTC".into()),
        "unix_timestamp" => f::clock::function(4, "UTC".into()),
        "watermark" => f::clock::function(5, "UTC".into()),
        "float_comparison" => f::numeric::comparison(14, &[ty.clone(), ty]).unwrap(),
        other => panic!("Unknown benchmark kernel {other}"),
    }
}

pub struct UpsertMerge(crate::keyed_upsert::KeyedUpsertBuffer);
impl UpsertMerge {
    pub fn new(kind_column: usize, first: bool) -> Self {
        Self(crate::keyed_upsert::KeyedUpsertBuffer::new(
            vec![0],
            kind_column,
            if first {
                crate::keyed_upsert::Keep::First
            } else {
                crate::keyed_upsert::Keep::Last
            },
            false,
        ))
    }
    pub fn run(&mut self, batch: &RecordBatch) -> RecordBatch {
        self.0.push(batch.clone(), 0);
        self.0.flush(false).unwrap().batch
    }
}
#[cfg(feature = "rocksdb-state")]
pub struct PersistentSort(crate::sorter::TemporalSorter);
#[cfg(feature = "rocksdb-state")]
pub struct PersistentFirstDedup(KeepFirstDeduplicator);

#[cfg(feature = "rocksdb-state")]
impl PersistentFirstDedup {
    pub fn checkpoint(&mut self, directory: &str) -> i64 {
        self.0
            .store_mut()
            .checkpoint(directory)
            .unwrap()
            .snapshot_id
    }

    pub fn restore(
        directory: &str,
        schema: SchemaRef,
        options: &str,
        source: &str,
        generation: i64,
        aligned: bool,
    ) -> Self {
        let store = crate::state::RocksKeepFirstDedupStore::open_merged(
            PersistentSort::config(directory, options),
            schema,
            &[0],
            &[(source.into(), generation)],
            0..=0,
            aligned,
        )
        .unwrap();
        Self(KeepFirstDeduplicator::new(vec![0], 2).with_store(store))
    }
    pub fn new(directory: &str, schema: SchemaRef, options: &str) -> Self {
        Self::with_ttl(directory, schema, options, 0)
    }

    pub fn with_ttl(directory: &str, schema: SchemaRef, options: &str, ttl_ms: i64) -> Self {
        let mut config = PersistentSort::config(directory, options);
        config.ttl_ms = ttl_ms;
        let store = crate::state::RocksKeepFirstDedupStore::create(config, schema, &[0]).unwrap();
        Self(
            KeepFirstDeduplicator::new(vec![0], 2)
                .with_store(store)
                .with_state_ttl(ttl_ms),
        )
    }

    pub fn push(&mut self, batch: &RecordBatch) {
        self.push_at(batch, 0);
    }

    pub fn push_at(&mut self, batch: &RecordBatch, now_ms: i64) {
        self.0.push(batch, now_ms).unwrap();
    }

    pub fn flush(&mut self, watermark: i64) -> RecordBatch {
        self.flush_at(watermark, 0)
    }

    pub fn flush_at(&mut self, watermark: i64, now_ms: i64) -> RecordBatch {
        self.0.flush(watermark, now_ms).unwrap()
    }
}

/// Fixed-schema interval state probes share the production joiner with the memory oracle.
#[cfg(feature = "rocksdb-state")]
pub struct IntervalState(IntervalJoiner);

#[cfg(feature = "rocksdb-state")]
impl IntervalState {
    pub fn memory(schema: SchemaRef, outer: bool) -> Self {
        Self(
            IntervalJoiner::new(
                vec![0],
                vec![0],
                2,
                2,
                -100,
                100,
                None,
                if outer {
                    JoinKind::LeftOuter
                } else {
                    JoinKind::Inner
                },
                schema.clone(),
                schema,
            )
            .with_key_timestamp_precisions(vec![-1]),
        )
    }

    pub fn new(directory: &str, schema: SchemaRef, options: &str, outer: bool) -> Self {
        let store = crate::state::RocksIntervalBuffer::create(
            PersistentSort::config(directory, options),
            schema.clone(),
            schema.clone(),
        )
        .unwrap();
        Self(Self::memory(schema, outer).0.with_store(store))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        directory: &str,
        schema: SchemaRef,
        options: &str,
        outer: bool,
        source: &str,
        generation: i64,
        aligned: bool,
    ) -> Self {
        let store = crate::state::RocksIntervalBuffer::open_merged(
            PersistentSort::config(directory, options),
            schema.clone(),
            schema.clone(),
            &[(source.into(), generation)],
            0..=0,
            aligned,
        )
        .unwrap();
        Self(Self::memory(schema, outer).0.with_store(store))
    }

    pub fn push_left(&mut self, batch: &RecordBatch) -> RecordBatch {
        self.0.push_left(batch.clone(), None).unwrap()
    }

    pub fn push_right(&mut self, batch: &RecordBatch) -> RecordBatch {
        self.0.push_right(batch.clone(), None).unwrap()
    }

    pub fn advance(&mut self, watermark: i64) -> RecordBatch {
        self.0.advance(watermark).unwrap()
    }

    pub fn timer_deadline(&mut self) -> i64 {
        self.0.store_mut().timer_deadline()
    }

    pub fn checkpoint(&mut self, directory: &str) -> i64 {
        self.0
            .store_mut()
            .checkpoint(500, directory)
            .unwrap()
            .snapshot_id
    }
}

#[cfg(feature = "rocksdb-state")]
impl PersistentSort {
    fn config(directory: &str, options: &str) -> crate::state::rocks_store::RocksStoreConfig {
        crate::state::rocks_store::RocksStoreConfig {
            table_dir: directory.into(),
            max_parallelism: 1,
            options_json: options.into(),
            ttl_ms: 0,
            shared_resources: 0,
        }
    }
    pub fn new(directory: &str, schema: SchemaRef, options: &str) -> Self {
        let store =
            crate::state::RocksTemporalSortBuffer::create(Self::config(directory, options), schema)
                .unwrap();
        Self(crate::sorter::TemporalSorter::new(1).with_store(store))
    }
    pub fn restore(
        directory: &str,
        schema: SchemaRef,
        options: &str,
        source: &str,
        generation: i64,
        aligned: bool,
    ) -> Self {
        let store = crate::state::RocksTemporalSortBuffer::open_merged(
            Self::config(directory, options),
            schema,
            &[(source.into(), generation)],
            0..=0,
            aligned,
        )
        .unwrap();
        Self(crate::sorter::TemporalSorter::new(1).with_store(store))
    }
    pub fn push(&mut self, batch: &RecordBatch) {
        self.0.push(batch.clone()).unwrap();
    }
    pub fn flush(&mut self) -> RecordBatch {
        self.0.flush(i64::MAX).unwrap()
    }
    pub fn checkpoint(&mut self, directory: &str) -> i64 {
        self.0
            .store_mut()
            .checkpoint(directory)
            .unwrap()
            .snapshot_id
    }
}

/// Production window rank with matched memory and disk configurations.
#[cfg(feature = "rocksdb-state")]
pub struct WindowRankState(crate::topn::WindowRanker);

#[cfg(feature = "rocksdb-state")]
impl WindowRankState {
    pub fn memory(limit: i64, keep_last: bool) -> Self {
        let mut ranker = crate::topn::WindowRanker::new(
            0,
            1,
            vec![2],
            vec![SortColumn {
                index: 3,
                ascending: true,
                nulls_first: false,
            }],
            limit,
            true,
        );
        ranker.set_keep_last_on_tie(keep_last);
        Self(ranker.with_key_timestamp_precisions(vec![-1]))
    }

    pub fn new(
        directory: &str,
        schema: SchemaRef,
        options: &str,
        limit: i64,
        keep_last: bool,
    ) -> Self {
        let types = schema
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let store = crate::state::RocksWindowRankStore::create(
            PersistentSort::config(directory, options),
            &types,
            0..=0,
        )
        .unwrap();
        Self(Self::memory(limit, keep_last).0.with_store(store, schema))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        directory: &str,
        schema: SchemaRef,
        options: &str,
        limit: i64,
        keep_last: bool,
        source: &str,
        generation: i64,
        aligned: bool,
    ) -> Self {
        let types = schema
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let store = crate::state::RocksWindowRankStore::open_merged(
            PersistentSort::config(directory, options),
            &types,
            0..=0,
            &[(source.into(), generation)],
            aligned,
        )
        .unwrap();
        Self(Self::memory(limit, keep_last).0.with_store(store, schema))
    }

    pub fn push(&mut self, batch: &RecordBatch) {
        self.0.push(batch).unwrap();
    }
    pub fn flush(&mut self, watermark: i64) -> RecordBatch {
        self.0.flush(watermark).unwrap()
    }
    pub fn late_drops(&self) -> u64 {
        self.0.late_drops
    }
    pub fn timer_deadline(&self) -> i64 {
        self.0.store_timer_deadline()
    }
    pub fn checkpoint(&mut self, directory: &str) -> i64 {
        self.0.checkpoint_store(200, directory).unwrap().snapshot_id
    }
}

/// Serialized Calc plans retain production compilation, scalar adaptation, and materialization.
pub struct CalcProgram(crate::calc::CalcExpression);
impl CalcProgram {
    pub fn new(
        kinds: Vec<i64>,
        payload: Vec<i64>,
        children: Vec<i64>,
        longs: Vec<i64>,
        doubles: Vec<f64>,
        strings: Vec<Option<String>>,
    ) -> Self {
        Self(crate::calc::CalcExpression {
            kinds,
            payload,
            child_counts: children,
            longs,
            doubles,
            strings,
            projection_roots: vec![0],
            condition_root: -1,
            output_names: vec!["result".into()],
            compiled: None,
        })
    }
    pub fn run(&mut self, batch: RecordBatch) -> RecordBatch {
        self.0.evaluate(batch)
    }
}

#[cfg(feature = "rocksdb-state")]
pub struct PersistentOver(OverWindowAggregator);

#[cfg(feature = "rocksdb-state")]
impl PersistentOver {
    pub fn new(directory: &str, schema: SchemaRef, options: &str) -> Self {
        let store = crate::state::RocksOverAggStore::create(
            PersistentSort::config(directory, options),
            &rocks_over_state_types(&[0], &[0], 0, false).unwrap(),
            &[],
            &[],
            schema,
            0..=0,
        )
        .expect("persistent OVER fixture");
        Self(Self::operator().with_store(store, vec![DataType::Int64]))
    }

    pub fn restore(
        directory: &str,
        schema: SchemaRef,
        options: &str,
        source: &str,
        generation: i64,
        aligned: bool,
    ) -> Self {
        let store = crate::state::RocksOverAggStore::open_merged(
            PersistentSort::config(directory, options),
            &rocks_over_state_types(&[0], &[0], 0, false).unwrap(),
            &[],
            &[],
            schema,
            0..=0,
            &[(source.to_owned(), generation)],
            aligned,
        )
        .expect("persistent OVER restore");
        Self(Self::operator().with_store(store, vec![DataType::Int64]))
    }

    fn operator() -> OverWindowAggregator {
        OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 0, 0, false)
            .with_key_timestamp_precisions(vec![-1])
    }

    pub fn push(&mut self, batch: &RecordBatch) {
        self.0.push(batch.clone(), 0).expect("persistent OVER push");
    }

    pub fn flush(&mut self) -> RecordBatch {
        self.0.flush(i64::MAX, 0).expect("persistent OVER flush")
    }

    pub fn checkpoint(&mut self, directory: &str) -> i64 {
        self.0
            .checkpoint_store(directory)
            .expect("persistent OVER checkpoint")
            .snapshot_id
    }
}
