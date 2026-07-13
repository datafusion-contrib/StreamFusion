use super::*;

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
        self.0.flush(watermark)
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
        self.0.flush(watermark)
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
        self.0.push(batch).expect("budget exceeded");
    }

    pub fn flush(&mut self, watermark: i64) -> RecordBatch {
        self.0.flush(watermark).expect("budget exceeded")
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

    pub fn push(&mut self, batch: &RecordBatch) -> RecordBatch {
        self.0.push(batch).expect("budget exceeded")
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
        self.0.push(batch).expect("budget exceeded")
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
        self.0.push(batch).expect("budget exceeded");
    }

    pub fn flush(&mut self, watermark: i64) -> RecordBatch {
        self.0.flush(watermark).expect("budget exceeded")
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
        self.0.push(batch).expect("budget exceeded")
    }

    pub fn flush(&mut self) -> RecordBatch {
        self.0.flush_mini_batch().expect("budget exceeded")
    }
}

/// The columnar exchange's by-key split (one call per batch, as the shuffle drives it).
pub fn split_by_key(
    batch: &RecordBatch,
    key_columns: &[usize],
    num_partitions: usize,
) -> Vec<(usize, RecordBatch)> {
    partition_batch(
        batch,
        key_columns,
        &vec![-1; key_columns.len()],
        num_partitions,
        num_partitions,
    )
}

/// A source-edge JSON decoder (one document per input row -> a typed columnar batch).
#[cfg(any(feature = "json", test))]
pub struct JsonDecode(JsonDecoder);

#[cfg(any(feature = "json", test))]
impl JsonDecode {
    pub fn new(schema: SchemaRef) -> Self {
        JsonDecode(JsonDecoder::new(schema, crate::json::JsonEnv::default()))
    }

    pub fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        self.0.decode(bodies)
    }
}

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
        self.0.update(batch).expect("budget exceeded")
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
        self.0.push(batch).expect("budget exceeded")
    }

    pub fn flush(&mut self) -> RecordBatch {
        self.0.flush_mini_batch().expect("budget exceeded")
    }
}

/// The transient local half of a two-phase GROUP BY, driven by logical bundle boundaries.
pub struct LocalGroupBy(LocalGroupAggregator);

impl LocalGroupBy {
    pub fn sum(value_column: i64, key_columns: Vec<usize>) -> Self {
        LocalGroupBy(LocalGroupAggregator::new(
            vec![0],
            vec![0],
            vec![value_column],
            vec![-1],
            key_columns,
            Vec::new(),
        ))
    }

    pub fn update(&mut self, batch: &RecordBatch) {
        self.0.update(batch).expect("budget exceeded");
    }

    pub fn flush(&mut self) -> RecordBatch {
        self.0.flush()
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
