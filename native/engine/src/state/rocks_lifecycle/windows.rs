use super::*;

pub(crate) struct TemporalSortOptions {
    pub(crate) rt_column: i32,
    pub(crate) memory_budget_bytes: i64,
    pub(crate) schema: SchemaRef,
}

pub(crate) fn create_temporal_sorter(
    storage: Storage,
    options: TemporalSortOptions,
) -> Result<TemporalSorter, DataFusionError> {
    let TemporalSortOptions {
        rt_column,
        memory_budget_bytes,
        schema,
    } = options;
    let config = storage.config;
    let store = open_temporal_sort_buffer(config, schema.clone(), &storage.restore);
    let sorter = store.and_then(|store| {
        let mut sorter = TemporalSorter::new(rt_column as usize)
            .with_store(store)
            .with_read_through_budget(memory_budget_bytes)?;
        let restored = storage.restore.partitions;
        sorter.migrate_legacy_snapshot_store()?;
        sorter.import_snapshots(&restored)?;
        Ok(sorter)
    });
    sorter
}

pub(crate) struct WindowOptions {
    pub(crate) window_millis: i64,
    pub(crate) slide_millis: i64,
    pub(crate) cumulative: bool,
    pub(crate) value_types: Vec<i64>,
    pub(crate) aggregate_kinds: Vec<i64>,
    pub(crate) key_types: Vec<i64>,
    pub(crate) key_timestamp_precisions: Vec<i32>,
    pub(crate) memory_budget_bytes: i64,
    pub(crate) restored_timer_deadline: i64,
}

pub(crate) fn create_window_aggregator(
    storage: Storage,
    options: WindowOptions,
) -> Result<TumblingAggregator, DataFusionError> {
    let WindowOptions {
        window_millis,
        slide_millis,
        cumulative,
        value_types,
        aggregate_kinds,
        key_types,
        key_timestamp_precisions,
        memory_budget_bytes,
        restored_timer_deadline,
    } = options;
    let kinds = aggregate_kinds;
    let key_descriptor = key_timestamp_precisions;
    let key_types = window_key_data_types(key_types, &key_descriptor);
    let state_codec = WindowStateCodec::new(&build_aggregates(&kinds, &value_types));
    let config = storage.config;
    let store = if rocks_row_supported(&key_types) {
        state_codec.and_then(|codec| open_window_agg_store(config, codec, &storage.restore))
    } else {
        Err(DataFusionError::Plan(
            "window key shape not supported by RocksDB".into(),
        ))
    };
    let aggregator = store.and_then(|store| {
        let mut aggregator =
            TumblingAggregator::new(window_millis, slide_millis, cumulative, value_types, kinds)
                .with_key_timestamp_precisions(key_descriptor)
                .with_store(store, key_types)
                .with_read_through_budget(memory_budget_bytes)?;
        aggregator.migrate_legacy_snapshot_store()?;
        let restored = storage.restore.partitions;
        aggregator.import_partitions(&restored, restored_timer_deadline)?;
        Ok(aggregator)
    });
    aggregator
}

pub(crate) struct SessionOptions {
    pub(crate) gap_millis: i64,
    pub(crate) value_types: Vec<i64>,
    pub(crate) aggregate_kinds: Vec<i64>,
    pub(crate) key_types: Vec<i64>,
    pub(crate) key_timestamp_precisions: Vec<i32>,
    pub(crate) memory_budget_bytes: i64,
    pub(crate) restored_timer_deadline: i64,
}

pub(crate) fn create_session_aggregator(
    storage: Storage,
    options: SessionOptions,
) -> Result<SessionAggregator, DataFusionError> {
    let SessionOptions {
        gap_millis,
        value_types,
        aggregate_kinds,
        key_types,
        key_timestamp_precisions,
        memory_budget_bytes,
        restored_timer_deadline,
    } = options;
    let kinds = aggregate_kinds;
    let key_descriptor = key_timestamp_precisions;
    let key_types = window_key_data_types(key_types, &key_descriptor);
    let state_codec = WindowStateCodec::new(&build_aggregates(&kinds, &value_types));
    let config = storage.config;
    let store = if rocks_row_supported(&key_types) {
        state_codec.and_then(|codec| open_session_agg_store(config, codec, &storage.restore))
    } else {
        Err(DataFusionError::Plan(
            "session key shape not supported by RocksDB".into(),
        ))
    };
    let aggregator = store.and_then(|store| {
        let mut aggregator = SessionAggregator::new(gap_millis, value_types, kinds)
            .with_key_timestamp_precisions(key_descriptor)
            .with_store(store, key_types)
            .with_read_through_budget(memory_budget_bytes)?;
        aggregator.migrate_legacy_snapshot_store()?;
        let restored = storage.restore.partitions;
        aggregator.import_partitions(&restored, restored_timer_deadline)?;
        Ok(aggregator)
    });
    aggregator
}

pub(crate) struct OverOptions {
    pub(crate) value_types: Vec<i64>,
    pub(crate) aggregate_kinds: Vec<i64>,
    pub(crate) rt_column: i32,
    pub(crate) value_columns: Vec<usize>,
    pub(crate) key_columns: Vec<usize>,
    pub(crate) frame_kind: i32,
    pub(crate) frame_offset: i64,
    pub(crate) proctime: bool,
    pub(crate) key_timestamp_precisions: Vec<i32>,
    pub(crate) state_ttl_millis: i64,
    pub(crate) now_millis: i64,
    pub(crate) memory_budget_bytes: i64,
    pub(crate) schema: SchemaRef,
}

pub(crate) fn create_over_aggregator(
    storage: Storage,
    options: OverOptions,
) -> Result<OverWindowAggregator, DataFusionError> {
    let OverOptions {
        value_types,
        aggregate_kinds,
        rt_column,
        value_columns,
        key_columns,
        frame_kind,
        frame_offset,
        proctime,
        key_timestamp_precisions,
        state_ttl_millis,
        now_millis,
        memory_budget_bytes,
        schema,
    } = options;
    let kinds = aggregate_kinds;
    let values = value_columns;
    let keys = key_columns;
    let key_types: Vec<DataType> = keys
        .iter()
        .map(|&column| schema.field(column).data_type().clone())
        .collect();
    let config = storage.config;
    let store = match rocks_over_state_types(&value_types, &kinds, frame_kind as i64, proctime) {
        Some(state_types) => open_over_agg_store(
            config,
            &state_types,
            &rocks_over_frame_value_types(&value_types, &kinds, frame_kind as i64),
            &rocks_over_distinct_element_types(&value_types, &kinds, frame_kind as i64),
            schema,
            &storage.restore,
        ),
        None => Err(DataFusionError::Plan(
            "over shape not supported by RocksDB".into(),
        )),
    };
    let aggregator = store.and_then(|store| {
        let mut aggregator = OverWindowAggregator::new(
            value_types,
            kinds,
            rt_column as usize,
            values,
            keys,
            frame_kind as i64,
            frame_offset,
            proctime,
        )
        .with_state_retention(state_ttl_millis)
        .with_key_timestamp_precisions(key_timestamp_precisions)
        .with_store(store, key_types);
        aggregator.migrate_legacy_snapshot_store()?;
        let restored = storage.restore.partitions;
        aggregator.import_partitions(&restored)?;
        if !crate::state::rocks_store::in_frozen_scope() {
            aggregator.adopt_store_retention(now_millis)?;
        }
        aggregator.with_read_through_budget(memory_budget_bytes)
    });
    aggregator
}

pub(crate) fn window_key_data_type(code: i64) -> DataType {
    match code {
        1 => DataType::Float64,
        6 => DataType::Float32,
        3 => DataType::Utf8,
        4 => DataType::Int16,
        5 => DataType::Int8,
        7 => DataType::Boolean,
        8 => DataType::Date32,
        9 => DataType::Time32(arrow::datatypes::TimeUnit::Millisecond),
        11 => DataType::Binary,
        12 => DataType::Int32,
        1000..=1009 => streamfusion_bridge::timestamp::timestamp_type(),
        code if code >= 2000 => {
            let packed = code - 2000;
            DataType::Decimal128((packed / 100) as u8, (packed % 100) as i8)
        }
        _ => DataType::Int64,
    }
}

pub(crate) fn window_key_data_types(codes: Vec<i64>, key_descriptor: &[i32]) -> Vec<DataType> {
    let stock_projection = key_descriptor.first() == Some(&i32::MIN);
    codes
        .into_iter()
        .map(|code| {
            if stock_projection && code == 2 {
                DataType::Int32
            } else {
                window_key_data_type(code)
            }
        })
        .collect()
}

#[cfg(test)]
mod window_key_schema_tests {
    use super::*;

    #[test]
    fn stock_window_keys_preserve_int_width_without_changing_legacy_state() {
        let codes = vec![2, 0, 12, 8, 1003];
        let legacy = vec![
            DataType::Int64,
            DataType::Int64,
            DataType::Int32,
            DataType::Date32,
            streamfusion_bridge::timestamp::timestamp_type(),
        ];
        assert_eq!(window_key_data_types(codes.clone(), &[]), legacy);
        assert_eq!(window_key_data_types(codes.clone(), &[-1; 5]), legacy);
        let mut stock = legacy;
        stock[0] = DataType::Int32;
        assert_eq!(
            window_key_data_types(codes, &[i32::MIN, -1, -1, -1, -1, 3]),
            stock
        );
        assert!(window_key_data_types(vec![], &[i32::MIN]).is_empty());
    }
}

pub(crate) fn checkpoint_temporal_sorter(
    operator: &mut TemporalSorter,
    directory: &str,
) -> Result<RocksCheckpointManifest, DataFusionError> {
    operator.store_mut().checkpoint(directory)
}

pub(crate) fn checkpoint_window_aggregator(
    operator: &mut TumblingAggregator,
    timer_deadline: i64,
    directory: &str,
) -> Result<RocksCheckpointManifest, DataFusionError> {
    operator.checkpoint_store(timer_deadline, directory)
}

pub(crate) fn checkpoint_session_aggregator(
    operator: &mut SessionAggregator,
    timer_deadline: i64,
    directory: &str,
) -> Result<RocksCheckpointManifest, DataFusionError> {
    operator.checkpoint_store(timer_deadline, directory)
}

pub(crate) fn checkpoint_over_aggregator(
    operator: &mut OverWindowAggregator,
    directory: &str,
) -> Result<RocksCheckpointManifest, DataFusionError> {
    operator.checkpoint_store(directory)
}
