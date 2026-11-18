//! Plain Rust lifecycle for persistent streaming operators. No JNI handles or JVM arguments.
use super::rocks_store::window_memberships::WindowStateCodec;
use crate::*;
mod joins;
mod keyed;
mod rank;
mod windows;
pub(crate) use joins::*;
pub(crate) use keyed::*;
pub(crate) use rank::*;
pub(crate) use windows::*;
pub(crate) type RocksGroupAggregator = GroupAggregator<RocksGroupStore>;
pub(crate) type RocksChangelogNormalizer = ChangelogNormalizer<RocksNormalizerStore>;
pub(crate) type RocksKeepLastDeduplicator = KeepLastDeduplicator<RocksDedupStore>;
pub(crate) type RocksUpdatingJoiner = UpdatingJoiner<RocksJoinStore>;

pub(crate) struct Restore {
    pub(crate) sources: Vec<(String, i64)>,
    pub(crate) key_groups: std::ops::RangeInclusive<i32>,
    pub(crate) aligned: bool,
    pub(crate) partitions: Vec<Vec<u8>>,
}
pub(crate) struct Storage {
    pub(crate) config: RocksStoreConfig,
    pub(crate) restore: Restore,
}
fn open_keep_first_dedup_store(
    config: RocksStoreConfig,
    schema: SchemaRef,
    partition_columns: &[usize],
    restore: &Restore,
) -> Result<RocksKeepFirstDedupStore, DataFusionError> {
    if restore.sources.is_empty() {
        RocksKeepFirstDedupStore::create(config, schema, partition_columns)
    } else {
        RocksKeepFirstDedupStore::open_merged(
            config,
            schema,
            partition_columns,
            &restore.sources,
            restore.key_groups.clone(),
            restore.aligned,
        )
    }
}

fn open_window_buffer(
    config: RocksStoreConfig,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    restore: &Restore,
) -> Result<RocksWindowBuffer, DataFusionError> {
    if restore.sources.is_empty() {
        RocksWindowBuffer::create(config, left_schema, right_schema)
    } else {
        RocksWindowBuffer::open_merged(
            config,
            left_schema,
            right_schema,
            &restore.sources,
            restore.key_groups.clone(),
            restore.aligned,
        )
    }
}

fn open_interval_buffer(
    config: RocksStoreConfig,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    restore: &Restore,
) -> Result<RocksIntervalBuffer, DataFusionError> {
    if restore.sources.is_empty() {
        RocksIntervalBuffer::create(config, left_schema, right_schema)
    } else {
        RocksIntervalBuffer::open_merged(
            config,
            left_schema,
            right_schema,
            &restore.sources,
            restore.key_groups.clone(),
            restore.aligned,
        )
    }
}

fn open_temporal_join_store(
    config: RocksStoreConfig,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    left_keys: &[usize],
    restore: &Restore,
) -> Result<RocksTemporalJoinStore, DataFusionError> {
    if restore.sources.is_empty() {
        RocksTemporalJoinStore::create(config, left_schema, right_schema, left_keys)
    } else {
        RocksTemporalJoinStore::open_merged(
            config,
            left_schema,
            right_schema,
            left_keys,
            &restore.sources,
            restore.key_groups.clone(),
            restore.aligned,
        )
    }
}

fn open_temporal_sort_buffer(
    config: RocksStoreConfig,
    schema: SchemaRef,
    restore: &Restore,
) -> Result<RocksTemporalSortBuffer, DataFusionError> {
    if restore.sources.is_empty() {
        RocksTemporalSortBuffer::create(config, schema)
    } else {
        RocksTemporalSortBuffer::open_merged(
            config,
            schema,
            &restore.sources,
            restore.key_groups.clone(),
            restore.aligned,
        )
    }
}

fn open_window_agg_store(
    config: RocksStoreConfig,
    state_codec: WindowStateCodec,
    restore: &Restore,
) -> Result<RocksWindowAggStore, DataFusionError> {
    let key_groups = restore.key_groups.clone();
    if restore.sources.is_empty() {
        RocksWindowAggStore::create_with_codec(config, state_codec, key_groups)
    } else {
        RocksWindowAggStore::open_merged_with_codec(
            config,
            state_codec,
            key_groups,
            &restore.sources,
            restore.aligned,
        )
    }
}

fn open_session_agg_store(
    config: RocksStoreConfig,
    state_codec: WindowStateCodec,
    restore: &Restore,
) -> Result<RocksSessionAggStore, DataFusionError> {
    let key_groups = restore.key_groups.clone();
    if restore.sources.is_empty() {
        RocksSessionAggStore::create_with_codec(config, state_codec, key_groups)
    } else {
        RocksSessionAggStore::open_merged_with_codec(
            config,
            state_codec,
            key_groups,
            &restore.sources,
            restore.aligned,
        )
    }
}

fn open_over_agg_store(
    config: RocksStoreConfig,
    state_types: &[DataType],
    frame_value_types: &[DataType],
    distinct_element_types: &[DataType],
    payload_schema: SchemaRef,
    restore: &Restore,
) -> Result<RocksOverAggStore, DataFusionError> {
    let key_groups = restore.key_groups.clone();
    if restore.sources.is_empty() {
        RocksOverAggStore::create(
            config,
            state_types,
            frame_value_types,
            distinct_element_types,
            payload_schema,
            key_groups,
        )
    } else {
        RocksOverAggStore::open_merged(
            config,
            state_types,
            frame_value_types,
            distinct_element_types,
            payload_schema,
            key_groups,
            &restore.sources,
            restore.aligned,
        )
    }
}

fn open_window_rank_store(
    config: RocksStoreConfig,
    row_types: &[DataType],
    restore: &Restore,
) -> Result<RocksWindowRankStore, DataFusionError> {
    let key_groups = restore.key_groups.clone();
    if restore.sources.is_empty() {
        RocksWindowRankStore::create(config, row_types, key_groups)
    } else {
        RocksWindowRankStore::open_merged(
            config,
            row_types,
            key_groups,
            &restore.sources,
            restore.aligned,
        )
    }
}

fn open_store<C: RocksStateCodec>(
    config: RocksStoreConfig,
    codec: C,
    restore: &Restore,
    now_millis: i64,
) -> Result<RocksStore<C>, DataFusionError> {
    if restore.sources.is_empty() {
        RocksStore::create(config, codec)
    } else {
        RocksStore::open_merged(
            config,
            codec,
            &restore.sources,
            restore.key_groups.clone(),
            restore.aligned,
            now_millis,
        )
    }
}

fn open_store_pair<C: RocksStateCodec>(
    config: RocksStoreConfig,
    second_ttl_ms: i64,
    codecs: (C, C),
    restore: &Restore,
    now_millis: i64,
) -> Result<(RocksStore<C>, RocksStore<C>), DataFusionError> {
    if restore.sources.is_empty() {
        RocksStore::create_pair(config, second_ttl_ms, codecs)
    } else {
        RocksStore::open_merged_pair(
            config,
            second_ttl_ms,
            codecs,
            &restore.sources,
            restore.key_groups.clone(),
            restore.aligned,
            now_millis,
        )
    }
}

pub(crate) fn checkpoint_snapshot_store(
    operator: &mut RocksSnapshotStore,
    partitions: &[Vec<u8>],
    timer_deadline: i64,
    directory: &str,
) -> Result<RocksCheckpointManifest, DataFusionError> {
    operator.checkpoint(partitions, timer_deadline, directory)
}

pub(crate) fn create_snapshot_store(
    storage: Storage,
) -> Result<RocksSnapshotStore, DataFusionError> {
    RocksSnapshotStore::open_merged(
        storage.config,
        &storage.restore.sources,
        storage.restore.key_groups,
        storage.restore.aligned,
    )
}

pub(crate) fn manifest_lines(manifest: &RocksCheckpointManifest) -> Vec<String> {
    let mut lines = Vec::with_capacity(1 + manifest.data_files.len() + manifest.meta_files.len());
    lines.push(if manifest.snapshot_id < 0 {
        String::new()
    } else {
        manifest.snapshot_id.to_string()
    });
    lines.extend(manifest.data_files.iter().map(|file| format!("d:{file}")));
    lines.extend(manifest.meta_files.iter().map(|file| format!("m:{file}")));
    lines
}

#[cfg(test)]
mod tests;
