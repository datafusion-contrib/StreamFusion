use rocksdb::{BlockBasedOptions, Cache, DBCompactionStyle, DBCompressionType, LogLevel, Options};
use serde::{Deserialize, Serialize};

/// The resolved subset of Flink's public RocksDB configuration. Java resolves Flink defaults and
/// predefined profiles before serializing this value, so Rust applies one unambiguous option set.
/// Keeping Flink's names at this boundary makes configuration-parity tests straightforward.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FlinkRocksOptions {
    pub max_background_threads: i32,
    pub max_open_files: i32,
    pub log_max_file_size: usize,
    pub log_file_num: usize,
    pub log_directory: Option<String>,
    pub log_level: String,
    pub compaction_style: String,
    pub compression_per_level: Vec<String>,
    pub use_dynamic_level_size: bool,
    pub target_file_size_base: u64,
    pub max_size_level_base: u64,
    pub write_buffer_size: usize,
    pub max_write_buffer_number: i32,
    pub min_write_buffer_number_to_merge: i32,
    pub write_batch_size: usize,
    pub compaction_filter_query_time_after_num_entries: u64,
    pub periodic_compaction_seconds: u64,
    pub block_size: usize,
    pub metadata_block_size: usize,
    pub block_cache_size: usize,
    pub use_bloom_filter: bool,
    pub bloom_filter_bits_per_key: f64,
    pub bloom_filter_block_based_mode: bool,
}

impl FlinkRocksOptions {
    pub(crate) fn from_json(json: &str) -> Result<Self, String> {
        serde_json::from_str(json)
            .map_err(|error| format!("invalid Flink RocksDB options: {error}"))
    }

    pub(crate) fn build(&self) -> Result<(Options, Option<Cache>), String> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_max_background_jobs(self.max_background_threads);
        options.set_max_open_files(self.max_open_files);
        options.set_max_log_file_size(self.log_max_file_size);
        options.set_keep_log_file_num(self.log_file_num);
        if let Some(directory) = self
            .log_directory
            .as_deref()
            .filter(|path| !path.is_empty())
        {
            options.set_db_log_dir(directory);
        }
        options.set_log_level(log_level(&self.log_level)?);
        if self.compaction_style.eq_ignore_ascii_case("NONE") {
            // rust-rocksdb omits RocksDB's kCompactionStyleNone enum value. Disabling automatic
            // compactions is its documented equivalent and preserves Flink's public setting.
            options.set_disable_auto_compactions(true);
        } else {
            options.set_compaction_style(compaction_style(&self.compaction_style)?);
        }
        let compression: Result<Vec<_>, _> = self
            .compression_per_level
            .iter()
            .map(|name| compression_type(name))
            .collect();
        options.set_compression_per_level(&compression?);
        options.set_level_compaction_dynamic_level_bytes(self.use_dynamic_level_size);
        options.set_target_file_size_base(self.target_file_size_base);
        options.set_max_bytes_for_level_base(self.max_size_level_base);
        options.set_write_buffer_size(self.write_buffer_size);
        options.set_max_write_buffer_number(self.max_write_buffer_number);
        options.set_min_write_buffer_number_to_merge(self.min_write_buffer_number_to_merge);
        options.set_periodic_compaction_seconds(self.periodic_compaction_seconds);

        let mut table = BlockBasedOptions::default();
        table.set_block_size(self.block_size);
        table.set_metadata_block_size(self.metadata_block_size);
        let cache = if self.block_cache_size == 0 {
            None
        } else {
            let cache = Cache::new_lru_cache(self.block_cache_size);
            table.set_block_cache(&cache);
            Some(cache)
        };
        if self.use_bloom_filter {
            table.set_bloom_filter(
                self.bloom_filter_bits_per_key,
                self.bloom_filter_block_based_mode,
            );
        }
        options.set_block_based_table_factory(&table);
        Ok((options, cache))
    }
}

fn log_level(name: &str) -> Result<LogLevel, String> {
    match name.to_ascii_uppercase().as_str() {
        "DEBUG_LEVEL" | "DEBUG" => Ok(LogLevel::Debug),
        "INFO_LEVEL" | "INFO" => Ok(LogLevel::Info),
        "WARN_LEVEL" | "WARN" => Ok(LogLevel::Warn),
        "ERROR_LEVEL" | "ERROR" => Ok(LogLevel::Error),
        "FATAL_LEVEL" | "FATAL" => Ok(LogLevel::Fatal),
        "HEADER_LEVEL" | "HEADER" => Ok(LogLevel::Header),
        other => Err(format!("unsupported RocksDB log level {other}")),
    }
}

fn compaction_style(name: &str) -> Result<DBCompactionStyle, String> {
    match name.to_ascii_uppercase().as_str() {
        "LEVEL" => Ok(DBCompactionStyle::Level),
        "UNIVERSAL" => Ok(DBCompactionStyle::Universal),
        "FIFO" => Ok(DBCompactionStyle::Fifo),
        other => Err(format!("unsupported RocksDB compaction style {other}")),
    }
}

fn compression_type(name: &str) -> Result<DBCompressionType, String> {
    match name.to_ascii_uppercase().as_str() {
        "NO_COMPRESSION" | "NONE" => Ok(DBCompressionType::None),
        "SNAPPY_COMPRESSION" | "SNAPPY" => Ok(DBCompressionType::Snappy),
        "ZLIB_COMPRESSION" | "ZLIB" => Ok(DBCompressionType::Zlib),
        "BZLIB2_COMPRESSION" | "BZIP2" => Ok(DBCompressionType::Bz2),
        "LZ4_COMPRESSION" | "LZ4" => Ok(DBCompressionType::Lz4),
        "LZ4HC_COMPRESSION" | "LZ4HC" => Ok(DBCompressionType::Lz4hc),
        "ZSTD_COMPRESSION" | "ZSTD" => Ok(DBCompressionType::Zstd),
        other => Err(format!("unsupported RocksDB compression type {other}")),
    }
}
