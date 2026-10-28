use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;
use iceberg::io::{FileIO, FileRead};
use iceberg::scan::FileScanTask;
use iceberg::{Error, ErrorKind, Result};
use parquet::arrow::async_reader::MetadataFetch;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use std::ops::Range;

struct Footer(Box<dyn FileRead>);

impl MetadataFetch for Footer {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            self.0
                .read(range)
                .await
                .map_err(|error| parquet::errors::ParquetError::External(Box::new(error)))
        }
        .boxed()
    }
}

/// The released reader estimates group offsets cumulatively. Translate the host's
/// physical midpoint ranges, including padding/index gaps, to that coordinate space.
pub(crate) async fn tasks(io: &FileIO, task: FileScanTask) -> Result<Vec<FileScanTask>> {
    if task.start() == 0 && task.length() >= task.file_size_in_bytes() {
        return Ok(vec![task]);
    }
    let file = io.new_input(task.data_file_path())?;
    let metadata = ParquetMetaDataReader::new()
        .load_and_finish(Footer(file.reader().await?), task.file_size_in_bytes())
        .await
        .map_err(|error| Error::new(ErrorKind::DataInvalid, error.to_string()))?;
    let ranges = ranges(&metadata, task.start(), task.length())?;
    ranges
        .into_iter()
        .map(|range| {
            FileScanTask::builder()
                .with_file_size_in_bytes(task.file_size_in_bytes())
                .with_start(range.start)
                .with_length(range.end - range.start)
                .with_record_count(task.record_count())
                .with_first_row_id(task.first_row_id())
                .with_data_sequence_number(task.data_sequence_number())
                .with_data_file_path(task.data_file_path().to_owned())
                .with_data_file_format(task.data_file_format())
                .with_schema(task.schema_ref())
                .with_project_field_ids(task.project_field_ids().to_vec())
                .with_predicate(task.predicate().cloned())
                .with_deletes(task.deletes().to_vec())
                .with_partition(task.partition().cloned())
                .with_partition_spec(task.partition_spec().cloned())
                .with_name_mapping(task.name_mapping().cloned())
                .with_unified_partition_type(task.unified_partition_type().cloned())
                .with_case_sensitive(task.case_sensitive())
                .with_key_metadata(task.key_metadata().map(Into::into))
                .build()
        })
        .collect()
}

fn ranges(metadata: &ParquetMetaData, start: u64, length: u64) -> Result<Vec<Range<u64>>> {
    let invalid = || {
        Error::new(
            ErrorKind::DataInvalid,
            "Invalid Parquet row-group byte range",
        )
    };
    let end = start.checked_add(length).ok_or_else(invalid)?;
    let mut virtual_offset = 4u64;
    let mut result: Vec<Range<u64>> = Vec::new();
    let mut previous_selected = false;
    for group in metadata.row_groups() {
        let size = u64::try_from(group.compressed_size()).map_err(|_| invalid())?;
        let physical = group
            .columns()
            .iter()
            .map(|column| column.byte_range().0)
            .min()
            .ok_or_else(invalid)?;
        let physical_midpoint = physical.checked_add(size / 2).ok_or_else(invalid)?;
        let virtual_midpoint = virtual_offset.checked_add(size / 2).ok_or_else(invalid)?;
        let selected = start <= physical_midpoint && physical_midpoint < end;
        if selected {
            let upper = virtual_midpoint.checked_add(1).ok_or_else(invalid)?;
            if previous_selected {
                result.last_mut().unwrap().end = upper;
            } else {
                result.push(virtual_midpoint..upper);
            }
        }
        previous_selected = selected;
        virtual_offset = virtual_offset.checked_add(size).ok_or_else(invalid)?;
    }
    Ok(result)
}
