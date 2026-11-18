use crate::*;
use arrow::array::UInt16Array;

pub fn string_cache_name(field: usize) -> String {
    format!("$string_java_{field}$")
}

pub fn string_cache_index(name: &str) -> Option<usize> {
    name.strip_prefix("$string_java_")?
        .strip_suffix('$')?
        .parse()
        .ok()
}

pub const STRING_CACHE_METADATA: &str = "streamfusion.java_string_cache";

pub fn is_string_cache_field(field: &Field) -> bool {
    field
        .metadata()
        .get(STRING_CACHE_METADATA)
        .is_some_and(|value| value == "v1")
}

pub fn string_cache_field(index: usize, ty: DataType) -> Field {
    Field::new(string_cache_name(index), ty, true).with_metadata(std::collections::HashMap::from([
        (STRING_CACHE_METADATA.to_owned(), "v1".to_owned()),
    ]))
}

pub fn string_cache_position(schema: &Schema, index: usize) -> Option<usize> {
    schema.fields().iter().position(|field| {
        is_string_cache_field(field) && string_cache_index(field.name()) == Some(index)
    })
}

pub fn string_cache_column(batch: &RecordBatch, index: usize) -> Option<&ArrayRef> {
    string_cache_position(batch.schema_ref(), index).map(|position| batch.column(position))
}

pub struct JavaStringCacheColumn<'a> {
    lists: &'a ListArray,
    units: &'a UInt16Array,
}

impl<'a> JavaStringCacheColumn<'a> {
    pub fn new(array: &'a ArrayRef) -> Self {
        let lists = array
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("Java string cache list");
        let units = lists
            .values()
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("Java string UTF-16 units");
        assert_eq!(
            units.null_count(),
            0,
            "Java string cache contains null units"
        );
        Self { lists, units }
    }

    pub fn value(&self, row: usize) -> Option<&'a [u16]> {
        if self.lists.is_null(row) {
            return None;
        }
        let offsets = self.lists.value_offsets();
        Some(&self.units.values()[offsets[row] as usize..offsets[row + 1] as usize])
    }
}

pub fn string_cache_array<'a>(rows: impl IntoIterator<Item = Option<&'a [u16]>>) -> ArrayRef {
    let mut builder = arrow::array::ListBuilder::new(arrow::array::UInt16Builder::new())
        .with_field(Arc::new(Field::new("item", DataType::UInt16, false)));
    for row in rows {
        if let Some(units) = row {
            builder.values().append_slice(units);
            builder.append(true);
        } else {
            builder.append(false);
        }
    }
    Arc::new(builder.finish())
}

pub fn has_string_caches(batch: &RecordBatch) -> bool {
    batch
        .schema()
        .fields()
        .iter()
        .any(|field| is_string_cache_field(field))
}

/// Host row serialization removes Java caches while retaining UTF-8 values.
pub fn cold_string_caches(batch: &RecordBatch) -> RecordBatch {
    let columns = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, array)| {
            if is_string_cache_field(field) {
                arrow::array::new_null_array(array.data_type(), batch.num_rows())
            } else {
                array.clone()
            }
        })
        .collect();
    RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .expect("serialized string caches")
}

pub fn strip_string_caches(batch: &RecordBatch) -> RecordBatch {
    let indices = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| (!is_string_cache_field(field)).then_some(index))
        .collect::<Vec<_>>();
    batch.project(&indices).expect("logical string fields")
}
