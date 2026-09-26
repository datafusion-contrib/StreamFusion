use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, ListArray, MapArray, StructArray};
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::{compute_leaves, ArrowColumnWriter, ArrowRowGroupWriterFactory};
use parquet::data_type::{Int96, Int96Type};
use parquet::errors::Result;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::TypePtr;
use std::io::Write;
use std::sync::Arc;

/// ArrowWriter cannot encode INT96. Keep its column encoders for every other leaf and use
/// parquet-rs' typed INT96 encoder only for timestamp leaves, in the same row group.
pub(crate) struct Int96Writer<W: Write + Send> {
    file: SerializedFileWriter<W>,
    factory: ArrowRowGroupWriterFactory,
    columns: Vec<Column>,
    paths: Vec<Option<Vec<usize>>>,
    rows: usize,
    block_size: usize,
    groups: usize,
}

enum Column {
    Arrow(ArrowColumnWriter),
    Timestamp(TimestampValues),
}

#[derive(Default)]
struct TimestampValues {
    values: Vec<Int96>,
    definitions: Vec<i16>,
    repetitions: Vec<i16>,
}

impl TimestampValues {
    fn append(
        &mut self,
        field: &Field,
        array: &ArrayRef,
        path: &[usize],
        row: usize,
        definition: i16,
        repetition: i16,
        depth: i16,
    ) {
        if array.is_null(row) {
            self.definitions.push(definition);
            self.repetitions.push(repetition);
            return;
        }
        let definition = definition + i16::from(field.is_nullable());
        if path.is_empty() {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let bytes = array.value(row);
            let mut value = Int96::new();
            value.set_data(
                u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
                u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
                u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            );
            self.values.push(value);
            self.definitions.push(definition);
            self.repetitions.push(repetition);
            return;
        }
        match field.data_type() {
            DataType::Struct(fields) => {
                let array = array.as_any().downcast_ref::<StructArray>().unwrap();
                self.append(
                    &fields[path[0]],
                    array.column(path[0]),
                    &path[1..],
                    row,
                    definition,
                    repetition,
                    depth,
                );
            }
            DataType::List(element) => {
                let array = array.as_any().downcast_ref::<ListArray>().unwrap();
                self.repeated(
                    element,
                    array.values(),
                    &path[1..],
                    array.value_offsets(),
                    row,
                    definition,
                    repetition,
                    depth,
                );
            }
            DataType::Map(entries, _) => {
                let array = array.as_any().downcast_ref::<MapArray>().unwrap();
                let values = Arc::new(array.entries().clone()) as ArrayRef;
                self.repeated(
                    entries,
                    &values,
                    &path[1..],
                    array.value_offsets(),
                    row,
                    definition,
                    repetition,
                    depth,
                );
            }
            other => panic!("invalid INT96 leaf path through {other:?}"),
        }
    }

    fn repeated(
        &mut self,
        field: &Field,
        values: &ArrayRef,
        path: &[usize],
        offsets: &[i32],
        row: usize,
        definition: i16,
        repetition: i16,
        depth: i16,
    ) {
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        if start == end {
            self.definitions.push(definition);
            self.repetitions.push(repetition);
        }
        for index in start..end {
            self.append(
                field,
                values,
                path,
                index,
                definition + 1,
                if index == start {
                    repetition
                } else {
                    depth + 1
                },
                depth + 1,
            );
        }
    }
}

impl<W: Write + Send> Int96Writer<W> {
    pub(crate) fn new(
        output: W,
        schema: SchemaRef,
        parquet: TypePtr,
        properties: WriterProperties,
        block_size: usize,
    ) -> Result<Self> {
        fn paths(field: &Field, path: Vec<usize>, out: &mut Vec<Option<Vec<usize>>>) {
            let children: Vec<&Field> = match field.data_type() {
                DataType::Struct(fields) => fields.iter().map(AsRef::as_ref).collect(),
                DataType::List(field) | DataType::Map(field, _) => vec![field],
                _ => {
                    out.push(
                        field
                            .metadata()
                            .contains_key(super::files::INT96_META_KEY)
                            .then_some(path),
                    );
                    return;
                }
            };
            for (index, child) in children.into_iter().enumerate() {
                let mut path = path.clone();
                path.push(index);
                paths(child, path, out);
            }
        }
        let file = SerializedFileWriter::new(output, parquet, Arc::new(properties))?;
        let factory = ArrowRowGroupWriterFactory::new(&file, schema.clone());
        let mut leaf_paths = Vec::new();
        for (index, field) in schema.fields().iter().enumerate() {
            paths(field, vec![index], &mut leaf_paths);
        }
        let mut result = Self {
            file,
            factory,
            columns: Vec::new(),
            paths: leaf_paths,
            rows: 0,
            block_size,
            groups: 0,
        };
        result.reset()?;
        Ok(result)
    }

    fn reset(&mut self) -> Result<()> {
        self.columns = self
            .factory
            .create_column_writers(self.groups)?
            .into_iter()
            .zip(&self.paths)
            .map(|(writer, path)| {
                if path.is_some() {
                    Column::Timestamp(TimestampValues::default())
                } else {
                    Column::Arrow(writer)
                }
            })
            .collect();
        Ok(())
    }

    pub(crate) fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        // Bound pending timestamp values even when the input is larger than a row group.
        for offset in (0..batch.num_rows()).step_by(1024) {
            let batch = batch.slice(offset, (batch.num_rows() - offset).min(1024));
            let schema = batch.schema();
            let mut leaf_index = 0;
            for (field, array) in schema.fields().iter().zip(batch.columns()) {
                for leaf in compute_leaves(field, array)? {
                    if let Column::Arrow(writer) = &mut self.columns[leaf_index] {
                        writer.write(&leaf)?;
                    }
                    leaf_index += 1;
                }
            }
            for (column, path) in self.columns.iter_mut().zip(&self.paths) {
                if let (Column::Timestamp(values), Some(path)) = (column, path) {
                    for row in 0..batch.num_rows() {
                        values.append(
                            schema.field(path[0]),
                            batch.column(path[0]),
                            &path[1..],
                            row,
                            0,
                            0,
                            0,
                        );
                    }
                }
            }
            self.rows += batch.num_rows();
            if self.in_progress_size() >= self.block_size {
                self.flush()?;
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let mut group = self.file.next_row_group()?;
        for column in self.columns.drain(..) {
            match column {
                Column::Arrow(writer) => writer.close()?.append_to_row_group(&mut group)?,
                Column::Timestamp(values) => {
                    let mut writer = group.next_column()?.expect("timestamp column");
                    writer.typed::<Int96Type>().write_batch(
                        &values.values,
                        Some(&values.definitions),
                        Some(&values.repetitions),
                    )?;
                    writer.close()?;
                }
            }
        }
        group.close()?;
        self.rows = 0;
        self.groups += 1;
        self.reset()
    }

    pub(crate) fn in_progress_size(&self) -> usize {
        self.columns
            .iter()
            .map(|column| match column {
                Column::Arrow(writer) => writer.get_estimated_total_bytes(),
                Column::Timestamp(values) => {
                    values.values.len() * 12 + values.definitions.len() * 4
                }
            })
            .sum()
    }
    pub(crate) fn bytes_written(&self) -> usize {
        self.file.bytes_written()
    }
    pub(crate) fn inner_mut(&mut self) -> &mut W {
        self.file.inner_mut()
    }
    pub(crate) fn finish(&mut self) -> Result<()> {
        self.flush()?;
        self.file.finish()?;
        Ok(())
    }
}
