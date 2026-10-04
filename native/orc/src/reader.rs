//! Released orc-rust decoder, preserving host FileIO and the operators' Arrow C Data ABI.
use super::*;
use orc_rust::{
    compression::{Compression, CompressionType, Decompressor},
    projection::ProjectionMask,
    proto,
    reader::ChunkReader,
    schema::DataType as OrcType,
    ArrowReader, ArrowReaderBuilder,
};
use prost13::Message;
use std::io::{self, Read};
use std::mem::ManuallyDrop;
use std::rc::Rc;

#[derive(Clone)]
struct HostChunks {
    host: Rc<HostIo>,
    length: u64,
}
struct HostRead {
    source: HostChunks,
    offset: u64,
}
impl Read for HostRead {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let length = (buffer.len() as u64).min(self.source.length.saturating_sub(self.offset));
        if length == 0 {
            return Ok(0);
        }
        if read_host(
            Rc::as_ptr(&self.source.host) as *mut c_void,
            self.offset,
            length,
            buffer.as_mut_ptr().cast(),
        ) != 0
        {
            return Err(io::Error::other("Host ORC input failed"));
        }
        self.offset += length;
        Ok(length as usize)
    }
}
impl ChunkReader for HostChunks {
    type T = HostRead;
    fn len(&self) -> u64 {
        self.length
    }
    fn get_read(&self, offset: u64) -> io::Result<Self::T> {
        if offset > self.length {
            return Err(io::Error::other("ORC offset beyond EOF"));
        }
        Ok(HostRead {
            source: self.clone(),
            offset,
        })
    }
}

pub(super) struct Decoder {
    reader: ArrowReader<HostChunks>,
    natural: SchemaRef,
    output: SchemaRef,
    columns: Vec<(usize, OrcType)>,
    pub memory: i64,
}
impl Decoder {
    pub fn open(
        host: HostIo,
        length: u64,
        output: SchemaRef,
        names: Vec<String>,
        batch_rows: usize,
        instant_zone: &str,
    ) -> Self {
        assert!(batch_rows > 0 && names.len() == output.fields().len());
        let chunks = HostChunks {
            host: Rc::new(host),
            length,
        };
        let builder = ArrowReaderBuilder::try_new(chunks.clone()).expect("Open ORC Rust reader");
        let root = builder.file_metadata().root_data_type();
        let mut physical = Vec::new();
        for (name, field) in names.iter().zip(output.fields()) {
            let column = root
                .children()
                .iter()
                .find(|c| c.name() == name)
                .expect("Missing ORC projection column");
            assert!(
                compatible(column.data_type(), field.data_type()),
                "ORC type mismatch for {name}"
            );
            physical.push(column.data_type().clone());
        }
        let memory = admission(
            &chunks,
            builder.file_metadata().compression(),
            &physical,
            instant_zone,
        )
        .unwrap_or(i64::MAX);
        let mask = ProjectionMask::named_roots(
            root,
            &names.iter().map(String::as_str).collect::<Vec<_>>(),
        );
        let builder = builder.with_projection(mask).with_batch_size(batch_rows);
        // orc-rust's released Decimal128 timestamp decoder retains the full nanosecond range.
        let decode_schema = Arc::new(arrow59::datatypes::Schema::new(
            builder
                .schema()
                .fields()
                .iter()
                .map(|field| wide_timestamp_field(field))
                .collect::<Vec<_>>(),
        ));
        let builder = builder.with_schema(decode_schema);
        let ffi59 = arrow59::ffi::FFI_ArrowSchema::try_from(builder.schema().as_ref()).unwrap();
        // Borrow the standard repr(C) schema while Arrow 58 copies its logical metadata.
        let ffi58 = unsafe { &*(&ffi59 as *const _ as *const FFI_ArrowSchema) };
        let natural = Arc::new(Schema::try_from(ffi58).expect("ORC Arrow schema"));
        let columns = names
            .iter()
            .zip(physical)
            .map(|(name, ty)| (natural.index_of(name).unwrap(), ty))
            .collect();
        Self {
            reader: builder.build(),
            natural,
            output,
            columns,
            memory,
        }
    }

    pub fn next(&mut self) -> Option<RecordBatch> {
        assert_ne!(
            self.memory,
            i64::MAX,
            "ORC file requires the stock Java reader"
        );
        use arrow59::array::Array as _;
        let batch = self.reader.next()?.expect("Decode ORC Rust batch");
        let data = arrow59::array::StructArray::from(batch).to_data();
        let ffi59 = ManuallyDrop::new(arrow59::ffi::FFI_ArrowArray::new(&data));
        // Transfer ownership without copying buffers. Arrow 58 eventually calls Arrow 59's release.
        let ffi58 = unsafe { std::ptr::read(&*ffi59 as *const _ as *const FFI_ArrowArray) };
        let data = unsafe {
            from_ffi_and_data_type(ffi58, DataType::Struct(self.natural.fields().clone()))
        }
        .expect("Import ORC Arrow batch");
        let batch = RecordBatch::from(StructArray::from(data));
        let arrays = self
            .columns
            .iter()
            .zip(self.output.fields())
            .map(|((index, physical), field)| {
                normalize(batch.column(*index), physical, field.data_type())
            })
            .collect();
        Some(RecordBatch::try_new(self.output.clone(), arrays).expect("Project ORC batch"))
    }
}

fn wide_timestamp_field(field: &arrow59::datatypes::Field) -> arrow59::datatypes::Field {
    use arrow59::datatypes::DataType as D;
    let child = |f: &Arc<arrow59::datatypes::Field>| Arc::new(wide_timestamp_field(f));
    let data_type = match field.data_type() {
        D::Timestamp(..) => D::Decimal128(38, 9),
        D::Struct(fields) => D::Struct(fields.iter().map(child).collect()),
        D::List(field) => D::List(child(field)),
        D::LargeList(field) => D::LargeList(child(field)),
        D::Map(field, sorted) => D::Map(child(field), *sorted),
        other => other.clone(),
    };
    field.clone().with_data_type(data_type)
}

fn compatible(physical: &OrcType, target: &DataType) -> bool {
    use OrcType as O;
    match (physical, target) {
        (O::Timestamp { .. } | O::TimestampWithLocalTimezone { .. }, data_type)
            if streamfusion_bridge::timestamp::is_component_timestamp(data_type) =>
        {
            true
        }
        (O::Boolean { .. }, DataType::Boolean)
        | (O::Byte { .. }, DataType::Int8)
        | (O::Short { .. }, DataType::Int16)
        | (O::Int { .. }, DataType::Int32)
        | (O::Long { .. }, DataType::Int64)
        | (O::Float { .. }, DataType::Float32)
        | (O::Double { .. }, DataType::Float64)
        | (O::Date { .. }, DataType::Date32)
        | (O::String { .. } | O::Varchar { .. } | O::Char { .. }, DataType::Utf8)
        | (O::Binary { .. }, DataType::Binary | DataType::FixedSizeBinary(_))
        | (O::Timestamp { .. } | O::TimestampWithLocalTimezone { .. }, DataType::Timestamp(..)) => {
            true
        }
        (
            O::Decimal {
                precision, scale, ..
            },
            DataType::Decimal128(p, s),
        ) => *precision == *p as u32 && *scale == *s as u32,
        (O::List { child, .. }, DataType::List(field)) => compatible(child, field.data_type()),
        (O::Struct { children, .. }, DataType::Struct(fields)) => {
            children.len() == fields.len()
                && children
                    .iter()
                    .zip(fields)
                    .all(|(c, f)| compatible(c.data_type(), f.data_type()))
        }
        (O::Map { key, value, .. }, DataType::Map(entries, _)) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => {
                compatible(key, fields[0].data_type()) && compatible(value, fields[1].data_type())
            }
            _ => false,
        },
        _ => false,
    }
}

pub(crate) fn normalize(array: &ArrayRef, physical: &OrcType, target: &DataType) -> ArrayRef {
    if matches!(
        physical,
        OrcType::Timestamp { .. } | OrcType::TimestampWithLocalTimezone { .. }
    ) {
        let nanos = array
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .expect("ORC full-range timestamp decoder");
        let values = nanos.iter().map(|v| {
            v.map(|n| {
                streamfusion_bridge::timestamp::TimestampValue::from_nanos(n)
                    .expect("ORC timestamp exceeds Flink's millisecond range")
            })
        });
        let components: ArrayRef =
            Arc::new(streamfusion_bridge::timestamp::timestamp_array(values));
        return streamfusion_bridge::timestamp::cast_timestamp(&components, target)
            .expect("ORC timestamp output type");
    }
    match (physical, target) {
        (OrcType::Char { .. }, DataType::Utf8) => {
            let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
            Arc::new(StringArray::from_iter(
                strings.iter().map(|s| s.map(|s| s.trim_end_matches(' '))),
            ))
        }
        (OrcType::Struct { children, .. }, DataType::Struct(fields)) => {
            let input = array.as_any().downcast_ref::<StructArray>().unwrap();
            let columns = input
                .columns()
                .iter()
                .zip(children)
                .zip(fields)
                .map(|((a, c), f)| normalize(a, c.data_type(), f.data_type()))
                .collect();
            Arc::new(StructArray::new(
                fields.clone(),
                columns,
                input.nulls().cloned(),
            ))
        }
        (OrcType::List { child, .. }, DataType::List(field)) => {
            let input = array.as_any().downcast_ref::<ListArray>().unwrap();
            Arc::new(ListArray::new(
                field.clone(),
                input.offsets().clone(),
                normalize(input.values(), child, field.data_type()),
                input.nulls().cloned(),
            ))
        }
        (OrcType::Map { key, value, .. }, DataType::Map(field, sorted)) => {
            let input = array.as_any().downcast_ref::<MapArray>().unwrap();
            let DataType::Struct(fields) = field.data_type() else {
                unreachable!()
            };
            let entries = StructArray::new(
                fields.clone(),
                vec![
                    normalize(input.keys(), key, fields[0].data_type()),
                    normalize(input.values(), value, fields[1].data_type()),
                ],
                input.entries().nulls().cloned(),
            );
            Arc::new(MapArray::new(
                field.clone(),
                input.offsets().clone(),
                entries,
                input.nulls().cloned(),
                *sorted,
            ))
        }
        _ => streamfusion_bridge::timestamp::cast_array(array, target)
            .expect("Convert ORC field to operator type"),
    }
}

fn decode<M: Message + Default>(bytes: bytes::Bytes, compression: Option<Compression>) -> M {
    let mut buffer = Vec::new();
    Decompressor::new(bytes, compression, vec![])
        .read_to_end(&mut buffer)
        .expect("Read ORC metadata");
    M::decode(buffer.as_slice()).expect("Decode ORC metadata")
}
fn utc(zone: &str) -> bool {
    matches!(zone, "UTC" | "GMT" | "Etc/UTC" | "Etc/GMT")
}
fn contains(ty: &OrcType, ntz: bool) -> bool {
    match ty {
        OrcType::Timestamp { .. } => ntz,
        OrcType::TimestampWithLocalTimezone { .. } => !ntz,
        OrcType::Struct { children, .. } => children.iter().any(|c| contains(c.data_type(), ntz)),
        OrcType::List { child, .. } => contains(child, ntz),
        OrcType::Map { key, value, .. } => contains(key, ntz) || contains(value, ntz),
        _ => false,
    }
}

// Inspect every stripe before emitting rows. None means the host must retain Java for this file.
fn admission(
    chunks: &HostChunks,
    compression: Option<Compression>,
    columns: &[OrcType],
    instant_zone: &str,
) -> Option<i64> {
    if matches!(
        compression.map(|c| c.compression_type()),
        Some(CompressionType::Lzo)
    ) {
        return None;
    }
    if !instant_zone.is_empty() && !utc(instant_zone) && columns.iter().any(|c| contains(c, false))
    {
        return None;
    }
    let tail_length = chunks.length.min(256);
    let tail = chunks
        .get_bytes(chunks.length - tail_length, tail_length)
        .expect("Read ORC postscript");
    let length = *tail.last().expect("Empty ORC file") as usize;
    let start = tail
        .len()
        .checked_sub(length + 1)
        .expect("Invalid ORC postscript length");
    let post = proto::PostScript::decode(&tail[start..tail.len() - 1]).expect("ORC postscript");
    let footer_offset = chunks
        .length
        .checked_sub(1 + length as u64 + post.footer_length())
        .expect("Invalid ORC footer offset");
    let footer: proto::Footer = decode(
        chunks
            .get_bytes(footer_offset, post.footer_length())
            .expect("Read ORC footer"),
        compression,
    );
    if footer.encryption.is_some() {
        return None;
    }
    let metadata_offset = footer_offset
        .checked_sub(post.metadata_length())
        .expect("Invalid ORC metadata offset");
    let metadata: proto::Metadata = decode(
        chunks
            .get_bytes(metadata_offset, post.metadata_length())
            .expect("Read ORC statistics"),
        compression,
    );
    let mut maximum = 0_u64;
    for (index, stripe) in footer.stripes.iter().enumerate() {
        let stripe_footer: proto::StripeFooter = decode(
            chunks
                .get_bytes(
                    stripe.offset() + stripe.index_length() + stripe.data_length(),
                    stripe.footer_length(),
                )
                .expect("Read ORC stripe footer"),
            compression,
        );
        if columns.iter().any(|c| contains(c, true))
            && !utc(stripe_footer.writer_timezone.as_deref().unwrap_or(""))
        {
            return None;
        }
        if !stripe_footer.encryption.is_empty() {
            return None;
        }
        let stats = metadata
            .stripe_stats
            .get(index)
            .map(|s| s.col_stats.as_slice())
            .unwrap_or(&[]);
        // Retained compressed streams, scratch space for each stream, plus a conservative whole-
        // stripe bound for decoded columns/dictionaries and their outgoing Arrow buffers.
        let mut bytes = stripe
            .index_length()
            .checked_add(stripe.data_length())?
            .checked_add(stripe.footer_length())?
            .checked_add(256 * 1024)?;
        let block = if compression.is_some() {
            post.compression_block_size.unwrap_or(256 * 1024)
        } else {
            0
        };
        for column in columns {
            let decoded = column_memory(column, stripe.number_of_rows(), stats)?;
            let indices = column.all_indices();
            let stream_count = stripe_footer
                .streams
                .iter()
                .filter(|s| indices.contains(&(s.column() as usize)))
                .count() as u64;
            // ORC declares a maximum compression block, not an allocation per tiny stream. The
            // decoded-column bound also bounds its encoded streams; allow RLE headers/scratch.
            let scratch = block.min(decoded.checked_add(4096)?).checked_mul(2)?;
            bytes = bytes.checked_add(stream_count.checked_mul(scratch)?)?;
            bytes = bytes.checked_add(decoded)?;
        }
        maximum = maximum.max(bytes);
    }
    i64::try_from(maximum).ok()
}
fn column_memory(ty: &OrcType, count: u64, stats: &[proto::ColumnStatistics]) -> Option<u64> {
    let stat = stats.get(ty.column_index());
    let mut bytes = count.checked_mul(32)?;
    match ty {
        OrcType::String { .. }
        | OrcType::Varchar { .. }
        | OrcType::Char { .. }
        | OrcType::Binary { .. } => {
            let stat = stat?;
            let length = if stat.number_of_values? == 0 {
                0
            } else {
                stat.string_statistics
                    .as_ref()
                    .and_then(|s| s.sum)
                    .or_else(|| stat.binary_statistics.as_ref().and_then(|s| s.sum))?
            };
            bytes = bytes.checked_add(u64::try_from(length).ok()?.checked_mul(3)?)?;
        }
        OrcType::Struct { children, .. } => {
            for child in children {
                bytes = bytes.checked_add(column_memory(child.data_type(), count, stats)?)?;
            }
        }
        OrcType::List { child, .. } => {
            let children = child_count(stat?)?;
            bytes = bytes.checked_add(column_memory(child, children, stats)?)?;
        }
        OrcType::Map { key, value, .. } => {
            let children = child_count(stat?)?;
            bytes = bytes
                .checked_add(column_memory(key, children, stats)?)?
                .checked_add(column_memory(value, children, stats)?)?;
        }
        _ => {}
    }
    Some(bytes)
}

fn child_count(stat: &proto::ColumnStatistics) -> Option<u64> {
    if stat.number_of_values? == 0 {
        Some(0)
    } else {
        stat.collection_statistics.as_ref()?.total_children
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_payload_statistics_cannot_be_treated_as_zero_bytes() {
        let ty = OrcType::String { column_index: 0 };
        let mut stats = vec![proto::ColumnStatistics::default()];
        assert!(column_memory(&ty, 10, &stats).is_none());
        stats[0].number_of_values = Some(0);
        assert!(column_memory(&ty, 10, &stats).is_some());
        stats[0].number_of_values = Some(1);
        assert!(column_memory(&ty, 10, &stats).is_none());
        stats[0].string_statistics = Some(proto::StringStatistics {
            sum: Some(-1),
            ..Default::default()
        });
        assert!(column_memory(&ty, 10, &stats).is_none());
        stats[0].string_statistics.as_mut().unwrap().sum = Some(100);
        assert!(column_memory(&ty, 10, &stats).unwrap() >= 100);
    }
}
