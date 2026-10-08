//! Fluss 1.0 compacted keys and byte-tail Murmur hashing, following Apache Fluss Rust's
//! row/compacted and util/murmur_hash implementations (Apache-2.0). Payload grouping uses the
//! existing Arrow partition kernel; no Flink row representation crosses the native boundary.
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use num_bigint::BigInt;
use std::cmp::Ordering;
use std::sync::Arc;

fn varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 128 {
        out.push(value as u8 | 128);
        value >>= 7;
    }
    out.push(value as u8);
}
fn bytes(out: &mut Vec<u8>, value: &[u8]) {
    varint(out, value.len() as u64);
    out.extend_from_slice(value);
}
fn key_field(
    out: &mut Vec<u8>,
    array: &dyn Array,
    row: usize,
    precision: usize,
) -> Result<(), String> {
    if array.is_null(row) {
        return Err("Fluss bucket keys cannot be null".into());
    }
    macro_rules! val {
        ($t:ty) => {
            array.as_any().downcast_ref::<$t>().unwrap().value(row)
        };
    }
    match array.data_type() {
        DataType::Boolean => out.push(u8::from(val!(BooleanArray))),
        DataType::Int8 => out.push(val!(Int8Array) as u8),
        DataType::Int16 => out.extend_from_slice(&val!(Int16Array).to_le_bytes()),
        DataType::Int32 => varint(out, val!(Int32Array) as u32 as u64),
        DataType::Int64 => varint(out, val!(Int64Array) as u64),
        DataType::Date32 => varint(out, val!(Date32Array) as u32 as u64),
        DataType::Time32(TimeUnit::Millisecond) => {
            varint(out, val!(Time32MillisecondArray) as u32 as u64)
        }
        DataType::Float32 => out.extend_from_slice(&val!(Float32Array).to_bits().to_le_bytes()),
        DataType::Float64 => out.extend_from_slice(&val!(Float64Array).to_bits().to_le_bytes()),
        DataType::Utf8 => bytes(out, val!(StringArray).as_bytes()),
        DataType::Binary => bytes(out, val!(BinaryArray)),
        DataType::FixedSizeBinary(_) => bytes(out, val!(FixedSizeBinaryArray)),
        DataType::Decimal128(p, _) => {
            let value = val!(Decimal128Array);
            if *p <= 18 {
                varint(out, value as i64 as u64);
            } else {
                bytes(out, &BigInt::from(value).to_signed_bytes_be());
            }
        }
        DataType::Struct(_)
            if streamfusion_bridge::timestamp::is_component_timestamp(array.data_type()) =>
        {
            let value = array.as_any().downcast_ref::<StructArray>().unwrap();
            let millis = value
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row);
            let nanos = value
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row);
            varint(out, millis as u64);
            if precision > 3 {
                varint(out, nanos as u32 as u64);
            }
        }
        DataType::Timestamp(unit, _) => {
            let (value, per_ms) = match unit {
                TimeUnit::Second => (
                    val!(TimestampSecondArray)
                        .checked_mul(1000)
                        .ok_or("timestamp overflow")?,
                    1,
                ),
                TimeUnit::Millisecond => (val!(TimestampMillisecondArray), 1),
                TimeUnit::Microsecond => (val!(TimestampMicrosecondArray), 1000),
                TimeUnit::Nanosecond => (val!(TimestampNanosecondArray), 1_000_000),
            };
            varint(out, value.div_euclid(per_ms) as u64);
            if precision > 3 {
                varint(
                    out,
                    (value.rem_euclid(per_ms) * (1_000_000 / per_ms)) as u64,
                );
            }
        }
        t => return Err(format!("Unsupported Fluss bucket key type {t}")),
    }
    Ok(())
}
fn mix_k(k: u32) -> u32 {
    k.wrapping_mul(0xcc9e2d51)
        .rotate_left(15)
        .wrapping_mul(0x1b873593)
}
fn mix_h(h: u32, k: u32) -> u32 {
    (h ^ k)
        .rotate_left(13)
        .wrapping_mul(5)
        .wrapping_add(0xe6546b64)
}
fn finish(mut h: u32) -> u32 {
    h ^= h >> 16;
    h = h.wrapping_mul(0x85ebca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2ae35);
    h ^ (h >> 16)
}
/// Fluss differs from ordinary Murmur3: each signed tail byte is mixed separately, then the
/// result is hashed again with Flink's positive integer hash before reducing by bucket count.
pub fn bucket_for_key(key: &[u8], count: i32) -> i32 {
    let mut h = 42;
    let mut chunks = key.chunks_exact(4);
    for chunk in &mut chunks {
        h = mix_h(h, mix_k(u32::from_le_bytes(chunk.try_into().unwrap())));
    }
    for &byte in chunks.remainder() {
        h = mix_h(h, mix_k(byte as i8 as u32));
    }
    let hash = finish(h ^ key.len() as u32);
    let positive = (finish(
        mix_k(hash)
            .rotate_left(13)
            .wrapping_mul(5)
            .wrapping_add(0xe6546b64)
            ^ 4,
    ) as i32)
        .checked_abs()
        .unwrap_or(0);
    positive % count
}
pub fn bucket_ids(
    batch: &RecordBatch,
    columns: &[usize],
    precisions: &[usize],
    count: i32,
) -> Result<Vec<i32>, String> {
    if count <= 0 || columns.is_empty() || columns.len() != precisions.len() {
        return Err("Invalid Fluss bucket routing parameters".into());
    }
    let mut key = Vec::with_capacity(64);
    let mut result = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        key.clear();
        for (&column, &precision) in columns.iter().zip(precisions) {
            let array = batch
                .columns()
                .get(column)
                .ok_or("bucket column out of range")?;
            key_field(&mut key, array.as_ref(), row, precision)?;
        }
        result.push(bucket_for_key(&key, count));
    }
    Ok(result)
}
pub fn split_by_bucket(
    batch: &RecordBatch,
    columns: &[usize],
    precisions: &[usize],
    count: i32,
) -> Result<Vec<(i32, RecordBatch)>, String> {
    if batch.num_rows() == 0 {
        return Ok(vec![]);
    }
    let ids = Int32Array::from(bucket_ids(batch, columns, precisions, count)?);
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(
        "__fluss_bucket",
        DataType::Int32,
        false,
    )));
    let mut arrays = batch.columns().to_vec();
    arrays.push(Arc::new(ids));
    let grouped =
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|e| e.to_string())?;
    streamfusion_bridge::partition::split_by_partition_columns(&grouped, &[batch.num_columns()])
        .into_iter()
        .map(|group| {
            let bucket = group
                .column(batch.num_columns())
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0);
            let payload = RecordBatch::try_new(
                batch.schema(),
                group.columns()[..batch.num_columns()].to_vec(),
            )
            .map_err(|e| e.to_string())?;
            Ok((bucket, payload))
        })
        .collect()
}
/// Return [minimum row, maximum row, null count] per statistics column. Only the two extrema
/// are exposed to the SDK's schema-aware serializer, keeping the scan columnar.
pub fn statistics(batch: &RecordBatch, columns: &[usize]) -> Result<Vec<i32>, String> {
    let mut result = Vec::with_capacity(columns.len() * 3);
    for &column in columns {
        let array = batch
            .columns()
            .get(column)
            .ok_or("statistics column out of range")?;
        let supported =
            matches!(
                array.data_type(),
                DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Utf8
                    | DataType::Decimal128(_, _)
                    | DataType::Date32
                    | DataType::Time32(TimeUnit::Millisecond)
                    | DataType::Timestamp(_, _)
            ) || streamfusion_bridge::timestamp::is_component_timestamp(array.data_type());
        let mut min = None;
        let mut max = None;
        if supported {
            let compare =
                arrow_ord::ord::make_comparator(array.as_ref(), array.as_ref(), Default::default())
                    .map_err(|e| e.to_string())?;
            let cmp = |a: usize, b: usize| -> Ordering {
                // Java Float/Double.compare canonicalize NaNs and distinguish signed zero.
                match array.data_type() {
                    DataType::Float32 => {
                        let v = array.as_any().downcast_ref::<Float32Array>().unwrap();
                        let a = v.value(a);
                        let b = v.value(b);
                        if a.is_nan() || b.is_nan() {
                            a.is_nan().cmp(&b.is_nan())
                        } else {
                            a.total_cmp(&b)
                        }
                    }
                    DataType::Float64 => {
                        let v = array.as_any().downcast_ref::<Float64Array>().unwrap();
                        let a = v.value(a);
                        let b = v.value(b);
                        if a.is_nan() || b.is_nan() {
                            a.is_nan().cmp(&b.is_nan())
                        } else {
                            a.total_cmp(&b)
                        }
                    }
                    _ => compare(a, b),
                }
            };
            for row in 0..array.len() {
                if array.is_null(row) {
                    continue;
                }
                if min.is_none_or(|i| cmp(row, i) == Ordering::Less) {
                    min = Some(row);
                }
                if max.is_none_or(|i| cmp(row, i) == Ordering::Greater) {
                    max = Some(row);
                }
            }
        }
        result.extend([
            min.map_or(-1, |i| i as i32),
            max.map_or(-1, |i| i as i32),
            array.null_count() as i32,
        ]);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn released_fluss_hash_vectors_include_signed_tail_bytes() {
        assert_eq!(bucket_for_key(&[0, 10], 7), 1);
        assert_eq!(bucket_for_key(&[0, 10, 10, 10], 12), 0);
        assert_eq!(bucket_for_key(&[0x80, 1], 7), 2);
        assert_eq!(
            bucket_for_key(b"The quick brown fox jumps over the lazy dog", 8),
            6
        );
    }
    #[test]
    fn user_struct_with_timestamp_field_names_has_only_null_count_statistics() {
        let fields = vec![
            Arc::new(Field::new("millis", DataType::Int64, false)),
            Arc::new(Field::new("nano_of_milli", DataType::Int32, false)),
        ];
        let array = StructArray::new(
            fields.into(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![0, 0])),
            ],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "row",
                array.data_type().clone(),
                false,
            )])),
            vec![Arc::new(array)],
        )
        .unwrap();
        assert_eq!(statistics(&batch, &[0]).unwrap(), [-1, -1, 0]);
        assert!(bucket_ids(&batch, &[0], &[9], 7).is_err());
    }
    #[test]
    fn empty_and_invalid_inputs_do_not_create_groups() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![None]))],
        )
        .unwrap();
        assert!(bucket_ids(&batch, &[0], &[0], 7).is_err());
        assert!(bucket_ids(&batch, &[0], &[0], 0).is_err());
        assert!(bucket_ids(&batch, &[1], &[0], 7).is_err());
        assert_eq!(statistics(&batch, &[0]).unwrap(), [-1, -1, 1]);
        assert!(split_by_bucket(&batch.slice(0, 0), &[0], &[0], 7)
            .unwrap()
            .is_empty());
    }
}
