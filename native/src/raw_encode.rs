//! Arrow → raw-bytes encoder for the Kafka sink's `raw` format, mirroring Flink's
//! `RawFormatSerializationSchema`: the single column's value IS the message — strings as their
//! UTF-8 bytes (the plan gate admits only UTF-8 `raw.charset` values), bytes verbatim, BOOLEAN as
//! one byte, and the fixed-width numerics in the table's `raw.endianness`. Flink writes a NULL
//! field as a null `byte[]` — a Kafka tombstone — which the sink's value path does not produce, so
//! the plan gate admits only NOT NULL columns and a null slot here is a hard error, not silence.

use crate::*;

pub(crate) struct RawEncodeOptions {
    little_endian: bool,
}

impl RawEncodeOptions {
    pub(crate) fn parse(encoded: &str) -> Result<RawEncodeOptions, String> {
        let mut little_endian = false;
        for line in encoded.lines().filter(|line| !line.is_empty()) {
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| format!("raw encode option is not key=value: {line}"))?;
            match key {
                "endianness" => {
                    little_endian = match value {
                        "little-endian" => true,
                        "big-endian" => false,
                        other => return Err(format!("unknown raw endianness {other}")),
                    }
                }
                other => return Err(format!("unknown raw encode option {other}")),
            }
        }
        Ok(RawEncodeOptions { little_endian })
    }
}

pub(crate) fn encode_raw_batch(
    batch: &RecordBatch,
    options: &RawEncodeOptions,
) -> Result<(Vec<u8>, Vec<std::ops::Range<usize>>), String> {
    if batch.num_columns() != 1 {
        return Err(format!(
            "raw encode expects the single physical column, got {}",
            batch.num_columns()
        ));
    }
    let column = batch.column(0);
    let mut bytes = Vec::new();
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if column.is_null(row) {
            // Flink writes a Kafka tombstone here; the plan gate admits only NOT NULL columns
            // precisely because the value path cannot carry one.
            return Err("raw encode cannot serialize a NULL value (a Kafka tombstone)".to_string());
        }
        let start = bytes.len();
        write_raw_value(column, row, options.little_endian, &mut bytes)?;
        rows.push(start..bytes.len());
    }
    Ok((bytes, rows))
}

fn write_raw_value(
    column: &ArrayRef,
    row: usize,
    little_endian: bool,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    use arrow::array::{FixedSizeBinaryArray, Float64Array};
    match column.data_type() {
        DataType::Utf8 => out.extend_from_slice(
            column.as_any().downcast_ref::<StringArray>().unwrap().value(row).as_bytes(),
        ),
        DataType::Binary => out.extend_from_slice(
            column.as_any().downcast_ref::<BinaryArray>().unwrap().value(row),
        ),
        DataType::FixedSizeBinary(_) => out.extend_from_slice(
            column.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap().value(row),
        ),
        DataType::Boolean => out.push(u8::from(
            column.as_any().downcast_ref::<BooleanArray>().unwrap().value(row),
        )),
        DataType::Int8 => out.extend_from_slice(
            &column.as_any().downcast_ref::<Int8Array>().unwrap().value(row).to_be_bytes(),
        ),
        DataType::Int16 => {
            let value = column.as_any().downcast_ref::<Int16Array>().unwrap().value(row);
            extend_endian(&value.to_be_bytes(), &value.to_le_bytes(), little_endian, out);
        }
        DataType::Int32 => {
            let value = column.as_any().downcast_ref::<Int32Array>().unwrap().value(row);
            extend_endian(&value.to_be_bytes(), &value.to_le_bytes(), little_endian, out);
        }
        DataType::Int64 => {
            let value = column.as_any().downcast_ref::<Int64Array>().unwrap().value(row);
            extend_endian(&value.to_be_bytes(), &value.to_le_bytes(), little_endian, out);
        }
        DataType::Float32 => {
            let value = column.as_any().downcast_ref::<Float32Array>().unwrap().value(row);
            extend_endian(&value.to_be_bytes(), &value.to_le_bytes(), little_endian, out);
        }
        DataType::Float64 => {
            let value = column.as_any().downcast_ref::<Float64Array>().unwrap().value(row);
            extend_endian(&value.to_be_bytes(), &value.to_le_bytes(), little_endian, out);
        }
        other => return Err(format!("raw encode does not support Arrow type {other}")),
    }
    Ok(())
}

fn extend_endian(big: &[u8], little: &[u8], little_endian: bool, out: &mut Vec<u8>) {
    out.extend_from_slice(if little_endian { little } else { big });
}
