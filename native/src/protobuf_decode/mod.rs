// Derived from ptars' Apache-2.0 protobuf-to-Arrow decoder. StreamFusion owns this narrower copy so
// its wire semantics can follow Flink while its hot loop is optimized for streaming batches.
mod config;
#[cfg(test)]
mod ported_tests;

pub(crate) use config::PtarsConfig;

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::array::builder::ArrayBuilder;
use arrow::array::builder::{
    BinaryBuilder, BooleanBuilder, LargeBinaryBuilder, LargeStringBuilder, PrimitiveBuilder,
    StringBuilder,
};
use arrow::array::types::{
    Date32Type, Float32Type, Float64Type, Int32Type, Int64Type, UInt32Type, UInt64Type,
};
use arrow::array::ArrayData;
use arrow::array::{Array, ArrowPrimitiveType, BinaryArray, MapArray, RecordBatch, StructArray};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, TimeUnit};
use chrono::Datelike;

use prost_reflect::{EnumDescriptor, FieldDescriptor, Kind, MessageDescriptor, Syntax};

use config::{ConfluentWirePolicy, EnumRepr};

#[cfg(test)]
static PREPARED_PLAN_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Descriptor-specific field layout prepared recursively when a task decoder opens. Arrow builders
/// remain batch-local, but every message's field operations and wire-tag dispatch are reused.
pub(crate) struct PreparedMessagePlan {
    fields: Vec<PreparedField>,
    tag_map: Vec<Option<usize>>,
    oneof_count: usize,
    #[cfg(test)]
    serial: usize,
}

struct PreparedField {
    descriptor: FieldDescriptor,
    children: PreparedChildren,
    oneof: Option<usize>,
}

enum PreparedChildren {
    None,
    Message(Box<PreparedMessagePlan>),
    RepeatedMessage(Box<PreparedMessagePlan>),
    Map {
        key: Box<PreparedField>,
        value: Box<PreparedField>,
    },
}

impl PreparedField {
    fn new(descriptor: FieldDescriptor, oneof: Option<usize>) -> Self {
        let children = if descriptor.is_map() {
            let kind = descriptor.kind();
            let map_entry = kind.as_message().expect("protobuf map entry");
            PreparedChildren::Map {
                key: Box::new(Self::new(map_entry.map_entry_key_field(), None)),
                value: Box::new(Self::new(map_entry.map_entry_value_field(), None)),
            }
        } else if descriptor.is_list() {
            match descriptor.kind() {
                Kind::Message(message) => {
                    PreparedChildren::RepeatedMessage(Box::new(PreparedMessagePlan::new(&message)))
                }
                _ => PreparedChildren::None,
            }
        } else {
            match descriptor.kind() {
                Kind::Message(message) => {
                    PreparedChildren::Message(Box::new(PreparedMessagePlan::new(&message)))
                }
                _ => PreparedChildren::None,
            }
        };
        Self {
            descriptor,
            children,
            oneof,
        }
    }
}

impl PreparedMessagePlan {
    pub(crate) fn new(descriptor: &MessageDescriptor) -> Self {
        let descriptors: Vec<_> = descriptor.fields().collect();
        let oneofs: Vec<_> = descriptor
            .oneofs()
            .map(|oneof| oneof.full_name().to_string())
            .collect();
        let max_field_number = descriptors
            .iter()
            .map(FieldDescriptor::number)
            .max()
            .unwrap_or(0);
        let mut tag_map = vec![None; max_field_number as usize + usize::from(max_field_number > 0)];
        for (index, field) in descriptors.iter().enumerate() {
            tag_map[field.number() as usize] = Some(index);
        }
        let fields = descriptors
            .into_iter()
            .map(|field| {
                let oneof = field.containing_oneof().and_then(|field_oneof| {
                    oneofs
                        .iter()
                        .position(|name| name == field_oneof.full_name())
                });
                PreparedField::new(field, oneof)
            })
            .collect();
        Self {
            fields,
            tag_map,
            oneof_count: oneofs.len(),
            #[cfg(test)]
            serial: PREPARED_PLAN_COUNT.fetch_add(1, Ordering::Relaxed) + 1,
        }
    }

    #[cfg(test)]
    pub(crate) fn serial(&self) -> usize {
        self.serial
    }
}

// ---------------------------------------------------------------------------
// Wire format decoding primitives
// ---------------------------------------------------------------------------

#[allow(deprecated)]
fn decode_error(msg: &str) -> prost::DecodeError {
    prost::DecodeError::new(msg.to_string())
}

#[inline(always)]
fn decode_varint(buf: &[u8]) -> Result<(u64, usize), prost::DecodeError> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    for (i, &byte) in buf.iter().enumerate() {
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err(decode_error("varint too large"));
        }
    }
    Err(decode_error("unexpected EOF in varint"))
}

#[inline(always)]
fn decode_tag(buf: &[u8]) -> Result<(u32, u8, usize), prost::DecodeError> {
    let (key, n) = decode_varint(buf)?;
    let wire_type = (key & 0x07) as u8;
    let field_number = (key >> 3) as u32;
    if field_number == 0 {
        return Err(decode_error("invalid field number 0"));
    }
    Ok((field_number, wire_type, n))
}

#[inline(always)]
fn skip_field(wire_type: u8, buf: &[u8]) -> Result<usize, prost::DecodeError> {
    match wire_type {
        0 => {
            let (_, n) = decode_varint(buf)?;
            Ok(n)
        }
        1 => {
            if buf.len() < 8 {
                return Err(decode_error("unexpected EOF"));
            }
            Ok(8)
        }
        2 => {
            let (len, n) = decode_varint(buf)?;
            let total = n + len as usize;
            if buf.len() < total {
                return Err(decode_error("unexpected EOF"));
            }
            Ok(total)
        }
        5 => {
            if buf.len() < 4 {
                return Err(decode_error("unexpected EOF"));
            }
            Ok(4)
        }
        _ => Err(decode_error("unsupported wire type")),
    }
}

/// Read a length-delimited field, returning (data_slice, bytes_consumed).
#[inline(always)]
fn read_length_delimited(buf: &[u8]) -> Result<(&[u8], usize), prost::DecodeError> {
    let (len, n) = decode_varint(buf)?;
    let len = len as usize;
    let total = n + len;
    if buf.len() < total {
        return Err(decode_error("unexpected EOF"));
    }
    Ok((&buf[n..total], total))
}

/// Strip the Confluent Schema Registry wire format prefix from a message.
fn strip_confluent_prefix(
    buf: &[u8],
    policy: ConfluentWirePolicy,
) -> Result<&[u8], prost::DecodeError> {
    match policy {
        ConfluentWirePolicy::Raw => Ok(buf),
        ConfluentWirePolicy::Standard => {
            if buf.len() < 5 {
                return Err(decode_error(
                    "message too short for Confluent wire format header",
                ));
            }
            Ok(&buf[5..])
        }
        ConfluentWirePolicy::Protobuf => {
            if buf.len() < 5 {
                return Err(decode_error(
                    "message too short for Confluent wire format header",
                ));
            }
            let remaining = &buf[5..];
            // Read varint-encoded count of message indexes
            let (count, mut offset) = decode_varint(remaining)?;
            // Skip `count` varints (the message indexes themselves)
            for _ in 0..count {
                let (_, n) = decode_varint(&remaining[offset..])?;
                offset += n;
            }
            Ok(&remaining[offset..])
        }
    }
}

#[inline]
fn decode_zigzag32(v: u64) -> i32 {
    let v = v as u32;
    ((v >> 1) as i32) ^ (-((v & 1) as i32))
}

#[inline]
fn decode_zigzag64(v: u64) -> i64 {
    ((v >> 1) as i64) ^ (-((v & 1) as i64))
}

fn convert_seconds_nanos_to_unit(seconds: i64, nanos: i32, unit: TimeUnit, type_name: &str) -> i64 {
    match unit {
        TimeUnit::Second => seconds,
        TimeUnit::Millisecond => seconds
            .checked_mul(1_000)
            .and_then(|s| s.checked_add(i64::from(nanos) / 1_000_000))
            .unwrap_or_else(|| panic!("{type_name} overflow")),
        TimeUnit::Microsecond => seconds
            .checked_mul(1_000_000)
            .and_then(|s| s.checked_add(i64::from(nanos) / 1_000))
            .unwrap_or_else(|| panic!("{type_name} overflow")),
        TimeUnit::Nanosecond => seconds
            .checked_mul(1_000_000_000)
            .and_then(|s| s.checked_add(i64::from(nanos)))
            .unwrap_or_else(|| panic!("{type_name} overflow")),
    }
}

static CE_OFFSET: i32 = 719163;

fn enum_name(enum_descriptor: &EnumDescriptor, number: i32) -> String {
    match enum_descriptor.get_value(number) {
        Some(v) => v.name().to_string(),
        None => number.to_string(),
    }
}

// ---------------------------------------------------------------------------
// String/Binary builders
// ---------------------------------------------------------------------------

enum StringBuilderInner {
    Regular(StringBuilder),
    Large(LargeStringBuilder),
}

impl StringBuilderInner {
    fn new(use_large: bool, rows: usize) -> Self {
        if use_large {
            Self::Large(LargeStringBuilder::with_capacity(rows, 0))
        } else {
            Self::Regular(StringBuilder::with_capacity(rows, 0))
        }
    }
    fn append_value(&mut self, value: &str) {
        match self {
            Self::Regular(builder) => builder.append_value(value),
            Self::Large(builder) => builder.append_value(value),
        }
    }
    fn append_null(&mut self) {
        match self {
            Self::Regular(builder) => builder.append_null(),
            Self::Large(builder) => builder.append_null(),
        }
    }
    fn append_default(&mut self) {
        self.append_value("");
    }
    fn append_default_n(&mut self, rows: usize) {
        match self {
            Self::Regular(builder) => builder.append_value_n("", rows),
            Self::Large(builder) => builder.append_value_n("", rows),
        }
    }
    fn append_value_n(&mut self, value: &str, rows: usize) {
        match self {
            Self::Regular(builder) => builder.append_value_n(value, rows),
            Self::Large(builder) => builder.append_value_n(value, rows),
        }
    }
    fn append_nulls(&mut self, rows: usize) {
        match self {
            Self::Regular(builder) => builder.append_nulls(rows),
            Self::Large(builder) => builder.append_nulls(rows),
        }
    }
    fn finish(&mut self) -> Arc<dyn Array> {
        match self {
            Self::Regular(builder) => Arc::new(std::mem::take(builder).finish()),
            Self::Large(builder) => Arc::new(std::mem::take(builder).finish()),
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::Regular(builder) => ArrayBuilder::len(builder),
            Self::Large(builder) => ArrayBuilder::len(builder),
        }
    }
}

enum BinaryBuilderInner {
    Regular(BinaryBuilder),
    Large(LargeBinaryBuilder),
}

impl BinaryBuilderInner {
    fn new(use_large: bool, rows: usize) -> Self {
        if use_large {
            Self::Large(LargeBinaryBuilder::with_capacity(rows, 0))
        } else {
            Self::Regular(BinaryBuilder::with_capacity(rows, 0))
        }
    }
    fn append_value(&mut self, value: &[u8]) {
        match self {
            Self::Regular(builder) => builder.append_value(value),
            Self::Large(builder) => builder.append_value(value),
        }
    }
    fn append_null(&mut self) {
        match self {
            Self::Regular(builder) => builder.append_null(),
            Self::Large(builder) => builder.append_null(),
        }
    }
    fn append_default(&mut self) {
        self.append_value(b"");
    }
    fn append_default_n(&mut self, rows: usize) {
        match self {
            Self::Regular(builder) => builder.append_value_n(b"", rows),
            Self::Large(builder) => builder.append_value_n(b"", rows),
        }
    }
    fn append_nulls(&mut self, rows: usize) {
        match self {
            Self::Regular(builder) => builder.append_nulls(rows),
            Self::Large(builder) => builder.append_nulls(rows),
        }
    }
    fn finish(&mut self) -> Arc<dyn Array> {
        match self {
            Self::Regular(builder) => Arc::new(std::mem::take(builder).finish()),
            Self::Large(builder) => Arc::new(std::mem::take(builder).finish()),
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::Regular(builder) => ArrayBuilder::len(builder),
            Self::Large(builder) => ArrayBuilder::len(builder),
        }
    }
}

// ---------------------------------------------------------------------------
// ListOffsets
// ---------------------------------------------------------------------------

enum ListOffsets {
    Regular(Vec<i32>),
    Large(Vec<i64>),
}

impl ListOffsets {
    fn new(use_large: bool) -> Self {
        Self::with_capacity(use_large, 0)
    }

    fn with_capacity(use_large: bool, rows: usize) -> Self {
        if use_large {
            let mut offsets = Vec::with_capacity(rows + 1);
            offsets.push(0);
            Self::Large(offsets)
        } else {
            let mut offsets = Vec::with_capacity(rows + 1);
            offsets.push(0);
            Self::Regular(offsets)
        }
    }
    fn push(&mut self, value: usize) {
        match self {
            Self::Regular(v) => v.push(value as i32),
            Self::Large(v) => v.push(value as i64),
        }
    }
    fn repeat_last(&mut self, rows: usize) {
        match self {
            Self::Regular(offsets) => {
                let last = *offsets.last().expect("list offset");
                offsets.extend(std::iter::repeat_n(last, rows));
            }
            Self::Large(offsets) => {
                let last = *offsets.last().expect("large-list offset");
                offsets.extend(std::iter::repeat_n(last, rows));
            }
        }
    }
    fn finish(self, values: Arc<dyn Array>, name: &str, nullable: bool) -> Arc<dyn Array> {
        let field = Arc::new(Field::new(name, values.data_type().clone(), nullable));
        match self {
            Self::Regular(offsets) => {
                let buf = Buffer::from_vec(offsets);
                let data = ArrayData::builder(DataType::List(field))
                    .len(buf.len() / 4 - 1)
                    .add_buffer(buf)
                    .add_child_data(values.to_data())
                    .build()
                    .unwrap();
                Arc::new(arrow::array::ListArray::from(data))
            }
            Self::Large(offsets) => {
                let buf = Buffer::from_vec(offsets);
                let data = ArrayData::builder(DataType::LargeList(field))
                    .len(buf.len() / 8 - 1)
                    .add_buffer(buf)
                    .add_child_data(values.to_data())
                    .build()
                    .unwrap();
                Arc::new(arrow::array::LargeListArray::from(data))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// RepeatedInner enum — repeated field value storage
// ---------------------------------------------------------------------------

enum RepeatedInner<'a> {
    Int32 {
        values_builder: PrimitiveBuilder<Int32Type>,
    },
    Int64 {
        values_builder: PrimitiveBuilder<Int64Type>,
    },
    UInt32 {
        values_builder: PrimitiveBuilder<UInt32Type>,
    },
    UInt64 {
        values_builder: PrimitiveBuilder<UInt64Type>,
    },
    Float {
        values_builder: PrimitiveBuilder<Float32Type>,
    },
    Double {
        values_builder: PrimitiveBuilder<Float64Type>,
    },
    Bool {
        values_builder: BooleanBuilder,
    },
    String {
        values_builder: StringBuilderInner,
    },
    Bytes {
        values_builder: BinaryBuilderInner,
    },
    Sint32 {
        values_builder: PrimitiveBuilder<Int32Type>,
    },
    Sint64 {
        values_builder: PrimitiveBuilder<Int64Type>,
    },
    Sfixed32 {
        values_builder: PrimitiveBuilder<Int32Type>,
    },
    Sfixed64 {
        values_builder: PrimitiveBuilder<Int64Type>,
    },
    Fixed32 {
        values_builder: PrimitiveBuilder<UInt32Type>,
    },
    Fixed64 {
        values_builder: PrimitiveBuilder<UInt64Type>,
    },
    EnumInt32 {
        values_builder: PrimitiveBuilder<Int32Type>,
    },
    EnumString {
        values_builder: StringBuilderInner,
        enum_descriptor: EnumDescriptor,
    },
    EnumBinary {
        values_builder: BinaryBuilderInner,
        enum_descriptor: EnumDescriptor,
    },
    Message {
        sub_decoder: MessageDecoder<'a>,
    },
    Timestamp {
        values_builder: PrimitiveBuilder<Int64Type>,
        unit: TimeUnit,
        tz: Option<Arc<str>>,
    },
    Duration {
        values_builder: PrimitiveBuilder<Int64Type>,
        unit: TimeUnit,
    },
    Date {
        values_builder: PrimitiveBuilder<Date32Type>,
    },
    TimeOfDay {
        values_builder: PrimitiveBuilder<Int64Type>,
        unit: TimeUnit,
    },
    WrapperDouble {
        values_builder: PrimitiveBuilder<Float64Type>,
    },
    WrapperFloat {
        values_builder: PrimitiveBuilder<Float32Type>,
    },
    WrapperInt64 {
        values_builder: PrimitiveBuilder<Int64Type>,
    },
    WrapperUInt64 {
        values_builder: PrimitiveBuilder<UInt64Type>,
    },
    WrapperInt32 {
        values_builder: PrimitiveBuilder<Int32Type>,
    },
    WrapperUInt32 {
        values_builder: PrimitiveBuilder<UInt32Type>,
    },
    WrapperBool {
        values_builder: BooleanBuilder,
    },
    WrapperString {
        values_builder: StringBuilderInner,
    },
    WrapperBytes {
        values_builder: BinaryBuilderInner,
    },
}

impl<'a> RepeatedInner<'a> {
    fn decode(&mut self, wire_type: u8, buf: &'a [u8]) -> Result<usize, prost::DecodeError> {
        match self {
            Self::Int32 { values_builder, .. } => {
                decode_repeated_varint(wire_type, buf, values_builder, |v| v as i32)
            }
            Self::Int64 { values_builder, .. } => {
                decode_repeated_varint(wire_type, buf, values_builder, |v| v as i64)
            }
            Self::UInt32 { values_builder, .. } => {
                decode_repeated_varint(wire_type, buf, values_builder, |v| v as u32)
            }
            Self::UInt64 { values_builder, .. } => {
                decode_repeated_varint(wire_type, buf, values_builder, |v| v)
            }
            Self::EnumInt32 { values_builder, .. } => {
                decode_repeated_varint(wire_type, buf, values_builder, |v| v as i32)
            }
            Self::Sint32 { values_builder, .. } => {
                decode_repeated_varint(wire_type, buf, values_builder, decode_zigzag32)
            }
            Self::Sint64 { values_builder, .. } => {
                decode_repeated_varint(wire_type, buf, values_builder, decode_zigzag64)
            }
            Self::Sfixed32 { values_builder, .. } => decode_repeated_fixed::<Int32Type, 4>(
                wire_type,
                5,
                buf,
                values_builder,
                i32::from_le_bytes,
            ),
            Self::Sfixed64 { values_builder, .. } => decode_repeated_fixed::<Int64Type, 8>(
                wire_type,
                1,
                buf,
                values_builder,
                i64::from_le_bytes,
            ),
            Self::Fixed32 { values_builder, .. } => decode_repeated_fixed::<UInt32Type, 4>(
                wire_type,
                5,
                buf,
                values_builder,
                u32::from_le_bytes,
            ),
            Self::Fixed64 { values_builder, .. } => decode_repeated_fixed::<UInt64Type, 8>(
                wire_type,
                1,
                buf,
                values_builder,
                u64::from_le_bytes,
            ),
            Self::Float { values_builder, .. } => decode_repeated_fixed::<Float32Type, 4>(
                wire_type,
                5,
                buf,
                values_builder,
                f32::from_le_bytes,
            ),
            Self::Double { values_builder, .. } => decode_repeated_fixed::<Float64Type, 8>(
                wire_type,
                1,
                buf,
                values_builder,
                f64::from_le_bytes,
            ),
            Self::Bool { values_builder, .. } => {
                if wire_type == 2 {
                    let (data, total) = read_length_delimited(buf)?;
                    let mut p = 0;
                    while p < data.len() {
                        let (v, n) = decode_varint(&data[p..])?;
                        values_builder.append_value(v != 0);
                        p += n;
                    }
                    Ok(total)
                } else if wire_type == 0 {
                    let (v, n) = decode_varint(buf)?;
                    values_builder.append_value(v != 0);
                    Ok(n)
                } else {
                    skip_field(wire_type, buf)
                }
            }
            Self::String { values_builder, .. } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let s = std::str::from_utf8(data).map_err(|_| decode_error("invalid UTF-8"))?;
                values_builder.append_value(s);
                Ok(total)
            }
            Self::Bytes { values_builder, .. } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                values_builder.append_value(data);
                Ok(total)
            }
            Self::EnumString {
                values_builder,
                enum_descriptor,
                ..
            } => {
                if wire_type == 2 {
                    let (data, total) = read_length_delimited(buf)?;
                    let mut p = 0;
                    while p < data.len() {
                        let (v, n) = decode_varint(&data[p..])?;
                        values_builder.append_value(&enum_name(enum_descriptor, v as i32));
                        p += n;
                    }
                    Ok(total)
                } else if wire_type == 0 {
                    let (v, n) = decode_varint(buf)?;
                    values_builder.append_value(&enum_name(enum_descriptor, v as i32));
                    Ok(n)
                } else {
                    skip_field(wire_type, buf)
                }
            }
            Self::EnumBinary {
                values_builder,
                enum_descriptor,
                ..
            } => {
                if wire_type == 2 {
                    let (data, total) = read_length_delimited(buf)?;
                    let mut p = 0;
                    while p < data.len() {
                        let (v, n) = decode_varint(&data[p..])?;
                        values_builder
                            .append_value(enum_name(enum_descriptor, v as i32).as_bytes());
                        p += n;
                    }
                    Ok(total)
                } else if wire_type == 0 {
                    let (v, n) = decode_varint(buf)?;
                    values_builder.append_value(enum_name(enum_descriptor, v as i32).as_bytes());
                    Ok(n)
                } else {
                    skip_field(wire_type, buf)
                }
            }
            Self::Message { sub_decoder, .. } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                sub_decoder.decode_row(data)?;
                Ok(total)
            }
            Self::Timestamp {
                values_builder,
                unit,
                ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let vals = decode_wkt_submessage(data, 2)?;
                values_builder.append_value(convert_seconds_nanos_to_unit(
                    vals[0],
                    vals[1] as i32,
                    *unit,
                    "Timestamp",
                ));
                Ok(total)
            }
            Self::Duration {
                values_builder,
                unit,
                ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let vals = decode_wkt_submessage(data, 2)?;
                values_builder.append_value(convert_seconds_nanos_to_unit(
                    vals[0],
                    vals[1] as i32,
                    *unit,
                    "Duration",
                ));
                Ok(total)
            }
            Self::Date { values_builder, .. } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let vals = decode_wkt_submessage(data, 3)?;
                let (y, m, d) = (vals[0] as i32, vals[1] as i32, vals[2] as i32);
                if y == 0 && m == 0 && d == 0 {
                    values_builder.append_value(0);
                } else {
                    values_builder.append_value(
                        chrono::NaiveDate::from_ymd_opt(y, m as u32, d as u32)
                            .unwrap()
                            .num_days_from_ce()
                            - CE_OFFSET,
                    );
                }
                Ok(total)
            }
            Self::TimeOfDay {
                values_builder,
                unit,
                ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let vals = decode_wkt_submessage(data, 4)?;
                let total_seconds = vals[0] * 3600 + vals[1] * 60 + vals[2];
                values_builder.append_value(convert_seconds_nanos_to_unit(
                    total_seconds,
                    vals[3] as i32,
                    *unit,
                    "TimeOfDay",
                ));
                Ok(total)
            }
            Self::WrapperDouble { values_builder, .. } => {
                decode_repeated_wrapper_fixed64(wire_type, buf, values_builder, f64::from_le_bytes)
            }
            Self::WrapperFloat { values_builder, .. } => {
                decode_repeated_wrapper_fixed32(wire_type, buf, values_builder, f32::from_le_bytes)
            }
            Self::WrapperInt64 { values_builder, .. } => {
                decode_repeated_wrapper_varint(wire_type, buf, values_builder, |v| v as i64)
            }
            Self::WrapperUInt64 { values_builder, .. } => {
                decode_repeated_wrapper_varint(wire_type, buf, values_builder, |v| v)
            }
            Self::WrapperInt32 { values_builder, .. } => {
                decode_repeated_wrapper_varint(wire_type, buf, values_builder, |v| v as i32)
            }
            Self::WrapperUInt32 { values_builder, .. } => {
                decode_repeated_wrapper_varint(wire_type, buf, values_builder, |v| v as u32)
            }
            Self::WrapperBool { values_builder, .. } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, _) = decode_wrapper_varint(data)?;
                values_builder.append_value(v != 0);
                Ok(total)
            }
            Self::WrapperString { values_builder, .. } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, found) = decode_wrapper_string(data)?;
                if found {
                    values_builder.append_value(unsafe { std::str::from_utf8_unchecked(&v) });
                } else {
                    values_builder.append_value("");
                }
                Ok(total)
            }
            Self::WrapperBytes { values_builder, .. } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, found) = decode_wrapper_bytes(data)?;
                if found {
                    values_builder.append_value(&v);
                } else {
                    values_builder.append_value(b"");
                }
                Ok(total)
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Int32 { values_builder, .. }
            | Self::Sint32 { values_builder, .. }
            | Self::Sfixed32 { values_builder, .. }
            | Self::EnumInt32 { values_builder, .. }
            | Self::WrapperInt32 { values_builder, .. } => values_builder.len(),
            Self::Int64 { values_builder, .. }
            | Self::Sint64 { values_builder, .. }
            | Self::Sfixed64 { values_builder, .. }
            | Self::Timestamp { values_builder, .. }
            | Self::Duration { values_builder, .. }
            | Self::WrapperInt64 { values_builder, .. }
            | Self::TimeOfDay { values_builder, .. } => values_builder.len(),
            Self::UInt32 { values_builder, .. }
            | Self::Fixed32 { values_builder, .. }
            | Self::WrapperUInt32 { values_builder, .. } => values_builder.len(),
            Self::UInt64 { values_builder, .. }
            | Self::Fixed64 { values_builder, .. }
            | Self::WrapperUInt64 { values_builder, .. } => values_builder.len(),
            Self::Float { values_builder, .. } | Self::WrapperFloat { values_builder, .. } => {
                values_builder.len()
            }
            Self::Double { values_builder, .. } | Self::WrapperDouble { values_builder, .. } => {
                values_builder.len()
            }
            Self::Bool { values_builder, .. } | Self::WrapperBool { values_builder, .. } => {
                values_builder.len()
            }
            Self::String { values_builder, .. }
            | Self::EnumString { values_builder, .. }
            | Self::WrapperString { values_builder, .. } => values_builder.len(),
            Self::Bytes { values_builder, .. }
            | Self::EnumBinary { values_builder, .. }
            | Self::WrapperBytes { values_builder, .. } => values_builder.len(),
            Self::Message { sub_decoder, .. } => sub_decoder.row_count(),
            Self::Date { values_builder, .. } => values_builder.len(),
        }
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        match self {
            Self::Int32 { values_builder, .. }
            | Self::Sint32 { values_builder, .. }
            | Self::Sfixed32 { values_builder, .. }
            | Self::EnumInt32 { values_builder, .. }
            | Self::WrapperInt32 { values_builder, .. } => {
                Arc::new(std::mem::take(values_builder).finish())
            }
            Self::Int64 { values_builder, .. }
            | Self::Sint64 { values_builder, .. }
            | Self::Sfixed64 { values_builder, .. }
            | Self::WrapperInt64 { values_builder, .. } => {
                Arc::new(std::mem::take(values_builder).finish())
            }
            Self::UInt32 { values_builder, .. }
            | Self::Fixed32 { values_builder, .. }
            | Self::WrapperUInt32 { values_builder, .. } => {
                Arc::new(std::mem::take(values_builder).finish())
            }
            Self::UInt64 { values_builder, .. }
            | Self::Fixed64 { values_builder, .. }
            | Self::WrapperUInt64 { values_builder, .. } => {
                Arc::new(std::mem::take(values_builder).finish())
            }
            Self::Float { values_builder, .. } | Self::WrapperFloat { values_builder, .. } => {
                Arc::new(std::mem::take(values_builder).finish())
            }
            Self::Double { values_builder, .. } | Self::WrapperDouble { values_builder, .. } => {
                Arc::new(std::mem::take(values_builder).finish())
            }
            Self::Bool { values_builder, .. } | Self::WrapperBool { values_builder, .. } => {
                Arc::new(std::mem::take(values_builder).finish())
            }
            Self::String { values_builder, .. }
            | Self::EnumString { values_builder, .. }
            | Self::WrapperString { values_builder, .. } => values_builder.finish(),
            Self::Bytes { values_builder, .. }
            | Self::EnumBinary { values_builder, .. }
            | Self::WrapperBytes { values_builder, .. } => values_builder.finish(),
            Self::Message { sub_decoder, .. } => Arc::new(sub_decoder.build_struct_array(None)),
            Self::Timestamp {
                values_builder,
                unit,
                tz,
            } => finish_timestamp(values_builder, *unit, tz),
            Self::Duration {
                values_builder,
                unit,
            } => finish_duration(values_builder, *unit),
            Self::Date { values_builder, .. } => Arc::new(std::mem::take(values_builder).finish()),
            Self::TimeOfDay {
                values_builder,
                unit,
                ..
            } => finish_time_of_day(values_builder, *unit),
        }
    }
}

// ---------------------------------------------------------------------------
// FieldDecoder enum — all types
// ---------------------------------------------------------------------------

enum FieldDecoder<'a> {
    // --- Singular scalars (buffered) ---
    Int32 {
        value: i32,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<Int32Type>,
    },
    Int64 {
        value: i64,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<Int64Type>,
    },
    UInt32 {
        value: u32,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<UInt32Type>,
    },
    UInt64 {
        value: u64,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<UInt64Type>,
    },
    Sint32 {
        value: i32,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<Int32Type>,
    },
    Sint64 {
        value: i64,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<Int64Type>,
    },
    Sfixed32 {
        value: i32,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<Int32Type>,
    },
    Sfixed64 {
        value: i64,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<Int64Type>,
    },
    Fixed32 {
        value: u32,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<UInt32Type>,
    },
    Fixed64 {
        value: u64,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<UInt64Type>,
    },
    Float {
        value: f32,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<Float32Type>,
    },
    Double {
        value: f64,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<Float64Type>,
    },
    Bool {
        value: bool,
        has_value: bool,
        has_presence: bool,
        builder: BooleanBuilder,
    },
    String {
        value: &'a str,
        has_value: bool,
        has_presence: bool,
        builder: StringBuilderInner,
    },
    Bytes {
        value: &'a [u8],
        has_value: bool,
        has_presence: bool,
        builder: BinaryBuilderInner,
    },
    EnumInt32 {
        value: i32,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<Int32Type>,
    },
    EnumString {
        value: i32,
        has_value: bool,
        has_presence: bool,
        builder: StringBuilderInner,
        enum_descriptor: EnumDescriptor,
    },
    EnumBinary {
        value: i32,
        has_value: bool,
        has_presence: bool,
        builder: BinaryBuilderInner,
        enum_descriptor: EnumDescriptor,
    },

    // --- Well-known types (singular, buffered) ---
    Timestamp {
        seconds: i64,
        nanos: i32,
        has_value: bool,
        builder: PrimitiveBuilder<Int64Type>,
        unit: TimeUnit,
        tz: Option<Arc<str>>,
    },
    Duration {
        seconds: i64,
        nanos: i32,
        has_value: bool,
        builder: PrimitiveBuilder<Int64Type>,
        unit: TimeUnit,
    },
    Date {
        year: i32,
        month: i32,
        day: i32,
        has_value: bool,
        builder: PrimitiveBuilder<Date32Type>,
    },
    TimeOfDay {
        hours: i32,
        minutes: i32,
        seconds_val: i32,
        nanos: i32,
        has_value: bool,
        builder: PrimitiveBuilder<Int64Type>,
        unit: TimeUnit,
    },

    // --- Wrapper types (singular) ---
    WrapperDouble {
        value: f64,
        has_value: bool,
        builder: PrimitiveBuilder<Float64Type>,
    },
    WrapperFloat {
        value: f32,
        has_value: bool,
        builder: PrimitiveBuilder<Float32Type>,
    },
    WrapperInt64 {
        value: i64,
        has_value: bool,
        builder: PrimitiveBuilder<Int64Type>,
    },
    WrapperUInt64 {
        value: u64,
        has_value: bool,
        builder: PrimitiveBuilder<UInt64Type>,
    },
    WrapperInt32 {
        value: i32,
        has_value: bool,
        builder: PrimitiveBuilder<Int32Type>,
    },
    WrapperUInt32 {
        value: u32,
        has_value: bool,
        builder: PrimitiveBuilder<UInt32Type>,
    },
    WrapperBool {
        value: bool,
        has_value: bool,
        builder: BooleanBuilder,
    },
    WrapperString {
        value: Vec<u8>,
        has_value: bool,
        builder: StringBuilderInner,
    },
    WrapperBytes {
        value: Vec<u8>,
        has_value: bool,
        builder: BinaryBuilderInner,
    },

    // --- Nested message ---
    Message {
        sub_decoder: MessageDecoder<'a>,
        has_value: bool,
        is_valid: BooleanBuilder,
    },

    // --- Repeated fields (all collapsed into one variant) ---
    Repeated {
        inner: RepeatedInner<'a>,
        offsets: ListOffsets,
        list_name: Arc<str>,
        list_nullable: bool,
    },

    // --- Map fields ---
    Map {
        key_decoder: Box<FieldDecoder<'a>>,
        value_decoder: Box<FieldDecoder<'a>>,
        offsets: Vec<i32>,
        map_value_name: Arc<str>,
        map_value_nullable: bool,
    },
}

// ---------------------------------------------------------------------------
// Helpers for decoding wire values inline
// ---------------------------------------------------------------------------

/// Decode fields of a well-known submessage with up to 4 int fields.
/// Returns (field1, field2, field3, field4) initialized to 0, scanning for field numbers 1..=max_field.
fn decode_wkt_submessage(buf: &[u8], max_field: u32) -> Result<[i64; 4], prost::DecodeError> {
    let mut vals = [0i64; 4];
    let mut pos = 0;
    while pos < buf.len() {
        let (fnum, wt, n) = decode_tag(&buf[pos..])?;
        pos += n;
        if fnum >= 1 && fnum <= max_field && wt == 0 {
            let (v, n) = decode_varint(&buf[pos..])?;
            vals[(fnum - 1) as usize] = v as i64;
            pos += n;
        } else {
            pos += skip_field(wt, &buf[pos..])?;
        }
    }
    Ok(vals)
}

/// Decode a wrapper submessage: field 1 with the given wire type.
/// For varint wrapper types.
fn decode_wrapper_varint(buf: &[u8]) -> Result<(u64, bool), prost::DecodeError> {
    let mut val = 0u64;
    let mut found = false;
    let mut pos = 0;
    while pos < buf.len() {
        let (fnum, wt, n) = decode_tag(&buf[pos..])?;
        pos += n;
        if fnum == 1 && wt == 0 {
            let (v, n) = decode_varint(&buf[pos..])?;
            val = v;
            found = true;
            pos += n;
        } else {
            pos += skip_field(wt, &buf[pos..])?;
        }
    }
    Ok((val, found))
}

fn decode_wrapper_fixed32(buf: &[u8]) -> Result<([u8; 4], bool), prost::DecodeError> {
    let mut val = [0u8; 4];
    let mut found = false;
    let mut pos = 0;
    while pos < buf.len() {
        let (fnum, wt, n) = decode_tag(&buf[pos..])?;
        pos += n;
        if fnum == 1 && wt == 5 {
            if buf.len() < pos + 4 {
                return Err(decode_error("unexpected EOF"));
            }
            val.copy_from_slice(&buf[pos..pos + 4]);
            found = true;
            pos += 4;
        } else {
            pos += skip_field(wt, &buf[pos..])?;
        }
    }
    Ok((val, found))
}

fn decode_wrapper_fixed64(buf: &[u8]) -> Result<([u8; 8], bool), prost::DecodeError> {
    let mut val = [0u8; 8];
    let mut found = false;
    let mut pos = 0;
    while pos < buf.len() {
        let (fnum, wt, n) = decode_tag(&buf[pos..])?;
        pos += n;
        if fnum == 1 && wt == 1 {
            if buf.len() < pos + 8 {
                return Err(decode_error("unexpected EOF"));
            }
            val.copy_from_slice(&buf[pos..pos + 8]);
            found = true;
            pos += 8;
        } else {
            pos += skip_field(wt, &buf[pos..])?;
        }
    }
    Ok((val, found))
}

fn decode_wrapper_string(buf: &[u8]) -> Result<(Vec<u8>, bool), prost::DecodeError> {
    let mut val = Vec::new();
    let mut found = false;
    let mut pos = 0;
    while pos < buf.len() {
        let (fnum, wt, n) = decode_tag(&buf[pos..])?;
        pos += n;
        if fnum == 1 && wt == 2 {
            let (data, consumed) = read_length_delimited(&buf[pos..])?;
            std::str::from_utf8(data).map_err(|_| decode_error("invalid UTF-8"))?;
            val.clear();
            val.extend_from_slice(data);
            found = true;
            pos += consumed;
        } else {
            pos += skip_field(wt, &buf[pos..])?;
        }
    }
    Ok((val, found))
}

fn decode_wrapper_bytes(buf: &[u8]) -> Result<(Vec<u8>, bool), prost::DecodeError> {
    let mut val = Vec::new();
    let mut found = false;
    let mut pos = 0;
    while pos < buf.len() {
        let (fnum, wt, n) = decode_tag(&buf[pos..])?;
        pos += n;
        if fnum == 1 && wt == 2 {
            let (data, consumed) = read_length_delimited(&buf[pos..])?;
            val.clear();
            val.extend_from_slice(data);
            found = true;
            pos += consumed;
        } else {
            pos += skip_field(wt, &buf[pos..])?;
        }
    }
    Ok((val, found))
}

// ---------------------------------------------------------------------------
// Macro for flush/finish boilerplate
// ---------------------------------------------------------------------------

macro_rules! flush_primitive {
    ($value:expr, $has_value:expr, $has_presence:expr, $builder:expr, $default:expr) => {
        if *$has_value {
            $builder.append_value(*$value);
        } else if *$has_presence {
            $builder.append_null();
        } else {
            $builder.append_value($default);
        }
        *$has_value = false;
        *$value = $default;
    };
}

fn finish_primitive<T: ArrowPrimitiveType>(builder: &mut PrimitiveBuilder<T>) -> Arc<dyn Array> {
    Arc::new(std::mem::take(builder).finish())
}

fn finish_timestamp(
    builder: &mut PrimitiveBuilder<Int64Type>,
    unit: TimeUnit,
    tz: &Option<Arc<str>>,
) -> Arc<dyn Array> {
    let values = std::mem::take(builder).finish();
    let dt = DataType::Timestamp(unit, tz.clone());
    let data = ArrayData::builder(dt)
        .len(values.len())
        .add_buffer(values.values().inner().clone())
        .null_bit_buffer(values.nulls().map(|n| n.buffer().clone()))
        .build()
        .unwrap();
    arrow::array::make_array(data)
}

fn finish_duration(builder: &mut PrimitiveBuilder<Int64Type>, unit: TimeUnit) -> Arc<dyn Array> {
    let values = std::mem::take(builder).finish();
    let dt = DataType::Duration(unit);
    let data = ArrayData::builder(dt)
        .len(values.len())
        .add_buffer(values.values().inner().clone())
        .null_bit_buffer(values.nulls().map(|n| n.buffer().clone()))
        .build()
        .unwrap();
    arrow::array::make_array(data)
}

fn finish_time_of_day(builder: &mut PrimitiveBuilder<Int64Type>, unit: TimeUnit) -> Arc<dyn Array> {
    let values = std::mem::take(builder).finish();
    let dt = match unit {
        TimeUnit::Second => DataType::Time32(TimeUnit::Second),
        TimeUnit::Millisecond => DataType::Time32(TimeUnit::Millisecond),
        TimeUnit::Microsecond => DataType::Time64(TimeUnit::Microsecond),
        TimeUnit::Nanosecond => DataType::Time64(TimeUnit::Nanosecond),
    };
    if matches!(unit, TimeUnit::Second | TimeUnit::Millisecond) {
        let i32_values: Vec<Option<i32>> = (0..values.len())
            .map(|i| {
                if values.is_null(i) {
                    None
                } else {
                    let v = values.value(i);
                    Some(i32::try_from(v).unwrap_or(if v > 0 { i32::MAX } else { i32::MIN }))
                }
            })
            .collect();
        let i32_array = arrow::array::Int32Array::from(i32_values);
        let data = ArrayData::builder(dt)
            .len(i32_array.len())
            .add_buffer(i32_array.values().inner().clone())
            .null_bit_buffer(i32_array.nulls().map(|n| n.buffer().clone()))
            .build()
            .unwrap();
        arrow::array::make_array(data)
    } else {
        let data = ArrayData::builder(dt)
            .len(values.len())
            .add_buffer(values.values().inner().clone())
            .null_bit_buffer(values.nulls().map(|n| n.buffer().clone()))
            .build()
            .unwrap();
        arrow::array::make_array(data)
    }
}

// ---------------------------------------------------------------------------
// FieldDecoder: decode + flush + finish
// ---------------------------------------------------------------------------

impl<'a> FieldDecoder<'a> {
    #[inline(always)]
    fn decode(&mut self, wire_type: u8, buf: &'a [u8]) -> Result<usize, prost::DecodeError> {
        match self {
            Self::Int32 {
                value, has_value, ..
            }
            | Self::EnumInt32 {
                value, has_value, ..
            } => {
                if wire_type != 0 {
                    return skip_field(wire_type, buf);
                }
                let (v, n) = decode_varint(buf)?;
                *value = v as i32;
                *has_value = true;
                Ok(n)
            }
            Self::EnumString {
                value, has_value, ..
            }
            | Self::EnumBinary {
                value, has_value, ..
            } => {
                if wire_type != 0 {
                    return skip_field(wire_type, buf);
                }
                let (v, n) = decode_varint(buf)?;
                *value = v as i32;
                *has_value = true;
                Ok(n)
            }
            Self::Int64 {
                value, has_value, ..
            } => {
                if wire_type != 0 {
                    return skip_field(wire_type, buf);
                }
                let (v, n) = decode_varint(buf)?;
                *value = v as i64;
                *has_value = true;
                Ok(n)
            }
            Self::UInt32 {
                value, has_value, ..
            } => {
                if wire_type != 0 {
                    return skip_field(wire_type, buf);
                }
                let (v, n) = decode_varint(buf)?;
                *value = v as u32;
                *has_value = true;
                Ok(n)
            }
            Self::UInt64 {
                value, has_value, ..
            } => {
                if wire_type != 0 {
                    return skip_field(wire_type, buf);
                }
                let (v, n) = decode_varint(buf)?;
                *value = v;
                *has_value = true;
                Ok(n)
            }
            Self::Sint32 {
                value, has_value, ..
            } => {
                if wire_type != 0 {
                    return skip_field(wire_type, buf);
                }
                let (v, n) = decode_varint(buf)?;
                *value = decode_zigzag32(v);
                *has_value = true;
                Ok(n)
            }
            Self::Sint64 {
                value, has_value, ..
            } => {
                if wire_type != 0 {
                    return skip_field(wire_type, buf);
                }
                let (v, n) = decode_varint(buf)?;
                *value = decode_zigzag64(v);
                *has_value = true;
                Ok(n)
            }
            Self::Sfixed32 {
                value, has_value, ..
            } => {
                if wire_type != 5 {
                    return skip_field(wire_type, buf);
                }
                if buf.len() < 4 {
                    return Err(decode_error("unexpected EOF"));
                }
                *value = i32::from_le_bytes(buf[..4].try_into().unwrap());
                *has_value = true;
                Ok(4)
            }
            Self::Sfixed64 {
                value, has_value, ..
            } => {
                if wire_type != 1 {
                    return skip_field(wire_type, buf);
                }
                if buf.len() < 8 {
                    return Err(decode_error("unexpected EOF"));
                }
                *value = i64::from_le_bytes(buf[..8].try_into().unwrap());
                *has_value = true;
                Ok(8)
            }
            Self::Fixed32 {
                value, has_value, ..
            } => {
                if wire_type != 5 {
                    return skip_field(wire_type, buf);
                }
                if buf.len() < 4 {
                    return Err(decode_error("unexpected EOF"));
                }
                *value = u32::from_le_bytes(buf[..4].try_into().unwrap());
                *has_value = true;
                Ok(4)
            }
            Self::Fixed64 {
                value, has_value, ..
            } => {
                if wire_type != 1 {
                    return skip_field(wire_type, buf);
                }
                if buf.len() < 8 {
                    return Err(decode_error("unexpected EOF"));
                }
                *value = u64::from_le_bytes(buf[..8].try_into().unwrap());
                *has_value = true;
                Ok(8)
            }
            Self::Float {
                value, has_value, ..
            } => {
                if wire_type != 5 {
                    return skip_field(wire_type, buf);
                }
                if buf.len() < 4 {
                    return Err(decode_error("unexpected EOF"));
                }
                *value = f32::from_le_bytes(buf[..4].try_into().unwrap());
                *has_value = true;
                Ok(4)
            }
            Self::Double {
                value, has_value, ..
            } => {
                if wire_type != 1 {
                    return skip_field(wire_type, buf);
                }
                if buf.len() < 8 {
                    return Err(decode_error("unexpected EOF"));
                }
                *value = f64::from_le_bytes(buf[..8].try_into().unwrap());
                *has_value = true;
                Ok(8)
            }
            Self::Bool {
                value, has_value, ..
            } => {
                if wire_type != 0 {
                    return skip_field(wire_type, buf);
                }
                let (v, n) = decode_varint(buf)?;
                *value = v != 0;
                *has_value = true;
                Ok(n)
            }
            Self::String {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                *value = if data.is_ascii() {
                    // ASCII is valid UTF-8, and this avoids the more general validator on the
                    // overwhelmingly common protobuf identifier/text path.
                    unsafe { std::str::from_utf8_unchecked(data) }
                } else {
                    std::str::from_utf8(data).map_err(|_| decode_error("invalid UTF-8"))?
                };
                *has_value = true;
                Ok(total)
            }
            Self::Bytes {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                *value = data;
                *has_value = true;
                Ok(total)
            }
            // Well-known types: decode submessage
            Self::Timestamp {
                seconds,
                nanos,
                has_value,
                ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let vals = decode_wkt_submessage(data, 2)?;
                *seconds = vals[0];
                *nanos = vals[1] as i32;
                *has_value = true;
                Ok(total)
            }
            Self::Duration {
                seconds,
                nanos,
                has_value,
                ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let vals = decode_wkt_submessage(data, 2)?;
                *seconds = vals[0];
                *nanos = vals[1] as i32;
                *has_value = true;
                Ok(total)
            }
            Self::Date {
                year,
                month,
                day,
                has_value,
                ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let vals = decode_wkt_submessage(data, 3)?;
                *year = vals[0] as i32;
                *month = vals[1] as i32;
                *day = vals[2] as i32;
                *has_value = true;
                Ok(total)
            }
            Self::TimeOfDay {
                hours,
                minutes,
                seconds_val,
                nanos,
                has_value,
                ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let vals = decode_wkt_submessage(data, 4)?;
                *hours = vals[0] as i32;
                *minutes = vals[1] as i32;
                *seconds_val = vals[2] as i32;
                *nanos = vals[3] as i32;
                *has_value = true;
                Ok(total)
            }
            // Wrapper types
            Self::WrapperDouble {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (bytes, found) = decode_wrapper_fixed64(data)?;
                if found {
                    *value = f64::from_le_bytes(bytes);
                }
                *has_value = true;
                Ok(total)
            }
            Self::WrapperFloat {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (bytes, found) = decode_wrapper_fixed32(data)?;
                if found {
                    *value = f32::from_le_bytes(bytes);
                }
                *has_value = true;
                Ok(total)
            }
            Self::WrapperInt64 {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, _) = decode_wrapper_varint(data)?;
                *value = v as i64;
                *has_value = true;
                Ok(total)
            }
            Self::WrapperUInt64 {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, _) = decode_wrapper_varint(data)?;
                *value = v;
                *has_value = true;
                Ok(total)
            }
            Self::WrapperInt32 {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, _) = decode_wrapper_varint(data)?;
                *value = v as i32;
                *has_value = true;
                Ok(total)
            }
            Self::WrapperUInt32 {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, _) = decode_wrapper_varint(data)?;
                *value = v as u32;
                *has_value = true;
                Ok(total)
            }
            Self::WrapperBool {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, _) = decode_wrapper_varint(data)?;
                *value = v != 0;
                *has_value = true;
                Ok(total)
            }
            Self::WrapperString {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, _) = decode_wrapper_string(data)?;
                value.clear();
                value.extend_from_slice(&v);
                *has_value = true;
                Ok(total)
            }
            Self::WrapperBytes {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, _) = decode_wrapper_bytes(data)?;
                value.clear();
                value.extend_from_slice(&v);
                *has_value = true;
                Ok(total)
            }
            // Nested message
            Self::Message {
                sub_decoder,
                has_value,
                ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                // For singular messages, if seen multiple times, the spec says merge.
                // For simplicity (and matching DynamicMessage behavior), we decode fresh each time.
                // Reset sub_decoder if already decoded this row.
                if !*has_value {
                    *has_value = true;
                }
                sub_decoder.decode_message_bytes(data)?;
                Ok(total)
            }
            // Repeated fields — delegate to inner
            Self::Repeated { inner, .. } => inner.decode(wire_type, buf),
            // Map: each occurrence is a length-delimited entry submessage
            Self::Map {
                key_decoder,
                value_decoder,
                ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                // Parse entry submessage: field 1 = key, field 2 = value
                let mut pos = 0;
                while pos < data.len() {
                    let (fnum, wt, n) = decode_tag(&data[pos..])?;
                    pos += n;
                    if fnum == 1 {
                        pos += key_decoder.decode(wt, &data[pos..])?;
                    } else if fnum == 2 {
                        pos += value_decoder.decode(wt, &data[pos..])?;
                    } else {
                        pos += skip_field(wt, &data[pos..])?;
                    }
                }
                // Flush key and value (they're buffered singular decoders)
                key_decoder.flush();
                value_decoder.flush();
                Ok(total)
            }
        }
    }

    fn clear_pending(&mut self) {
        match self {
            Self::Int32 { has_value, .. }
            | Self::Int64 { has_value, .. }
            | Self::UInt32 { has_value, .. }
            | Self::UInt64 { has_value, .. }
            | Self::Sint32 { has_value, .. }
            | Self::Sint64 { has_value, .. }
            | Self::Sfixed32 { has_value, .. }
            | Self::Sfixed64 { has_value, .. }
            | Self::Fixed32 { has_value, .. }
            | Self::Fixed64 { has_value, .. }
            | Self::Float { has_value, .. }
            | Self::Double { has_value, .. }
            | Self::Bool { has_value, .. }
            | Self::String { has_value, .. }
            | Self::Bytes { has_value, .. }
            | Self::EnumInt32 { has_value, .. }
            | Self::EnumString { has_value, .. }
            | Self::EnumBinary { has_value, .. }
            | Self::Timestamp { has_value, .. }
            | Self::Duration { has_value, .. }
            | Self::Date { has_value, .. }
            | Self::TimeOfDay { has_value, .. }
            | Self::WrapperDouble { has_value, .. }
            | Self::WrapperFloat { has_value, .. }
            | Self::WrapperInt64 { has_value, .. }
            | Self::WrapperUInt64 { has_value, .. }
            | Self::WrapperInt32 { has_value, .. }
            | Self::WrapperUInt32 { has_value, .. }
            | Self::WrapperBool { has_value, .. }
            | Self::WrapperString { has_value, .. }
            | Self::WrapperBytes { has_value, .. } => *has_value = false,
            Self::Message {
                sub_decoder,
                has_value,
                ..
            } => {
                sub_decoder.discard_current_row();
                *has_value = false;
            }
            Self::Repeated { .. } | Self::Map { .. } => {
                unreachable!("protobuf oneof fields cannot be repeated or maps")
            }
        }
    }

    #[inline(always)]
    fn flush(&mut self) {
        match self {
            Self::Int32 {
                value,
                has_value,
                has_presence,
                builder,
            }
            | Self::Sint32 {
                value,
                has_value,
                has_presence,
                builder,
            }
            | Self::Sfixed32 {
                value,
                has_value,
                has_presence,
                builder,
            }
            | Self::EnumInt32 {
                value,
                has_value,
                has_presence,
                builder,
            } => {
                flush_primitive!(value, has_value, has_presence, builder, 0i32);
            }
            Self::Int64 {
                value,
                has_value,
                has_presence,
                builder,
            }
            | Self::Sint64 {
                value,
                has_value,
                has_presence,
                builder,
            }
            | Self::Sfixed64 {
                value,
                has_value,
                has_presence,
                builder,
            } => {
                flush_primitive!(value, has_value, has_presence, builder, 0i64);
            }
            Self::UInt32 {
                value,
                has_value,
                has_presence,
                builder,
            }
            | Self::Fixed32 {
                value,
                has_value,
                has_presence,
                builder,
            } => {
                flush_primitive!(value, has_value, has_presence, builder, 0u32);
            }
            Self::UInt64 {
                value,
                has_value,
                has_presence,
                builder,
            }
            | Self::Fixed64 {
                value,
                has_value,
                has_presence,
                builder,
            } => {
                flush_primitive!(value, has_value, has_presence, builder, 0u64);
            }
            Self::Float {
                value,
                has_value,
                has_presence,
                builder,
            } => {
                flush_primitive!(value, has_value, has_presence, builder, 0.0f32);
            }
            Self::Double {
                value,
                has_value,
                has_presence,
                builder,
            } => {
                flush_primitive!(value, has_value, has_presence, builder, 0.0f64);
            }
            Self::EnumString {
                value,
                has_value,
                has_presence,
                builder,
                enum_descriptor,
            } => {
                if *has_value {
                    builder.append_value(&enum_name(enum_descriptor, *value));
                } else if *has_presence {
                    builder.append_null();
                } else {
                    builder.append_value(&enum_name(enum_descriptor, 0));
                }
                *has_value = false;
                *value = 0;
            }
            Self::EnumBinary {
                value,
                has_value,
                has_presence,
                builder,
                enum_descriptor,
            } => {
                if *has_value {
                    builder.append_value(enum_name(enum_descriptor, *value).as_bytes());
                } else if *has_presence {
                    builder.append_null();
                } else {
                    builder.append_value(enum_name(enum_descriptor, 0).as_bytes());
                }
                *has_value = false;
                *value = 0;
            }
            Self::Bool {
                value,
                has_value,
                has_presence,
                builder,
            } => {
                if *has_value {
                    builder.append_value(*value);
                } else if *has_presence {
                    builder.append_null();
                } else {
                    builder.append_value(false);
                }
                *has_value = false;
                *value = false;
            }
            Self::String {
                value,
                has_value,
                has_presence,
                builder,
            } => {
                if *has_value {
                    builder.append_value(value);
                } else if *has_presence {
                    builder.append_null();
                } else {
                    builder.append_default();
                }
                *has_value = false;
                *value = "";
            }
            Self::Bytes {
                value,
                has_value,
                has_presence,
                builder,
            } => {
                if *has_value {
                    builder.append_value(value);
                } else if *has_presence {
                    builder.append_null();
                } else {
                    builder.append_default();
                }
                *has_value = false;
                *value = &[];
            }
            // Well-known types
            Self::Timestamp {
                seconds,
                nanos,
                has_value,
                builder,
                unit,
                ..
            } => {
                if *has_value {
                    builder.append_value(convert_seconds_nanos_to_unit(
                        *seconds,
                        *nanos,
                        *unit,
                        "Timestamp",
                    ));
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *seconds = 0;
                *nanos = 0;
            }
            Self::Duration {
                seconds,
                nanos,
                has_value,
                builder,
                unit,
                ..
            } => {
                if *has_value {
                    builder.append_value(convert_seconds_nanos_to_unit(
                        *seconds, *nanos, *unit, "Duration",
                    ));
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *seconds = 0;
                *nanos = 0;
            }
            Self::Date {
                year,
                month,
                day,
                has_value,
                builder,
            } => {
                if *has_value {
                    if *year == 0 && *month == 0 && *day == 0 {
                        builder.append_value(0);
                    } else {
                        builder.append_value(
                            chrono::NaiveDate::from_ymd_opt(*year, *month as u32, *day as u32)
                                .unwrap()
                                .num_days_from_ce()
                                - CE_OFFSET,
                        );
                    }
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *year = 0;
                *month = 0;
                *day = 0;
            }
            Self::TimeOfDay {
                hours,
                minutes,
                seconds_val,
                nanos,
                has_value,
                builder,
                unit,
            } => {
                if *has_value {
                    let total_seconds = i64::from(*hours) * 3600
                        + i64::from(*minutes) * 60
                        + i64::from(*seconds_val);
                    builder.append_value(convert_seconds_nanos_to_unit(
                        total_seconds,
                        *nanos,
                        *unit,
                        "TimeOfDay",
                    ));
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *hours = 0;
                *minutes = 0;
                *seconds_val = 0;
                *nanos = 0;
            }
            // Wrapper types: present → value, absent → null
            Self::WrapperDouble {
                value,
                has_value,
                builder,
            } => {
                if *has_value {
                    builder.append_value(*value);
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *value = 0.0;
            }
            Self::WrapperFloat {
                value,
                has_value,
                builder,
            } => {
                if *has_value {
                    builder.append_value(*value);
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *value = 0.0;
            }
            Self::WrapperInt64 {
                value,
                has_value,
                builder,
            } => {
                if *has_value {
                    builder.append_value(*value);
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *value = 0;
            }
            Self::WrapperUInt64 {
                value,
                has_value,
                builder,
            } => {
                if *has_value {
                    builder.append_value(*value);
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *value = 0;
            }
            Self::WrapperInt32 {
                value,
                has_value,
                builder,
            } => {
                if *has_value {
                    builder.append_value(*value);
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *value = 0;
            }
            Self::WrapperUInt32 {
                value,
                has_value,
                builder,
            } => {
                if *has_value {
                    builder.append_value(*value);
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *value = 0;
            }
            Self::WrapperBool {
                value,
                has_value,
                builder,
            } => {
                if *has_value {
                    builder.append_value(*value);
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *value = false;
            }
            Self::WrapperString {
                value,
                has_value,
                builder,
            } => {
                if *has_value {
                    builder.append_value(unsafe { std::str::from_utf8_unchecked(value) });
                } else {
                    builder.append_null();
                }
                *has_value = false;
                value.clear();
            }
            Self::WrapperBytes {
                value,
                has_value,
                builder,
            } => {
                if *has_value {
                    builder.append_value(value.as_slice());
                } else {
                    builder.append_null();
                }
                *has_value = false;
                value.clear();
            }
            // Nested message
            Self::Message {
                sub_decoder,
                has_value,
                is_valid,
            } => {
                if *has_value {
                    is_valid.append_value(true);
                    sub_decoder.flush_row();
                } else {
                    is_valid.append_value(false);
                    sub_decoder.defer_defaults(1);
                }
                *has_value = false;
            }
            // Repeated fields: push offset
            Self::Repeated { inner, offsets, .. } => {
                offsets.push(inner.len());
            }
            // Map: push offset based on key builder length
            Self::Map {
                key_decoder,
                offsets,
                ..
            } => {
                let count = match key_decoder.as_ref() {
                    FieldDecoder::Int32 { builder, .. } => ArrayBuilder::len(builder),
                    FieldDecoder::Int64 { builder, .. } => ArrayBuilder::len(builder),
                    FieldDecoder::UInt32 { builder, .. } => ArrayBuilder::len(builder),
                    FieldDecoder::UInt64 { builder, .. } => ArrayBuilder::len(builder),
                    FieldDecoder::Sint32 { builder, .. } => ArrayBuilder::len(builder),
                    FieldDecoder::Sint64 { builder, .. } => ArrayBuilder::len(builder),
                    FieldDecoder::Bool { builder, .. } => ArrayBuilder::len(builder),
                    FieldDecoder::String { builder, .. } => builder.len(),
                    _ => *offsets.last().unwrap() as usize,
                };
                offsets.push(count as i32);
            }
        }
    }

    #[inline(always)]
    fn flush_defaults_n(&mut self, rows: usize) {
        match self {
            Self::Int32 {
                has_presence,
                builder,
                ..
            }
            | Self::Sint32 {
                has_presence,
                builder,
                ..
            }
            | Self::Sfixed32 {
                has_presence,
                builder,
                ..
            }
            | Self::EnumInt32 {
                has_presence,
                builder,
                ..
            } => {
                if *has_presence {
                    builder.append_nulls(rows);
                } else {
                    builder.append_value_n(0, rows);
                }
            }
            Self::Int64 {
                has_presence,
                builder,
                ..
            }
            | Self::Sint64 {
                has_presence,
                builder,
                ..
            }
            | Self::Sfixed64 {
                has_presence,
                builder,
                ..
            } => {
                if *has_presence {
                    builder.append_nulls(rows);
                } else {
                    builder.append_value_n(0, rows);
                }
            }
            Self::UInt32 {
                has_presence,
                builder,
                ..
            }
            | Self::Fixed32 {
                has_presence,
                builder,
                ..
            } => {
                if *has_presence {
                    builder.append_nulls(rows);
                } else {
                    builder.append_value_n(0, rows);
                }
            }
            Self::UInt64 {
                has_presence,
                builder,
                ..
            }
            | Self::Fixed64 {
                has_presence,
                builder,
                ..
            } => {
                if *has_presence {
                    builder.append_nulls(rows);
                } else {
                    builder.append_value_n(0, rows);
                }
            }
            Self::Float {
                has_presence,
                builder,
                ..
            } => {
                if *has_presence {
                    builder.append_nulls(rows);
                } else {
                    builder.append_value_n(0.0, rows);
                }
            }
            Self::Double {
                has_presence,
                builder,
                ..
            } => {
                if *has_presence {
                    builder.append_nulls(rows);
                } else {
                    builder.append_value_n(0.0, rows);
                }
            }
            Self::Bool {
                has_presence,
                builder,
                ..
            } => {
                if *has_presence {
                    builder.append_nulls(rows);
                } else {
                    builder.append_n(rows, false);
                }
            }
            Self::String {
                has_presence,
                builder,
                ..
            } => {
                if *has_presence {
                    builder.append_nulls(rows);
                } else {
                    builder.append_default_n(rows);
                }
            }
            Self::Bytes {
                has_presence,
                builder,
                ..
            } => {
                if *has_presence {
                    builder.append_nulls(rows);
                } else {
                    builder.append_default_n(rows);
                }
            }
            Self::EnumString {
                has_presence,
                builder,
                enum_descriptor,
                ..
            } => {
                if *has_presence {
                    builder.append_nulls(rows);
                } else {
                    let value = enum_name(enum_descriptor, 0);
                    builder.append_value_n(&value, rows);
                }
            }
            Self::Message {
                sub_decoder,
                is_valid,
                ..
            } => {
                is_valid.append_n(rows, false);
                sub_decoder.defer_defaults(rows);
            }
            Self::Repeated { offsets, .. } => offsets.repeat_last(rows),
            Self::Map { offsets, .. } => {
                let last = *offsets.last().expect("map offset");
                offsets.extend(std::iter::repeat_n(last, rows));
            }
            other => {
                for _ in 0..rows {
                    other.flush();
                }
            }
        }
    }

    fn finish(&mut self, nullable: bool) -> (Field, Arc<dyn Array>) {
        // This is called by MessageDecoder::finish, which provides the field name separately
        // We return a dummy field name here; the caller replaces it.
        let array: Arc<dyn Array> = match self {
            Self::Int32 { builder, .. } | Self::EnumInt32 { builder, .. } => {
                finish_primitive(builder)
            }
            Self::Int64 { builder, .. } => finish_primitive(builder),
            Self::UInt32 { builder, .. } => finish_primitive(builder),
            Self::UInt64 { builder, .. } => finish_primitive(builder),
            Self::Sint32 { builder, .. } | Self::Sfixed32 { builder, .. } => {
                finish_primitive(builder)
            }
            Self::Sint64 { builder, .. } | Self::Sfixed64 { builder, .. } => {
                finish_primitive(builder)
            }
            Self::Fixed32 { builder, .. } => finish_primitive(builder),
            Self::Fixed64 { builder, .. } => finish_primitive(builder),
            Self::Float { builder, .. } => finish_primitive(builder),
            Self::Double { builder, .. } => finish_primitive(builder),
            Self::Bool { builder, .. } => Arc::new(std::mem::take(builder).finish()),
            Self::String { builder, .. } | Self::EnumString { builder, .. } => builder.finish(),
            Self::Bytes { builder, .. } | Self::EnumBinary { builder, .. } => builder.finish(),
            Self::Timestamp {
                builder, unit, tz, ..
            } => finish_timestamp(builder, *unit, tz),
            Self::Duration { builder, unit, .. } => finish_duration(builder, *unit),
            Self::Date { builder, .. } => finish_primitive(builder),
            Self::TimeOfDay { builder, unit, .. } => finish_time_of_day(builder, *unit),
            Self::WrapperDouble { builder, .. } => finish_primitive(builder),
            Self::WrapperFloat { builder, .. } => finish_primitive(builder),
            Self::WrapperInt64 { builder, .. } => finish_primitive(builder),
            Self::WrapperUInt64 { builder, .. } => finish_primitive(builder),
            Self::WrapperInt32 { builder, .. } => finish_primitive(builder),
            Self::WrapperUInt32 { builder, .. } => finish_primitive(builder),
            Self::WrapperBool { builder, .. } => Arc::new(std::mem::take(builder).finish()),
            Self::WrapperString { builder, .. } => builder.finish(),
            Self::WrapperBytes { builder, .. } => builder.finish(),
            Self::Message {
                sub_decoder,
                is_valid,
                ..
            } => Arc::new(sub_decoder.build_struct_array(Some(std::mem::take(is_valid).finish()))),
            Self::Repeated {
                inner,
                offsets,
                list_name,
                list_nullable,
            } => {
                let vals = inner.finish();
                std::mem::replace(offsets, ListOffsets::new(false)).finish(
                    vals,
                    list_name,
                    *list_nullable,
                )
            }
            Self::Map {
                key_decoder,
                value_decoder,
                offsets,
                map_value_name,
                map_value_nullable,
            } => {
                let (_, key_array) = key_decoder.finish(false);
                let (_, value_array) = value_decoder.finish(*map_value_nullable);
                let key_field = Arc::new(Field::new("key", key_array.data_type().clone(), false));
                let value_field = Arc::new(Field::new(
                    &**map_value_name,
                    value_array.data_type().clone(),
                    *map_value_nullable,
                ));
                let entries_struct_type = DataType::Struct(
                    vec![key_field.as_ref().clone(), value_field.as_ref().clone()].into(),
                );
                let entry_struct =
                    StructArray::from(vec![(key_field, key_array), (value_field, value_array)]);
                let map_dt = DataType::Map(
                    Arc::new(Field::new("entries", entries_struct_type, false)),
                    false,
                );
                let len = offsets.len() - 1;
                let offsets_buf = Buffer::from_vec(std::mem::take(offsets));
                let map_data = ArrayData::builder(map_dt)
                    .len(len)
                    .add_buffer(offsets_buf)
                    .add_child_data(entry_struct.into_data())
                    .build()
                    .unwrap();
                Arc::new(MapArray::from(map_data))
            }
        };
        let field = Field::new("", array.data_type().clone(), nullable);
        (field, array)
    }
}

// ---------------------------------------------------------------------------
// Helpers for repeated varint/fixed decoding
// ---------------------------------------------------------------------------

fn decode_repeated_varint<T: ArrowPrimitiveType>(
    wire_type: u8,
    buf: &[u8],
    builder: &mut PrimitiveBuilder<T>,
    convert: fn(u64) -> T::Native,
) -> Result<usize, prost::DecodeError> {
    if wire_type == 2 {
        // packed
        let (data, total) = read_length_delimited(buf)?;
        let mut p = 0;
        while p < data.len() {
            let (v, n) = decode_varint(&data[p..])?;
            builder.append_value(convert(v));
            p += n;
        }
        Ok(total)
    } else if wire_type == 0 {
        let (v, n) = decode_varint(buf)?;
        builder.append_value(convert(v));
        Ok(n)
    } else {
        skip_field(wire_type, buf)
    }
}

fn decode_repeated_fixed<T: ArrowPrimitiveType, const WIDTH: usize>(
    wire_type: u8,
    expected_wt: u8,
    buf: &[u8],
    builder: &mut PrimitiveBuilder<T>,
    convert: fn([u8; WIDTH]) -> T::Native,
) -> Result<usize, prost::DecodeError> {
    if wire_type == 2 {
        let (data, total) = read_length_delimited(buf)?;
        let mut p = 0;
        while p + WIDTH <= data.len() {
            let mut bytes = [0u8; WIDTH];
            bytes.copy_from_slice(&data[p..p + WIDTH]);
            builder.append_value(convert(bytes));
            p += WIDTH;
        }
        Ok(total)
    } else if wire_type == expected_wt {
        if buf.len() < WIDTH {
            return Err(decode_error("unexpected EOF"));
        }
        let mut bytes = [0u8; WIDTH];
        bytes.copy_from_slice(&buf[..WIDTH]);
        builder.append_value(convert(bytes));
        Ok(WIDTH)
    } else {
        skip_field(wire_type, buf)
    }
}

fn decode_repeated_wrapper_varint<T: ArrowPrimitiveType>(
    wire_type: u8,
    buf: &[u8],
    builder: &mut PrimitiveBuilder<T>,
    convert: fn(u64) -> T::Native,
) -> Result<usize, prost::DecodeError> {
    if wire_type != 2 {
        return skip_field(wire_type, buf);
    }
    let (data, total) = read_length_delimited(buf)?;
    let (v, _) = decode_wrapper_varint(data)?;
    builder.append_value(convert(v));
    Ok(total)
}

fn decode_repeated_wrapper_fixed32<T: ArrowPrimitiveType>(
    wire_type: u8,
    buf: &[u8],
    builder: &mut PrimitiveBuilder<T>,
    convert: fn([u8; 4]) -> T::Native,
) -> Result<usize, prost::DecodeError> {
    if wire_type != 2 {
        return skip_field(wire_type, buf);
    }
    let (data, total) = read_length_delimited(buf)?;
    let (bytes, _) = decode_wrapper_fixed32(data)?;
    builder.append_value(convert(bytes));
    Ok(total)
}

fn decode_repeated_wrapper_fixed64<T: ArrowPrimitiveType>(
    wire_type: u8,
    buf: &[u8],
    builder: &mut PrimitiveBuilder<T>,
    convert: fn([u8; 8]) -> T::Native,
) -> Result<usize, prost::DecodeError> {
    if wire_type != 2 {
        return skip_field(wire_type, buf);
    }
    let (data, total) = read_length_delimited(buf)?;
    let (bytes, _) = decode_wrapper_fixed64(data)?;
    builder.append_value(convert(bytes));
    Ok(total)
}

// ---------------------------------------------------------------------------
// MessageDecoder
// ---------------------------------------------------------------------------

struct DecoderEntry<'a> {
    decoder: FieldDecoder<'a>,
    descriptor: FieldDescriptor,
    oneof: Option<usize>,
    last_touched: Option<usize>,
    completed_rows: usize,
}

impl<'a> DecoderEntry<'a> {
    #[inline(always)]
    fn touch(&mut self, row: usize) {
        match self.last_touched {
            Some(previous) if previous == row => return,
            Some(previous) => {
                debug_assert!(previous < row);
                self.decoder.flush();
                self.completed_rows += 1;
            }
            None => {}
        }
        let missing = row - self.completed_rows;
        if missing > 0 {
            self.decoder.flush_defaults_n(missing);
            self.completed_rows += missing;
        }
        self.last_touched = Some(row);
    }

    fn finish(&mut self, rows: usize) {
        if let Some(previous) = self.last_touched.take() {
            debug_assert_eq!(previous, self.completed_rows);
            self.decoder.flush();
            self.completed_rows += 1;
        }
        let missing = rows - self.completed_rows;
        if missing > 0 {
            self.decoder.flush_defaults_n(missing);
            self.completed_rows += missing;
        }
    }
}

pub struct MessageDecoder<'a> {
    decoders: Vec<DecoderEntry<'a>>,
    tag_map: Vec<Option<usize>>,
    active_oneofs: Vec<Option<(usize, usize)>>,
    list_nullable: bool,
    map_nullable: bool,
    num_rows: usize,
}

impl<'a> MessageDecoder<'a> {
    pub fn new(descriptor: &MessageDescriptor, config: &PtarsConfig, rows: usize) -> Self {
        let plan = PreparedMessagePlan::new(descriptor);
        Self::from_plan(&plan, config, rows)
    }

    fn from_plan(plan: &PreparedMessagePlan, config: &PtarsConfig, rows: usize) -> Self {
        let mut decoders = Vec::new();
        for field in &plan.fields {
            let decoder = build_prepared_field_decoder(field, config, rows).unwrap_or_else(|| {
                panic!(
                    "unsupported protobuf field {} in prepared plan",
                    field.descriptor.full_name()
                )
            });
            decoders.push(DecoderEntry {
                decoder,
                descriptor: field.descriptor.clone(),
                oneof: field.oneof,
                last_touched: None,
                completed_rows: 0,
            });
        }

        Self {
            decoders,
            tag_map: plan.tag_map.clone(),
            active_oneofs: vec![None; plan.oneof_count],
            list_nullable: config.list_nullable,
            map_nullable: config.map_nullable,
            num_rows: 0,
        }
    }

    #[inline(always)]
    fn activate_oneof(&mut self, field: usize) {
        let Some(group) = self.decoders[field].oneof else {
            return;
        };
        if let Some((row, previous)) = self.active_oneofs[group] {
            if row == self.num_rows && previous != field {
                self.decoders[previous].decoder.clear_pending();
            }
        }
        self.active_oneofs[group] = Some((self.num_rows, field));
    }

    #[inline]
    fn decode_row(&mut self, buf: &'a [u8]) -> Result<(), prost::DecodeError> {
        self.decode_fields(buf)?;
        self.num_rows += 1;
        Ok(())
    }

    #[inline]
    fn decode_fields(&mut self, buf: &'a [u8]) -> Result<(), prost::DecodeError> {
        if self.active_oneofs.is_empty() {
            self.decode_fields_without_oneofs(buf)
        } else {
            self.decode_fields_with_oneofs(buf)
        }
    }

    #[inline(always)]
    fn decode_fields_without_oneofs(&mut self, buf: &'a [u8]) -> Result<(), prost::DecodeError> {
        let mut pos = 0;
        while pos < buf.len() {
            let (field_num, wire_type, n) = decode_tag(&buf[pos..])?;
            pos += n;
            let idx = if (field_num as usize) < self.tag_map.len() {
                self.tag_map[field_num as usize]
            } else {
                None
            };
            if let Some(idx) = idx {
                let entry = &mut self.decoders[idx];
                entry.touch(self.num_rows);
                pos += entry.decoder.decode(wire_type, &buf[pos..])?;
            } else {
                pos += skip_field(wire_type, &buf[pos..])?;
            }
        }
        Ok(())
    }

    #[inline]
    fn decode_fields_with_oneofs(&mut self, buf: &'a [u8]) -> Result<(), prost::DecodeError> {
        let mut pos = 0;
        while pos < buf.len() {
            let (field_num, wire_type, n) = decode_tag(&buf[pos..])?;
            pos += n;
            let idx = if (field_num as usize) < self.tag_map.len() {
                self.tag_map[field_num as usize]
            } else {
                None
            };
            if let Some(idx) = idx {
                self.activate_oneof(idx);
                let entry = &mut self.decoders[idx];
                entry.touch(self.num_rows);
                pos += entry.decoder.decode(wire_type, &buf[pos..])?;
            } else {
                pos += skip_field(wire_type, &buf[pos..])?;
            }
        }
        Ok(())
    }

    /// Decode a submessage's bytes without flushing — used for singular message fields
    /// where the parent will call flush.
    #[inline]
    fn decode_message_bytes(&mut self, buf: &'a [u8]) -> Result<(), prost::DecodeError> {
        self.decode_fields(buf)
    }

    fn flush_row(&mut self) {
        self.num_rows += 1;
    }

    fn defer_defaults(&mut self, rows: usize) {
        self.finish_fields();
        for entry in &mut self.decoders {
            entry.decoder.flush_defaults_n(rows);
            entry.completed_rows += rows;
        }
        self.num_rows += rows;
    }

    fn flush_pending_defaults(&mut self) {
        self.finish_fields();
    }

    fn decode_null_row(&mut self) {
        self.num_rows += 1;
    }

    fn discard_current_row(&mut self) {
        for entry in &mut self.decoders {
            if entry.last_touched == Some(self.num_rows) {
                entry.decoder.clear_pending();
                entry.last_touched = None;
            }
        }
    }

    fn finish_fields(&mut self) {
        for entry in &mut self.decoders {
            entry.finish(self.num_rows);
        }
    }

    fn row_count(&self) -> usize {
        self.num_rows
    }

    fn build_struct_array(&mut self, validity: Option<arrow::array::BooleanArray>) -> StructArray {
        self.flush_pending_defaults();
        if self.decoders.is_empty() {
            let len = validity.as_ref().map_or(self.num_rows, |v| v.len());
            return StructArray::new_empty_fields(
                len,
                validity.map(|v| arrow::buffer::NullBuffer::new(v.values().clone())),
            );
        }

        let (fields, columns): (Vec<_>, Vec<_>) = self
            .decoders
            .iter_mut()
            .map(|entry| {
                let field_desc = &entry.descriptor;
                let nullable = if field_desc.is_list() {
                    self.list_nullable
                } else if field_desc.is_map() {
                    self.map_nullable
                } else {
                    field_desc.supports_presence()
                };
                let (_, array) = entry.decoder.finish(nullable);
                let field = Field::new(field_desc.name(), array.data_type().clone(), nullable);
                (field, array)
            })
            .unzip();

        StructArray::new(
            arrow::datatypes::Fields::from(fields),
            columns,
            validity.map(|v| arrow::buffer::NullBuffer::new(v.values().clone())),
        )
    }

    pub fn finish(mut self) -> RecordBatch {
        if self.decoders.is_empty() {
            let schema = Arc::new(arrow::datatypes::Schema::empty());
            return RecordBatch::try_new_with_options(
                schema,
                vec![],
                &arrow::array::RecordBatchOptions::new().with_row_count(Some(self.num_rows)),
            )
            .unwrap();
        }
        let struct_array = self.build_struct_array(None);
        RecordBatch::from(struct_array)
    }
}

// ---------------------------------------------------------------------------
// build_field_decoder
// ---------------------------------------------------------------------------

fn build_prepared_field_decoder<'a>(
    field: &PreparedField,
    config: &PtarsConfig,
    capacity: usize,
) -> Option<FieldDecoder<'a>> {
    match &field.children {
        PreparedChildren::None => build_field_decoder(&field.descriptor, config, capacity),
        PreparedChildren::Message(plan) => Some(FieldDecoder::Message {
            sub_decoder: MessageDecoder::from_plan(plan, config, capacity),
            has_value: false,
            is_valid: BooleanBuilder::with_capacity(capacity),
        }),
        PreparedChildren::RepeatedMessage(plan) => Some(FieldDecoder::Repeated {
            inner: RepeatedInner::Message {
                sub_decoder: MessageDecoder::from_plan(plan, config, capacity),
            },
            offsets: ListOffsets::with_capacity(config.use_large_list, capacity),
            list_name: config.list_value_name.clone(),
            list_nullable: config.list_value_nullable,
        }),
        PreparedChildren::Map { key, value } => Some(FieldDecoder::Map {
            key_decoder: Box::new(build_prepared_map_value(key, config, capacity)?),
            value_decoder: Box::new(build_prepared_map_value(value, config, capacity)?),
            offsets: {
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                offsets
            },
            map_value_name: config.map_value_name.clone(),
            map_value_nullable: config.map_value_nullable,
        }),
    }
}

fn build_prepared_map_value<'a>(
    field: &PreparedField,
    config: &PtarsConfig,
    capacity: usize,
) -> Option<FieldDecoder<'a>> {
    match &field.children {
        PreparedChildren::Message(plan) => Some(FieldDecoder::Message {
            sub_decoder: MessageDecoder::from_plan(plan, config, capacity),
            has_value: false,
            is_valid: BooleanBuilder::with_capacity(capacity),
        }),
        PreparedChildren::None => {
            build_singular_decoder_for_map(&field.descriptor, config, capacity)
        }
        PreparedChildren::RepeatedMessage(_) | PreparedChildren::Map { .. } => None,
    }
}

fn build_field_decoder<'a>(
    field: &FieldDescriptor,
    config: &PtarsConfig,
    capacity: usize,
) -> Option<FieldDecoder<'a>> {
    if field.is_map() {
        return build_map_decoder(field, config, capacity);
    }
    if field.is_list() {
        return build_repeated_decoder(field, config, capacity);
    }

    // Flink's generated converter always reads proto3 scalar getters, including optional and oneof
    // fields, so an unset scalar becomes its protobuf default. Proto2 preserves explicit presence.
    let has_presence = field.supports_presence() && field.parent_file().syntax() != Syntax::Proto3;
    match field.kind() {
        Kind::Int32 => Some(FieldDecoder::Int32 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Int64 => Some(FieldDecoder::Int64 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Uint32 => Some(FieldDecoder::UInt32 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Uint64 => Some(FieldDecoder::UInt64 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Sint32 => Some(FieldDecoder::Sint32 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Sint64 => Some(FieldDecoder::Sint64 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Sfixed32 => Some(FieldDecoder::Sfixed32 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Sfixed64 => Some(FieldDecoder::Sfixed64 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Fixed32 => Some(FieldDecoder::Fixed32 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Fixed64 => Some(FieldDecoder::Fixed64 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Float => Some(FieldDecoder::Float {
            value: 0.0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Double => Some(FieldDecoder::Double {
            value: 0.0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Bool => Some(FieldDecoder::Bool {
            value: false,
            has_value: false,
            has_presence,
            builder: BooleanBuilder::with_capacity(capacity),
        }),
        Kind::String => Some(FieldDecoder::String {
            value: "",
            has_value: false,
            has_presence,
            builder: StringBuilderInner::new(config.use_large_string, capacity),
        }),
        Kind::Bytes => Some(FieldDecoder::Bytes {
            value: &[],
            has_value: false,
            has_presence,
            builder: BinaryBuilderInner::new(config.use_large_binary, capacity),
        }),
        Kind::Enum(enum_desc) => match config.enum_repr {
            EnumRepr::Int32 => Some(FieldDecoder::EnumInt32 {
                value: 0,
                has_value: false,
                has_presence,
                builder: PrimitiveBuilder::with_capacity(capacity),
            }),
            EnumRepr::String => Some(FieldDecoder::EnumString {
                value: 0,
                has_value: false,
                has_presence,
                builder: StringBuilderInner::new(config.use_large_string, capacity),
                enum_descriptor: enum_desc,
            }),
            EnumRepr::Binary => Some(FieldDecoder::EnumBinary {
                value: 0,
                has_value: false,
                has_presence,
                builder: BinaryBuilderInner::new(config.use_large_binary, capacity),
                enum_descriptor: enum_desc,
            }),
        },
        Kind::Message(msg_desc) => build_message_field_decoder(msg_desc, config, capacity),
    }
}

fn build_message_field_decoder<'a>(
    msg_desc: MessageDescriptor,
    config: &PtarsConfig,
    capacity: usize,
) -> Option<FieldDecoder<'a>> {
    // Flink treats well-known protobuf messages exactly like any other nested ROW. It does not
    // reinterpret Timestamp, Duration, wrappers, or google.type messages as Arrow temporal types.
    let sub_decoder = MessageDecoder::new(&msg_desc, config, capacity);
    Some(FieldDecoder::Message {
        sub_decoder,
        has_value: false,
        is_valid: BooleanBuilder::with_capacity(capacity),
    })
}

fn build_repeated_decoder<'a>(
    field: &FieldDescriptor,
    config: &PtarsConfig,
    capacity: usize,
) -> Option<FieldDecoder<'a>> {
    let ln = config.list_value_name.clone();
    let lnb = config.list_value_nullable;
    let offsets = || ListOffsets::with_capacity(config.use_large_list, capacity);

    let inner = match field.kind() {
        Kind::Int32 => RepeatedInner::Int32 {
            values_builder: PrimitiveBuilder::with_capacity(capacity),
        },
        Kind::Sint32 => RepeatedInner::Sint32 {
            values_builder: PrimitiveBuilder::with_capacity(capacity),
        },
        Kind::Sfixed32 => RepeatedInner::Sfixed32 {
            values_builder: PrimitiveBuilder::with_capacity(capacity),
        },
        Kind::Int64 => RepeatedInner::Int64 {
            values_builder: PrimitiveBuilder::with_capacity(capacity),
        },
        Kind::Sint64 => RepeatedInner::Sint64 {
            values_builder: PrimitiveBuilder::with_capacity(capacity),
        },
        Kind::Sfixed64 => RepeatedInner::Sfixed64 {
            values_builder: PrimitiveBuilder::with_capacity(capacity),
        },
        Kind::Uint32 => RepeatedInner::UInt32 {
            values_builder: PrimitiveBuilder::with_capacity(capacity),
        },
        Kind::Fixed32 => RepeatedInner::Fixed32 {
            values_builder: PrimitiveBuilder::with_capacity(capacity),
        },
        Kind::Uint64 => RepeatedInner::UInt64 {
            values_builder: PrimitiveBuilder::with_capacity(capacity),
        },
        Kind::Fixed64 => RepeatedInner::Fixed64 {
            values_builder: PrimitiveBuilder::with_capacity(capacity),
        },
        Kind::Float => RepeatedInner::Float {
            values_builder: PrimitiveBuilder::with_capacity(capacity),
        },
        Kind::Double => RepeatedInner::Double {
            values_builder: PrimitiveBuilder::with_capacity(capacity),
        },
        Kind::Bool => RepeatedInner::Bool {
            values_builder: BooleanBuilder::with_capacity(capacity),
        },
        Kind::String => RepeatedInner::String {
            values_builder: StringBuilderInner::new(config.use_large_string, capacity),
        },
        Kind::Bytes => RepeatedInner::Bytes {
            values_builder: BinaryBuilderInner::new(config.use_large_binary, capacity),
        },
        Kind::Enum(enum_desc) => match config.enum_repr {
            EnumRepr::Int32 => RepeatedInner::EnumInt32 {
                values_builder: PrimitiveBuilder::with_capacity(capacity),
            },
            EnumRepr::String => RepeatedInner::EnumString {
                values_builder: StringBuilderInner::new(config.use_large_string, capacity),
                enum_descriptor: enum_desc,
            },
            EnumRepr::Binary => RepeatedInner::EnumBinary {
                values_builder: BinaryBuilderInner::new(config.use_large_binary, capacity),
                enum_descriptor: enum_desc,
            },
        },
        Kind::Message(msg_desc) => {
            return build_repeated_message_decoder(&msg_desc, config, offsets(), ln, lnb, capacity);
        }
    };

    Some(FieldDecoder::Repeated {
        inner,
        offsets: offsets(),
        list_name: ln,
        list_nullable: lnb,
    })
}

fn build_repeated_message_decoder<'a>(
    msg_desc: &MessageDescriptor,
    config: &PtarsConfig,
    offsets: ListOffsets,
    ln: Arc<str>,
    lnb: bool,
    capacity: usize,
) -> Option<FieldDecoder<'a>> {
    let inner = RepeatedInner::Message {
        sub_decoder: MessageDecoder::new(msg_desc, config, capacity),
    };

    Some(FieldDecoder::Repeated {
        inner,
        offsets,
        list_name: ln,
        list_nullable: lnb,
    })
}

fn build_map_decoder<'a>(
    field: &FieldDescriptor,
    config: &PtarsConfig,
    capacity: usize,
) -> Option<FieldDecoder<'a>> {
    let map_entry = match field.kind() {
        Kind::Message(desc) => desc,
        _ => return None,
    };
    let key_field = map_entry.get_field_by_name("key")?;
    let value_field = map_entry.get_field_by_name("value")?;

    // Build singular decoders for key and value (they buffer per-entry, not per-row)
    let key_decoder = build_singular_decoder_for_map(&key_field, config, capacity)?;
    let value_decoder = build_singular_decoder_for_map(&value_field, config, capacity)?;

    Some(FieldDecoder::Map {
        key_decoder: Box::new(key_decoder),
        value_decoder: Box::new(value_decoder),
        offsets: {
            let mut offsets = Vec::with_capacity(capacity + 1);
            offsets.push(0);
            offsets
        },
        map_value_name: config.map_value_name.clone(),
        map_value_nullable: config.map_value_nullable,
    })
}

/// Build a singular decoder for use inside a map entry (no presence tracking).
fn build_singular_decoder_for_map<'a>(
    field: &FieldDescriptor,
    config: &PtarsConfig,
    capacity: usize,
) -> Option<FieldDecoder<'a>> {
    // Map keys/values are never "optional" in protobuf sense — they use proto3 defaults
    match field.kind() {
        Kind::Int32 => Some(FieldDecoder::Int32 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Int64 => Some(FieldDecoder::Int64 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Uint32 => Some(FieldDecoder::UInt32 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Uint64 => Some(FieldDecoder::UInt64 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Sint32 => Some(FieldDecoder::Sint32 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Sint64 => Some(FieldDecoder::Sint64 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Sfixed32 => Some(FieldDecoder::Sfixed32 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Sfixed64 => Some(FieldDecoder::Sfixed64 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Fixed32 => Some(FieldDecoder::Fixed32 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Fixed64 => Some(FieldDecoder::Fixed64 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Float => Some(FieldDecoder::Float {
            value: 0.0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Double => Some(FieldDecoder::Double {
            value: 0.0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::with_capacity(capacity),
        }),
        Kind::Bool => Some(FieldDecoder::Bool {
            value: false,
            has_value: false,
            has_presence: false,
            builder: BooleanBuilder::with_capacity(capacity),
        }),
        Kind::String => Some(FieldDecoder::String {
            value: "",
            has_value: false,
            has_presence: false,
            builder: StringBuilderInner::new(config.use_large_string, capacity),
        }),
        Kind::Bytes => Some(FieldDecoder::Bytes {
            value: &[],
            has_value: false,
            has_presence: false,
            builder: BinaryBuilderInner::new(config.use_large_binary, capacity),
        }),
        Kind::Enum(enum_desc) => match config.enum_repr {
            EnumRepr::Int32 => Some(FieldDecoder::EnumInt32 {
                value: 0,
                has_value: false,
                has_presence: false,
                builder: PrimitiveBuilder::with_capacity(capacity),
            }),
            EnumRepr::String => Some(FieldDecoder::EnumString {
                value: 0,
                has_value: false,
                has_presence: false,
                builder: StringBuilderInner::new(config.use_large_string, capacity),
                enum_descriptor: enum_desc,
            }),
            EnumRepr::Binary => Some(FieldDecoder::EnumBinary {
                value: 0,
                has_value: false,
                has_presence: false,
                builder: BinaryBuilderInner::new(config.use_large_binary, capacity),
                enum_descriptor: enum_desc,
            }),
        },
        Kind::Message(msg_desc) => build_message_field_decoder(msg_desc, config, capacity),
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Decode a BinaryArray of serialized protobuf messages directly into a RecordBatch.
///
/// Parses protobuf wire format directly into Arrow builders — no intermediate
/// message objects are created.
pub fn binary_array_to_record_batch_direct(
    array: &BinaryArray,
    descriptor: &MessageDescriptor,
    config: &PtarsConfig,
) -> Result<RecordBatch, prost::DecodeError> {
    let mut decoder = MessageDecoder::new(descriptor, config, array.len());
    let policy = config.confluent_wire_policy;
    for i in 0..array.len() {
        if array.is_null(i) {
            decoder.decode_null_row();
        } else {
            let bytes = strip_confluent_prefix(array.value(i), policy)?;
            decoder.decode_row(bytes)?;
        }
    }
    Ok(decoder.finish())
}

pub(crate) fn binary_array_to_record_batch_prepared(
    array: &BinaryArray,
    plan: &PreparedMessagePlan,
    config: &PtarsConfig,
) -> Result<RecordBatch, prost::DecodeError> {
    let mut decoder = MessageDecoder::from_plan(plan, config, array.len());
    let policy = config.confluent_wire_policy;
    for i in 0..array.len() {
        if array.is_null(i) {
            decoder.decode_null_row();
        } else {
            let bytes = strip_confluent_prefix(array.value(i), policy)?;
            decoder.decode_row(bytes)?;
        }
    }
    Ok(decoder.finish())
}

/// Reconciles the decoder's protobuf-native physical types with the Arrow schema Flink derived from
/// the table. In particular, protobuf's unsigned wire kinds use Java's signed bit-pattern getters,
/// enums may be declared as either strings or integral columns, and projected fields may be reordered.
pub(crate) fn align_to_flink_schema(
    batch: &RecordBatch,
    descriptor: &MessageDescriptor,
    target_schema: &arrow::datatypes::SchemaRef,
    read_defaults: bool,
) -> Result<RecordBatch, String> {
    let mut columns = Vec::with_capacity(target_schema.fields().len());
    for field in target_schema.fields() {
        let proto = descriptor
            .get_field_by_name(field.name())
            .ok_or_else(|| format!("protobuf message has no field {}", field.name()))?;
        let source = batch
            .column_by_name(field.name())
            .ok_or_else(|| format!("decoded protobuf batch has no field {}", field.name()))?;
        columns.push(align_array(source, &proto, field, read_defaults)?);
    }
    RecordBatch::try_new(target_schema.clone(), columns).map_err(|error| error.to_string())
}

fn align_array(
    source: &Arc<dyn Array>,
    proto: &FieldDescriptor,
    target: &Arc<Field>,
    read_defaults: bool,
) -> Result<Arc<dyn Array>, String> {
    match target.data_type() {
        DataType::Struct(target_fields) => {
            let source = source
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| format!("protobuf field {} is not a struct", proto.name()))?;
            let Kind::Message(message) = proto.kind() else {
                return Err(format!("protobuf field {} is not a message", proto.name()));
            };
            let mut children = Vec::with_capacity(target_fields.len());
            for field in target_fields {
                let child_proto = message
                    .get_field_by_name(field.name())
                    .ok_or_else(|| format!("protobuf message has no field {}", field.name()))?;
                let child = source.column_by_name(field.name()).ok_or_else(|| {
                    format!("decoded protobuf struct has no field {}", field.name())
                })?;
                children.push(align_array(child, &child_proto, field, read_defaults)?);
            }
            let nulls = if read_defaults {
                None
            } else {
                source.nulls().cloned()
            };
            Ok(Arc::new(StructArray::new(
                target_fields.clone(),
                children,
                nulls,
            )))
        }
        DataType::List(target_value) => {
            let source = source
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .ok_or_else(|| format!("protobuf field {} is not a list", proto.name()))?;
            let values = align_array(source.values(), proto, target_value, read_defaults)?;
            Ok(Arc::new(arrow::array::ListArray::new(
                target_value.clone(),
                source.offsets().clone(),
                values,
                source.nulls().cloned(),
            )))
        }
        DataType::Map(target_entries, ordered) => {
            let source = source
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| format!("protobuf field {} is not a map", proto.name()))?;
            let Kind::Message(entry) = proto.kind() else {
                return Err(format!(
                    "protobuf map {} has no entry message",
                    proto.name()
                ));
            };
            let target_fields = match target_entries.data_type() {
                DataType::Struct(fields) => fields,
                other => return Err(format!("Arrow map entries are {other:?}, not struct")),
            };
            let source_entries = source.entries();
            let mut children = Vec::with_capacity(target_fields.len());
            for field in target_fields {
                let child_proto = entry
                    .get_field_by_name(field.name())
                    .ok_or_else(|| format!("protobuf map entry has no {}", field.name()))?;
                let child = source_entries
                    .column_by_name(field.name())
                    .ok_or_else(|| format!("decoded map entry has no {}", field.name()))?;
                children.push(align_array(child, &child_proto, field, read_defaults)?);
            }
            let entries = StructArray::new(target_fields.clone(), children, None);
            Ok(Arc::new(MapArray::new(
                target_entries.clone(),
                source.offsets().clone(),
                entries,
                source.nulls().cloned(),
                *ordered,
            )))
        }
        DataType::Utf8 if matches!(proto.kind(), Kind::Enum(_)) => {
            let source = source
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| format!("protobuf enum {} is not int32", proto.name()))?;
            let Kind::Enum(descriptor) = proto.kind() else {
                unreachable!()
            };
            let mut builder = StringBuilder::with_capacity(source.len(), source.len() * 8);
            for value in source.iter() {
                match value {
                    None => builder.append_null(),
                    Some(number) => builder.append_value(
                        descriptor
                            .get_value(number)
                            .map(|value| value.name().to_string())
                            .unwrap_or_else(|| "UNRECOGNIZED".to_string()),
                    ),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        target_type
            if source.data_type() == target_type && !matches!(proto.kind(), Kind::Enum(_)) =>
        {
            Ok(source.clone())
        }
        DataType::Int32 if matches!(source.data_type(), DataType::UInt32) => {
            reinterpret(source, DataType::Int32)
        }
        DataType::Int64 if matches!(source.data_type(), DataType::UInt64) => {
            reinterpret(source, DataType::Int64)
        }
        target_type
            if matches!(proto.kind(), Kind::Enum(_))
                && matches!(
                    target_type,
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                ) =>
        {
            let Kind::Enum(descriptor) = proto.kind() else {
                unreachable!()
            };
            let values = source
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| format!("protobuf enum {} is not int32", proto.name()))?;
            if let Some(number) = values
                .iter()
                .flatten()
                .find(|number| descriptor.get_value(*number).is_none())
            {
                return Err(format!(
                    "protobuf enum {} has unrecognized number {number}",
                    proto.name()
                ));
            }
            arrow::compute::cast(source, target_type).map_err(|error| error.to_string())
        }
        target_type => arrow::compute::cast(source, target_type).map_err(|error| error.to_string()),
    }
}

fn reinterpret(source: &Arc<dyn Array>, target: DataType) -> Result<Arc<dyn Array>, String> {
    source
        .to_data()
        .into_builder()
        .data_type(target)
        .build()
        .map(arrow::array::make_array)
        .map_err(|error| error.to_string())
}

/// Convert DynamicMessage instances to a RecordBatch using the default configuration.
///
/// Each message is serialized to protobuf wire format, then decoded directly
/// into Arrow arrays.
pub fn messages_to_record_batch(
    messages: &[prost_reflect::DynamicMessage],
    message_descriptor: &MessageDescriptor,
) -> RecordBatch {
    messages_to_record_batch_with_config(messages, message_descriptor, &PtarsConfig::default())
}

/// Convert DynamicMessage instances to a RecordBatch using the specified configuration.
///
/// Each message is serialized to protobuf wire format, then decoded directly
/// into Arrow arrays.
pub fn messages_to_record_batch_with_config(
    messages: &[prost_reflect::DynamicMessage],
    message_descriptor: &MessageDescriptor,
    config: &PtarsConfig,
) -> RecordBatch {
    use arrow::array::builder::BinaryBuilder;
    use prost::Message;

    let mut bin_builder = BinaryBuilder::new();
    for msg in messages {
        bin_builder.append_value(msg.encode_to_vec());
    }
    let binary_array = bin_builder.finish();
    binary_array_to_record_batch_direct(&binary_array, message_descriptor, config)
        .expect("failed to decode messages")
}

/// Decode a BinaryArray into a vector of DynamicMessage.
///
/// Each element in the binary array is expected to be a serialized protobuf message.
/// Null values in the array result in default (empty) messages.
pub fn binary_array_to_messages(
    array: &BinaryArray,
    message_descriptor: &MessageDescriptor,
) -> Result<Vec<prost_reflect::DynamicMessage>, prost::DecodeError> {
    let mut messages = Vec::with_capacity(array.len());
    for i in 0..array.len() {
        let message = if array.is_null(i) {
            prost_reflect::DynamicMessage::new(message_descriptor.clone())
        } else {
            prost_reflect::DynamicMessage::decode(message_descriptor.clone(), array.value(i))?
        };
        messages.push(message);
    }
    Ok(messages)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_confluent_prefix_raw() {
        let buf = b"\x00\x00\x00\x00\x01\x08\x96\x01";
        let result = strip_confluent_prefix(buf, ConfluentWirePolicy::Raw).unwrap();
        assert_eq!(result, buf);
    }

    #[test]
    fn test_strip_confluent_prefix_standard() {
        // magic byte + 4-byte schema ID + payload
        let buf = b"\x00\x00\x00\x00\x01\x08\x96\x01";
        let result = strip_confluent_prefix(buf, ConfluentWirePolicy::Standard).unwrap();
        assert_eq!(result, b"\x08\x96\x01");
    }

    #[test]
    fn test_strip_confluent_prefix_standard_too_short() {
        let buf = b"\x00\x01\x02";
        let result = strip_confluent_prefix(buf, ConfluentWirePolicy::Standard);
        assert!(result.is_err());
    }

    #[test]
    fn test_strip_confluent_prefix_protobuf_zero_indexes() {
        // magic byte + 4-byte schema ID + varint 0 (count=0) + payload
        let buf = b"\x00\x00\x00\x00\x01\x00\x08\x96\x01";
        let result = strip_confluent_prefix(buf, ConfluentWirePolicy::Protobuf).unwrap();
        assert_eq!(result, b"\x08\x96\x01");
    }

    #[test]
    fn test_strip_confluent_prefix_protobuf_one_index() {
        // magic byte + 4-byte schema ID + varint 1 (count=1) + varint 0 (index) + payload
        let buf = b"\x00\x00\x00\x00\x01\x01\x00\x08\x96\x01";
        let result = strip_confluent_prefix(buf, ConfluentWirePolicy::Protobuf).unwrap();
        assert_eq!(result, b"\x08\x96\x01");
    }

    #[test]
    fn test_strip_confluent_prefix_protobuf_two_indexes() {
        // magic byte + 4-byte schema ID + varint 2 (count) + varint 4 + varint 2 + payload
        let buf = b"\x00\x00\x00\x00\x01\x02\x04\x02\x08\x96\x01";
        let result = strip_confluent_prefix(buf, ConfluentWirePolicy::Protobuf).unwrap();
        assert_eq!(result, b"\x08\x96\x01");
    }

    #[test]
    fn test_strip_confluent_prefix_protobuf_too_short() {
        let buf = b"\x00\x01";
        let result = strip_confluent_prefix(buf, ConfluentWirePolicy::Protobuf);
        assert!(result.is_err());
    }
}
