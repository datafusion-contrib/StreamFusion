//! Arrow → protobuf encoder for the Kafka sink's `protobuf` format, replicating Flink's
//! `PbCodegenRowSerializer`/`PbCodegenUtils` semantics over the field shapes the planner's
//! descriptor gate admits (proto3, no explicit-presence scalars, signed ints / float / double /
//! bool / string leaves, nested messages, repeated fields, maps).
//!
//! Contract: batch columns map to proto fields **by name** (order-independent), exactly the
//! decoder's schema derivation in reverse; a Struct column's children map to the nested message's
//! fields by name, recursively. Every column must name a field of the message — the sink wiring
//! owns projection (including stripping any changelog column), so an unmatched column is a bug and
//! panics. Fields no column names stay unset. The batch is assumed insert-only.
//!
//! Null semantics, mirroring Flink's serializer exactly:
//! - A null column (at any row-nesting level) leaves the proto field unset — Flink guards every
//!   field with `if(!rowData.isNullAt(i))` (PbCodegenRowSerializer#codegen), so a null ARRAY/MAP/
//!   ROW column is never touched, and a null field inside a non-null nested row leaves the nested
//!   builder's field unset.
//! - Nulls INSIDE containers (protobuf forbids them) become type defaults — 0 / 0.0 / false, the
//!   `protobuf.write-null-string-literal` value for strings, the default (empty) instance for
//!   messages — for array elements and for both map keys and map values
//!   (PbCodegenUtils#pbDefaultValueCode + #convertFlinkArrayElementToPbWithDefaultValueCode,
//!   reached from both PbCodegenArraySerializer and PbCodegenMapSerializer).
//!
//! Correctness-first: each row builds a `prost-reflect` `DynamicMessage` (which owns the
//! sint/sfixed wire encodings and proto3 default-skipping from the descriptor). Encoding the wire
//! format directly from the Arrow columns, without the per-row message, is a later optimization.

use crate::*;

use arrow::array::Float64Array;
use prost_reflect::{
    DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MapKey, MessageDescriptor, Value,
};

/// One bare serialized protobuf message per row, all in a single encode buffer (the JSON sink's
/// `EncodedLines` shape): producing and JNI materialization read the per-row slices in place.
/// Rows stay 1:1 with the input batch — a row with every field unset is a zero-length slice,
/// the same empty `byte[]` Flink's serializer produces for it.
pub(crate) struct EncodedMessages {
    bytes: Vec<u8>,
    rows: Vec<std::ops::Range<usize>>,
}

#[allow(dead_code)] // len/is_empty/message read by tests; the dispatch consumes into_parts
impl EncodedMessages {
    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub(crate) fn message(&self, index: usize) -> &[u8] {
        &self.bytes[self.rows[index].clone()]
    }

    /// The single buffer and per-row ranges, in the sink's `EncodedLines` shape.
    pub(crate) fn into_parts(self) -> (Vec<u8>, Vec<std::ops::Range<usize>>) {
        (self.bytes, self.rows)
    }
}

/// The sink seam's option lines for a protobuf format instance: the base64-encoded
/// `FileDescriptorSet` the JVM serialized off the generated message class, the fully-qualified
/// message name, and Flink's `protobuf.write-null-string-literal` (default empty).
pub(crate) struct ProtobufEncodeOptions {
    descriptor: Vec<u8>,
    message: String,
    null_literal: String,
}

impl ProtobufEncodeOptions {
    pub(crate) fn parse(encoded: &str) -> Result<ProtobufEncodeOptions, String> {
        use base64::Engine as _;
        let mut descriptor = None;
        let mut message = None;
        let mut null_literal = String::new();
        for line in encoded.lines().filter(|line| !line.is_empty()) {
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| format!("protobuf encode option is not key=value: {line}"))?;
            match key {
                "descriptor" => {
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(value)
                        .map_err(|error| format!("invalid protobuf descriptor payload: {error}"))?;
                    descriptor = Some(bytes);
                }
                "message" => message = Some(value.to_string()),
                "null-literal" => null_literal = value.to_string(),
                other => return Err(format!("unknown protobuf encode option {other}")),
            }
        }
        Ok(ProtobufEncodeOptions {
            descriptor: descriptor.ok_or("protobuf encode options carry no descriptor")?,
            message: message.ok_or("protobuf encode options carry no message name")?,
            null_literal,
        })
    }

    pub(crate) fn encoder(&self) -> ProtobufEncoder {
        ProtobufEncoder::new(&self.descriptor, &self.message, &self.null_literal)
    }
}

pub(crate) struct ProtobufEncoder {
    message: MessageDescriptor,
    null_string_literal: String,
}

#[allow(dead_code)] // consumed by the sink format dispatch when the wiring pass lands
impl ProtobufEncoder {
    /// `descriptor_set` is an encoded protobuf `FileDescriptorSet` (the message's file + its
    /// transitive dependencies); `message_name` is the fully-qualified message type each row
    /// serializes as; `null_string_literal` is Flink's `protobuf.write-null-string-literal`
    /// option (default ""), substituted for null strings inside containers.
    pub(crate) fn new(
        descriptor_set: &[u8],
        message_name: &str,
        null_string_literal: &str,
    ) -> ProtobufEncoder {
        let pool = DescriptorPool::decode(descriptor_set)
            .expect("failed to decode protobuf FileDescriptorSet");
        let message = pool
            .get_message_by_name(message_name)
            .unwrap_or_else(|| panic!("protobuf message {message_name} not found in descriptor"));
        ProtobufEncoder { message, null_string_literal: null_string_literal.to_string() }
    }

    pub(crate) fn encode(&self, batch: &RecordBatch) -> EncodedMessages {
        let fields = batch.schema_ref().fields().clone();
        let mut bytes = Vec::new();
        let mut rows = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let start = bytes.len();
            self.encode_message(&self.message, &fields, batch.columns(), row, &mut bytes);
            rows.push(start..bytes.len());
        }
        EncodedMessages { bytes, rows }
    }

    /// Writes one message's fields in ascending field-number order — protobuf-java's serialization
    /// order, which the byte-parity referee pins. The wire is written by hand rather than through
    /// `DynamicMessage` for one load-bearing reason: protobuf-java serializes a map entry's key
    /// and value fields UNCONDITIONALLY (a `k → 0` entry carries an explicit `value: 0`), while
    /// prost omits default-valued entry fields — both are legal proto3, but only one matches
    /// Flink's bytes. Singular proto3 implicit-presence scalars skip their defaults exactly like
    /// protobuf-java's generated `writeTo` (floats bitwise, so -0.0 is written).
    fn encode_message(
        &self,
        descriptor: &MessageDescriptor,
        fields: &Fields,
        columns: &[ArrayRef],
        row: usize,
        out: &mut Vec<u8>,
    ) {
        for field in fields.iter() {
            if descriptor.get_field_by_name(field.name()).is_none() {
                panic!(
                    "column {} names no field of protobuf message {}",
                    field.name(),
                    descriptor.full_name()
                );
            }
        }
        let mut ordered: Vec<FieldDescriptor> = descriptor.fields().collect();
        ordered.sort_by_key(FieldDescriptor::number);
        for proto_field in ordered {
            let Some(column) = fields
                .iter()
                .position(|field| field.name() == proto_field.name())
                .map(|index| &columns[index])
            else {
                continue; // fields no column names stay unset
            };
            if column.is_null(row) {
                continue; // null column = unset field, Flink's isNullAt guard
            }
            if proto_field.is_map() {
                self.encode_map_field(&proto_field, column, row, out);
            } else if proto_field.is_list() {
                self.encode_repeated_field(&proto_field, column, row, out);
            } else {
                self.encode_singular(&proto_field.kind(), proto_field.number(), column, row, true, out);
            }
        }
    }

    /// One scalar or message field from `array[index]`; `skip_default` is proto3
    /// implicit-presence semantics (top-level and nested-row scalars), while map entry fields
    /// pass false to match protobuf-java's unconditional entry serialization.
    fn encode_singular(
        &self,
        kind: &Kind,
        number: u32,
        array: &ArrayRef,
        index: usize,
        skip_default: bool,
        out: &mut Vec<u8>,
    ) {
        match kind {
            Kind::Int32 => {
                let value = typed::<Int32Array>(array, kind).value(index);
                if !(skip_default && value == 0) {
                    tag(number, 0, out);
                    varint(value as i64 as u64, out); // negative int32 sign-extends to ten bytes
                }
            }
            Kind::Sint32 => {
                let value = typed::<Int32Array>(array, kind).value(index);
                if !(skip_default && value == 0) {
                    tag(number, 0, out);
                    varint(zigzag32(value), out);
                }
            }
            Kind::Sfixed32 => {
                let value = typed::<Int32Array>(array, kind).value(index);
                if !(skip_default && value == 0) {
                    tag(number, 5, out);
                    out.extend_from_slice(&value.to_le_bytes());
                }
            }
            Kind::Int64 => {
                let value = typed::<Int64Array>(array, kind).value(index);
                if !(skip_default && value == 0) {
                    tag(number, 0, out);
                    varint(value as u64, out);
                }
            }
            Kind::Sint64 => {
                let value = typed::<Int64Array>(array, kind).value(index);
                if !(skip_default && value == 0) {
                    tag(number, 0, out);
                    varint(zigzag64(value), out);
                }
            }
            Kind::Sfixed64 => {
                let value = typed::<Int64Array>(array, kind).value(index);
                if !(skip_default && value == 0) {
                    tag(number, 1, out);
                    out.extend_from_slice(&value.to_le_bytes());
                }
            }
            Kind::Float => {
                let value = typed::<Float32Array>(array, kind).value(index);
                if !(skip_default && value.to_bits() == 0) {
                    tag(number, 5, out);
                    out.extend_from_slice(&value.to_le_bytes());
                }
            }
            Kind::Double => {
                let value = typed::<Float64Array>(array, kind).value(index);
                if !(skip_default && value.to_bits() == 0) {
                    tag(number, 1, out);
                    out.extend_from_slice(&value.to_le_bytes());
                }
            }
            Kind::Bool => {
                let value = typed::<BooleanArray>(array, kind).value(index);
                if !(skip_default && !value) {
                    tag(number, 0, out);
                    out.push(u8::from(value));
                }
            }
            Kind::String => {
                let value = typed::<StringArray>(array, kind).value(index);
                if !(skip_default && value.is_empty()) {
                    tag(number, 2, out);
                    varint(value.len() as u64, out);
                    out.extend_from_slice(value.as_bytes());
                }
            }
            Kind::Message(descriptor) => {
                // Message fields have presence: a non-null column writes the field even when the
                // nested content is empty (tag + zero length), exactly like a set Java builder.
                let strukt = typed::<StructArray>(array, kind);
                let mut nested = Vec::new();
                self.encode_message(descriptor, strukt.fields(), strukt.columns(), index, &mut nested);
                tag(number, 2, out);
                varint(nested.len() as u64, out);
                out.extend_from_slice(&nested);
            }
            unsupported => panic!("{}", outside_gate(unsupported)),
        }
    }

    fn encode_repeated_field(
        &self,
        field: &FieldDescriptor,
        column: &ArrayRef,
        row: usize,
        out: &mut Vec<u8>,
    ) {
        let kind = field.kind();
        let list = typed::<ListArray>(column, &kind);
        let elements = list.value(row);
        if elements.is_empty() {
            return; // an empty repeated field is absent from the wire on both engines
        }
        match &kind {
            // Numeric/bool elements pack (proto3 default on both engines); a null element writes
            // the type default into the packed run, Flink's container-null substitution.
            Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 | Kind::Int64 | Kind::Sint64
            | Kind::Sfixed64 | Kind::Float | Kind::Double | Kind::Bool => {
                let mut packed = Vec::new();
                for index in 0..elements.len() {
                    self.packed_element(&kind, &elements, index, &mut packed);
                }
                tag(field.number(), 2, out);
                varint(packed.len() as u64, out);
                out.extend_from_slice(&packed);
            }
            Kind::String => {
                for index in 0..elements.len() {
                    let value = if elements.is_null(index) {
                        self.null_string_literal.as_str()
                    } else {
                        typed::<StringArray>(&elements, &kind).value(index)
                    };
                    tag(field.number(), 2, out);
                    varint(value.len() as u64, out);
                    out.extend_from_slice(value.as_bytes());
                }
            }
            Kind::Message(descriptor) => {
                let strukt = typed::<StructArray>(&elements, &kind);
                for index in 0..elements.len() {
                    // A null element substitutes the default (empty) instance.
                    let mut nested = Vec::new();
                    if !elements.is_null(index) {
                        self.encode_message(
                            descriptor,
                            strukt.fields(),
                            strukt.columns(),
                            index,
                            &mut nested,
                        );
                    }
                    tag(field.number(), 2, out);
                    varint(nested.len() as u64, out);
                    out.extend_from_slice(&nested);
                }
            }
            unsupported => panic!("{}", outside_gate(unsupported)),
        }
    }

    /// One packed element's payload (no tag); a null element is the type default.
    fn packed_element(&self, kind: &Kind, array: &ArrayRef, index: usize, out: &mut Vec<u8>) {
        let null = array.is_null(index);
        match kind {
            Kind::Int32 => {
                let value = if null { 0 } else { typed::<Int32Array>(array, kind).value(index) };
                varint(value as i64 as u64, out);
            }
            Kind::Sint32 => {
                let value = if null { 0 } else { typed::<Int32Array>(array, kind).value(index) };
                varint(zigzag32(value), out);
            }
            Kind::Sfixed32 => {
                let value = if null { 0 } else { typed::<Int32Array>(array, kind).value(index) };
                out.extend_from_slice(&value.to_le_bytes());
            }
            Kind::Int64 => {
                let value = if null { 0 } else { typed::<Int64Array>(array, kind).value(index) };
                varint(value as u64, out);
            }
            Kind::Sint64 => {
                let value = if null { 0 } else { typed::<Int64Array>(array, kind).value(index) };
                varint(zigzag64(value), out);
            }
            Kind::Sfixed64 => {
                let value = if null { 0 } else { typed::<Int64Array>(array, kind).value(index) };
                out.extend_from_slice(&value.to_le_bytes());
            }
            Kind::Float => {
                let value = if null { 0.0 } else { typed::<Float32Array>(array, kind).value(index) };
                out.extend_from_slice(&value.to_le_bytes());
            }
            Kind::Double => {
                let value = if null { 0.0 } else { typed::<Float64Array>(array, kind).value(index) };
                out.extend_from_slice(&value.to_le_bytes());
            }
            Kind::Bool => {
                let value = !null && typed::<BooleanArray>(array, kind).value(index);
                out.push(u8::from(value));
            }
            unsupported => panic!("{}", outside_gate(unsupported)),
        }
    }

    fn encode_map_field(
        &self,
        field: &FieldDescriptor,
        column: &ArrayRef,
        row: usize,
        out: &mut Vec<u8>,
    ) {
        let Kind::Message(entry) = field.kind() else {
            panic!("protobuf map field {} has a non-message entry kind", field.name())
        };
        let key_field = entry.map_entry_key_field();
        let value_field = entry.map_entry_value_field();
        let map = typed::<MapArray>(column, &field.kind());
        let entries = map.value(row);
        let (keys, values) = (entries.column(0), entries.column(1));
        for index in 0..entries.len() {
            // Key and value are written UNCONDITIONALLY — protobuf-java's MapEntry serializes
            // both fields even at their defaults, unlike ordinary proto3 scalars. Null keys and
            // values substitute Flink's container defaults first.
            let mut body = Vec::new();
            self.encode_entry_field(&key_field, keys, index, &mut body);
            self.encode_entry_field(&value_field, values, index, &mut body);
            tag(field.number(), 2, out);
            varint(body.len() as u64, out);
            out.extend_from_slice(&body);
        }
    }

    /// One map-entry field (key or value), written even at its default; a null slot substitutes
    /// the type default (strings the write-null-string-literal, messages the empty instance).
    fn encode_entry_field(
        &self,
        field: &FieldDescriptor,
        array: &ArrayRef,
        index: usize,
        out: &mut Vec<u8>,
    ) {
        let kind = field.kind();
        if array.is_null(index) {
            match &kind {
                Kind::Int32 | Kind::Sint32 | Kind::Int64 | Kind::Sint64 => {
                    tag(field.number(), 0, out);
                    varint(0, out);
                }
                Kind::Sfixed32 => {
                    tag(field.number(), 5, out);
                    out.extend_from_slice(&0i32.to_le_bytes());
                }
                Kind::Sfixed64 => {
                    tag(field.number(), 1, out);
                    out.extend_from_slice(&0i64.to_le_bytes());
                }
                Kind::Float => {
                    tag(field.number(), 5, out);
                    out.extend_from_slice(&0f32.to_le_bytes());
                }
                Kind::Double => {
                    tag(field.number(), 1, out);
                    out.extend_from_slice(&0f64.to_le_bytes());
                }
                Kind::Bool => {
                    tag(field.number(), 0, out);
                    out.push(0);
                }
                Kind::String => {
                    tag(field.number(), 2, out);
                    varint(self.null_string_literal.len() as u64, out);
                    out.extend_from_slice(self.null_string_literal.as_bytes());
                }
                Kind::Message(_) => {
                    tag(field.number(), 2, out);
                    varint(0, out);
                }
                unsupported => panic!("{}", outside_gate(unsupported)),
            }
            return;
        }
        self.encode_singular(&kind, field.number(), array, index, false, out);
    }
}

fn tag(number: u32, wire_type: u32, out: &mut Vec<u8>) {
    varint(u64::from((number << 3) | wire_type), out);
}

fn varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn zigzag32(value: i32) -> u64 {
    u64::from(((value << 1) ^ (value >> 31)) as u32)
}

fn zigzag64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn typed<'a, T: Array + 'static>(array: &'a ArrayRef, kind: &Kind) -> &'a T {
    array.as_any().downcast_ref::<T>().unwrap_or_else(|| {
        panic!(
            "protobuf {kind:?} field cannot encode from an Arrow {:?} column",
            array.data_type()
        )
    })
}

fn outside_gate(kind: &Kind) -> String {
    format!(
        "protobuf field kind {kind:?} is outside the native encode gate \
         (proto3 signed ints, float, double, bool, string, nested messages)"
    )
}
