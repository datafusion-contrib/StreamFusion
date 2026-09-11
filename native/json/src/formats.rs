use crate::*;
#[cfg(test)]
pub(crate) fn new_decoder(
    format: i32,
    schema: SchemaRef,
    writer: &str,
    reader: &str,
    schema_id: i32,
    skip_errors: bool,
    options: &str,
) -> MessageDecoder {
    create_message_decoder(
        build_decoder,
        format,
        schema,
        writer,
        reader,
        schema_id,
        skip_errors,
        options,
    )
}

fn build_decoder(
    format: i32,
    output_schema: SchemaRef,
    _avro_schema: &str,
    _reader_avro_schema: &str,
    _schema_id: i32,
    skip_errors: bool,
    options: &FormatOptions,
) -> MessageDecoder {
    let decoder: Box<dyn Decoder> = match format {
        FORMAT_JSON => {
            return MessageDecoder {
                decoder: Box::new(JsonDecoder::new(
                    output_schema,
                    crate::json::JsonEnv {
                        mode: options.timestamp_mode,
                        lenient: skip_errors,
                        tree_duplicates: false,
                    },
                )),
                skip_errors: false,
            }
        }
        FORMAT_DEBEZIUM_JSON..=FORMAT_CANAL_JSON => Box::new(CdcJsonDecoder::new(
            output_schema,
            CdcDialect::for_format(format),
            crate::json::JsonEnv {
                mode: options.timestamp_mode,
                lenient: false,
                tree_duplicates: true,
            },
            skip_errors,
        )),
        _ => panic!("unsupported JSON format {format}"),
    };
    MessageDecoder {
        decoder,
        skip_errors,
    }
}

impl Decoder for JsonDecoder {
    fn decode(&self, body: &RecordBatch) -> RecordBatch {
        self.decode(body)
    }
    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
impl Decoder for CdcJsonDecoder {
    fn decode(&self, body: &RecordBatch) -> RecordBatch {
        self.decode(body)
    }
    fn output_schema(&self) -> SchemaRef {
        self.output.clone()
    }
}
/// Decodes a scalar CDC changelog JSON format (Debezium/OGG/Maxwell) straight to a columnar changelog
/// batch: the physical columns plus a trailing `$row_kind$` byte, with one input message fanning out to
/// 0–2 output rows (an update becomes UPDATE_BEFORE + UPDATE_AFTER; a tombstone/empty message, zero).
/// An unknown op or a null pre-image on an update/delete *fails* (Flink's default throw), never a silent
/// drop — matching Flink's default mode; with `ignore-parse-errors` the wrapping [`MessageDecoder`]
/// isolates each message and turns those failures into per-message skips, matching Flink's skip mode.
/// This mirrors Flink's `*JsonDeserializationSchema` — decode the envelope to a row, then emit the
/// physical row(s) by op with a `RowKind` — but vectorized: every body's envelope is decoded in one
/// `arrow-json` pass, then each physical column is gathered with a single `interleave` choosing the
/// right pre/post-image struct child per output row. RisingWave's row-at-a-time `DebeziumChangeEvent`
/// (`access_field(before/after)` + an `Ops` array) is the reference; this is its batch form, where
/// `$row_kind$` is our columnar `RowKind` (divergences/13). It feeds the existing native changelog
/// operators, so a CDC → GROUP BY/join/Top-N pipeline materializes zero rows end to end.
pub(crate) struct CdcJsonDecoder {
    /// Decodes the envelope: the pre/post images as nested structs of the physical columns (made
    /// nullable, since the absent side / unchanged fields are null), plus the op field as Utf8.
    /// Envelope fields not in this schema (`source`, `ts_ms`, `database`, …) are ignored.
    envelope: JsonDecoder,
    /// Output schema: the physical columns (nullable) + trailing `$row_kind$` Int8.
    output: SchemaRef,
    /// Number of physical columns (envelope/output arity excludes op and `$row_kind$`).
    arity: usize,
    /// Physical column names, for the `old`-key presence scan of the `DataOld` dialects.
    field_names: Vec<String>,
    dialect: CdcDialect,
    /// Flink's `ignore-parse-errors`, handled here (not by a generic wrapper) because the CDC skip
    /// has three granularities at once: a structurally bad message drops whole, a bad value inside
    /// an image nulls just that field (the inner JSON schema gets the flag), and a failure while
    /// fanning a message out KEEPS the rows emitted before it — Flink's deserializers accumulate
    /// into a list and collect whatever it holds after the catch.
    skip_errors: bool,
}

impl CdcJsonDecoder {
    fn new(
        physical: SchemaRef,
        dialect: CdcDialect,
        env: crate::json::JsonEnv,
        skip_errors: bool,
    ) -> CdcJsonDecoder {
        let spec = dialect.spec();
        // The images are null on the absent side / for unchanged fields, so the nested physical fields
        // must be nullable regardless of the table's declared nullability.
        let nullable: Fields = physical
            .fields()
            .iter()
            .map(|f| Arc::new(f.as_ref().clone().with_nullable(true)))
            .collect();
        let image = DataType::Struct(nullable.clone());
        // Canal wraps each image in a JSON array of rows.
        let image = if spec.arrays {
            DataType::List(Arc::new(Field::new("item", image, true)))
        } else {
            image
        };
        // Column 0 = pre-image, 1 = post-image, 2 = op (arrow-json matches the JSON keys by name).
        let envelope = Arc::new(Schema::new(vec![
            Field::new(spec.before_field, image.clone(), true),
            Field::new(spec.after_field, image, true),
            Field::new(spec.op_field, DataType::Utf8, true),
        ]));
        let mut output_fields: Vec<FieldRef> = nullable.iter().cloned().collect();
        output_fields.push(Arc::new(Field::new(ROW_KIND_COLUMN, DataType::Int8, false)));
        let output = Arc::new(Schema::new(output_fields));
        if spec.shape == CdcShape::DataOld {
            assert!(
                nullable.len() <= 128,
                "the old-key presence bitmask carries up to 128 columns"
            );
        }
        // A CDC envelope never fans a top-level array out the way the plain `json` format does:
        // Maxwell/Canal hand the root to the tree converter (any array is corrupt), while
        // Debezium/OGG decode through Flink's deprecated one-row entry, which unwraps an array
        // holding exactly one envelope — see `ArrayRootPolicy`.
        let array_roots = match dialect {
            CdcDialect::Debezium | CdcDialect::Ogg => ArrayRootPolicy::UnwrapSingle,
            CdcDialect::Maxwell | CdcDialect::Canal => ArrayRootPolicy::Corrupt,
        };
        CdcJsonDecoder {
            envelope: JsonDecoder::single_object(
                envelope,
                crate::json::JsonEnv {
                    mode: env.mode,
                    lenient: skip_errors,
                    tree_duplicates: env.tree_duplicates,
                },
                array_roots,
            ),
            output,
            arity: nullable.len(),
            field_names: physical.fields().iter().map(|f| f.name().clone()).collect(),
            dialect,
            skip_errors,
        }
    }

    /// Emits one envelope row's — one message's — output rows. Any failure in here is the
    /// message's own corruption (unknown op, null image, uneven Canal arrays), which fails the job
    /// in default mode and is caught per message in skip mode, keeping the rows already emitted.
    #[allow(clippy::too_many_arguments)]
    fn emit_message(
        &self,
        row: usize,
        spec: &CdcSpec,
        presence: &[u128],
        envelope: &RecordBatch,
        before: &StructArray,
        after: &StructArray,
        out_rows: &mut Vec<(i8, usize, usize, RowSource)>,
    ) {
        use arrow::array::ListArray;
        let ops = envelope
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("op string");
        // A missing op field is malformed; Flink fails on it (NPE caught → rethrown). Match that.
        let op = if ops.is_valid(row) {
            ops.value(row)
        } else {
            panic!("CDC message has no operation field");
        };
        let action = match self.dialect.classify(op) {
            CdcOp::Change(action) => action,
            CdcOp::Skip => return,
            // Flink throws on an unrecognized op by default; we fail too (never drop it silently).
            CdcOp::Unknown => panic!("unknown CDC operation \"{op}\""),
        };
        let mask = presence.get(row).copied().unwrap_or(0);
        if spec.arrays {
            let after_list = envelope
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let before_list = envelope
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            if after_list.is_null(row) {
                // Flink reads row.getArray(0) unconditionally for a change op — a null `data`
                // is a corrupt message, not an empty fan-out.
                panic!("CDC {} has no \"data\" array", action.name());
            }
            let (after_off, after_len) = (
                after_list.value_offsets()[row] as usize,
                after_list.value_length(row) as usize,
            );
            let (before_off, before_len) = (
                before_list.value_offsets()[row] as usize,
                before_list.value_length(row) as usize,
            );
            for i in 0..after_len {
                // Canal pairs data[i] with old[i]. Flink indexes `old` unchecked for an UPDATE, so
                // a shorter (or absent) `old` array is a corrupt message there; other ops never
                // read it.
                let before_idx = match action {
                    CdcAction::Update if i >= before_len => {
                        panic!("CDC UPDATE \"old\" array is shorter than \"data\"")
                    }
                    _ if i < before_len => before_off + i,
                    _ => after_off + i,
                };
                cdc_emit(
                    &action,
                    before_idx,
                    after_off + i,
                    spec.shape,
                    mask,
                    before,
                    after,
                    out_rows,
                );
            }
        } else {
            cdc_emit(&action, row, row, spec.shape, mask, before, after, out_rows);
        }
    }

    /// Enforces Flink's CDC tombstone rule on the body batch before anything decodes it: the CDC
    /// deserializers skip only a null or ZERO-LENGTH message, so a whitespace-only body is not a
    /// tombstone — it reaches Jackson, yields no envelope row, and the op read NPEs: a corrupt
    /// message (job failure in default mode, a whole-message drop under `ignore-parse-errors`).
    /// The plain JSON decode underneath has its own whitespace rule (it drops such a body without
    /// error, per its Flink parity), so the CDC granularity is applied here, never inherited.
    fn strip_tombstones(&self, bodies: &RecordBatch) -> RecordBatch {
        let column = bodies.column(0);
        let mut kept: Vec<u32> = Vec::with_capacity(bodies.num_rows());
        for row in 0..bodies.num_rows() {
            match binary_body(column, row) {
                None | Some([]) => {}
                Some(bytes) if bytes.iter().all(u8::is_ascii_whitespace) => {
                    if !self.skip_errors {
                        panic!(
                            "Corrupt {} JSON message '{}'.",
                            self.dialect.name(),
                            String::from_utf8_lossy(bytes)
                        );
                    }
                }
                Some(_) => kept.push(row as u32),
            }
        }
        if kept.len() == bodies.num_rows() {
            return bodies.clone();
        }
        let indices = arrow::array::UInt32Array::from(kept);
        let column = take(column, &indices, None).expect("failed to drop CDC tombstones");
        RecordBatch::try_new(bodies.schema(), vec![column])
            .expect("failed to rebuild the CDC body batch")
    }

    /// The `DataOld` presence scan: per surviving body (same null/whitespace skips as the envelope
    /// decode, asserted below), the set of physical field keys Flink's `oldField.findValue` would
    /// find under the message's `old` node — a recursive depth-first search of the whole subtree
    /// (nested objects AND arrays; for Canal the array node's elements fall out of the same
    /// descent). The envelope's `old` is the LAST top-level occurrence, matching the Jackson tree
    /// `root.get` reads. A message without a usable `old` contributes an empty mask (only updates
    /// read it, and a null `old` on an update already failed in `cdc_emit`).
    fn old_key_presence(&self, bodies: &RecordBatch) -> Vec<u128> {
        use simd_json::prelude::*;
        let spec = self.dialect.spec();
        let column = bodies.column(0);
        let mut masks = Vec::with_capacity(bodies.num_rows());
        let mut scratch: Vec<u8> = Vec::new();
        let mut buffers = simd_json::Buffers::default();
        for row in 0..bodies.num_rows() {
            let Some(bytes) = binary_body(column, row) else {
                continue;
            };
            if bytes.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            scratch.clear();
            scratch.extend_from_slice(bytes);
            let Ok(tape) = simd_json::to_tape_with_buffers(&mut scratch, &mut buffers) else {
                if self.skip_errors {
                    continue; // the lenient envelope decode dropped this message too
                }
                masks.push(0); // the strict envelope decode fails this body first
                continue;
            };
            let root = tape.as_value();
            if self.skip_errors && root.as_object().is_none() {
                continue; // ditto: a non-object root is dropped by the lenient envelope decode
            }
            let old = root
                .as_object()
                .and_then(|envelope| {
                    envelope
                        .iter()
                        .filter(|(key, _)| *key == spec.before_field)
                        .last()
                })
                .map(|(_, value)| value);
            let mut mask = 0u128;
            if let Some(value) = old {
                self.find_value_mask(value, &mut mask);
            }
            masks.push(mask);
        }
        masks
    }

    /// Jackson `findValue` presence over a tape subtree: sets bit `i` when physical field `i`'s
    /// name appears as an object key anywhere under `value`. A duplicate key within an object
    /// collapses to its LAST occurrence first (Jackson's tree build overwrites the earlier value,
    /// so names reachable only through a discarded subtree are not found).
    fn find_value_mask(&self, value: simd_json::tape::Value<'_, '_>, mask: &mut u128) {
        use simd_json::prelude::*;
        if let Some(object) = value.as_object() {
            let entries: Vec<(&str, simd_json::tape::Value<'_, '_>)> = object.iter().collect();
            for (i, (key, child)) in entries.iter().enumerate() {
                if entries[i + 1..].iter().any(|(later, _)| later == key) {
                    continue;
                }
                if let Some(field) = self.field_names.iter().position(|name| name == key) {
                    *mask |= 1 << field;
                }
                self.find_value_mask(*child, mask);
            }
        } else if let Some(array) = value.as_array() {
            for element in &array {
                self.find_value_mask(element, mask);
            }
        }
    }

    fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        use arrow::array::ListArray;
        let bodies = &self.strip_tombstones(bodies);
        let envelope = self.envelope.decode(bodies);
        if envelope.num_rows() == 0 {
            return RecordBatch::new_empty(self.output.clone());
        }

        let spec = self.dialect.spec();
        let ops = envelope
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("op string");

        // The pre/post images as struct arrays the gather reads from. For Canal they are the *flattened*
        // values of the `old`/`data` list columns, and a list's element pairs `old[i]` with `data[i]`;
        // for scalar dialects each envelope row is itself the single unit (pre/post index = the row).
        let (before, after) = if spec.arrays {
            let before_list = envelope
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("old list");
            let after_list = envelope
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("data list");
            (before_list.values().clone(), after_list.values().clone())
        } else {
            (envelope.column(0).clone(), envelope.column(1).clone())
        };
        let before = before
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("pre-image struct");
        let after = after
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("post-image struct");

        // The DataOld dialects need per-message key presence in `old` (Flink's findValue rule);
        // the scan mirrors the envelope decode's skip conditions, asserted here.
        let presence = if spec.shape == CdcShape::DataOld {
            let masks = self.old_key_presence(bodies);
            assert_eq!(
                masks.len(),
                envelope.num_rows(),
                "old-key presence misaligned"
            );
            masks
        } else {
            Vec::new()
        };

        // Per output row: its RowKind byte (0 +I, 1 -U, 2 +U, 3 -D — `RowKind.toByteValue()`), and the
        // rows to read in the pre/post-image struct arrays, and which image to read each column from.
        let mut out_rows: Vec<(i8, usize, usize, RowSource)> =
            Vec::with_capacity(envelope.num_rows());
        for row in 0..envelope.num_rows() {
            if self.skip_errors {
                // Flink's skip keeps whatever a message emitted before its failure: the
                // deserializer accumulates rows into a list and collects it after the catch, so a
                // Canal fan-out that dies mid-array still emits the earlier elements. out_rows is
                // append-only, so the partial state is exactly that list.
                use std::panic::{catch_unwind, AssertUnwindSafe};
                let _ = silence_expected_decode_panics(|| {
                    catch_unwind(AssertUnwindSafe(|| {
                        self.emit_message(
                            row,
                            &spec,
                            &presence,
                            &envelope,
                            before,
                            after,
                            &mut out_rows,
                        )
                    }))
                });
            } else {
                self.emit_message(
                    row,
                    &spec,
                    &presence,
                    &envelope,
                    before,
                    after,
                    &mut out_rows,
                );
            }
        }

        gather_cdc_batch(&out_rows, before, after, self.arity, &self.output)
    }
}

/// Builds the fanned-out changelog batch: each physical column gathered from the pre/post-image
/// struct children per output row, plus the trailing `$row_kind$` bytes. The source is the same
/// across columns except for `Coalesce`, which picks per field by the key's presence in the/// Builds a binary input column over a contiguous values region. `copy_values` is used for keyed
/// raw columns because a raw decode may legally pass those buffers through to the exported output;
/// JSON bodies are consumed synchronously into fresh Arrow builders and can borrow the JVM slab.
fn contiguous_binary_array(
    address: usize,
    values_len: usize,
    lengths: impl Iterator<Item = i32>,
    copy_values: bool,
) -> BinaryArray {
    let mut offsets = Vec::new();
    let mut nulls = NullBufferBuilder::new(0);
    offsets.push(0_i32);
    let mut offset = 0_i32;
    for length in lengths {
        if length < 0 {
            nulls.append_null();
        } else {
            nulls.append_non_null();
            offset = offset
                .checked_add(length)
                .expect("contiguous Kafka byte batch exceeds Arrow binary offsets");
        }
        offsets.push(offset);
    }
    assert_eq!(
        offset as usize, values_len,
        "Kafka byte slab length mismatch"
    );
    let values = if copy_values {
        let bytes = unsafe { std::slice::from_raw_parts(address as *const u8, values_len) };
        arrow::buffer::Buffer::from(bytes)
    } else {
        let pointer =
            std::ptr::NonNull::new(address as *mut u8).unwrap_or_else(std::ptr::NonNull::dangling);
        // The Java ArrowBuf owns this memory for the duration of the JNI call. JSON decode consumes
        // it synchronously and never exposes it in the output batch; the custom owner intentionally
        // performs no deallocation when this temporary input array drops.
        unsafe { arrow::buffer::Buffer::from_custom_allocation(pointer, values_len, Arc::new(())) }
    };
    BinaryArray::new(
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        values,
        nulls.finish(),
    )
}

/// JSON-source fast boundary: the JVM copies polled Kafka records once into `[keys][values]` and
/// passes the slab plus lengths here. This avoids building/exporting/importing Arrow binary input
/// vectors; the persistent MessageDecoder still owns all format options and schema-compiled state.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_format_json_NativeJsonFormat_decodeContiguousBytesInto<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    data_address: jlong,
    data_length: jlong,
    key_bytes: jlong,
    lengths: JIntArray<'local>,
    count: jint,
    keyed: jboolean,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let decoded = catch_unwind(AssertUnwindSafe(|| {
        assert!(count >= 0, "negative Kafka byte batch row count");
        let count = count as usize;
        let data_length = usize::try_from(data_length).expect("negative Kafka byte slab length");
        let key_bytes = usize::try_from(key_bytes).expect("negative Kafka key slab length");
        assert!(key_bytes <= data_length, "Kafka key slab exceeds byte slab");
        let mut row_lengths = vec![0_i32; count.checked_mul(2).expect("row count overflow")];
        assert_eq!(
            env.get_array_length(&lengths)
                .expect("Kafka lengths array length") as usize,
            row_lengths.len(),
            "Kafka lengths array shape mismatch"
        );
        env.get_int_array_region(&lengths, 0, &mut row_lengths)
            .expect("read Kafka lengths array");
        let body_address = (data_address as usize)
            .checked_add(key_bytes)
            .expect("Kafka byte slab address overflow");
        let bodies = contiguous_binary_array(
            body_address,
            data_length - key_bytes,
            row_lengths.iter().skip(1).step_by(2).copied(),
            false,
        );
        let columns: Vec<ArrayRef> = if keyed != 0 {
            let keys = contiguous_binary_array(
                data_address as usize,
                key_bytes,
                row_lengths.iter().step_by(2).copied(),
                true,
            );
            vec![Arc::new(keys), Arc::new(bodies)]
        } else {
            assert_eq!(key_bytes, 0, "unkeyed Kafka byte batch carried key bytes");
            vec![Arc::new(bodies)]
        };
        let fields = if keyed != 0 {
            vec![
                Field::new("key", DataType::Binary, true),
                Field::new("body", DataType::Binary, true),
            ]
        } else {
            vec![Field::new("body", DataType::Binary, true)]
        };
        let input = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("build contiguous Kafka byte batch");
        let decoder = unsafe { &*(handle as *mut MessageDecoder) };
        decoder.decode(&input)
    }));
    match decoded {
        Ok(batch) => export_record_batch(batch, out_array_address, out_schema_address),
        Err(panic) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("native contiguous decode failed: {}", panic_message(panic)),
            );
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_format_json_NativeJsonFormat_createDecoder<'local>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    format: jint,
    schema_array_address: jlong,
    schema_address: jlong,
    skip_parse_errors: jboolean,
    format_options: JString<'local>,
) -> jlong {
    let empty_writer = env.new_string("").expect("empty writer schema");
    let empty_reader = env.new_string("").expect("empty reader schema");
    create_decoder(
        build_decoder,
        env,
        class,
        format,
        schema_array_address,
        schema_address,
        empty_writer,
        empty_reader,
        0,
        skip_parse_errors,
        format_options,
    )
}
