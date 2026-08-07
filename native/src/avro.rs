use crate::avro_datum::DatumSkipper;
use crate::*;
use arrow::array::types::{ArrowTimestampType, TimestampMicrosecondType, TimestampMillisecondType};
use arrow::array::PrimitiveArray;
use arrow::compute::kernels::arity::unary;
use arrow::datatypes::TimeUnit;
use arrow_avro::schema::{AvroSchema, Fingerprint, FingerprintAlgorithm, SchemaStore};

/// Decodes Avro message bodies through arrow-avro — bare datums (Flink's `avro`) or
/// Confluent-framed ones (`avro-confluent`) — and reconciles the decoded batch with the Arrow
/// boundary schema the operators expect. arrow-avro derives its own Arrow types from the Avro
/// schema, which differ from the boundary's conventions (timestamp units, small ints, nested child
/// field names); [`reconcile`] closes that gap so the rest of the pipeline never sees an
/// avro-shaped batch.
pub(crate) struct AvroDecoder {
    store: SchemaStore,
    /// One frame-measuring skipper per writer schema id, keyed like the store. Flink reads exactly
    /// one datum per Kafka message and never looks at the bytes after it, so each message's frame
    /// is measured up front and the streaming decoder — which would otherwise keep consuming
    /// frames until the buffer runs out — sees exactly one datum's bytes.
    skippers: HashMap<u32, DatumSkipper>,
    reader: Option<AvroSchema>,
    /// The boundary schema the JVM exported from the table's row type. Empty for the
    /// benchmark-only counting path, which skips reconciliation.
    target: SchemaRef,
    /// Bare datums (Flink's `avro`): the one writer schema sits at synthetic id 0 and each message
    /// gets the 5-byte id-0 Confluent header prepended so the framed decoder applies.
    bare: bool,
    /// Whether a zero-length body is a tombstone to skip. Debezium's deserializer returns on an
    /// empty message like it does on null; the plain formats instead fail it (Flink hits EOF
    /// reading the frame or datum), so only the CDC composition sets this.
    skip_empty: bool,
}

/// An arrow-avro writer store keyed by integer id (the Confluent / id-framing layout), plus the
/// matching frame skippers. An empty schema string builds them empty — the Confluent path starts
/// with no writer schemas and feeds them in by id as the JVM fetches them from the schema registry
/// (`registerAvroSchema`).
fn store(avro_schema: &str, id: u32) -> (SchemaStore, HashMap<u32, DatumSkipper>) {
    let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
    let mut skippers = HashMap::default();
    if !avro_schema.is_empty() {
        store
            .set(
                Fingerprint::Id(id),
                AvroSchema::new(avro_schema.to_string()),
            )
            .expect("failed to register avro schema");
        skippers.insert(id, skipper(avro_schema));
    }
    (store, skippers)
}

fn skipper(avro_schema: &str) -> DatumSkipper {
    DatumSkipper::parse(avro_schema)
        .unwrap_or_else(|error| panic!("failed to register avro schema: {error}"))
}

impl AvroDecoder {
    pub(crate) fn confluent(
        avro_schema: &str,
        schema_id: u32,
        reader: Option<AvroSchema>,
        target: SchemaRef,
    ) -> AvroDecoder {
        let (store, skippers) = store(avro_schema, schema_id);
        AvroDecoder {
            store,
            skippers,
            reader,
            target,
            bare: false,
            skip_empty: false,
        }
    }

    pub(crate) fn bare(
        avro_schema: &str,
        reader: Option<AvroSchema>,
        target: SchemaRef,
    ) -> AvroDecoder {
        let (store, skippers) = store(avro_schema, 0);
        AvroDecoder {
            store,
            skippers,
            reader,
            target,
            bare: true,
            skip_empty: false,
        }
    }

    /// Treats zero-length bodies as tombstones (skipped) — the Debezium envelope contract.
    pub(crate) fn skipping_empty_bodies(mut self) -> AvroDecoder {
        self.skip_empty = true;
        self
    }

    /// Registers a writer schema under a Confluent schema id, so subsequent decodes resolve
    /// messages framed with that id. Only the Confluent variant carries an id-keyed store.
    pub(crate) fn register_writer_schema(&mut self, id: u32, schema: &str) {
        assert!(!self.bare, "registerAvroSchema on a bare-avro decoder");
        self.store
            .set(Fingerprint::Id(id), AvroSchema::new(schema.to_string()))
            .expect("failed to register avro schema");
        self.skippers.insert(id, skipper(schema));
    }

    /// The message's schema id and datum offset: the 5-byte Confluent header parsed, or the
    /// synthetic id 0 for bare datums. Fails the job like Flink does on a malformed header.
    fn frame_id(&self, bytes: &[u8]) -> (u32, usize) {
        if self.bare {
            return (0, 0);
        }
        if bytes.len() < 5 || bytes[0] != 0 {
            panic!("avro decode failed: message is not Confluent-framed");
        }
        (u32::from_be_bytes(bytes[1..5].try_into().unwrap()), 5)
    }

    /// Decodes a binary "body" batch into typed Arrow against the local schema-id store. A null
    /// body contributes no row — Flink's deserializer returns null for a null Kafka value (a
    /// tombstone), which the collector drops silently.
    pub(crate) fn decode(&self, body: &RecordBatch) -> RecordBatch {
        let column = body
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary body");
        let build = || {
            let mut builder = arrow_avro::reader::ReaderBuilder::new()
                .with_writer_schema_store(self.store.clone())
                .with_batch_size(column.len().max(1));
            // With a reader schema, Avro resolution decodes the full writer datum but materializes
            // only the reader's (subset of) fields — projection pushed into the decode. Writer
            // fields the reader omits are parsed and discarded, never built into Arrow.
            if let Some(reader_schema) = &self.reader {
                builder = builder.with_reader_schema(reader_schema.clone());
            }
            builder
                .build_decoder()
                .expect("failed to build avro decoder")
        };
        // Built on the first surviving body: an all-tombstone batch must decode to zero rows even
        // before any writer schema has been registered (arrow-avro refuses an empty store).
        let mut decoder = None;
        let mut framed = Vec::new();
        // A message framed with a different schema id than its predecessor makes the decoder stop
        // consuming until the rows decoded so far are flushed (it can't mix writer schemas in one
        // build), so decode in a loop, flushing whenever a frame is only partially consumed. With
        // a reader schema every flushed batch has the same (reader) shape, so the flushes
        // concatenate.
        let mut batches = Vec::new();
        // The last message's skipper, reused while the schema id repeats (the overwhelmingly
        // common shape — a per-message map lookup shows up at these per-message costs).
        let mut cached: Option<(u32, &DatumSkipper)> = None;
        for i in 0..column.len() {
            if !column.is_valid(i) {
                continue;
            }
            if column.value(i).is_empty() {
                if self.skip_empty {
                    continue;
                }
                // Flink's plain avro/avro-confluent deserializers hit EOF on an empty body and
                // fail the job; silently dropping it would diverge.
                panic!("avro decode failed: empty message body");
            }
            // Flink reads exactly one datum per message and ignores anything after it, so trim
            // the message to its first frame before the streaming decoder sees it. A malformed
            // datum overrunning the message fails the job like Flink's EOF does.
            let (id, start) = self.frame_id(column.value(i));
            let skipper = match cached {
                Some((cached_id, skipper)) if cached_id == id => skipper,
                _ => {
                    let found = self
                        .skippers
                        .get(&id)
                        .unwrap_or_else(|| panic!("avro decode failed: unknown schema id {id}"));
                    cached = Some((id, found));
                    found
                }
            };
            let end = skipper
                .datum_end(column.value(i), start)
                .unwrap_or_else(|error| panic!("avro decode failed: {error}"));
            let bytes = if self.bare {
                framed.clear();
                framed.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00]); // id-0 Confluent header
                framed.extend_from_slice(&column.value(i)[..end]);
                &framed[..]
            } else {
                &column.value(i)[..end]
            };
            let decoder = decoder.get_or_insert_with(build);
            let mut consumed = 0;
            while consumed < bytes.len() {
                let n = decoder
                    .decode(&bytes[consumed..])
                    .expect("avro decode failed");
                consumed += n;
                if consumed < bytes.len() {
                    match decoder.flush().expect("avro flush failed") {
                        Some(batch) => batches.push(batch),
                        // No progress and nothing to flush: the frame walk and the decoder
                        // disagree on the datum's extent — fail loudly rather than emit a
                        // possibly-diverging row.
                        None if n == 0 => panic!("avro decode stalled on a malformed message"),
                        None => {}
                    }
                }
            }
        }
        if let Some(batch) = decoder
            .as_mut()
            .and_then(|d| d.flush().expect("avro flush failed"))
        {
            batches.push(batch);
        }
        if self.target.fields().is_empty() {
            // Benchmark-only counting path: no boundary schema, so no reconciliation.
            return match batches.len() {
                0 => panic!("an all-null avro body batch needs the boundary schema"),
                1 => batches.into_iter().next().unwrap(),
                _ => {
                    let schema = batches[0].schema();
                    concat_batches(&schema, &batches).expect("avro batch concat failed")
                }
            };
        }
        // Reconcile each flush before concatenating: writer schemas differing mid-batch can flush
        // under reader shapes that differ in field metadata (arrow-avro annotates a defaulted
        // field), and reconciliation lands every flush on the one boundary schema.
        let mut reconciled = batches
            .into_iter()
            .map(|batch| reconcile(&self.target, batch));
        match (reconciled.next(), reconciled.next()) {
            (None, _) => RecordBatch::new_empty(self.target.clone()),
            (Some(single), None) => single,
            (Some(first), Some(second)) => {
                let batches: Vec<RecordBatch> =
                    [first, second].into_iter().chain(reconciled).collect();
                concat_batches(&self.target, &batches).expect("avro batch concat failed")
            }
        }
    }
}

/// Rebuilds a decoded batch onto the boundary schema. arrow-avro's Arrow mapping is faithful to the
/// Avro logical types; Flink's converters are not, and parity means reproducing Flink:
///
/// - Every avro timestamp long is epoch *milliseconds* to Flink regardless of the schema's declared
///   unit — `AvroToRowDataConverters` reads the raw long with `fromEpochMillis` even for a
///   `*-timestamp-micros` schema — so every source unit scales by 1e6 to the boundary's nanoseconds.
/// - TINYINT/SMALLINT narrow from the avro int with Java's wrapping `byteValue()`/`shortValue()`.
/// - A decimal whose digits exceed the declared precision is NULL (`DecimalData.fromBigDecimal`).
/// - Nested arrays/maps/structs are rebuilt onto the boundary's child fields (arrow-avro names a
///   list child `item` and map entries `entries`; the boundary uses `element`/`items`).
fn reconcile(target: &SchemaRef, batch: RecordBatch) -> RecordBatch {
    let columns = target
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| reconcile_array(field, column.clone()))
        .collect();
    RecordBatch::try_new(target.clone(), columns)
        .expect("decoded avro batch does not fit the boundary schema")
}

fn reconcile_array(field: &Field, array: ArrayRef) -> ArrayRef {
    if let DataType::Decimal128(precision, scale) = field.data_type() {
        return reconcile_decimal(array, *precision, *scale);
    }
    if field.data_type() == array.data_type() {
        return array;
    }
    match field.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, None) => flink_timestamp_nanos(&array),
        DataType::Int8 => {
            let ints = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("avro int for TINYINT");
            Arc::new(unary::<Int32Type, _, Int8Type>(ints, |v| v as i8))
        }
        DataType::Int16 => {
            let ints = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("avro int for SMALLINT");
            Arc::new(unary::<Int32Type, _, Int16Type>(ints, |v| v as i16))
        }
        DataType::List(element) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("avro array");
            let (_, offsets, values, nulls) = list.clone().into_parts();
            Arc::new(ListArray::new(
                element.clone(),
                offsets,
                reconcile_array(element, values),
                nulls,
            ))
        }
        DataType::Struct(children) => {
            let source = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("avro record");
            let columns = children
                .iter()
                .zip(source.columns())
                .map(|(child, column)| reconcile_array(child, column.clone()))
                .collect();
            Arc::new(StructArray::new(
                children.clone(),
                columns,
                source.nulls().cloned(),
            ))
        }
        DataType::Map(entries, sorted) => {
            let source = array.as_any().downcast_ref::<MapArray>().expect("avro map");
            let DataType::Struct(children) = entries.data_type() else {
                panic!("map entries are not a struct")
            };
            let key = reconcile_array(&children[0], source.keys().clone());
            let value = reconcile_array(&children[1], source.values().clone());
            let struct_entries = StructArray::new(children.clone(), vec![key, value], None);
            Arc::new(MapArray::new(
                entries.clone(),
                source.offsets().clone(),
                struct_entries,
                source.nulls().cloned(),
                *sorted,
            ))
        }
        other => arrow::compute::cast(&array, other).unwrap_or_else(|e| {
            panic!(
                "avro decode produced {} where the boundary needs {other}: {e}",
                array.data_type()
            )
        }),
    }
}

/// See [`reconcile`]: the raw stored long is epoch millis to Flink whatever the avro unit says.
fn flink_timestamp_nanos(array: &ArrayRef) -> ArrayRef {
    fn scale<T: ArrowTimestampType>(array: &ArrayRef) -> ArrayRef {
        let raw = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        Arc::new(unary::<T, _, TimestampNanosecondType>(raw, |v| {
            v.wrapping_mul(1_000_000)
        }))
    }
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Millisecond, _) => scale::<TimestampMillisecondType>(array),
        DataType::Timestamp(TimeUnit::Microsecond, _) => scale::<TimestampMicrosecondType>(array),
        other => panic!("avro decode produced {other} for a timestamp column"),
    }
}

/// The reader schema pins the decoded type to `Decimal128(p, s)` (Flink caps precision at 38), but
/// arrow-avro does not validate the unscaled value against the precision; Flink NULLs a value whose
/// digits exceed it (`DecimalData.fromBigDecimal`).
fn reconcile_decimal(array: ArrayRef, precision: u8, scale: i8) -> ArrayRef {
    let expected = DataType::Decimal128(precision, scale);
    let array = if array.data_type() == &expected {
        array
    } else {
        arrow::compute::cast(&array, &expected).expect("avro decimal does not fit Decimal128")
    };
    let decimals = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
    let bound = 10i128.pow(precision as u32);
    if decimals.iter().flatten().all(|v| v.abs() < bound) {
        return array;
    }
    let bounded: Decimal128Array = decimals
        .iter()
        .map(|v| v.filter(|v| v.abs() < bound))
        .collect();
    Arc::new(bounded.with_precision_and_scale(precision, scale).unwrap())
}

/// One Avro sink format instance's encode parameters: the writer schema the JVM derived with
/// Flink's own schema converter (shipped verbatim so record names, union order, and logical types
/// match Flink's bytes), and — for `avro-confluent` — the schema id the JVM registered at sink
/// open, framed as `0x00` + big-endian id ahead of each datum. Bare `avro` writes the raw datum.
#[cfg(all(feature = "kafka", feature = "avro"))]
pub(crate) struct AvroEncodeOptions {
    schema_json: String,
    schema_id: Option<u32>,
}

#[cfg(all(feature = "kafka", feature = "avro"))]
impl AvroEncodeOptions {
    /// Parses one format instance's `EncodeFormat` option lines. Only options the planner resolved
    /// reach here, so an unknown key or a missing schema is a wiring bug, not a fallback.
    pub(crate) fn parse(encoded: &str, confluent: bool) -> Result<AvroEncodeOptions, String> {
        let mut schema_json = None;
        let mut schema_id = None;
        for line in encoded.lines().filter(|line| !line.is_empty()) {
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| format!("encode option is not key=value: {line}"))?;
            match key {
                "avro-schema" => schema_json = Some(value.to_string()),
                "schema-id" => {
                    let id = value
                        .parse::<u32>()
                        .map_err(|error| format!("invalid Avro schema id {value}: {error}"))?;
                    schema_id = Some(id);
                }
                other => return Err(format!("unknown Avro encode option {other}")),
            }
        }
        let schema_json =
            schema_json.ok_or_else(|| "Avro encode options carry no writer schema".to_string())?;
        if confluent && schema_id.is_none() {
            return Err("avro-confluent encoding requires the registered schema id".to_string());
        }
        if !confluent && schema_id.is_some() {
            return Err("bare avro does not frame a schema id".to_string());
        }
        Ok(AvroEncodeOptions {
            schema_json,
            schema_id,
        })
    }
}

/// Flink's Debezium envelope over one changelog batch — the sink side of `debezium-avro-confluent`.
/// `before` carries the row image for UPDATE_BEFORE/DELETE (`op` = `d`), `after` for
/// INSERT/UPDATE_AFTER (`op` = `c`), exactly `DebeziumAvroSerializationSchema`'s minimal envelope.
/// The physical columns are shared between the two struct images (Arc clones); only the validity
/// masks differ, and the envelope batch then rides the ordinary Avro encode against the envelope
/// writer schema. An absent row-kind column is an insert-only edge (every row is an INSERT).
#[cfg(all(feature = "kafka", feature = "avro"))]
pub(crate) fn encode_debezium_avro_batch(
    batch: &RecordBatch,
    kinds: Option<&Int8Array>,
    options: &AvroEncodeOptions,
    logical_types: &[String],
    field_names: &[String],
) -> Result<crate::kafka::EncodedLines, String> {
    use arrow::array::StructArray;
    use arrow::buffer::NullBuffer;

    // The envelope declares its own top-level names, but the image structs resolve their fields
    // by name against the derived envelope schema — the batch must carry the declared sink field
    // names, not the plan's generated expression names.
    let batch = &crate::kafka::annotate_flink_types(batch, logical_types, field_names)?;
    let rows = batch.num_rows();
    let mut delete = Vec::with_capacity(rows);
    for row in 0..rows {
        delete.push(match kinds.map_or(0, |kinds| kinds.value(row)) {
            0 | 2 => false,
            1 | 3 => true,
            other => return Err(format!("Unsupported operation '{other}' for row kind.")),
        });
    }
    let physical: Fields = batch.schema().fields().clone();
    let before = StructArray::new(
        physical.clone(),
        batch.columns().to_vec(),
        Some(NullBuffer::from_iter(delete.iter().copied())),
    );
    let after = StructArray::new(
        physical.clone(),
        batch.columns().to_vec(),
        Some(NullBuffer::from_iter(delete.iter().map(|delete| !delete))),
    );
    let op = StringArray::from_iter_values(delete.iter().map(|d| if *d { "d" } else { "c" }));
    let envelope = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("before", DataType::Struct(physical.clone()), true),
            Field::new("after", DataType::Struct(physical), true),
            Field::new("op", DataType::Utf8, false),
        ])),
        vec![Arc::new(before), Arc::new(after), Arc::new(op)],
    )
    .map_err(|error| format!("failed to build the Debezium envelope batch: {error}"))?;
    let image = format!("ROW<{}>", logical_types.join(","));
    let envelope_types = [image.clone(), image, "STRING".to_string()];
    let envelope_names = ["before".to_string(), "after".to_string(), "op".to_string()];
    let framed = encode_avro_batch(&envelope, options, &envelope_types, &envelope_names)?;
    // Flink registers the envelope as a [null, record] union (the derived row type's root is
    // nullable) and its datum writer emits the union's branch index before every record — a
    // constant zigzag varint 1. The native writer serialized the record branch; splice the
    // marker between the 5-byte Confluent header and the datum to match Flink's frame.
    let mut bytes = Vec::new();
    let mut lines = Vec::with_capacity(framed.len());
    for row in 0..framed.len() {
        let line = framed.line(row);
        let start = bytes.len();
        bytes.extend_from_slice(&line[..5]);
        bytes.push(0x02);
        bytes.extend_from_slice(&line[5..]);
        lines.push(start..bytes.len());
    }
    Ok(crate::kafka::EncodedLines::new(bytes, lines))
}

/// Encodes one projected sink batch as per-row Avro payloads with Flink's exact bytes. The batch
/// arrives in boundary form; [`flink_avro_array`] first rewrites it the way Flink's converters
/// mangle values before Avro sees them (millisecond longs for every timestamp, HashMap-ordered map
/// entries), then arrow-avro serializes against the shipped writer schema.
#[cfg(all(feature = "kafka", feature = "avro"))]
pub(crate) fn encode_avro_batch(
    batch: &RecordBatch,
    options: &AvroEncodeOptions,
    logical_types: &[String],
    field_names: &[String],
) -> Result<crate::kafka::EncodedLines, String> {
    use arrow_avro::schema::{FingerprintStrategy, SCHEMA_METADATA_KEY};
    use arrow_avro::writer::format::{AvroBinaryFormat, AvroSoeFormat};
    use arrow_avro::writer::WriterBuilder;

    let batch = crate::kafka::annotate_flink_types(batch, logical_types, field_names)?;
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        let column = flink_avro_array(column.clone())
            .map_err(|error| format!("failed to serialize Avro field {}: {error}", field.name()))?;
        fields.push(
            field
                .as_ref()
                .clone()
                .with_data_type(column.data_type().clone()),
        );
        columns.push(column);
    }
    // The derived writer schema rides as schema metadata, so arrow-avro serializes against
    // Flink's exact schema (record names, null-first unions, logical types) instead of one it
    // would derive from the Arrow types.
    let metadata = std::collections::HashMap::from([(
        SCHEMA_METADATA_KEY.to_string(),
        options.schema_json.clone(),
    )]);
    let schema = Schema::new_with_metadata(fields, metadata);
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)
        .map_err(|error| format!("failed to rebuild the Avro sink batch: {error}"))?;
    let builder = WriterBuilder::new(schema);
    let mut encoder = match options.schema_id {
        Some(id) => builder
            .with_fingerprint_strategy(FingerprintStrategy::Id(id))
            .build_encoder::<AvroSoeFormat>(),
        None => builder.build_encoder::<AvroBinaryFormat>(),
    }
    .map_err(|error| format!("failed to build the Avro encoder: {error}"))?;
    encoder
        .encode(&batch)
        .map_err(|error| format!("failed to encode Kafka Avro batch: {error}"))?;
    let rows = encoder.flush();
    Ok(crate::kafka::EncodedLines::from_offsets(
        rows.bytes().to_vec(),
        rows.offsets(),
    ))
}

/// Rewrites one boundary column into the exact values Flink's Avro converter hands its datum
/// writer:
///
/// - Every timestamp flavor serializes as an epoch-*milliseconds* long — `RowDataToAvroConverters`
///   calls `toEpochMilli()` in all four timestamp branches, even into a `*-timestamp-micros`
///   schema (sub-millisecond digits dropped, micros values 1000x small) — so the boundary's
///   nanoseconds floor-divide to a millisecond column and arrow-avro writes the raw long.
/// - BINARY(n)'s fixed-size boundary form widens to variable bytes (the Avro schema says `bytes`).
/// - Map entries reorder into `java.util.HashMap` iteration order (see [`java_hash_map_order`]);
///   a NULL key fails the batch the way Flink's converter NPEs, and a duplicate key keeps the
///   first position with the last value exactly like `HashMap.put`.
#[cfg(all(feature = "kafka", feature = "avro"))]
fn flink_avro_array(array: ArrayRef) -> Result<ArrayRef, String> {
    use arrow::array::cast::AsArray;

    match array.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let nanos = array.as_primitive::<TimestampNanosecondType>();
            Ok(Arc::new(unary::<_, _, TimestampMillisecondType>(
                nanos,
                |value: i64| value.div_euclid(1_000_000),
            )))
        }
        DataType::FixedSizeBinary(_) => arrow::compute::cast(&array, &DataType::Binary)
            .map_err(|error| format!("BINARY column does not widen to Avro bytes: {error}")),
        DataType::Struct(_) => {
            let source = array.as_struct();
            let mut fields = Vec::with_capacity(source.num_columns());
            let mut columns = Vec::with_capacity(source.num_columns());
            for (field, column) in source.fields().iter().zip(source.columns()) {
                let column = flink_avro_array(column.clone())?;
                fields.push(Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(column.data_type().clone()),
                ));
                columns.push(column);
            }
            Ok(Arc::new(StructArray::new(
                fields.into(),
                columns,
                source.nulls().cloned(),
            )))
        }
        DataType::List(_) => {
            let (field, offsets, values, nulls) = array.as_list::<i32>().clone().into_parts();
            let values = flink_avro_array(values)?;
            let field = Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(values.data_type().clone()),
            );
            Ok(Arc::new(ListArray::new(field, offsets, values, nulls)))
        }
        DataType::Map(_, _) => flink_avro_map(array.as_map()),
        _ => Ok(array),
    }
}

#[cfg(all(feature = "kafka", feature = "avro"))]
fn flink_avro_map(map: &MapArray) -> Result<ArrayRef, String> {
    use arrow::buffer::OffsetBuffer;

    let keys = map
        .keys()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "Avro map serialization requires string keys".to_string())?;
    let values = flink_avro_array(map.values().clone())?;
    let offsets = map.offsets();
    let mut take_indices = Vec::with_capacity(values.len());
    let mut new_offsets = Vec::with_capacity(map.len() + 1);
    new_offsets.push(0i32);
    for row in 0..map.len() {
        if map.is_valid(row) {
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            for entry in java_hash_map_order(keys, start, end)? {
                take_indices.push(entry as u32);
            }
        }
        new_offsets.push(take_indices.len() as i32);
    }
    let indices = UInt32Array::from(take_indices);
    let taken_keys = arrow::compute::take(keys, &indices, None)
        .map_err(|error| format!("failed to reorder Avro map keys: {error}"))?;
    let taken_values = arrow::compute::take(&values, &indices, None)
        .map_err(|error| format!("failed to reorder Avro map values: {error}"))?;
    let DataType::Map(entry_field, ordered) = map.data_type() else {
        unreachable!("flink_avro_map called for a non-map array");
    };
    let DataType::Struct(children) = entry_field.data_type() else {
        return Err("map entries are not a struct".to_string());
    };
    let children = Fields::from(vec![
        children[0]
            .as_ref()
            .clone()
            .with_data_type(taken_keys.data_type().clone()),
        children[1]
            .as_ref()
            .clone()
            .with_data_type(taken_values.data_type().clone()),
    ]);
    let entries = StructArray::new(children, vec![taken_keys, taken_values], None);
    let entry_field = Arc::new(
        entry_field
            .as_ref()
            .clone()
            .with_data_type(entries.data_type().clone()),
    );
    Ok(Arc::new(MapArray::new(
        entry_field,
        OffsetBuffer::new(new_offsets.into()),
        entries,
        map.nulls().cloned(),
        *ordered,
    )))
}

/// The iteration order of the `java.util.HashMap` Flink's map converter funnels every map value
/// through: `RowDataToAvroConverters` copies each `MapData` into a `HashMap` sized by
/// `CollectionUtil.newHashMapWithExpectedSize`, and Avro's datum writer walks `entrySet()`, so the
/// wire order of map entries is the HashMap's bucket order — not the row's. This mirrors
/// `HashMap.putVal` (bucket = spread `String.hashCode` masked by a power-of-two table, chains in
/// insertion order, replace keeps the chain position, the doubling resize splits chains preserving
/// order). The one path not reproduced is a treeified bin — nine keys sharing a bucket of a
/// 64-slot-or-larger table — where Java switches to red-black-tree order; that fails the batch
/// loudly instead of silently diverging.
///
/// Returns the entry indices (into the map's child arrays) in serialization order; the index of a
/// duplicated key is the last one written, matching `put` overwriting the value.
#[cfg(all(feature = "kafka", feature = "avro"))]
fn java_hash_map_order(keys: &StringArray, start: usize, end: usize) -> Result<Vec<usize>, String> {
    struct Entry {
        hash: i32,
        key_index: usize,
        value_index: usize,
    }

    fn resize(buckets: &mut Vec<Vec<Entry>>, threshold: &mut usize) {
        let old_capacity = buckets.len();
        let new_capacity = old_capacity * 2;
        // Java doubles the threshold only from the default table size up; smaller tables
        // recompute it from the load factor.
        *threshold = if old_capacity >= 16 {
            *threshold * 2
        } else {
            (new_capacity as f32 * 0.75) as usize
        };
        let mut new_buckets: Vec<Vec<Entry>> = (0..new_capacity).map(|_| Vec::new()).collect();
        for (index, chain) in buckets.drain(..).enumerate() {
            for entry in chain {
                let high = entry.hash as u32 & old_capacity as u32 != 0;
                new_buckets[if high { index + old_capacity } else { index }].push(entry);
            }
        }
        *buckets = new_buckets;
    }

    let expected = end - start;
    let mut order = Vec::with_capacity(expected);
    if expected == 0 {
        return Ok(order);
    }
    // CollectionUtil.newHashMapWithExpectedSize + HashMap.tableSizeFor.
    let required = if expected <= 2 {
        expected + 1
    } else {
        (expected as f64 / 0.75).ceil() as usize
    };
    let mut buckets: Vec<Vec<Entry>> = (0..required.next_power_of_two())
        .map(|_| Vec::new())
        .collect();
    let mut threshold = (buckets.len() as f32 * 0.75) as usize;
    let mut size = 0;
    for index in start..end {
        if keys.is_null(index) {
            // Flink's converter reads the key through getString and NPEs on a null slot.
            return Err("a NULL map key cannot be serialized as an Avro map entry".to_string());
        }
        let key = keys.value(index);
        let hash = {
            let h = key
                .encode_utf16()
                .fold(0i32, |h, unit| h.wrapping_mul(31).wrapping_add(unit as i32));
            h ^ ((h as u32) >> 16) as i32
        };
        let bucket = (hash as u32 & (buckets.len() as u32 - 1)) as usize;
        let chain = &mut buckets[bucket];
        if let Some(entry) = chain
            .iter_mut()
            .find(|entry| entry.hash == hash && keys.value(entry.key_index) == key)
        {
            entry.value_index = index;
            continue;
        }
        let chain_length = chain.len();
        chain.push(Entry {
            hash,
            key_index: index,
            value_index: index,
        });
        if chain_length >= 8 {
            // Java's treeify threshold: a small table resizes instead of treeifying.
            if buckets.len() < 64 {
                resize(&mut buckets, &mut threshold);
            } else {
                return Err(
                    "map keys collide into a Java tree bin, whose iteration order is not \
                     natively reproduced"
                        .to_string(),
                );
            }
        }
        size += 1;
        if size > threshold {
            resize(&mut buckets, &mut threshold);
        }
    }
    for chain in &buckets {
        for entry in chain {
            order.push(entry.value_index);
        }
    }
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Date32Array, Time32MillisecondArray};

    pub(super) fn zigzag(n: i64) -> Vec<u8> {
        let mut zz = ((n << 1) ^ (n >> 63)) as u64;
        let mut out = Vec::new();
        loop {
            let mut b = (zz & 0x7f) as u8;
            zz >>= 7;
            if zz != 0 {
                b |= 0x80;
            }
            out.push(b);
            if zz == 0 {
                break;
            }
        }
        out
    }

    pub(super) fn avro_bytes(bytes: &[u8]) -> Vec<u8> {
        let mut out = zigzag(bytes.len() as i64);
        out.extend_from_slice(bytes);
        out
    }

    pub(super) fn avro_string(s: &str) -> Vec<u8> {
        avro_bytes(s.as_bytes())
    }

    fn bodies(messages: Vec<Option<&[u8]>>) -> RecordBatch {
        let array = BinaryArray::from(messages);
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "body",
                DataType::Binary,
                true,
            )])),
            vec![Arc::new(array)],
        )
        .unwrap()
    }

    fn timestamp_ns(name: &str) -> Field {
        Field::new(name, DataType::Timestamp(TimeUnit::Nanosecond, None), true)
    }

    // The boundary schema for the full reconciled scalar family plus the nested renames: the list
    // child is `element` (arrow-avro emits `item`) and the map entries struct is `items`
    // (arrow-avro emits `entries`).
    fn boundary_schema() -> SchemaRef {
        let map_entries = Field::new(
            "items",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ])),
            false,
        );
        Arc::new(Schema::new(vec![
            Field::new("ti", DataType::Int8, true),
            timestamp_ns("ts"),
            timestamp_ns("tsu"),
            Field::new("dec", DataType::Decimal128(5, 2), true),
            Field::new("d", DataType::Date32, true),
            Field::new("t", DataType::Time32(TimeUnit::Millisecond), true),
            Field::new(
                "arr",
                DataType::List(Arc::new(Field::new("element", DataType::Int64, true))),
                true,
            ),
            Field::new("m", DataType::Map(Arc::new(map_entries), false), true),
        ]))
    }

    const BOUNDARY_WRITER: &str = r#"{"type":"record","name":"Row","fields":[
        {"name":"ti","type":"int"},
        {"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
        {"name":"tsu","type":{"type":"long","logicalType":"local-timestamp-micros"}},
        {"name":"dec","type":{"type":"bytes","logicalType":"decimal","precision":5,"scale":2}},
        {"name":"d","type":{"type":"int","logicalType":"date"}},
        {"name":"t","type":{"type":"int","logicalType":"time-millis"}},
        {"name":"arr","type":{"type":"array","items":["null","long"]}},
        {"name":"m","type":{"type":"map","values":["null","long"]}}]}"#;

    fn boundary_datum(ti: i64, millis: i64, unscaled_decimal: &[u8]) -> Vec<u8> {
        let mut datum = zigzag(ti); // int ti
        datum.extend(zigzag(millis)); // ts
        datum.extend(zigzag(millis * 1000)); // tsu (a genuine micros writer)
        datum.extend(avro_bytes(unscaled_decimal)); // dec
        datum.extend(zigzag(19_000)); // d
        datum.extend(zigzag(45_296_789)); // t
        datum.extend(zigzag(1)); // arr: one block of one item
        datum.extend(zigzag(1)); // union branch 1 = long
        datum.extend(zigzag(7));
        datum.extend(zigzag(0)); // arr terminator
        datum.extend(zigzag(1)); // m: one block of one entry
        datum.extend(avro_string("a"));
        datum.extend(zigzag(1)); // union branch 1 = long
        datum.extend(zigzag(5));
        datum.extend(zigzag(0)); // m terminator
        datum
    }

    #[test]
    fn reconciles_decoded_batch_onto_the_boundary_schema() {
        let target = boundary_schema();
        let decoder = AvroDecoder::bare(BOUNDARY_WRITER, None, target.clone());
        // ti=300 wraps to 44 (Java byteValue); dec row 1 = 123.45, row 2 overflows DECIMAL(5,2).
        let m1 = boundary_datum(300, 1_000, &[0x30, 0x39]); // 12345
        let m2 = boundary_datum(-1, -1, &[0x01, 0xE2, 0x40]); // 123456: 6 digits > precision 5
        let out = decoder.decode(&bodies(vec![Some(&m1), Some(&m2)]));

        assert_eq!(out.schema(), target);
        let ti = out.column(0).as_any().downcast_ref::<Int8Array>().unwrap();
        assert_eq!(ti.values(), &[44, -1]);
        let ts = out
            .column(1)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(ts.values(), &[1_000_000_000, -1_000_000]);
        // Flink reads the micros long as epoch millis (its converter has no micros path); the
        // reconciliation reproduces that: raw x 1e6, not x 1e3.
        let tsu = out
            .column(2)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(tsu.values(), &[1_000_000_000_000, -1_000_000_000]);
        let dec = out
            .column(3)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!((dec.value(0), dec.is_null(1)), (12345, true));
        let d = out
            .column(4)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(d.values(), &[19_000, 19_000]);
        let t = out
            .column(5)
            .as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .unwrap();
        assert_eq!(t.values(), &[45_296_789, 45_296_789]);
        let arr = out.column(6).as_any().downcast_ref::<ListArray>().unwrap();
        let first = arr.value(0);
        assert_eq!(
            first
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[7]
        );
        let m = out.column(7).as_any().downcast_ref::<MapArray>().unwrap();
        let keys = m.keys().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!((keys.value(0), keys.value(1)), ("a", "a"));
    }

    // Flink's deserializer returns null for a null Kafka value and the collector drops it — an
    // all-tombstone batch must decode to zero rows, not fail the task.
    #[test]
    fn all_null_bodies_decode_to_an_empty_boundary_batch() {
        let target = boundary_schema();
        let decoder = AvroDecoder::bare(BOUNDARY_WRITER, None, target.clone());
        let out = decoder.decode(&bodies(vec![None, None]));
        assert_eq!((out.num_rows(), out.schema()), (0, target));
    }

    // A zero-length body is NOT a tombstone on the plain formats: Flink's deserializer hits EOF
    // and fails the job (only the Debezium envelope skips empty messages).
    #[test]
    #[should_panic(expected = "empty message body")]
    fn empty_body_fails_the_plain_avro_decode() {
        let decoder = AvroDecoder::bare(BOUNDARY_WRITER, None, boundary_schema());
        decoder.decode(&bodies(vec![Some(&[])]));
    }

    // Flink reads exactly one datum per message and never checks the remaining buffer: trailing
    // junk after a complete datum is ignored, and a second concatenated datum is dead bytes — one
    // row per message, from the first datum, never a failure and never an extra row.
    #[test]
    fn trailing_bytes_after_the_datum_are_ignored_like_flink() {
        let target = boundary_schema();
        let decoder = AvroDecoder::bare(BOUNDARY_WRITER, None, target.clone());
        let datum = boundary_datum(1, 1_000, &[0x30, 0x39]);
        let mut with_junk = datum.clone();
        with_junk.extend_from_slice(&[0xFF, 0x07, 0x00]);
        let mut concatenated = datum.clone();
        concatenated.extend(boundary_datum(2, 2_000, &[0x01]));

        let out = decoder.decode(&bodies(vec![Some(&with_junk), Some(&concatenated)]));
        assert_eq!(out.num_rows(), 2);
        let ti = out.column(0).as_any().downcast_ref::<Int8Array>().unwrap();
        assert_eq!(ti.values(), &[1, 1]); // both rows are the FIRST datum's image
    }

    #[test]
    fn trailing_bytes_after_the_confluent_frame_are_ignored_like_flink() {
        let writer = r#"{"type":"record","name":"Row","fields":[
            {"name":"id","type":"long"},
            {"name":"s","type":"string"}]}"#;
        let target = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let mut decoder = AvroDecoder::confluent("", 0, None, target);
        decoder.register_writer_schema(7, writer);
        let frame = |id: i64, s: &str| {
            let mut frame = vec![0x00, 0, 0, 0, 7];
            frame.extend(zigzag(id));
            frame.extend(avro_string(s));
            frame
        };
        let mut with_junk = frame(1, "first");
        with_junk.extend_from_slice(&[0xAB, 0xCD]);
        let mut concatenated = frame(1, "first");
        concatenated.extend(frame(2, "second"));

        let out = decoder.decode(&bodies(vec![Some(&with_junk), Some(&concatenated)]));
        assert_eq!(out.num_rows(), 2);
        let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id.values(), &[1, 1]);
        let s = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!((s.value(0), s.value(1)), ("first", "first"));
    }

    // A datum cut short fails the job on both engines (Flink's decoder hits EOF).
    #[test]
    #[should_panic(expected = "avro decode failed")]
    fn truncated_datum_fails_the_decode() {
        let target = boundary_schema();
        let decoder = AvroDecoder::bare(BOUNDARY_WRITER, None, target);
        let datum = boundary_datum(1, 1_000, &[0x30, 0x39]);
        decoder.decode(&bodies(vec![Some(&datum[..datum.len() - 3])]));
    }

    // A registry writer schema can declare timestamp-micros while the reader (derived from the
    // table under Flink's hard-wired legacy mapping) says timestamp-millis. Avro Java resolves the
    // raw long without unit conversion and Flink then reads it as millis; arrow-avro likewise takes
    // the logical type from the reader and passes the raw long through. Pin that passthrough — a
    // rescale here would silently diverge from Flink.
    #[test]
    fn registry_writer_micros_reads_as_millis_like_flink() {
        let reader = r#"{"type":"record","name":"Row","fields":[
            {"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}"#;
        let writer = r#"{"type":"record","name":"Row","fields":[
            {"name":"ts","type":{"type":"long","logicalType":"timestamp-micros"}}]}"#;
        let target = Arc::new(Schema::new(vec![timestamp_ns("ts")]));
        let mut decoder = AvroDecoder::confluent(
            "",
            0,
            Some(AvroSchema::new(reader.to_string())),
            target.clone(),
        );
        decoder.register_writer_schema(7, writer);
        let mut framed = vec![0x00, 0, 0, 0, 7];
        framed.extend(zigzag(5_000)); // 5000 micros on the wire; Flink reads 5000 millis
        let out = decoder.decode(&bodies(vec![Some(&framed)]));
        let ts = out
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(ts.values(), &[5_000_000_000]);
    }

    // Schema evolution: a reader field the writer lacks materializes its default (the null default
    // every nullable table column carries), and the batch still lands on the boundary schema.
    #[test]
    fn missing_writer_field_takes_the_reader_default() {
        let reader = r#"{"type":"record","name":"Row","fields":[
            {"name":"id","type":"long"},
            {"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}],"default":null}]}"#;
        let writer = r#"{"type":"record","name":"Row","fields":[{"name":"id","type":"long"}]}"#;
        let target = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            timestamp_ns("ts"),
        ]));
        let mut decoder = AvroDecoder::confluent(
            "",
            0,
            Some(AvroSchema::new(reader.to_string())),
            target.clone(),
        );
        decoder.register_writer_schema(3, writer);
        let mut framed = vec![0x00, 0, 0, 0, 3];
        framed.extend(zigzag(42));
        let out = decoder.decode(&bodies(vec![Some(&framed)]));
        assert_eq!(out.schema(), target);
        let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id.values(), &[42]);
        assert!(out.column(1).is_null(0));
    }
}

#[cfg(all(test, feature = "kafka", feature = "avro"))]
mod encode_tests {
    use super::tests::{avro_bytes, avro_string, zigzag};
    use super::*;
    use arrow::array::{
        Date32Array, Int8Array, MapArray, StringArray, StructArray, Time32MillisecondArray,
        TimestampNanosecondArray,
    };
    use arrow::buffer::OffsetBuffer;

    fn encode(
        schema_json: &str,
        schema_id: Option<u32>,
        batch: &RecordBatch,
    ) -> Result<crate::kafka::EncodedLines, String> {
        let options = AvroEncodeOptions {
            schema_json: schema_json.to_string(),
            schema_id,
        };
        encode_avro_batch(batch, &options, &[], &[])
    }

    // The full boundary-to-wire value mapping over a scalar row, pinned byte for byte: null-first
    // unions marked 0x02/0x00, strings and decimal unscaled bytes length-prefixed, date/time raw
    // ints, and the boundary's nanosecond timestamps floor-divided to the epoch-millisecond longs
    // Flink writes even into a micros schema (its converters call toEpochMilli unconditionally).
    #[test]
    fn encodes_bare_datums_with_flinks_exact_bytes() {
        let schema_json = r#"{"type":"record","name":"record","namespace":"org.apache.flink.avro.generated","fields":[
            {"name":"b","type":["null","long"],"default":null},
            {"name":"s","type":"string"},
            {"name":"ti","type":["null","int"],"default":null},
            {"name":"dec","type":{"type":"bytes","logicalType":"decimal","precision":5,"scale":2}},
            {"name":"t","type":{"type":"int","logicalType":"time-millis"}},
            {"name":"d","type":{"type":"int","logicalType":"date"}},
            {"name":"ts","type":["null",{"type":"long","logicalType":"local-timestamp-micros"}],"default":null}]}"#;
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int64, true),
            Field::new("s", DataType::Utf8, false),
            Field::new("ti", DataType::Int8, true),
            Field::new("dec", DataType::Decimal128(5, 2), false),
            Field::new("t", DataType::Time32(TimeUnit::Millisecond), false),
            Field::new("d", DataType::Date32, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(42), None])),
                Arc::new(StringArray::from(vec!["hi", ""])),
                Arc::new(Int8Array::from(vec![Some(-2), None])),
                Arc::new(
                    Decimal128Array::from(vec![12345i128, 0])
                        .with_precision_and_scale(5, 2)
                        .unwrap(),
                ),
                Arc::new(Time32MillisecondArray::from(vec![45_296_789, 0])),
                Arc::new(Date32Array::from(vec![19_000, 0])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(1_500_123_456),
                    Some(-1),
                ])),
            ],
        )
        .unwrap();

        let encoded = encode(schema_json, None, &batch).unwrap();
        assert_eq!(encoded.len(), 2);
        let mut first = vec![0x02];
        first.extend(zigzag(42)); // b
        first.extend(avro_string("hi")); // s
        first.extend([0x02]);
        first.extend(zigzag(-2)); // ti
        first.extend(avro_bytes(&[0x30, 0x39])); // dec: unscaled 12345
        first.extend(zigzag(45_296_789)); // t: raw millis int
        first.extend(zigzag(19_000)); // d
        first.extend([0x02]);
        first.extend(zigzag(1_500)); // ts: 1_500_123_456ns floors to 1500ms, micros dropped
        assert_eq!(encoded.line(0), &first[..]);
        let mut second = vec![0x00]; // b null
        second.extend(avro_string("")); // s
        second.extend([0x00]); // ti null
        second.extend(avro_bytes(&[0x00])); // dec: BigInteger.toByteArray(0)
        second.extend(zigzag(0)); // t
        second.extend(zigzag(0)); // d
        second.extend([0x02]);
        second.extend(zigzag(-1)); // ts: -1ns floor-divides to -1ms, not 0
        assert_eq!(encoded.line(1), &second[..]);
    }

    #[test]
    fn frames_confluent_messages_with_the_registered_id() {
        let schema_json = r#"{"type":"record","name":"record","namespace":"org.apache.flink.avro.generated","fields":[
            {"name":"id","type":"long"}]}"#;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7]))]).unwrap();
        let encoded = encode(schema_json, Some(258), &batch).unwrap();
        let mut expected = vec![0x00, 0x00, 0x00, 0x01, 0x02]; // magic + big-endian id 258
        expected.extend(zigzag(7));
        assert_eq!(encoded.line(0), &expected[..]);
    }

    fn map_batch(entries: Vec<(Option<&str>, i64)>) -> RecordBatch {
        let keys = StringArray::from(entries.iter().map(|(key, _)| *key).collect::<Vec<_>>());
        let values = Int64Array::from(entries.iter().map(|(_, v)| *v).collect::<Vec<_>>());
        // The boundary declares map keys nullable — a null key is data Flink only rejects at
        // serialization time.
        let children = Fields::from(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("value", DataType::Int64, false),
        ]);
        let struct_entries = StructArray::new(
            children.clone(),
            vec![Arc::new(keys), Arc::new(values)],
            None,
        );
        let entry_field = Arc::new(Field::new("entries", DataType::Struct(children), false));
        let offsets = OffsetBuffer::new(vec![0, struct_entries.len() as i32].into());
        let map = MapArray::new(entry_field.clone(), offsets, struct_entries, None, false);
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "m",
                DataType::Map(entry_field, false),
                false,
            )])),
            vec![Arc::new(map)],
        )
        .unwrap()
    }

    const MAP_SCHEMA: &str = r#"{"type":"record","name":"record","namespace":"org.apache.flink.avro.generated","fields":[
        {"name":"m","type":{"type":"map","values":"long"}}]}"#;

    // Flink funnels every map through a java.util.HashMap before Avro writes it, so the wire
    // order is bucket order, not row order; a duplicate key keeps its first position with the
    // last value, exactly like HashMap.put. Expected orders taken from a real HashMap sized by
    // Flink's CollectionUtil.
    #[test]
    fn orders_map_entries_like_javas_hash_map() {
        let batch = map_batch(vec![
            (Some("zebra"), 0),
            (Some("apple"), 1),
            (Some("mango"), 2),
            (Some("kiwi"), 3),
        ]);
        let encoded = encode(MAP_SCHEMA, None, &batch).unwrap();
        let mut expected = zigzag(4);
        for (key, value) in [("zebra", 0), ("apple", 1), ("kiwi", 3), ("mango", 2)] {
            expected.extend(avro_string(key));
            expected.extend(zigzag(value));
        }
        expected.extend(zigzag(0));
        assert_eq!(encoded.line(0), &expected[..]);

        let batch = map_batch(vec![(Some("a"), 0), (Some("b"), 1), (Some("a"), 2)]);
        let encoded = encode(MAP_SCHEMA, None, &batch).unwrap();
        let mut expected = zigzag(2);
        for (key, value) in [("a", 2), ("b", 1)] {
            expected.extend(avro_string(key));
            expected.extend(zigzag(value));
        }
        expected.extend(zigzag(0));
        assert_eq!(encoded.line(0), &expected[..]);
    }

    // The pure ordering simulation against reference orders captured from java.util.HashMap: a
    // 13-key map spreads across a 32-slot table, and nine keys sharing one hashCode force the
    // resize-instead-of-treeify path (a 16-slot table doubles), which preserves insertion order.
    #[test]
    fn hash_map_order_matches_java_reference_orders() {
        let keys: Vec<String> = (0..13).map(|i| format!("key{}", 12 - i)).collect();
        let array = StringArray::from(keys.iter().map(String::as_str).collect::<Vec<_>>());
        let order = java_hash_map_order(&array, 0, 13).unwrap();
        assert_eq!(order, vec![11, 10, 12, 7, 6, 9, 8, 3, 5, 4, 1, 2, 0]);

        let parts = ["Aa", "BB"];
        let colliding: Vec<String> = (0..9)
            .map(|i: usize| {
                format!(
                    "{}{}{}{}",
                    parts[(i >> 3) & 1],
                    parts[(i >> 2) & 1],
                    parts[(i >> 1) & 1],
                    parts[i & 1]
                )
            })
            .collect();
        let array = StringArray::from(colliding.iter().map(String::as_str).collect::<Vec<_>>());
        let order = java_hash_map_order(&array, 0, 9).unwrap();
        assert_eq!(order, (0..9).collect::<Vec<_>>());
    }

    // Flink's converter NPEs reading a null map key; the native encode fails the batch with an
    // explicit error instead of writing bytes Flink never could.
    #[test]
    fn null_map_key_fails_the_batch() {
        let batch = map_batch(vec![(Some("a"), 0), (None, 1)]);
        let error = encode(MAP_SCHEMA, None, &batch)
            .err()
            .expect("a null key must fail");
        assert!(error.contains("NULL map key"), "{error}");
    }
}
