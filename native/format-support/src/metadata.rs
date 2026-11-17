use crate::*;

/// A readable metadata column of Flink's Kafka connector. The keys, types and values follow
/// `KafkaDynamicSource.ReadableMetadata`, identical on both supported connector lines.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KafkaMetadata {
    /// `STRING NOT NULL`: the record's topic.
    Topic,
    /// `INT NOT NULL`: the record's partition.
    Partition,
    /// `MAP<STRING, BYTES> NOT NULL`: the record's headers, in Flink's `HashMap` order.
    Headers,
    /// `INT NULL`: the partition leader epoch, NULL when the record batch carries none.
    LeaderEpoch,
    /// `BIGINT NOT NULL`: the record's offset.
    Offset,
    /// `TIMESTAMP_LTZ(3) NOT NULL`: the record's timestamp in epoch milliseconds.
    Timestamp,
    /// `STRING NOT NULL`: the name of the record's timestamp type.
    TimestampType,
}

impl KafkaMetadata {
    pub fn parse(key: &str) -> KafkaMetadata {
        match key {
            "topic" => KafkaMetadata::Topic,
            "partition" => KafkaMetadata::Partition,
            "headers" => KafkaMetadata::Headers,
            "leader-epoch" => KafkaMetadata::LeaderEpoch,
            "offset" => KafkaMetadata::Offset,
            "timestamp" => KafkaMetadata::Timestamp,
            "timestamp-type" => KafkaMetadata::TimestampType,
            other => panic!("unsupported Kafka metadata key {other}"),
        }
    }
}

/// The leader epoch the JVM writes for a record whose `leaderEpoch()` is empty.
pub const ABSENT_LEADER_EPOCH: i64 = i64::MIN;

/// One batch's per-record connector metadata, borrowed from the JVM's direct buffer for the
/// duration of the decode call. A column the table does not read is empty.
pub struct RecordMetadata<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub offsets: &'a [i64],
    pub timestamps: &'a [i64],
    /// Kafka's `TimestampType.id`: -1 none, 0 create time, 1 log-append time.
    pub timestamp_types: &'a [i8],
    /// [`ABSENT_LEADER_EPOCH`] where the record has none.
    pub leader_epochs: &'a [i64],
    pub headers: RecordHeaders<'a>,
}

/// Each record's headers in Kafka's order: per header a native-endian `i32` key length, the key's
/// UTF-8 bytes, an `i32` value length (-1 for a null value) and the value's bytes.
pub struct RecordHeaders<'a> {
    counts: &'a [i32],
    bytes: &'a [u8],
    starts: Vec<usize>,
}

impl<'a> RecordHeaders<'a> {
    pub fn new(counts: &'a [i32], bytes: &'a [u8]) -> RecordHeaders<'a> {
        let mut starts = Vec::with_capacity(counts.len());
        let mut at = 0;
        for &count in counts {
            starts.push(at);
            for _ in 0..count {
                at = skip_header_part(bytes, at);
                at = skip_header_part(bytes, at);
            }
        }
        assert_eq!(
            at,
            bytes.len(),
            "Kafka header bytes do not match their counts"
        );
        RecordHeaders {
            counts,
            bytes,
            starts,
        }
    }

    /// The `(key, value)` pairs of record `record`, in Kafka's order.
    fn of(&self, record: usize) -> impl Iterator<Item = (&'a [u8], Option<&'a [u8]>)> + '_ {
        let mut at = self.starts[record];
        (0..self.counts[record]).map(move |_| {
            let (key, next) = header_part(self.bytes, at);
            let (value, next) = header_part(self.bytes, next);
            at = next;
            (key.expect("Kafka header keys are never null"), value)
        })
    }
}

fn header_part(bytes: &[u8], at: usize) -> (Option<&[u8]>, usize) {
    let length = i32::from_ne_bytes(
        bytes[at..at + 4]
            .try_into()
            .expect("Kafka header length is four bytes"),
    );
    let start = at + 4;
    if length < 0 {
        return (None, start);
    }
    let end = start + length as usize;
    (Some(&bytes[start..end]), end)
}

fn skip_header_part(bytes: &[u8], at: usize) -> usize {
    header_part(bytes, at).1
}

/// Composes a table's decoded value rows with their records' connector metadata, in the scan's
/// output schema. `value_positions[i]` receives value column `i` (a CDC decode's trailing row-kind
/// column included), and every metadata column is gathered through the rows' sources, so a record
/// that fans out repeats its metadata and a skipped one contributes none.
pub struct MetadataDecoder {
    pub value: MessageDecoder,
    pub value_positions: Vec<usize>,
    pub metadata: Vec<(usize, KafkaMetadata)>,
    pub output: SchemaRef,
    header_order: std::cell::RefCell<StringDataMapOrder>,
}

impl MetadataDecoder {
    pub fn new(
        value: MessageDecoder,
        value_positions: Vec<usize>,
        metadata: Vec<(usize, KafkaMetadata)>,
        output: SchemaRef,
    ) -> MetadataDecoder {
        MetadataDecoder {
            value,
            value_positions,
            metadata,
            output,
            header_order: std::cell::RefCell::new(StringDataMapOrder::default()),
        }
    }

    pub fn decode_records(&self, records: &RecordBatch, metadata: &RecordMetadata) -> RecordBatch {
        let (values, sources) = self.value.decode_traced(records);
        let rows = values.num_rows();
        let mut columns: Vec<Option<ArrayRef>> = vec![None; self.output.fields().len()];
        for (index, &position) in self.value_positions.iter().enumerate() {
            columns[position] = Some(values.column(index).clone());
        }
        for &(position, key) in &self.metadata {
            let field = self.output.field(position);
            columns[position] = Some(self.metadata_column(key, field, &sources, rows, metadata));
        }
        let columns = columns
            .into_iter()
            .map(|column| column.expect("metadata decode left an output column unfilled"))
            .collect();
        RecordBatch::try_new_with_options(
            self.output.clone(),
            columns,
            &arrow::array::RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .expect("failed to compose the metadata decode batch")
    }

    fn metadata_column(
        &self,
        key: KafkaMetadata,
        field: &Field,
        sources: &Sources,
        rows: usize,
        metadata: &RecordMetadata,
    ) -> ArrayRef {
        let record = |row: usize| sources.message(row);
        match key {
            KafkaMetadata::Topic => Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                metadata.topic,
                rows,
            ))),
            KafkaMetadata::Partition => Arc::new(Int32Array::from_value(metadata.partition, rows)),
            KafkaMetadata::Offset => Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| metadata.offsets[record(row)]),
            )),
            KafkaMetadata::LeaderEpoch => Arc::new(Int32Array::from_iter((0..rows).map(|row| {
                let epoch = metadata.leader_epochs[record(row)];
                (epoch != ABSENT_LEADER_EPOCH).then_some(epoch as i32)
            }))),
            KafkaMetadata::TimestampType => {
                Arc::new(StringArray::from_iter_values((0..rows).map(|row| {
                    match metadata.timestamp_types[record(row)] {
                        -1 => "NoTimestampType",
                        0 => "CreateTime",
                        1 => "LogAppendTime",
                        other => panic!("unknown Kafka timestamp type id {other}"),
                    }
                })))
            }
            KafkaMetadata::Timestamp => {
                let DataType::Struct(components) = field.data_type() else {
                    panic!("Kafka timestamp metadata is not a timestamp struct: {field:?}");
                };
                let millis = Int64Array::from_iter_values(
                    (0..rows).map(|row| metadata.timestamps[record(row)]),
                );
                let nanos = Int32Array::from_value(0, rows);
                Arc::new(StructArray::new(
                    components.clone(),
                    vec![Arc::new(millis), Arc::new(nanos)],
                    None,
                ))
            }
            KafkaMetadata::Headers => self.headers_column(field, sources, rows, &metadata.headers),
        }
    }

    /// Flink builds each record's map with `put(StringData, byte[])` into a fresh `HashMap`, and
    /// the map's iteration order is observable downstream, so entries follow that order: a
    /// repeated key keeps its first position and its last value.
    fn headers_column(
        &self,
        field: &Field,
        sources: &Sources,
        rows: usize,
        headers: &RecordHeaders,
    ) -> ArrayRef {
        let DataType::Map(entries, sorted) = field.data_type() else {
            panic!("Kafka headers metadata is not a map: {field:?}");
        };
        let DataType::Struct(entry_fields) = entries.data_type() else {
            panic!("Kafka headers map entries are not a struct: {entries:?}");
        };
        let mut order = self.header_order.borrow_mut();
        let mut keys = StringBuilder::new();
        let mut values = BinaryBuilder::new();
        let mut offsets = Vec::with_capacity(rows + 1);
        offsets.push(0_i32);
        let mut entries_written = 0_i32;
        // A record that fans out into several rows repeats its map, ordered once.
        let mut record_headers: Vec<(&[u8], Option<&[u8]>)> = Vec::new();
        let mut header_keys: Vec<&[u8]> = Vec::new();
        let mut ordered: Vec<(usize, usize)> = Vec::new();
        let mut cached = None;
        for row in 0..rows {
            let record = sources.message(row);
            if cached != Some(record) {
                record_headers.clear();
                record_headers.extend(headers.of(record));
                header_keys.clear();
                header_keys.extend(record_headers.iter().map(|(key, _)| *key));
                ordered.clear();
                ordered.extend_from_slice(order.entries(&header_keys));
                cached = Some(record);
            }
            for &(first, last) in &ordered {
                let key = std::str::from_utf8(record_headers[first].0)
                    .expect("the JVM encodes Kafka header keys as UTF-8");
                keys.append_value(key);
                match record_headers[last].1 {
                    Some(value) => values.append_value(value),
                    None => values.append_null(),
                }
                entries_written += 1;
            }
            offsets.push(entries_written);
        }
        let entries_array = StructArray::new(
            entry_fields.clone(),
            vec![Arc::new(keys.finish()), Arc::new(values.finish())],
            None,
        );
        Arc::new(MapArray::new(
            entries.clone(),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            entries_array,
            None,
            *sorted,
        ))
    }
}

impl Decoder for MetadataDecoder {
    fn decode_traced(&self, _body: &RecordBatch) -> (RecordBatch, Sources) {
        panic!("a table reading Kafka metadata decodes whole records, with their metadata")
    }
    fn output_may_alias_input(&self) -> bool {
        self.value.output_may_alias_input()
    }
    fn metadata_decoder(&self) -> Option<&MetadataDecoder> {
        Some(self)
    }
    fn register_writer_schema(&mut self, id: u32, schema: &str) {
        self.value.register_writer_schema(id, schema);
    }
    fn register_key_writer_schema(&mut self, id: u32, schema: &str) {
        self.value.register_key_writer_schema(id, schema);
    }
}

/// Builds a binary input column over a contiguous values region. `copy_values` is used for keyed
/// raw columns because a raw decode may legally pass those buffers through to the exported output;
/// JSON bodies are consumed synchronously into fresh Arrow builders and can borrow the JVM slab.
pub fn contiguous_binary_array(
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

/// Where the JVM wrote one Kafka split batch in its direct buffer: indexes into the `layout` array
/// of byte offsets. A metadata column the table does not read has offset -1.
pub const LAYOUT_VALUES: usize = 0;
pub const LAYOUT_VALUES_LEN: usize = 1;
/// One native-endian `i32` per record: the value's length, -1 for a null value.
pub const LAYOUT_VALUE_LENGTHS: usize = 2;
pub const LAYOUT_OFFSETS: usize = 3;
pub const LAYOUT_TIMESTAMPS: usize = 4;
pub const LAYOUT_LEADER_EPOCHS: usize = 5;
pub const LAYOUT_TIMESTAMP_TYPES: usize = 6;
pub const LAYOUT_HEADER_COUNTS: usize = 7;
pub const LAYOUT_HEADERS: usize = 8;
pub const LAYOUT_HEADERS_LEN: usize = 9;
/// A keyed table's keys, laid out like the values; absent (-1) for an unkeyed table.
pub const LAYOUT_KEYS: usize = 10;
pub const LAYOUT_KEYS_LEN: usize = 11;
pub const LAYOUT_KEY_LENGTHS: usize = 12;
pub const LAYOUT_LEN: usize = 13;

/// One Kafka split batch in the JVM's direct buffer, borrowed for the duration of a decode call.
pub struct RecordSlab<'a> {
    address: usize,
    bytes: &'a [u8],
    layout: [i64; LAYOUT_LEN],
    count: usize,
}

impl<'a> RecordSlab<'a> {
    /// # Safety
    /// `address` must point to `length` readable bytes that stay valid and unmodified for `'a`.
    pub unsafe fn new(
        address: usize,
        length: usize,
        layout: [i64; LAYOUT_LEN],
        count: usize,
    ) -> RecordSlab<'a> {
        let bytes = if length == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(address as *const u8, length)
        };
        RecordSlab {
            address,
            bytes,
            layout,
            count,
        }
    }

    fn region<T>(&self, index: usize, len: usize) -> &'a [T] {
        let start = self.layout[index];
        if start < 0 {
            return &[];
        }
        let start = start as usize;
        let bytes = len
            .checked_mul(std::mem::size_of::<T>())
            .expect("Kafka record region size overflow");
        let region = &self.bytes[start..start + bytes];
        assert_eq!(
            region.as_ptr() as usize % std::mem::align_of::<T>(),
            0,
            "Kafka record region {index} is misaligned"
        );
        // Plain integers of the JVM's native byte order, bounds- and alignment-checked above.
        unsafe { std::slice::from_raw_parts(region.as_ptr() as *const T, len) }
    }

    /// The batch's decode input: its values as a one-column binary batch, or a keyed table's
    /// `[key, body]` pair. Values are copied out of the JVM's buffer when `copy` (a decode that may
    /// pass input bytes through to its output); keys always are, since a raw key decode does.
    pub fn records(&self, copy: bool) -> RecordBatch {
        let bodies =
            Arc::new(self.binary(LAYOUT_VALUES, LAYOUT_VALUES_LEN, LAYOUT_VALUE_LENGTHS, copy));
        if self.layout[LAYOUT_KEYS] < 0 {
            return RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "body",
                    DataType::Binary,
                    true,
                )])),
                vec![bodies],
            )
            .expect("build Kafka value batch");
        }
        let keys = Arc::new(self.binary(LAYOUT_KEYS, LAYOUT_KEYS_LEN, LAYOUT_KEY_LENGTHS, true));
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Binary, true),
                Field::new("body", DataType::Binary, true),
            ])),
            vec![keys, bodies],
        )
        .expect("build Kafka key and value batch")
    }

    fn binary(&self, bytes: usize, bytes_len: usize, lengths: usize, copy: bool) -> BinaryArray {
        let lengths: &[i32] = self.region(lengths, self.count);
        let start = self.layout[bytes] as usize;
        let len = self.layout[bytes_len] as usize;
        assert!(
            start + len <= self.bytes.len(),
            "Kafka record region exceeds the buffer"
        );
        contiguous_binary_array(self.address + start, len, lengths.iter().copied(), copy)
    }

    pub fn metadata(&self, topic: &'a str, partition: i32) -> RecordMetadata<'a> {
        let header_counts: &[i32] = self.region(LAYOUT_HEADER_COUNTS, self.count);
        let headers: &[u8] = if self.layout[LAYOUT_HEADERS] < 0 {
            &[]
        } else {
            self.region(LAYOUT_HEADERS, self.layout[LAYOUT_HEADERS_LEN] as usize)
        };
        RecordMetadata {
            topic,
            partition,
            offsets: self.region(LAYOUT_OFFSETS, self.count),
            timestamps: self.region(LAYOUT_TIMESTAMPS, self.count),
            timestamp_types: self.region(LAYOUT_TIMESTAMP_TYPES, self.count),
            leader_epochs: self.region(LAYOUT_LEADER_EPOCHS, self.count),
            headers: RecordHeaders::new(header_counts, headers),
        }
    }
}
