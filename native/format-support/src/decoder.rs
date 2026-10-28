use crate::*;

/// Which input message each decoded row came from: output row `i` was decoded from input message
/// `rows[i]`. Messages yield their rows in order, so the indices never decrease. A message may
/// yield no row (a tombstone, a skipped or filtered message) or several (a JSON array, a Canal
/// data list, an update's before and after images). Per-record columns such as Kafka metadata are
/// gathered through it, the way Arroyo aligns a message's metadata with the rows it produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Sources {
    /// Every message yielded exactly one row: row `i` came from message `i`.
    Identity,
    Rows(Vec<u32>),
}

impl Sources {
    /// The recorded rows, collapsed to [`Sources::Identity`] when each of the batch's `messages`
    /// yielded exactly its own row.
    pub fn from_rows(rows: Vec<u32>, messages: usize) -> Sources {
        if rows.len() == messages && rows.iter().enumerate().all(|(row, &m)| m as usize == row) {
            Sources::Identity
        } else {
            Sources::Rows(rows)
        }
    }

    /// The source message of output row `row`.
    pub fn message(&self, row: usize) -> usize {
        match self {
            Sources::Identity => row,
            Sources::Rows(rows) => rows[row] as usize,
        }
    }

    /// These sources traced back through an earlier stage of the same decode: this stage's input
    /// row `m` came from message `earlier.message(m)` of the original batch.
    pub fn after(self, earlier: &Sources) -> Sources {
        match (self, earlier) {
            (sources, Sources::Identity) => sources,
            (Sources::Identity, Sources::Rows(earlier)) => Sources::Rows(earlier.clone()),
            (Sources::Rows(rows), Sources::Rows(earlier)) => {
                Sources::Rows(rows.iter().map(|&m| earlier[m as usize]).collect())
            }
        }
    }
}

/// Records the rows a decode loop appends for each message: after message `message` is decoded
/// and the output holds `rows` rows, every row not yet attributed came from it.
#[derive(Default)]
pub struct SourceRecorder {
    rows: Vec<u32>,
}

impl SourceRecorder {
    pub fn with_capacity(capacity: usize) -> SourceRecorder {
        SourceRecorder {
            rows: Vec::with_capacity(capacity),
        }
    }

    pub fn message_done(&mut self, message: usize, rows: usize) {
        debug_assert!(rows >= self.rows.len(), "decoded rows never shrink");
        self.rows.resize(rows, message as u32);
    }

    pub fn finish(self, messages: usize) -> Sources {
        Sources::from_rows(self.rows, messages)
    }
}

/// The messages whose decode collected a null row, each with the rows it produced first, in
/// message order. Flink 2.x's JSON format collects a null for a `null` top-level array element;
/// only a decoder built to report them does, for a composition whose Flink counterpart trips over
/// them.
pub type NullRows = Vec<(u32, u32)>;

/// Records `message`'s first null row, after the `rows` it produced before it.
pub fn record_null_row(null_rows: &mut NullRows, message: usize, rows: usize) {
    if null_rows.last().map(|&(last, _)| last as usize) != Some(message) {
        null_rows.push((message as u32, rows as u32));
    }
}

/// A decoder is constructed and called only inside its owning native library.
pub trait Decoder {
    /// Decodes one batch of messages, reporting the message each output row came from.
    fn decode_traced(&self, body: &RecordBatch) -> (RecordBatch, Sources);
    /// [`Decoder::decode_traced`], plus the messages whose decode collected a null row.
    fn decode_traced_null_rows(&self, body: &RecordBatch) -> (RecordBatch, Sources, NullRows) {
        let (batch, sources) = self.decode_traced(body);
        (batch, sources, NullRows::new())
    }
    fn decode(&self, body: &RecordBatch) -> RecordBatch {
        self.decode_traced(body).0
    }
    fn output_schema(&self) -> SchemaRef {
        panic!("skip-mode decode is only wired for JSON and CDC formats")
    }
    fn register_writer_schema(&mut self, _id: u32, _schema: &str) {
        panic!("registerAvroSchema on a non-Confluent-Avro decoder")
    }
    /// Registers a writer schema with a keyed table's Confluent-Avro key decoder.
    fn register_key_writer_schema(&mut self, _id: u32, _schema: &str) {
        panic!("registerKeyWriterSchema on a decoder without a Confluent-Avro key")
    }
    /// Whether a decoded batch may share buffers with the input bodies (a pass-through decode),
    /// so input borrowed from the JVM must be copied first.
    fn output_may_alias_input(&self) -> bool {
        true
    }
    /// The metadata composition of a table that reads Kafka connector metadata.
    fn metadata_decoder(&self) -> Option<&MetadataDecoder> {
        None
    }
}

pub struct MessageDecoder {
    pub decoder: Box<dyn Decoder>,
    pub skip_errors: bool,
}

impl MessageDecoder {
    pub fn register_writer_schema(&mut self, id: u32, schema: &str) {
        self.decoder.register_writer_schema(id, schema);
    }
    pub fn register_key_writer_schema(&mut self, id: u32, schema: &str) {
        self.decoder.register_key_writer_schema(id, schema);
    }
    pub fn output_may_alias_input(&self) -> bool {
        self.decoder.output_may_alias_input()
    }
    /// Decodes whole Kafka records: the bodies (a keyed table's keys too) with their records'
    /// connector metadata.
    pub fn decode_records(&self, records: &RecordBatch, metadata: &RecordMetadata) -> RecordBatch {
        self.decoder
            .metadata_decoder()
            .expect("this table's decode reads no Kafka metadata")
            .decode_records(records, metadata)
    }
    pub fn decode(&self, body: &RecordBatch) -> RecordBatch {
        self.decode_traced(body).0
    }

    /// The decode with its null rows; a skip-mode retry wrapper never reports any (only the JSON
    /// decoder collects them, and it skips errors itself).
    pub fn decode_traced_null_rows(&self, body: &RecordBatch) -> (RecordBatch, Sources, NullRows) {
        if self.skip_errors {
            let (batch, sources) = self.decode_traced(body);
            return (batch, sources, NullRows::new());
        }
        self.decoder.decode_traced_null_rows(body)
    }

    pub fn decode_traced(&self, body: &RecordBatch) -> (RecordBatch, Sources) {
        if !self.skip_errors {
            return self.decoder.decode_traced(body);
        }
        // `ignore-parse-errors`: Flink wraps each message's whole decode in a catch-everything and
        // skips the message on any failure — malformed JSON, a bad envelope shape, an unconvertible
        // value alike. The native equivalent: decode the batch optimistically, and only when
        // something in it fails, redo it message by message, dropping the messages that fail. The
        // per-message state is fresh each try, so a failed attempt leaves nothing behind.
        use std::panic::{catch_unwind, AssertUnwindSafe};
        silence_expected_decode_panics(|| {
            if let Ok(decoded) = catch_unwind(AssertUnwindSafe(|| self.decoder.decode_traced(body)))
            {
                return decoded;
            }
            let mut kept = Vec::new();
            let mut sources = SourceRecorder::default();
            let mut rows = 0;
            for row in 0..body.num_rows() {
                let single = body.slice(row, 1);
                if let Ok(batch) = catch_unwind(AssertUnwindSafe(|| self.decoder.decode(&single))) {
                    rows += batch.num_rows();
                    if batch.num_rows() > 0 {
                        kept.push(batch);
                    }
                }
                sources.message_done(row, rows);
            }
            let batch = match kept.len() {
                0 => RecordBatch::new_empty(self.decoder.output_schema()),
                1 => kept.into_iter().next().unwrap(),
                _ => {
                    let schema = kept[0].schema();
                    arrow::compute::concat_batches(&schema, &kept)
                        .expect("skip-mode batch concat failed")
                }
            };
            (batch, sources.finish(body.num_rows()))
        })
    }
}
thread_local! {
    /// Whether the current thread is inside a skip-mode per-message decode (see
    /// [`silence_expected_decode_panics`]).
    pub static IN_SKIP_DECODE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Marks the current thread as inside a skip-mode per-message decode, silencing the panic hook for
/// the expected decode failures (Flink's `ignore-parse-errors` skips silently; a hook line per bad
/// message would flood the log). The hook replacement happens once, delegating to the previous hook
/// for every panic outside a skip-mode decode.
pub fn silence_expected_decode_panics<R>(work: impl FnOnce() -> R) -> R {
    use std::cell::Cell;
    use std::sync::Once;
    static INSTALL_HOOK: Once = Once::new();
    INSTALL_HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !IN_SKIP_DECODE.with(Cell::get) {
                previous(info);
            }
        }));
    });
    // Reset on drop, not fallthrough: a panic escaping `work` (a failure beyond the expected
    // per-message skips) must not leave the thread's panics permanently silenced.
    struct Unsilence;
    impl Drop for Unsilence {
        fn drop(&mut self) {
            IN_SKIP_DECODE.with(|flag| flag.set(false));
        }
    }
    IN_SKIP_DECODE.with(|flag| flag.set(true));
    let _unsilence = Unsilence;
    work()
}

impl Decoder for KeyedDecoder {
    fn decode_traced(&self, body: &RecordBatch) -> (RecordBatch, Sources) {
        self.decode_traced(body)
    }
    /// A slab batch's keys are always copied, so only its values can alias the JVM's buffer.
    fn output_may_alias_input(&self) -> bool {
        self.value.output_may_alias_input()
    }
    fn register_writer_schema(&mut self, id: u32, schema: &str) {
        self.value.register_writer_schema(id, schema);
    }
    fn register_key_writer_schema(&mut self, id: u32, schema: &str) {
        self.key.register_writer_schema(id, schema);
    }
}
