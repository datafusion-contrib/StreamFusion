use crate::*;

/// A decoder is constructed and called only inside its owning native library.
pub trait Decoder {
    fn decode(&self, body: &RecordBatch) -> RecordBatch;
    fn output_schema(&self) -> SchemaRef {
        panic!("skip-mode decode is only wired for JSON and CDC formats")
    }
    fn register_writer_schema(&mut self, _id: u32, _schema: &str) {
        panic!("registerAvroSchema on a non-Confluent-Avro decoder")
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
    pub fn decode(&self, body: &RecordBatch) -> RecordBatch {
        if !self.skip_errors {
            return self.decoder.decode(body);
        }
        // `ignore-parse-errors`: Flink wraps each message's whole decode in a catch-everything and
        // skips the message on any failure — malformed JSON, a bad envelope shape, an unconvertible
        // value alike. The native equivalent: decode the batch optimistically, and only when
        // something in it fails, redo it message by message, dropping the messages that fail. The
        // per-message state is fresh each try, so a failed attempt leaves nothing behind.
        use std::panic::{catch_unwind, AssertUnwindSafe};
        silence_expected_decode_panics(|| {
            if let Ok(batch) = catch_unwind(AssertUnwindSafe(|| self.decoder.decode(body))) {
                return batch;
            }
            let mut kept = Vec::new();
            for row in 0..body.num_rows() {
                let single = body.slice(row, 1);
                if let Ok(batch) = catch_unwind(AssertUnwindSafe(|| self.decoder.decode(&single))) {
                    if batch.num_rows() > 0 {
                        kept.push(batch);
                    }
                }
            }
            match kept.len() {
                0 => RecordBatch::new_empty(self.decoder.output_schema()),
                1 => kept.into_iter().next().unwrap(),
                _ => {
                    let schema = kept[0].schema();
                    arrow::compute::concat_batches(&schema, &kept)
                        .expect("skip-mode batch concat failed")
                }
            }
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
    fn decode(&self, body: &RecordBatch) -> RecordBatch {
        self.decode(body)
    }
}
