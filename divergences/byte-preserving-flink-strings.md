# Preserve Flink string bytes instead of normalizing at ingress

Flink `BinaryStringData` can retain arbitrary bytes. Its CONCAT copies those bytes, so `FF` followed
by `!` must produce `FF21`. Decoding invalid bytes with replacement before execution would instead
produce `EFBFBD21`. This is an observable result change, including when the final sink receives
`StringData` and never constructs a Java String.

Comet's `native/common/src/utf8.rs` validates imported string payloads and per-value boundaries,
recursively handles supported containers, reuses valid buffers, and repairs invalid values according
to its Spark compatibility policy. Its ownership and validation structure are useful references;
its repair policy cannot implement Flink's byte-preserving concatenation contract. ClickHouse's
byte-backed String storage and Spark UTF8String's separation of validation from repair support
keeping physical bytes distinct from a promise of valid Unicode.

The first verified StreamFusion path keeps logical STRING but chooses Arrow Binary at planning
time for direct entry into final, byte-only CONCAT and CONCAT_WS projections. Existing Arrow buffers carry both
representations without a decode/re-encode round trip. The kernel accepts Binary and valid Utf8
operands, emits Binary whenever any operand is Binary, and never creates Rust strings from arbitrary
bytes. Final row conversion restores StringData. No runtime schema switching or whole-batch JVM
fallback is introduced.

This is a staged implementation, not universal STRING admission. Arbitrary bytes must not be passed
to other DataFusion text kernels, key comparators, state serializers or native sinks merely by
changing their Arrow type. Future verified text consumers can obtain validated buffer-sharing Utf8
views, with cached validation tied to immutable values and their element boundaries. Java UTF-16
caches must be preserved or discarded at the same observable boundaries as Flink.

Comet delegates CONCAT_WS to DataFusion Spark's text implementation, with a wrapper for scalar
broadcasting. StreamFusion retains its existing DataFusion path for valid Utf8 and uses a byte
kernel when an operand is Binary. This preserves Flink's raw separators and values as well as its
NULL-skipping rule without creating a text view of malformed bytes.

See [coverage](../docs/operators/calc-filter.md#byte-preserving-string-input) and
[#622](https://github.com/datafusion-contrib/StreamFusion/issues/622) for remaining work.
