# Preserve JDK decoding with fewer copies

Binary-to-STRING casts and UTF-8 DECODE use the same native decoder. Released
Flink constructs a Java String with the UTF-8 charset, whose replacement grouping
differs from Rust's lossy UTF-8 conversion. For example, `ED A0 80` becomes one
U+FFFD. The native path preserves that grouping; this is separate from raw
FROM_BASE64 STRING transport.

Well-formed Arrow Binary input reuses its value, offset and validity buffers as
Utf8. FixedSizeBinary constructs offsets and shares its sliced bytes and NULLs.
For malformed input, the decoder copies the validated prefix in one operation,
then handles malformed units according to the JDK contract. Long ASCII runs use
block checks. Runs of ASCII and independently invalid bytes are encoded in a
bounded stack buffer, reducing three-byte builder writes. Decoding writes into
the final Arrow value buffer, avoiding a temporary String and a second copy.
Other charsets retain their existing conversion paths.

This follows Comet's valid-UTF-8 borrowing, explicit JDK replacement rules and
direct Arrow-builder writing structure. Flink's released results define the
contract. No JNI interface, dependency or inter-operator string representation
changes. The Calc coverage page documents retained bounded-cast and STRING
identity restrictions in [issue #493](https://github.com/datafusion-contrib/StreamFusion/issues/493).

Correctness checks compare complete output against released generated casts,
including malformed continuation prefixes, surrogate encodings, NULLs, fixed
padding, slices and empty batches. Repeated batches must retain independent
output ownership. Decoder tests exercise long valid runs on both sides of a
malformed unit and stack-buffer boundaries.

Measurements and the reproducible stock/previous/native comparison are recorded
in the [scalar benchmark report](../benchmarks/scalar-functions.md#binary-to-string).
