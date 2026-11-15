# Boolean Word Blocks

BOOLEAN character CAST has only two non-NULL spellings. The released modern cast uses
cached uppercase `TRUE`/`FALSE` StringData; legacy mode creates lowercase text. A native
formatter must still produce UTF-8 values, offsets and validity, so replacing the JVM
callback with a per-row byte writer does not automatically beat the cached modern cast.

The common four/five-byte spellings use two immutable 12 KiB lookup tables, one per mode.
Eight bits from Arrow's BooleanBuffer select a block containing the eight spellings and
their relative end offsets. Append one 32-40-byte block and add the base to eight offsets,
instead of selecting and copying one small word at a time. Arrow's bit-chunk iterator
handles sliced bitmap offsets and partial tails. The tables are compiled into the native
library; there is no per-worker JDK snapshot or per-row JVM call.

The output shares the input validity bitmap. Payload bytes under NULL positions are
unspecified in Arrow and may contain a word; the validity bit still makes the SQL value
NULL. Empty and all-NULL batches take a separate path. Truncated/padded targets whose
spellings no longer have lengths four and five retain the general batch writer. Padding
is constructed once per spelling per batch. Every output owns its new UTF-8 offsets and
bytes; no pointer into a temporary block escapes.

The first nullable 1,024-row modern STRING probe loses to stock: 6.162 native versus
4.050 stock ns/row, or 0.657x stock/native. Bitmap blocks reduce the native median to
2.446 ns/row with 3.916 stock, or 1.601x. These are separate warmed release/default-feature/
mimalloc processes, JDK 17.0.12, Flink 2.2.1, one thread, 16 warmups and nine measurements.
The Rust timer covers production Calc and matching result consumption, excluding the
initial JNI/Arrow import/export and perimeter transposes. It is a prepared-operation
diagnostic, not a full-job speedup.

The untimed allocation probe remains at 22 requests per batch; requested bytes move from
10,491 to 10,053. Shared output buffers remain 128 bytes, while new output buffer bytes
increase from 8,047 to 8,782 because masked NULL positions can carry payload. This is a
branch/copy optimization, not a claim of lower retained memory. JVM, C++ and other-thread
allocation is excluded. The
[benchmark record](../benchmarks/scalar-functions.md#boolean-cast-to-character) retains
both probes, raw trials, the complete type/mode matrix and conversion-inclusive jobs.

Correctness coverage includes every eight-bit pattern, nullable and NOT NULL arrays,
unaligned slices, 64-bit chunk boundaries, partial tails, scalar NULLs and both cast modes.
Runtime SQL witnesses compare complete cross-batch values, types, NULLs and row identity
on released Flink 1.18.1/2.2.1 with JDK 11/17, and require consumed native work, no fallback
and empty evaluator descriptors. General STRING producers and unverified cast operands
retain their existing representation and evaluation guards.
