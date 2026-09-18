# Scalar function allocation and text scans

These techniques describe the current function kernels inside native Calc. The
[scalar function benchmark page](../benchmarks/scalar-functions.md) reports Flink and native
complete-job results for the same workloads, including both row/Arrow transposes. Those
measurements do not isolate kernel cost, and native admission does not guarantee a speedup
for each standalone query.

The optional Criterion `scalar_functions` benchmark invokes production UDFs on 4,096-row Arrow
arrays, including scalar adaptation, NULL handling, and result allocation. Input creation,
planner coercion, JNI, and transposes are excluded from that diagnostic:

```sh
cargo bench --manifest-path native/Cargo.toml -p streamfusion --features mimalloc --bench scalar_functions
```

Run one benchmark at a time. Kernel diagnostics and complete SQL jobs measure different work.

## FROM_UNIXTIME

Literal numeric patterns in fixed-offset zones compile to Rust formatting tokens once per
expression. This removes the scalar JVM callback and its Arrow export/import from the
formatting path. Integer calendar arithmetic preserves Java's historical calendar cutover
and overflow range; unsupported formats and transition zones keep the Flink evaluator.

For the same two-million-row queries with both transposes, two warmups and five measured
runs, switching from the previous JVM bridge to native formatting reduced the default-format
median from 1.696912s to 1.003518s (1.691x), and `yyyyMMddHHmm` from 1.543752s to 0.958994s
(1.610x). The identity control remained 0.655586s versus 0.651136s. These sequential native-only
comparisons are separate from the interleaved Flink/native measurements on the
[scalar benchmark page](../benchmarks/scalar-functions.md).

## LOCATE

The two-argument form reverses operands into DataFusion strpos. The three-argument Rust kernel follows DataFusion's batch ASCII detection, one reusable memmem::Finder for literal needles, and memmem searches for column needles. Constant needles/starts stay scalar, following Comet's contains pattern. Shared positioning code preserves Flink's empty-needle and signed-start rules; no JVM callback is added.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#locate).

## GREATEST

Integer and matching-scale Decimal extrema use primitive Arrow comparison loops, folding constants once and combining validity masks. ASCII-provable string/Boolean extrema reuse DataFusion with Flink's strict NULL mask. This avoids intermediate Boolean selection arrays and expanded scalar arrays.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#greatest).

## LEAST

Reuses the shared extremum kernel with minimum comparison, including primitive Arrow loops, folded constants, and strict NULL masking. Matching decimal scale is preserved on the output.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#least).

## TRANSLATE

ASCII mappings use direct lookup; non-ASCII codepoints use the project's ahash map. Consecutive equal alphabets reuse the mapping. Duplicate source positions retain their first mapping, including deletion mappings.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#translate).

## BTRIM

Default and literal-set trimming both reuse DataFusion's btrim kernel. Keeping one implementation avoids a duplicate space-only scan while preserving the verified literal-set semantics.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#btrim).

## ELT

A scalar index returns the selected array with its buffers/NULL mask intact. Longer dynamic outputs are sized before writing; short strings keep a single writing pass to avoid a selection vector larger than their payload.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#elt).

## OVERLAY

Copies intact UTF-8 prefix/replacement/suffix slices when UTF-16 cuts align with codepoints; ASCII needs no UTF-16 conversion. Cuts inside surrogate pairs retain full UTF-16 reconstruction, recombined pairs, and Java's encoding of lone surrogates.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#overlay).

## ENCODE

The verified UTF-8 form reinterprets the same Arrow offsets, values, and validity as Binary, so the kernel copies no payload. The charset stays scalar through invocation, avoiding a full-length constant array; single-byte charsets reuse a scratch buffer.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#encode).

## DECODE

A successful safe Arrow Utf8 construction validates the BinaryArray once and reuses its buffers. Malformed byte sequences use the existing JDK-compatible replacement decoder and one reusable output buffer. This avoids per-row output allocation on valid UTF-8 without assuming that arbitrary bytes are valid.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#decode).

## JSON_UNQUOTE

Combines first-token JSON validation and unescaping in one scan, writes UTF-8 directly, and retains only a pending high surrogate instead of a whole intermediate UTF-16 vector. Flink's treatment of trailing text remains covered by SQL differential tests.

This kernel serves direct projections. JSON_UNQUOTE and its scalar consumers fuse into a Flink
expression when intermediate UTF-16 identity is observable; strings passed to another operator
fall back under the [Calc/filter restrictions](../operators/calc-filter.md#json_value).

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#json_unquote).

## SPLIT

Single-byte separators use Rust's character searcher (memchr) instead of the general substring searcher. One Arrow List builder accumulates each batch; complete-job results also include materializing the array output back into Flink rows.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#split).

## SUBSTRING

ASCII uses byte offsets. Negative positions in Unicode strings traverse backward only to the requested start, replacing a full character count followed by a second forward scan. The selected UTF-8 span is borrowed and copied into the Arrow builder.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#substring).

## LPAD

Following Comet's padding structure, the kernel writes intact UTF-8 prefixes and repeated pattern spans directly into the Arrow builder. A small prefix object tracks UTF-16 units and a cut surrogate, avoiding three whole-row UTF-16 buffers. Unpaired high surrogates still become the JDK '?' replacement. The ASCII path computes prefix positions from byte offsets.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#lpad).

## RPAD

Reuses the same prefix and padding-span machinery on the right, including Flink's UTF-16 cut behavior. It avoids whole-row transcoding while retaining exact results for cuts through supplementary characters.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#rpad).

## QUARTER

Fuses timestamp unit conversion, Flink's truncating day calculation, and integer Julian-calendar extraction into one primitive Arrow mapping. Besides eliminating intermediate arrays, this preserves Flink's expanded-year integer arithmetic outside chrono's range.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#quarter).

## Integer/string casts

CAST and TRY_CAST between character strings and signed integers now build Arrow arrays directly.
This removes the previous host cast's per-batch JNI callback and Arrow export/import. Parsing
borrows each string, validates Flink's decimal-text grammar and appends the requested integer
width without a temporary array. Formatting widens primitive inputs to BIGINT, then reuses one
small decimal scratch string per batch before writing the final Arrow string buffer. Error mode
and output length are part of the immutable expression, following Comet's cast dispatch pattern.
The [coverage page](../operators/calc-filter.md#integerstring-casts) records Flink-specific semantics.

On September 16, 2026, Apple M1 Max, JDK 17, Flink 2.2.1, release Rust with mimalloc and one
codegen unit: 2,000,000 rows, parallelism 1, two warmups and five interleaved trials per engine,
separate JVMs before/after. The before code is `baf2aa63` plus the same benchmark cases. All plans
assert NativeCalc and both row/Arrow transposes; input remains a row source and output a row
blackhole sink. Median complete-job seconds:

| Expression | Flink before | Flink after | Native host cast before | Native kernel after | Native time reduction |
|---|---:|---:|---:|---:|---:|
| `CAST(s AS INT)` | 0.581 | 0.586 | 1.014 | 0.835 | 17.7% |
| `CAST(n AS STRING)` | 0.740 | 0.751 | 1.222 | 1.142 | 6.6% |

String-source native identity controls were 1.035s before and 1.049s after; integer-source controls
were 0.789s and 0.819s. Both cast jobs improve relative to the previous native implementation,
but remain slower than stock Flink (0.702x and 0.657x after). These figures do not isolate kernel
cost or establish an overall workload speedup. The string input cycles through signed extrema,
zero, ordinary digits and a space-padded signed decimal; its fixed values ignore `scalar.bytes`.

```sh
SF_BENCHMARK=true mvn -B -ntp -pl streamfusion-runtime -am test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' \
  -Dscalar.functions=STRING_TO_INT,INT_TO_STRING \
  -Dscalar.output=target/integer-string-casts.csv
```
