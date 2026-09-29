# Direct native integer ARRAY_MIN and ARRAY_MAX admission

**Status:** rejected default admission, 2026-09-28. This does not close the broader
collection issue [#234](https://github.com/datafusion-contrib/StreamFusion/issues/234).

A prototype registered DataFusion 54's released `min_max::array_min_udf()` and
`array_max_udf()` under scalar opcodes 166/167 and admitted a single ARRAY operand
with TINYINT, SMALLINT, INT or BIGINT elements. No custom kernel, JNI boundary or
state format was introduced. DataFusion already scans primitive short arrays
directly and uses Arrow reduction kernels for lengths of at least 32.

Flink 2.2.1 parity passed 74 collection cases, including all four integer widths,
NULL/empty/all-NULL arrays, extrema, nested expressions, filters and nullable
results for non-null element schemas. Flink 1.18.1 passed 65 with nine skips:
seven ARRAY_MIN cases because that function is absent from its released catalog,
and two existing small-integer lookup skips. ARRAY_MAX is available on both lines.
The prototype's native suite passed 582 tests with one ignored, including sliced
lists, hidden children of NULL parents, empty batches and scalar adaptation.
These counts describe the prototype, not retained native extrema coverage.

## Whole-job measurements

Baseline `2201e98f` (the integer ARRAY_DISTINCT change), Intel Core i7-12650H,
Linux, JDK 17, Flink 2.2.1, release Rust with mimalloc, parallelism one, 2 GiB heap,
1,024-row batches. Each scenario used a fresh JVM, five warmups and five
alternating trials per engine. No other local build/test ran during timing.
Queries project `ARRAY_MIN(arr)` or `ARRAY_MAX(arr)` and a TRUE anchor from a
rowwise DataStream source to a rowwise blackhole sink. Native plans assert both
transposes and NativeCalc, and completed jobs check substitutions.

Every eighth array is NULL; every seventh element is NULL. Other values are
`(31 * elementIndex + rowIndex) % width - width / 2`. Domain equals width.
Elapsed seconds include planning, startup, conversion, JNI, reduction and sink.

| Function | Type | Rows | Width | Flink median (range), s | Native median (range), s |
| --- | --- | ---: | ---: | --- | --- |
| MIN | BIGINT | 2M | 8 | 0.480 (0.469–0.525) | 0.502 (0.496–0.507) |
| MIN | BIGINT | 20M | 8 | 3.967 (3.932–4.072) | 4.549 (4.537–4.572) |
| MIN | BIGINT | 200k | 64 | 0.218 (0.208–0.240) | 0.217 (0.215–0.222) |
| MIN | BIGINT | 200k | 256 | 0.525 (0.514–0.540) | 0.497 (0.484–0.521) |
| MIN | INT | 200k | 64 | 0.216 (0.204–0.234) | 0.210 (0.204–0.221) |
| MAX | BIGINT | 2M | 8 | 0.465 (0.451–0.496) | 0.496 (0.487–0.501) |
| MAX | BIGINT | 200k | 64 | 0.212 (0.208–0.240) | 0.216 (0.209–0.219) |
| MAX | BIGINT | 200k | 256 | 0.532 (0.517–0.565) | 0.498 (0.492–0.499) |
| MAX | INT | 200k | 64 | 0.210 (0.206–0.239) | 0.204 (0.199–0.212) |

The wide-array results are modestly favorable, but the long short-array MIN run
is 14.7% slower natively. This cannot be dismissed as fixed startup overhead.
A type-only admission gate cannot distinguish these runtime array lengths.
Raw candidate and previous-admission trials are retained in
`docs/benchmarks/array-extrema-2026-09-28.csv`; profile timings are excluded.

Previous-admission controls removed only the new Java admission while retaining
the same native runtime. The harness verified zero substitutions and the exact
unsupported-function fallback. These are admission ablations, not rebuilt old
releases. Five warmups and five trials per engine yielded:

| Function / case | Stock median (range), s | Previous fallback median (range), s |
| --- | --- | --- |
| MIN / 2M x 8 | 0.412 (0.407–0.442) | 0.412 (0.400–0.500) |
| MIN / 200k x 64 | 0.211 (0.202–0.215) | 0.207 (0.202–0.213) |
| MAX / 2M x 8 | 0.427 (0.425–0.437) | 0.430 (0.413–0.493) |
| MAX / 200k x 64 | 0.208 (0.202–0.209) | 0.208 (0.197–0.214) |

Stock controls moved between JVMs, so cross-run ratios should not be attributed
entirely to native execution. The candidate's paired regressions and sustained
MIN regression support retaining fallback independently of that movement.

## Profile and decision

A separate two-million-row MIN run under async-profiler's 1ms CPU timer, with
DWARF native stacks, reproduced the regression (0.515s native, 0.468s stock).
Of 7,275 mixed Java/native samples, 418 stacks contained NativeCalc processing,
168 contained Arrow field writing (166 array writing), and 60 contained the
primitive array reduction. NativeCalc leaf samples also included Flink row copying
and columnar value/null access. Counts overlap and include both engines and
startup; they do not establish isolated kernel CPU percentages.

The released kernel already has a primitive short-array path. This evidence does
not justify enabling a slower default or replacing it with an unmeasured bespoke
kernel. Keep stock Flink admission and revisit after demonstrating a whole-job
improvement, potentially through the separately tracked boundary investigation
[#248](https://github.com/datafusion-contrib/StreamFusion/issues/248). No boundary
architecture is changed by this investigation.

To reproduce the prototype, restore the two DataFusion registrations and the
integer-only Java admissions described above, then vary function, rows and width:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=DynamicCollectionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.extraJvmArgs=-Xmx2g '-Dcollection.expression=ARRAY_MIN(arr)' \
  -Dcollection.map=false -Dcollection.rows=2000000 -Dcollection.width=8 \
  -Dcollection.domain=8 -Dcollection.warmup=5
```

On production admission this intentionally fails the native-plan assertion.
For the previous behavior, leave admission unchanged and add
`-Dcollection.native=false` and
`'-Dcollection.fallbackReason=unsupported function/operator: ARRAY_MIN'`.
