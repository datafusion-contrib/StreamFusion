# Host-exact builtins over the same upcall, with a faster pure-Rust opt-in

**Applies to:** REGEXP_EXTRACT, UPPER/LOWER, temporal parsing/formatting/arithmetic/casts

Builtins whose Rust implementation can diverge from the JVM's — REGEXP_EXTRACT (regex dialects),
UPPER/LOWER (locale case folding), DATE_FORMAT/EXTRACT over `TIMESTAMP_LTZ` (time-zone database
edges) — default to calling Flink's own implementation through the same batch upcall used for user
UDFs: byte parity, island preserved.

The pure-Rust path stays available behind `allowIncompatible` (see
[Configuration](../configuration.md)) for callers who want the faster path and can accept the
divergence risk.

q21 measures the honest price of the guarantee: 0.76x via the upcall vs 1.57x pure-native. For the
zone-aware datetime functions the two paths measure within noise of each other — the call itself
isn't the bottleneck there.

Temporal expressions also use Flink's generated expression code through this bridge. Adjacent
temporal calls fuse into a single evaluator: for `DATE_FORMAT(TO_TIMESTAMP(text), pattern)`,
only the input strings and final formatted result cross the bridge. The intermediate timestamp
stays in Flink's internal representation, preserving its full range without another upcall.
This expands coverage; the original generic-callback measurements did not demonstrate an
end-to-end speedup for isolated temporal projections. The exact canonical TRY_CAST subset below
is measured separately.
See [the temporal contract](../operators/temporal-functions.md) and
[release measurements](../benchmarks/scalar-functions.md#temporal-coverage-diagnostic-2026-09-15).

## Canonical timestamp TRY_CAST

Profiles of STRING-to-TIMESTAMP TRY_CAST identify Flink's general `DateTimeFormatter` field maps
as a major cost after caching Arrow timestamp writers. Standalone temporal TRY_CAST recognizes
verified ASCII forms before falling back to the released parser: DATE with four-digit years
and one- or two-digit month/day fields, and TIMESTAMP with `yyyy-MM-dd HH:mm:ss[.fraction]`,
years 1–9999 and 1–9 fractional digits. Timestamp parsing preserves Flink's SMART clamping of
days 29–31 and next-day normalization of zero-fraction `24:00:00`. Components are checked before
constructing Java time objects, avoiding an exception followed by a second parse for these forms.
TIMESTAMP_LTZ retains the session `TimeZone` and `atZone` conversion, including DST transitions.

This is a fast path inside the existing batch callback, so JNI and both perimeter transposes
remain. In modern cast mode, composed character children retain generated evaluation exactly
once, outside the parser's failure handler; legacy mode only speculates on direct timestamp
inputs. Unrecognized text uses the released parser. Complete-row evaluators retain original generated code. TIME recognizes `HH:mm:ss` with
optional 1–3 fractional digits, then applies the released version's logical-type precision rule.
SQL parity exposed why calling only the low-level parser was insufficient: Flink 2.2 truncates
to the converted logical type's precision, while 1.18 preserves parsed milliseconds.

Differential tests compare all precisions 0–9 in four zones over deterministic random dates,
the timestamp range limits, pre-epoch fractions, and DST transitions. SQL tests additionally
verify the fallback's NULL/error behavior, legacy configuration, child failures, and untaken
branches on both released Flink lines.

On Linux x86-64 (Core i7-12650H), JDK 17, released Flink 2.2.1 and release+mimalloc,
2M rows at parallelism one, a 2 GiB heap, the system session zone (`America/New_York`), two warmups and five alternating trials yield:

| Target | Generic callback + cached writer median (range), s | Canonical parser median (range), s | Stock Flink median (range), s | Flink/native |
| --- | ---: | ---: | ---: | ---: |
| TIMESTAMP(9) | 3.776 (3.764–3.813) | 0.855 (0.841–0.875) | 3.444 (3.402–3.458) | 4.03x |
| TIMESTAMP_LTZ(9) | 3.791 (3.779–3.865) | 0.861 (0.855–0.881) | 3.474 (3.466–3.499) | 4.03x |

The source-matched identity control is 0.680 s native / 0.411 s Flink. The benchmark alternates
`2000-02-29 12:34:56` and `1969-12-31 23:59:59`, without NULLs; both row/Arrow transposes,
JNI, the runtime row source and row sink remain included and the plan requires native Calc.
The parser reduces native time by about 77% versus the cached-writer implementation and 80%
versus the original callback (4.329/4.381 s). These gains apply to the measured canonical subset;
noncanonical parsing and Boolean TRY_CAST remain separate performance work.

```sh
SF_BENCHMARK=true mvn test -Pbench -pl streamfusion-runtime -am \
  -Dtest=ScalarFunctionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dscalar.functions=TRY_STRING_TO_TIMESTAMP,TRY_STRING_TO_TIMESTAMP_LTZ \
  -Dscalar.rows=2000000 -Dscalar.warmup=2 -Dscalar.runs=5 -Dsf.extraJvmArgs=-Xmx2g
```

Repeating the same workload with `-Dscalar.nullEvery=7` gives TIMESTAMP 0.817 s native
(range 0.800–0.861) versus 3.002 s Flink (2.965–3.027), and TIMESTAMP_LTZ 0.830 s native
(0.817–0.848) versus 3.053 s Flink (3.049–3.080): 3.68x for both. The nullable identity
control is 0.633 s native / 0.391 s Flink. Both runs preserve the original input generator.

### Composed expressions and SMART normalization

A follow-up profile attributed 18.5% of DATE samples to general parsing. Expanding the
benchmark also exposed a timestamp regression: SMART-normalized dates first threw in the
speculative parser, then ran Flink's general parser. Recognizing those forms directly removes
both costs. The following September 27, 2026 run uses the same machine, released versions,
2M rows, 2 GiB heap, two warmups and five alternating trials described above. Default batch
size is 1,024, with no injected NULLs. Median complete-job seconds:

| Query | Previous native | Updated native (range) | Stock Flink (range) |
| --- | ---: | ---: | ---: |
| Direct DATE | 0.756 | 0.610 (0.607–0.621) | 0.608 (0.596–0.619) |
| DATE of TRIM | 0.925 | 0.735 (0.725–0.747) | 0.768 (0.765–0.772) |
| TIMESTAMP of TRIM | 3.796 | 0.862 (0.852–0.883) | 3.674 (3.611–3.699) |
| Unpadded DATE | 0.748 | 0.615 (0.610–0.620) | 0.582 (0.581–0.603) |
| SMART-normalized TIMESTAMP | 7.298 | 0.724 (0.715–0.727) | 3.575 (3.544–3.624) |

Composed and SMART-normalized timestamps take 77.3% and 90.1% less native time than before,
and outperform Flink by 4.26x and 4.94x. Direct DATE is tied and unpadded DATE remains slower;
these results do not establish an across-the-board temporal speedup. The source-matched
identity medians span 0.465–0.477 native and 0.380–0.405 Flink. Boolean also remains slower:
a separate 20M-row length-dispatch experiment measured 3.497s native versus 3.402s Flink;
control drift did not establish an improvement, so that kernel change was withdrawn.
The PR remains draft pending the remaining performance blockers.

Fixtures alternate `2000-2-29` / `1969-1-2` for unpadded dates and
`2024-02-30 00:00:00` / `2024-02-29 24:00:00` for SMART timestamps. Run
`ScalarFunctionBenchmark#individualFunctions` with
`-Dscalar.functions=TRY_STRING_TO_DATE,TRY_DATE_COMPOSED,TRY_TIMESTAMP_COMPOSED,TRY_DATE_NONCANONICAL,TRY_TIMESTAMP_NONCANONICAL`
and the same row/warmup/run settings above. Both transposes, JNI and the row sink remain included.

After applying each released Flink line's TIME precision rule, the same isolated 2M-row method
measures the following. Earlier native measurements used generated TIME conversion:

| TIME query | Previous native | Updated native (range) | Stock Flink (range) |
| --- | ---: | ---: | ---: |
| Direct TIME | 0.788 | 0.609 (0.604–0.629) | 0.630 (0.622–0.644) |
| TIME of TRIM | 0.951 | 0.750 (0.734–0.754) | 0.780 (0.773–0.789) |
| One-/two-digit fractions | 0.790 | 0.621 (0.613–0.627) | 0.637 (0.632–0.646) |

Native time decreases 21–23%, with only a small lead over Flink. Identity controls are
0.472/0.477s native and 0.391/0.385s Flink. Reproduce using
`-Dscalar.functions=TRY_STRING_TO_TIME,TRY_TIME_COMPOSED,TRY_TIME_NONCANONICAL`.
These results preserve the SQL output, including Flink 2.2's truncation of fractions; they do
not compare only the raw parser. The 91 parser/SQL checks pass on each released Flink line,
including composed children, stateful child evaluation, malformed text, legacy mode and zones.
