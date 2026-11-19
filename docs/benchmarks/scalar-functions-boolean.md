# Recorded scalar workloads: boolean

[Topic index and original links](scalar-functions.md). Sections retain their recorded scope, dates and evidence.

## BOOLEAN CAST to character {#boolean-cast-to-character}

Direct BOOLEAN inputs/literals use a native formatter for STRING and bounded VARCHAR/CHAR.
Modern mode emits uppercase text, truncates overlength values and pads bounded CHAR;
legacy mode emits lowercase text without target-length enforcement. NULL remains NULL.
Unverified computed operands retain generated Flink evaluation; independent TRY_CAST and logical-operand qualification follow below.
No production switch is added. Both modes use Flink's existing cast configuration.

The [result record](results/boolean-string-cast-2026-11-01.json) and
[raw trials](results/boolean-string-cast-2026-11-01.csv.gz) retain 152 configurations and
15,200 main trials. Modern bulk cells cover STRING, VARCHAR(1/3/4/5/8) and
CHAR(1/3/4/5/8/32); legacy cells cover STRING, VARCHAR(3) and CHAR(8), including their
ignored target lengths. Both released lines use matching nullable/NOT NULL declarations,
alternating true/false values and actual NULLs every seven rows where selected, with
GenericRowData and BinaryRowData controls. Tiny STRING/CHAR(8) cells retain both modes.

Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro, one thread, 16 warmups/nine
measurements and 131,072 rows/trial compare released generated Java on prebuilt RowData
with production Calc on matching Arrow buffers. Both consume UTF-8 length and boundary
bytes; Java uses BinaryStringData access without copying to a byte array. The Rust timer
covers the native operation and equivalent consumption, excluding initial JNI/import/export
and both perimeter transposes. Intrinsic previous JVM computation stays inside timing.
The actual independently compiled preceding encoder must bind a JVM evaluator; the
candidate requires the intended function and empty descriptors. Every checksum agrees.

| Flink line | Mode | Batch rows | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1.18.1 | modern | 1024 | 48 | 1.626-11.810x | 9.879-19.183x | 0 |
| 1.18.1 | modern | 16 | 8 | 0.190-1.836x | 22.304-24.939x | 4 |
| 1.18.1 | legacy | 1024 | 12 | 6.949-17.607x | 12.056-20.007x | 0 |
| 1.18.1 | legacy | 16 | 8 | 0.825-1.385x | 20.465-24.113x | 4 |
| 2.2.1 | modern | 1024 | 48 | 1.551-10.159x | 10.185-19.556x | 0 |
| 2.2.1 | modern | 16 | 8 | 0.198-1.764x | 21.865-23.653x | 4 |
| 2.2.1 | legacy | 1024 | 12 | 8.237-11.477x | 12.273-17.424x | 0 |
| 2.2.1 | legacy | 16 | 8 | 0.838-1.031x | 22.531-25.358x | 6 |

Ratios above one favor native. All 120 bulk cells pass the prepared-operation gate;
18 of 32 tiny cells remain slower than stock. Paired stock-control drift ranges from
0.597 to 1.844x. Keep every range and unfavorable case rather than treating
cross-process ratios as causal isolation.

The initial row writer loses the modern cached-STRING probe. The
[bitmap block technique](../optimizations/boolean-word-blocks.md) reduces the matched
native median from 6.162 to 2.446 ns/row; paired stock/native moves from 0.657x to 1.601x.
The [initial](results/boolean-string-cast-probe-2026-11-01.csv.gz) and
[optimized](results/boolean-string-cast-blocks-probe-2026-11-01.csv.gz) diagnostics retain
eight cells each, their stock drift and the untimed allocation/buffer observations.
They remain separate from the main matrix. The output validity is shared; masked NULL
positions can carry payload, so fewer requested bytes do not imply smaller output buffers.

Separate million-row nullable BOOLEAN source/blackhole jobs retain production JNI,
planning/startup and both transposes, parallelism one, two warmups and five measurements.
They require consumed NativeCalc work, no fallback and empty candidate descriptors.

| Flink line / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / modern-string | 0.158820 (0.157408-0.167913) | 0.159093 (0.154768-0.173915) | 0.157794 (0.144974-0.175266) | 0.191205 (0.188041-0.197415) |
| 1.18.1 / modern-varchar3 | 0.173374 (0.170471-0.180415) | 0.158684 (0.156780-0.167511) | 0.173464 (0.172478-0.178372) | 0.219246 (0.216342-0.222373) |
| 1.18.1 / modern-char8 | 0.198373 (0.190325-0.201441) | 0.166334 (0.157919-0.168544) | 0.190850 (0.184823-0.195628) | 0.230782 (0.227496-0.245033) |
| 1.18.1 / legacy-string | 0.183357 (0.172087-0.197666) | 0.169239 (0.158804-0.179719) | 0.186869 (0.176384-0.253488) | 0.190473 (0.182322-0.195269) |
| 1.18.1 / legacy-char8 | 0.175425 (0.173670-0.219901) | 0.167376 (0.157756-0.171659) | 0.178774 (0.171553-0.242362) | 0.200614 (0.194548-0.215857) |
| 2.2.1 / modern-string | 0.161098 (0.151663-0.162549) | 0.162494 (0.156651-0.167074) | 0.155657 (0.150932-0.157231) | 0.193431 (0.183526-0.199987) |
| 2.2.1 / modern-varchar3 | 0.181900 (0.180263-0.197655) | 0.161911 (0.158735-0.177116) | 0.181335 (0.174057-0.184622) | 0.220254 (0.214121-0.229879) |
| 2.2.1 / modern-char8 | 0.194581 (0.188651-0.204043) | 0.162995 (0.158961-0.171878) | 0.193474 (0.183651-0.200218) | 0.227816 (0.222634-0.248081) |
| 2.2.1 / legacy-string | 0.170059 (0.169012-0.170231) | 0.161957 (0.159721-0.165669) | 0.178505 (0.172566-0.190951) | 0.189525 (0.187156-0.201872) |
| 2.2.1 / legacy-char8 | 0.179689 (0.173855-0.195317) | 0.160113 (0.159534-0.168144) | 0.171615 (0.170076-0.182689) | 0.182692 (0.178134-0.195459) |

Across these jobs, native/stock elapsed ratios range from 0.838
to 1.009, and native/previous ratios range from
0.715 to 0.889. Ratios above one are slower.
These short full jobs retain startup and overlapping ranges; prepared bulk qualification
does not establish universal complete-job acceleration.

The initial 172 focused JVM invocations pass, including existing numeric and decimal-cast
regressions. After the bitmap optimization, all 40 focused invocations pass on released
Flink 1.18.1/2.2.1 with JDK 11/17, including added string consumers. Runtime-source
witnesses compare complete 5,003-row outputs, resolved types, NULLs, cardinality and unique
IDs across batches, and require consumed native work, no fallback and empty descriptors.
The unchanged released CastRulesTest supplies 535 passing stock-oracle cases on each
Flink/JDK pair; the separate runtime witnesses establish native execution. Four additional
shared-state invocations pass. Twelve existing integer/predicate fixture smokes retain
their types and checksums. Java compiles for release 11; release/default/mimalloc Rust core
passes 1,019 tests with two pre-existing ignores, including every bitmap word and unaligned
tail. Strict docs, formatting and source/privacy checks pass.

Reproduce the prepared path with `integral_expression`, BOOLEAN input and
`SF_INTEGRAL_EXPRESSION='CAST(v AS STRING)'`; set `SF_INTEGRAL_LEGACY=true` for the legacy
control. Complete jobs use `IntegralExpressionPipelineBenchmark`, `SF_BENCHMARK=true`,
`sf.integral.booleanInput=true`, `sf.integral.expression=CAST(v AS CHAR(8))` and
`sf.integral.legacy=true` where selected. These are benchmark controls, not production
options. Remaining cast coverage stays in [#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).


## BOOLEAN TRY_CAST to character {#boolean-try_cast-to-character}

Direct BOOLEAN inputs/literals now use the same native STRING/VARCHAR/CHAR formatter for
TRY_CAST as for CAST. Formatting cannot produce a conversion error. Modern and legacy
spelling, truncation/padding and NULL behavior match the released generated code.
Unverified computed operands keep the complete generated expression; TRY_CAST must not swallow
failures while evaluating an operand or drop shared stateful calls. An unconfigured
encoder also keeps generated evaluation. No production option is added.

The [independent result record](results/boolean-try-string-cast-2026-11-01.json) and
[raw trials](results/boolean-try-string-cast-2026-11-01.csv.gz) retain 152 configurations
and 15,200 trials for TRY_CAST itself. The baseline is the actual preceding CAST encoder,
which still binds a JVM evaluator for TRY_CAST; the candidate requires empty descriptors
and the intended native function. Both configure the same released cast mode. The Rust
kernel, release DSO and benchmark executable are unchanged from CAST qualification;
source and DSO hashes are checked rather than rerunning the identical Rust suite.

The prepared boundary and matching BOOLEAN/NULL/cached-or-binary RowData inputs are the
same as CAST above: release/default/mimalloc, JDK 17.0.12, Apple M4 Pro, one thread,
16 warmups/nine measurements and 131,072 rows/trial. Rust times the production operation
and equivalent UTF-8 boundary consumption. Initial JNI/import/export and both perimeter
transposes are outside this timer; intrinsic previous JVM calls remain timed. All
checksums agree. Tiny controls and independent stock drift remain separate from bulk
qualification.

| Flink line | Mode | Batch rows | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1.18.1 | modern | 1024 | 48 | 1.646-8.972x | 8.592-18.641x | 0 |
| 1.18.1 | modern | 16 | 8 | 0.194-2.023x | 22.171-25.598x | 4 |
| 1.18.1 | legacy | 1024 | 12 | 7.273-11.839x | 16.625-24.167x | 0 |
| 1.18.1 | legacy | 16 | 8 | 0.807-1.134x | 23.742-26.719x | 4 |
| 2.2.1 | modern | 1024 | 48 | 1.604-13.670x | 8.472-18.768x | 0 |
| 2.2.1 | modern | 16 | 8 | 0.198-1.783x | 22.358-26.250x | 4 |
| 2.2.1 | legacy | 1024 | 12 | 8.882-12.173x | 15.719-21.836x | 0 |
| 2.2.1 | legacy | 16 | 8 | 0.846-1.052x | 23.919-27.109x | 6 |

All 120 bulk cells pass the prepared-operation gate;
18 of 32 tiny cells are slower
than stock. Ratios above one favor native. Paired stock-control drift spans
0.674-1.940x; cross-process ratios are not causal isolation.

Ten million-row nullable BOOLEAN source/blackhole configurations retain production JNI,
planning/startup and both transposes, parallelism one, two warmups/five measurements.
They require consumed NativeCalc work, no fallback and empty candidate descriptors.

| Flink line / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / modern-string | 0.164568 (0.152968-0.178889) | 0.155482 (0.154097-0.169737) | 0.163534 (0.150881-0.166770) | 0.178076 (0.175590-0.190067) |
| 1.18.1 / modern-varchar3 | 0.172382 (0.168989-0.187415) | 0.163684 (0.158259-0.172744) | 0.182536 (0.172397-0.188274) | 0.205375 (0.204545-0.218700) |
| 1.18.1 / modern-char8 | 0.199662 (0.192069-0.280178) | 0.159020 (0.156592-0.169004) | 0.193263 (0.188125-0.200050) | 0.224147 (0.213284-0.226749) |
| 1.18.1 / legacy-string | 0.181813 (0.172095-0.202011) | 0.161006 (0.159682-0.172754) | 0.173408 (0.164714-0.192122) | 0.208225 (0.194695-0.216245) |
| 1.18.1 / legacy-char8 | 0.179340 (0.176745-0.217252) | 0.166424 (0.164905-0.172886) | 0.183752 (0.171472-0.191367) | 0.205981 (0.200621-0.224427) |
| 2.2.1 / modern-string | 0.167586 (0.155133-0.190176) | 0.164438 (0.157190-0.166998) | 0.155230 (0.150007-0.166039) | 0.181543 (0.179630-0.195766) |
| 2.2.1 / modern-varchar3 | 0.176160 (0.168682-0.181995) | 0.163558 (0.160969-0.169159) | 0.174333 (0.167268-0.188351) | 0.207104 (0.199274-0.218739) |
| 2.2.1 / modern-char8 | 0.195855 (0.191255-0.209562) | 0.158579 (0.157120-0.160678) | 0.192746 (0.189712-0.197713) | 0.216276 (0.211593-0.218344) |
| 2.2.1 / legacy-string | 0.171966 (0.168650-0.194207) | 0.160860 (0.154903-0.172927) | 0.169731 (0.160435-0.179396) | 0.206011 (0.197393-0.216290) |
| 2.2.1 / legacy-char8 | 0.175236 (0.169376-0.186207) | 0.159021 (0.156173-0.166410) | 0.174426 (0.166370-0.183576) | 0.193287 (0.187956-0.213551) |

Native/stock elapsed ratios range from 0.796 to
0.981; native/previous ratios range from
0.709 to 0.906. Ratios above one are slower.
Keep overlapping ranges and unfavorable outcomes; prepared qualification does not prove
universal full-job acceleration.

All 68 focused JVM invocations pass across Flink 1.18.1/2.2.1 and JDK 11/17, including
CAST regression controls. Complete 5,003-row outputs, resolved types, cardinality, unique
IDs and NULLs match stock across batches; direct runtime witnesses require consumed native
work with no fallback or evaluator descriptor. Selected operand division errors, lazy
branches, empty filtered batches, string consumers and unconfigured encoder behavior are
covered. Four additional shared-state invocations pass. The unchanged released CastRules
stock oracle (535 cases per pair) and 1,019 Rust core passes/two pre-existing ignores are
reused from the identical kernel qualification; they are separate from the new native
TRY_CAST witnesses. Java compiles for release 11. Strict docs, diff and privacy checks pass.

Reproduce with `SF_INTEGRAL_EXPRESSION='TRY_CAST(v AS STRING)'` and BOOLEAN input for
`integral_expression`; complete jobs use `sf.integral.booleanInput=true` and
`sf.integral.expression=TRY_CAST(v AS CHAR(8))`. The existing benchmark-only legacy control
selects the corresponding mode. Remaining casts stay in
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).


## BOOLEAN CAST over logical operands {#boolean-cast-over-logical-operands}

The formatter accepts recursively verified BOOLEAN-only NOT, AND, OR, IS NULL,
IS NOT NULL, IS TRUE, IS NOT TRUE, IS FALSE and IS NOT FALSE over BOOLEAN references
and literals. These operations cannot fail or mutate shared state. At this logical-only qualification, numeric comparisons, division, UDFs and CASE kept their generated
or short-circuit fallback routes. The proof never discards a computed operand.

The [result record](results/boolean-logical-cast-2026-11-01.json) and
[raw trials](results/boolean-logical-cast-2026-11-01.csv.gz) retain 76 configurations
and 7,600 trials. Representative producers are NOT(v) and nullable
v IS TRUE. The initial Flink 2.2 NOT NULL v IS TRUE control simplifies to v before encoding and
is already native in the actual preceding encoder. NOT NULL truth-test cells are
excluded from new-feature performance coverage on both lines.
The initial strict previous-route assertion exposed that simplification rather than
allowing it to count as callback removal. Its independent candidate-only
[diagnostic](results/boolean-logical-cast-simplified-control-2026-11-01.csv.gz) retains
all 50 samples separately. Correctness witnesses still cover both declarations.

Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro, one thread, 16 warmups/nine
measurements and 131,072 rows/trial use the prepared Rust/Arrow versus released
Java/RowData boundary described above. Modern targets are STRING/VARCHAR(3)/CHAR(8);
legacy targets are STRING/CHAR(8). Cached and binary RowData controls match logical
values and NULLs. The actual preceding encoder must bind a JVM evaluator; candidate
encoding must have none. Equivalent UTF-8 boundary consumption and every checksum agree.
The identical production Rust kernel/DSO is reused, with source and DSO hashes checked.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | logical bulk | 30 | 1.456-11.667x | 11.497-20.919x | 0 |
| 1.18.1 | tiny NOT control | 8 | 0.175-0.918x | 19.655-21.435x | 8 |
| 2.2.1 | logical bulk | 30 | 1.460-10.751x | 12.686-17.764x | 0 |
| 2.2.1 | tiny NOT control | 8 | 0.180-0.896x | 19.915-22.033x | 8 |

All 60 bulk cells pass the prepared-operation gate;
16 of 16 tiny cells are slower
than stock. Ratios above one favor native. Independent stock-control drift spans
0.573-1.499x. These measurements qualify the representative logical
producers; they do not assert a speedup for every expression tree.

Six million-row source/blackhole configurations retain planning/startup, production JNI
and both transposes. Modern NOT-to-STRING, modern truth-test-to-CHAR(8) and legacy
NOT-to-STRING run at parallelism one with two warmups/five measurements. Runtime witnesses
require consumed native Calc work, no fallback and empty candidate evaluator descriptors.

| Flink / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / modern-not | 0.155199 (0.152203-0.180638) | 0.157988 (0.153672-0.165963) | 0.156686 (0.155055-0.184734) | 0.187435 (0.182388-0.190953) |
| 1.18.1 / modern-truth | 0.239740 (0.234626-0.249306) | 0.193730 (0.190997-0.201901) | 0.240761 (0.235625-0.248413) | 0.275991 (0.274064-0.291803) |
| 1.18.1 / legacy-not | 0.172859 (0.170668-0.216929) | 0.165043 (0.159584-0.165952) | 0.193202 (0.175767-0.262754) | 0.191391 (0.189054-0.218440) |
| 2.2.1 / modern-not | 0.162671 (0.154038-0.169849) | 0.155008 (0.153929-0.162233) | 0.159649 (0.156157-0.168375) | 0.196254 (0.186756-0.199247) |
| 2.2.1 / modern-truth | 0.251202 (0.245248-0.265937) | 0.206193 (0.203454-0.212622) | 0.245309 (0.244001-0.258694) | 0.269516 (0.266690-0.276784) |
| 2.2.1 / legacy-not | 0.177856 (0.172548-0.199941) | 0.163611 (0.160215-0.168126) | 0.176808 (0.172573-0.188163) | 0.191048 (0.186868-0.204655) |

Native/stock elapsed ratios span 0.808-1.018;
native/previous ratios span 0.702-0.862.
Ratios above one are slower. Retain overlapping ranges and all unfavorable cases;
bulk qualification does not establish universal full-job acceleration.

All 84 focused JVM invocations and four additional shared-state invocations pass on
released Flink 1.18.1/2.2.1 with JDK 11/17. Runtime witnesses compare complete 5,003-row
outputs, resolved types, NULLs, cardinality and unique IDs across batches; independent
BOOLEAN columns cover AND/OR combinations and nesting. They require consumed native work,
no fallback and empty descriptors for every admitted logical query. Failing comparisons
under logical wrappers, lazy/empty branches and stateful producers retain their exact
prior route and successful/exception behavior. The unchanged released
ScalarFunctionsTest.testIsTrueIsFalse and ScalarOperatorsTest.testFunctionWithBooleanExpression
also pass on all four pairs; they are separate from the new native execution witnesses.
Java targets release 11. The unchanged kernel retains its prior 1,019 Rust core passes,
two pre-existing ignores and released CastRules stock oracle. Strict docs, diff, source
and privacy checks pass.

Reproduce prepared qualification with `integral_expression`, BOOLEAN input and
`SF_INTEGRAL_EXPRESSION='CAST(NOT v AS STRING)'`. Complete jobs use
`sf.integral.booleanInput=true` and `sf.integral.expression=CAST(v IS TRUE AS CHAR(8))`.
No production option is introduced. Remaining conversions are tracked in
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).


## BOOLEAN TRY_CAST over logical operands {#boolean-try_cast-over-logical-operands}

The formatter accepts recursively verified BOOLEAN-only NOT, AND, OR, IS NULL,
IS NOT NULL, IS TRUE, IS NOT TRUE, IS FALSE and IS NOT FALSE over BOOLEAN references
and literals. These operations cannot fail or mutate shared state. At this logical-only qualification, numeric comparisons, division, UDFs and CASE kept their generated
or short-circuit fallback routes. The proof never discards a computed operand.

The [result record](results/boolean-logical-try-cast-2026-11-01.json) and
[raw trials](results/boolean-logical-try-cast-2026-11-01.csv.gz) retain 76 configurations
and 7,600 trials. Representative producers are NOT(v) and nullable
v IS TRUE. The initial Flink 2.2 NOT NULL v IS TRUE control simplifies to v before encoding and
is already native in the actual preceding encoder. NOT NULL truth-test cells are
excluded from new-feature performance coverage on both lines.
The preceding CAST strict previous-route assertion exposed that simplification rather than
allowing it to count as callback removal. Its independent candidate-only
[diagnostic](results/boolean-logical-cast-simplified-control-2026-11-01.csv.gz) retains
all 50 samples separately. Correctness witnesses still cover both declarations.

Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro, one thread, 16 warmups/nine
measurements and 131,072 rows/trial use the prepared Rust/Arrow versus released
Java/RowData boundary described above. Modern targets are STRING/VARCHAR(3)/CHAR(8);
legacy targets are STRING/CHAR(8). Cached and binary RowData controls match logical
values and NULLs. The actual preceding encoder must bind a JVM evaluator; candidate
encoding must have none. Equivalent UTF-8 boundary consumption and every checksum agree.
The identical production Rust kernel/DSO is reused, with source and DSO hashes checked.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | logical bulk | 30 | 1.429-18.184x | 8.887-21.064x | 0 |
| 1.18.1 | tiny NOT control | 8 | 0.175-0.939x | 19.482-25.024x | 8 |
| 2.2.1 | logical bulk | 30 | 1.453-12.490x | 9.313-21.843x | 0 |
| 2.2.1 | tiny NOT control | 8 | 0.175-1.104x | 19.154-27.388x | 7 |

All 60 bulk cells pass the prepared-operation gate;
15 of 16 tiny cells are slower
than stock. Ratios above one favor native. Independent stock-control drift spans
0.632-1.992x. These measurements qualify the representative logical
producers; they do not assert a speedup for every expression tree.

Six million-row source/blackhole configurations retain planning/startup, production JNI
and both transposes. Modern NOT-to-STRING, modern truth-test-to-CHAR(8) and legacy
NOT-to-STRING run at parallelism one with two warmups/five measurements. Runtime witnesses
require consumed native Calc work, no fallback and empty candidate evaluator descriptors.

| Flink / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / modern-not | 0.192990 (0.170395-0.289754) | 0.188779 (0.175542-0.276829) | 0.168986 (0.155901-0.175384) | 0.190369 (0.185188-0.193414) |
| 1.18.1 / modern-truth | 0.240194 (0.235374-0.249937) | 0.189541 (0.177280-0.190945) | 0.218437 (0.205282-0.244150) | 0.235512 (0.227074-0.263892) |
| 1.18.1 / legacy-not | 0.211747 (0.189535-0.228928) | 0.179852 (0.169331-0.181138) | 0.185579 (0.179722-0.194138) | 0.224362 (0.216911-0.230450) |
| 2.2.1 / modern-not | 0.164041 (0.156972-0.166769) | 0.162113 (0.158810-0.163989) | 0.178893 (0.172315-0.193451) | 0.206872 (0.204361-0.213012) |
| 2.2.1 / modern-truth | 0.234089 (0.214258-0.237513) | 0.168890 (0.167765-0.186218) | 0.234878 (0.214046-0.243389) | 0.250744 (0.240538-0.263417) |
| 2.2.1 / legacy-not | 0.226982 (0.200094-0.244595) | 0.205476 (0.184333-0.210453) | 0.198807 (0.191128-0.219500) | 0.244790 (0.222760-0.250576) |

Native/stock elapsed ratios span 0.721-0.988;
native/previous ratios span 0.674-0.992.
Ratios above one are slower. Retain overlapping ranges and all unfavorable cases;
bulk qualification does not establish universal full-job acceleration.

All 92 focused JVM invocations and four additional shared-state invocations pass on
released Flink 1.18.1/2.2.1 with JDK 11/17. Runtime witnesses compare complete 5,003-row
outputs, resolved types, NULLs, cardinality and unique IDs across batches; independent
BOOLEAN columns cover AND/OR combinations and nesting. They require consumed native work,
no fallback and empty descriptors for every admitted logical query. Failing comparisons
under logical wrappers, lazy/empty branches and stateful producers retain their exact
prior route and successful/exception behavior. The unchanged released
ScalarFunctionsTest.testIsTrueIsFalse and ScalarOperatorsTest.testFunctionWithBooleanExpression
retain their unchanged preceding CAST qualification on all four pairs; the new TRY_CAST runtime witnesses establish the changed native route.
Java targets release 11. The unchanged kernel retains its prior 1,019 Rust core passes,
two pre-existing ignores and released CastRules stock oracle. Strict docs, diff, source
and privacy checks pass.

Reproduce prepared qualification with `integral_expression`, BOOLEAN input and
`SF_INTEGRAL_EXPRESSION='TRY_CAST(NOT v AS STRING)'`. Complete jobs use
`sf.integral.booleanInput=true` and `sf.integral.expression=TRY_CAST(v IS TRUE AS CHAR(8))`.
No production option is introduced. Remaining conversions are tracked in
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).


## BOOLEAN CAST over integer comparisons {#boolean-cast-over-integer-comparisons}

Integer =, <>, <, <=, > and >= comparisons reuse the native BOOLEAN formatter.
The recursive proof admits verified integer references/literals, wrapping addition,
subtraction and multiplication, lossless widening and the existing integral rounding
forms. BOOLEAN logical/null/truth wrappers remain native; integer IS NULL/IS NOT NULL
also cover the optimizer's expansion of nullable comparisons. The proof excludes
failing and stateful producers. DECIMAL, character, floating and temporal comparisons,
division, remainder and other unverified producers keep their existing routes.
Modern and legacy text/width rules are unchanged. No production switch is introduced.

The [result record](results/boolean-integer-comparison-cast-2026-11-01.json) and
[raw trials](results/boolean-integer-comparison-cast-2026-11-01.csv.gz) retain 64 configurations/6,400 trials.
Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro, one thread,
16 warmups/nine measurements and 131,072 rows/trial use the prepared Rust/Arrow
versus released Java/RowData boundary. INT/BIGINT extrema and values beyond 2^53
match across GenericRowData/BinaryRowData and nullable/NOT NULL declarations.
Representative bulk expressions are modern v>=0 to STRING, modern wrapping v+1<0
to CHAR(8), and legacy v<0 to CHAR(8). Sixteen-row modern comparison-to-STRING
controls remain separate. All checksums agree. The independently recompiled actual
preceding encoder must bind a JVM evaluator; the candidate must have no evaluator.
The unchanged Rust formatter, Calc and comparison kernels retain their exact DSO.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 24 | 1.571-9.760x | 11.270-14.952x | 0 |
| 1.18.1 | tiny control | 8 | 0.123-0.162x | 14.109-15.029x | 8 |
| 2.2.1 | bulk | 24 | 1.581-9.791x | 11.132-15.929x | 0 |
| 2.2.1 | tiny control | 8 | 0.123-0.163x | 14.003-15.089x | 8 |

All 48 bulk cells beat stock; 16 of 16 tiny controls are slower.
Independent stock-control drift spans 0.650-1.393x. All ranges are retained.
For the Flink 2.2 NOT NULL INT/GenericRowData comparison, untimed native allocation
probes report 29 requests for both batch sizes: 10,534 requested bytes at 1,024 rows
and 1,816 at 16 rows. Native medians rise from 2.315 to 33.023 ns/row while stock
remains near 4.7 ns/row. Fixed per-batch work remains a small-batch limitation;
removing the preceding JVM evaluator does not eliminate that cost.

Six complete million-row source/blackhole configurations retain source generation,
planning/startup, production JNI and both transposes. Modern INT comparison-to-STRING,
modern BIGINT arithmetic comparison-to-CHAR(8), and legacy INT comparison-to-CHAR(8)
run at parallelism one with two warmups/five measurements. Each native query requires
consumed NativeCalc, no fallback and empty candidate evaluator descriptors.

| Flink / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / modern-comparison | 0.188399 (0.186934-0.194068) | 0.191475 (0.190601-0.197079) | 0.192767 (0.183027-0.197884) | 0.217869 (0.215422-0.238049) |
| 1.18.1 / modern-arithmetic | 0.235702 (0.231248-0.249417) | 0.199672 (0.196615-0.223385) | 0.242474 (0.237011-0.247843) | 0.273772 (0.270471-0.284878) |
| 1.18.1 / legacy-comparison | 0.208412 (0.200292-0.227803) | 0.188325 (0.185272-0.198546) | 0.212331 (0.208584-0.216080) | 0.223269 (0.220450-0.231086) |
| 2.2.1 / modern-comparison | 0.185857 (0.181895-0.190989) | 0.187063 (0.184722-0.196945) | 0.187830 (0.180328-0.192330) | 0.219429 (0.215679-0.226802) |
| 2.2.1 / modern-arithmetic | 0.225332 (0.219251-0.228684) | 0.190797 (0.190280-0.194885) | 0.229180 (0.218205-0.244467) | 0.258519 (0.251868-0.277756) |
| 2.2.1 / legacy-comparison | 0.205362 (0.204444-0.214410) | 0.192941 (0.189956-0.201685) | 0.204743 (0.194486-0.218907) | 0.212190 (0.207976-0.218663) |

Native/stock elapsed ratios span 0.847-1.016;
native/previous ratios span 0.729-0.909. Ratios above one are slower.
All unfavorable results and overlapping ranges remain. Prepared qualification does
not establish universal end-to-end acceleration.

All 268 focused JVM invocations, four additional shared-state invocations and
12 existing fixture smokes pass on released Flink 1.18.1/2.2.1 with JDK 11/17
(Java release 11). Complete 5,003-row witnesses compare all values, schema, NULLs,
IDs and cardinality across batches for TINYINT/SMALLINT/INT/BIGINT, independent input
columns, nullable/NOT NULL declarations, modern/legacy mode, signed extrema and BIGINT
values beyond 2^53. They require consumed native work, no fallback and no evaluator.
String consumers, grouping, lazy branches and empty filtered batches match stock.
Failing and stateful inputs keep their released behavior and exact previous routes.
Unchanged released boolean-expression/truth tests, 535 CastRules stock-oracle cases
per pair, and the identical DSO's 1,019 core passes/two pre-existing ignores retain
their preceding qualification; new runtime witnesses establish the changed route.
Source/DSO hashes and independently recompiled baseline classes are retained.

Reproduce prepared qualification with `integral_expression`, INT/BIGINT input and
`SF_INTEGRAL_EXPRESSION='CAST(v>=CAST(0 AS $TYPE) AS STRING)'`. Complete jobs use
`sf.integral.expression=CAST(v>=0 AS STRING)`; BIGINT controls additionally use
`sf.integral.bigintInput=true`. Remaining conversions are tracked in
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).


## BOOLEAN TRY_CAST over integer comparisons {#boolean-try_cast-over-integer-comparisons}

Integer =, <>, <, <=, > and >= comparisons reuse the native BOOLEAN formatter.
The recursive proof admits verified integer references/literals, wrapping addition,
subtraction and multiplication, lossless widening and the existing integral rounding
forms. BOOLEAN logical/null/truth wrappers remain native; integer IS NULL/IS NOT NULL
also cover the optimizer's expansion of nullable comparisons. The proof excludes
failing and stateful producers. DECIMAL, character, floating and temporal comparisons,
division, remainder and other unverified producers keep their existing routes.
Modern and legacy text/width rules are unchanged. No production switch is introduced.

The [result record](results/boolean-integer-comparison-try-cast-2026-11-01.json) and
[raw trials](results/boolean-integer-comparison-try-cast-2026-11-01.csv.gz) retain 64 configurations/6,400 trials.
Release/default-feature/mimalloc, JDK 17.0.12, Apple M4 Pro, one thread,
16 warmups/nine measurements and 131,072 rows/trial use the prepared Rust/Arrow
versus released Java/RowData boundary. INT/BIGINT extrema and values beyond 2^53
match across GenericRowData/BinaryRowData and nullable/NOT NULL declarations.
Representative bulk expressions are modern v>=0 to STRING, modern wrapping v+1<0
to CHAR(8), and legacy v<0 to CHAR(8). Sixteen-row modern comparison-to-STRING
controls remain separate. All checksums agree. The independently recompiled actual
preceding encoder must bind a JVM evaluator; the candidate must have no evaluator.
The unchanged Rust formatter, Calc and comparison kernels retain their exact DSO.

| Flink | Workload | Cells | Stock/native median range | Previous/native median range | Slower than stock |
| --- | --- | ---: | ---: | ---: | ---: |
| 1.18.1 | bulk | 24 | 1.502-16.474x | 10.559-18.565x | 0 |
| 1.18.1 | tiny control | 8 | 0.124-0.162x | 14.250-15.230x | 8 |
| 2.2.1 | bulk | 24 | 1.531-10.600x | 10.074-19.601x | 0 |
| 2.2.1 | tiny control | 8 | 0.122-0.163x | 14.110-14.918x | 8 |

All 48 bulk cells beat stock; 16 of 16 tiny controls are slower.
Independent stock-control drift spans 0.756-1.905x. All ranges are retained.

Six complete million-row source/blackhole configurations retain source generation,
planning/startup, production JNI and both transposes. Modern INT comparison-to-STRING,
modern BIGINT arithmetic comparison-to-CHAR(8), and legacy INT comparison-to-CHAR(8)
run at parallelism one with two warmups/five measurements. Each native query requires
consumed NativeCalc, no fallback and empty candidate evaluator descriptors.

| Flink / input | Candidate stock median (range), s | Native median (range), s | Previous stock median (range), s | Previous median (range), s |
| --- | ---: | ---: | ---: | ---: |
| 1.18.1 / modern-comparison | 0.188606 (0.183451-0.213412) | 0.194802 (0.194257-0.200845) | 0.193022 (0.181021-0.197639) | 0.212879 (0.212570-0.221866) |
| 1.18.1 / modern-arithmetic | 0.240461 (0.234930-0.242738) | 0.202671 (0.191075-0.207119) | 0.240431 (0.227648-0.241450) | 0.244998 (0.240404-0.252949) |
| 1.18.1 / legacy-comparison | 0.210634 (0.209291-0.225512) | 0.193515 (0.189962-0.200454) | 0.215743 (0.197997-0.227196) | 0.245962 (0.234360-0.359305) |
| 2.2.1 / modern-comparison | 0.189071 (0.184989-0.199581) | 0.189496 (0.185077-0.194949) | 0.192146 (0.185846-0.196981) | 0.207776 (0.204363-0.213585) |
| 2.2.1 / modern-arithmetic | 0.229806 (0.221967-0.238040) | 0.198334 (0.195886-0.204989) | 0.224424 (0.220635-0.238883) | 0.247177 (0.244628-0.259436) |
| 2.2.1 / legacy-comparison | 0.195756 (0.192557-0.200052) | 0.185488 (0.183502-0.196643) | 0.202816 (0.201091-0.213579) | 0.228695 (0.224209-0.232692) |

Native/stock elapsed ratios span 0.843-1.033;
native/previous ratios span 0.787-0.915. Ratios above one are slower.
All unfavorable results and overlapping ranges remain. Prepared qualification does
not establish universal end-to-end acceleration.

All 292 focused JVM invocations, four additional shared-state invocations and
12 existing fixture smokes pass on released Flink 1.18.1/2.2.1 with JDK 11/17
(Java release 11). Complete 5,003-row witnesses compare all values, schema, NULLs,
IDs and cardinality across batches for TINYINT/SMALLINT/INT/BIGINT, independent input
columns, nullable/NOT NULL declarations, modern/legacy mode, signed extrema and BIGINT
values beyond 2^53. They require consumed native work, no fallback and no evaluator.
String consumers, grouping, lazy branches and empty filtered batches match stock.
Failing and stateful inputs keep their released behavior and exact previous routes.
Unchanged released boolean-expression/truth tests, 535 CastRules stock-oracle cases
per pair, and the identical DSO's 1,019 core passes/two pre-existing ignores retain
their preceding qualification; new runtime witnesses establish the changed route.
Source/DSO hashes and independently recompiled baseline classes are retained.

Reproduce prepared qualification with `integral_expression`, INT/BIGINT input and
`SF_INTEGRAL_EXPRESSION='TRY_CAST(v>=CAST(0 AS $TYPE) AS STRING)'`. Complete jobs use
`sf.integral.expression=TRY_CAST(v>=0 AS STRING)`; BIGINT controls additionally use
`sf.integral.bigintInput=true`. Remaining conversions are tracked in
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).


## BOOLEAN-to-integer TRY_CAST {#boolean-to-integer-try_cast}

Verified BOOLEAN input/literal, logical, NULL-check and integer-comparison trees now use
the existing primitive native cast for TRY_CAST to TINYINT/SMALLINT/INT/BIGINT. TRUE is 1,
FALSE is 0 and NULL stays NULL in modern/legacy modes. Conversion cannot fail; failing or
unverified producers keep their preceding route. Ordinary CAST already becomes Flink's
CASE expression during released planning and needs no admission change. This follows
Flink's BooleanToNumeric rule and Comet's DataFusion-compatible primitive conversion.
There is no new native opcode, JNI entry point, setting or Rust production change.

The [record](results/boolean-integer-try-cast-2026-11-03.json) retains 48 cells and
[4,800 raw trials](results/boolean-integer-try-cast-2026-11-03.csv.gz), including warmups/checksums/ranges.
Released Flink 1.18.1/2.2.1, Oracle JDK 17.0.12, Apple M4 Pro, one thread and release/mimalloc
artifacts are used. Equal prepared GenericRowData/BinaryRowData and Arrow contain true,
false and NULL with nullable/NOT NULL schemas and four target integer widths. Computed
controls use `NOT v OR v IS NULL`. Each trial consumes 131,072 rows, with 16 warmups and
nine measurements. The native timer runs inside Rust after import and before export,
covering production Calc and equivalent output consumption. Stock runs on prepared RowData;
setup and perimeter conversion are outside only the small-function gate. Intrinsic costs
remain timed. The actual previous encoder from `8ccec369` verifies each exact fallback
reason and executes released generated Java without invented native overhead. Current and
previous controls use identical logical data/resources. The existing release runtime and
its mimalloc aliases are checked by hash and reused unchanged.

| Scope | Cells | Paired stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk (1,024 rows) | 40 | 1.092-2.819x | 1.075-2.135x | 1.071-2.147x |
| Tiny (16 rows) | 8 | 0.281-0.301x | 0.277-0.302x | 0.277-0.303x |

All bulk cells beat stock and actual previous fallback. Tiny controls retain
8 stock losses and 8 previous losses;
small batches are not described as accelerated when they lose.

Eight million-row complete jobs retain bounded row input, blackhole output, startup, JNI
and both transposes. Two warmups/five measurements alternate order. Candidate controls
require consumed NativeCalc work and empty evaluator descriptors. Actual previous controls
require exact fallback, zero substitutions and zero native operators. Seconds, median:

| Flink / source / target | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / BOOLEAN-TINYINT | 0.170140 | 0.167920 | 0.143392 | 0.145854 |
| 2.2.1 / BOOLEAN-SMALLINT | 0.158451 | 0.162561 | 0.142876 | 0.142270 |
| 2.2.1 / BOOLEAN-INT | 0.167679 | 0.173542 | 0.146190 | 0.146495 |
| 2.2.1 / BOOLEAN-BIGINT | 0.161557 | 0.169256 | 0.140627 | 0.157842 |
| 1.18.1 / BOOLEAN-TINYINT | 0.155952 | 0.170372 | 0.141720 | 0.140429 |
| 1.18.1 / BOOLEAN-SMALLINT | 0.165398 | 0.164696 | 0.136228 | 0.137099 |
| 1.18.1 / BOOLEAN-INT | 0.157851 | 0.164143 | 0.149238 | 0.144749 |
| 1.18.1 / BOOLEAN-BIGINT | 0.157443 | 0.170163 | 0.139337 | 0.143281 |

Native/stock elapsed spans **0.987-1.092**; native/previous spans
**1.072-1.213**. All slower results remain; prepared-kernel qualification does
not establish universal complete-job acceleration. Six of eight complete jobs lose to
paired stock; all eight lose to the independently measured actual previous fallback.

All **740 focused JVM invocations** pass without skips across both released Flink lines
and JDK 11/17, targeting Java release 11. Complete 5,003-row maps/schema, both cast modes,
NULLs/logical producers, filters, CASE, character consumers, all-filtered batches, selected
producer failures and adjacent function regressions pass. Admitted pure paths require
consumed NativeCalc work and empty evaluators. Sixteen actual previous positive cases
compare maps/schema then fail only the expected native-route assertion; eight control
methods stop earlier at their positive filter assertion, so their later negative checks
are not claimed as previous result comparisons. The unchanged released CastRules oracle
passes **eight original BOOLEAN-to-integer assertions** for each release/JDK pair. No Rust
code changes; the prior 904-test accepted-runtime result retains its historical provenance.
Other type/producer coverage remains in [#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

Reproduce with release `integral_expression`, `BOOLEAN` input, the matching nullable/binary/
batch arguments and `SF_INTEGRAL_EXPRESSION=TRY_CAST(v AS INT)`. Previous uses the frozen
encoder, `IntegralExpressionBenchmarkFunctions`, `sf.integral.previous=true`,
`sf.integral.fallbackControl=true` and the exact fallback reason. Complete jobs use
`IntegralExpressionPipelineBenchmark` with `sf.integral.booleanInput=true` and the same
expression/previous settings; all resources and data otherwise match.



## Integer-to-BOOLEAN TRY_CAST {#integer-to-boolean-try_cast}

TRY_CAST of verified TINYINT/SMALLINT/INT/BIGINT inputs and failure-free arithmetic,
widening/rounding producers uses the existing native comparison with a zero of the
input's resolved width. Nonzero values, including signed minima, become TRUE; zero is
FALSE and NULL stays NULL in both modes. This mirrors Flink's NumericToBoolean rule.
Comet also treats integer-to-BOOLEAN as a DataFusion-compatible conversion; using the
existing comparison avoids a new cast target/opcode, JNI entry point or Rust kernel.
Ordinary CAST already lowers to this comparison through released planning. Failing,
stateful or unverified producers keep their previous route. At this qualification revision,
character formatting of the new boolean TRY_CAST result
remained generated; the later character-consumer qualification extends that producer proof.

The [record](results/integer-boolean-try-cast-2026-11-03.json) retains 48 cells and
[4,800 raw trials](results/integer-boolean-try-cast-2026-11-03.csv.gz), including warmups/checksums/ranges.
Released Flink 1.18.1/2.2.1, Oracle JDK 17.0.12, Apple M4 Pro, one thread and release/mimalloc
artifacts are used. Equal prepared GenericRowData/BinaryRowData and Arrow include all
four signed integer source widths, nullable/NOT NULL, extrema and zero. Computed controls
use `v+CAST(1 AS source)`. Each trial consumes 131,072 rows, with 16 warmups and nine
measurements. The native timer runs inside Rust after import and before export, retaining
production Calc and equivalent output consumption. Stock uses prepared Java RowData.
Setup and perimeter conversions are outside only the small-function gate; intrinsic costs
remain. The actual previous encoder from `49f269d9` rejects each exact integer-to-BOOLEAN
TRY_CAST and its control executes released generated Java without synthetic native costs.
Current and previous inputs/resources match. The release runtime is unchanged and verified
by hash, including mimalloc malloc aliases.

| Scope | Cells | Paired stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk (1,024 rows) | 40 | 2.658-20.095x | 2.656-6.831x | 2.645-6.790x |
| Tiny (16 rows) | 8 | 0.259-0.272x | 0.257-0.277x | 0.255-0.274x |

The largest paired ratio comes from the Flink 1.18.1 NOT NULL GenericRowData INT cell:
paired stock is 12.271 ns/row (3.524-17.732), versus independent stock 2.088 ns/row.
A [diagnostic replay](results/integer-boolean-try-cast-2026-11-03-stock-drift-replay.csv.gz) with 64 warmups/nine
measurements gives stock/native 3.524x
and previous/native 3.352x.
This retains all 292 replay samples separately and does not overwrite the primary data or
prove the cause of the stock drift. The original 20x ratio is not a stable speedup claim.

All bulk cells beat stock and actual previous fallback. Tiny controls retain
8 stock losses and 8 previous losses.
Small batches are not called accelerated when they lose.

Eight complete million-row jobs cover direct/computed INT/BIGINT inputs. They retain the
bounded row source, blackhole output, startup, JNI and both transposes. Two warmups/five
measurements alternate order. Candidate requires consumed NativeCalc work and empty JVM
evaluator descriptors. Actual previous controls require exact fallback, zero substitutions
and zero native operators. Seconds, median:

| Flink / source / shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / INT-BOOLEAN | 0.162491 | 0.166682 | 0.147023 | 0.148354 |
| 2.2.1 / INT-BOOLEAN-computed | 0.155750 | 0.167884 | 0.149599 | 0.156696 |
| 2.2.1 / BIGINT-BOOLEAN | 0.158948 | 0.167414 | 0.155004 | 0.155063 |
| 2.2.1 / BIGINT-BOOLEAN-computed | 0.161442 | 0.179230 | 0.147702 | 0.153635 |
| 1.18.1 / INT-BOOLEAN | 0.167345 | 0.173842 | 0.144691 | 0.145597 |
| 1.18.1 / INT-BOOLEAN-computed | 0.163659 | 0.167112 | 0.141206 | 0.142495 |
| 1.18.1 / BIGINT-BOOLEAN | 0.169389 | 0.180325 | 0.161704 | 0.161217 |
| 1.18.1 / BIGINT-BOOLEAN-computed | 0.169641 | 0.184478 | 0.152336 | 0.159917 |

Native/stock elapsed spans **1.021-1.110**; native/previous spans
**1.071-1.194**. 8 of eight complete jobs lose to paired
stock; 8 lose to independently measured actual previous fallback.
All slower results remain. Prepared-function gains do not establish universal complete-job
acceleration.

All **528 focused JVM invocations** pass without skips across both released Flink lines
and JDK 11/17, targeting Java release 11. The new complete 5,003-row maps/schema cover
four integer widths, modern/legacy modes, NULLs, signed extrema and zero, arithmetic,
widening, rounding, logic, CASE and empty filtered batches. Real filters and failing/lazy
producer controls pass; at this qualification revision, character consumers explicitly
retain their generated route. The following qualification removes those evaluators for
the separately verified character-consumer subset.
Related integer/BOOLEAN TRY_CAST, temporal parsing and BOOLEAN CASE regressions also pass.
Admitted pure claims require consumed NativeCalc work and empty evaluators. Sixty-four
actual previous positive cases compare full maps/schema before the expected fallback
assertion; their empty/character follow-ups are not reached. Eight control methods stop
at the positive filter route assertion before their later negative checks. Four unchanged
released CastRules oracle runs pass **12 original integer-to-BOOLEAN assertions each**.
No Rust production code changes; the previously qualified accepted runtime's 904-test
result retains its historical provenance. Other producers, types and unverified character
consumers remain in [#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

Reproduce with release `integral_expression`, matching integer type/nullability/row-layout/
batch arguments and `SF_INTEGRAL_EXPRESSION=TRY_CAST(v AS BOOLEAN)`. Previous uses its
frozen encoder, `IntegralExpressionBenchmarkFunctions`, `sf.integral.previous=true`,
`sf.integral.fallbackControl=true` and the exact fallback reason. Complete jobs use
`IntegralExpressionPipelineBenchmark`, `sf.integral.bigintInput=true` for BIGINT, the same
expression and previous settings, preserving resources/data and perimeter costs.



## Character consumers of integer truth {#character-consumers-of-integer-truth}

CAST/TRY_CAST of the verified integer-to-BOOLEAN TRY_CAST result to STRING/VARCHAR/CHAR
reuse the existing BOOLEAN formatter. The same total-integer producer proof governs the
inner nonzero comparison and its outer character/logical consumers. Integer-target casts
retain their separate preceding proof. This follows
Flink's NumericToBoolean and BooleanToString rules, including mode-dependent spelling,
NULL, CHAR padding and VARCHAR truncation. Comet's compatible numeric/boolean conversion
paths are consulted; no Rust kernel, opcode, JNI entry point, setting or dependency changes.
Division, narrowing and unverified/failing/stateful producers retain their preceding routes.
Ordinary integer-to-BOOLEAN CAST already has a released native comparison rewrite.

The [record](results/integer-truth-character-casts-2026-11-03.json) retains 96 prepared cells and
[9,600 raw samples](results/integer-truth-character-casts-2026-11-03.csv.gz), including warmups, checksums and variability.
Released Flink 1.18.1/2.2.1, Oracle JDK 17.0.12, Apple M4 Pro, one thread and release/mimalloc
artifacts are used. Equal GenericRowData/BinaryRowData and Arrow contain four signed
integer widths, nullable/NOT NULL, extrema and zero. Short STRING and padded CHAR(8)
forms use direct or addition producers. Each trial consumes 131,072 rows, with 16 warmups
and nine measurements. Rust times production Calc and equivalent output consumption after
import/before export; stock uses equal prepared RowData. Setup and perimeter conversions
are excluded only from this small-function timer; intrinsic costs remain. The actual
preceding encoder `45cdf27a` runs on the exact same unchanged DSO/executable and fixture.
It retains its registered evaluators and any production shortcuts. A retained descriptor
alone does not prove Java executes on every row. Artifact hashes and mimalloc aliases match.

| Scope | Cells | Paired stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk (1,024 rows) | 80 | 1.301-10.373x | 9.137-18.033x | 1.399-7.896x |
| Tiny (16 rows) | 16 | 0.113-0.962x | 13.991-15.825x | 0.112-0.990x |

All bulk cells beat stock and the actual preceding behavior. Tiny controls retain
16 stock losses and 0 previous losses.
All independent samples remain; the function gate does not establish universal job gains.

Sixteen complete million-row INT/BIGINT jobs cover direct/computed short/padded forms.
They retain bounded row input, blackhole output, startup, JNI and both transposes. Two
warmups/five measurements alternate order. New controls require consumed NativeCalc work
and empty evaluators; actual preceding controls require retained evaluators and consumed
work. Seconds, median:

| Flink / source / shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / INT-short | 0.188565 | 0.190101 | 0.190131 | 0.222412 |
| 2.2.1 / INT-padded | 0.219376 | 0.191142 | 0.231627 | 0.248017 |
| 2.2.1 / INT-short-computed | 0.186346 | 0.195479 | 0.191713 | 0.223141 |
| 2.2.1 / INT-padded-computed | 0.220819 | 0.192531 | 0.224752 | 0.242662 |
| 2.2.1 / BIGINT-short | 0.191168 | 0.197340 | 0.190325 | 0.220678 |
| 2.2.1 / BIGINT-padded | 0.236820 | 0.198431 | 0.244500 | 0.255661 |
| 2.2.1 / BIGINT-short-computed | 0.203478 | 0.202725 | 0.190485 | 0.226375 |
| 2.2.1 / BIGINT-padded-computed | 0.237860 | 0.198294 | 0.233832 | 0.250998 |
| 1.18.1 / INT-short | 0.192091 | 0.203404 | 0.190491 | 0.216337 |
| 1.18.1 / INT-padded | 0.216036 | 0.193191 | 0.219586 | 0.238627 |
| 1.18.1 / INT-short-computed | 0.190646 | 0.194826 | 0.186903 | 0.218633 |
| 1.18.1 / INT-padded-computed | 0.228368 | 0.196992 | 0.220991 | 0.245635 |
| 1.18.1 / BIGINT-short | 0.201844 | 0.210436 | 0.202028 | 0.239229 |
| 1.18.1 / BIGINT-padded | 0.242544 | 0.208591 | 0.240534 | 0.258406 |
| 1.18.1 / BIGINT-short-computed | 0.206283 | 0.214572 | 0.203259 | 0.234902 |
| 1.18.1 / BIGINT-padded-computed | 0.229412 | 0.210455 | 0.233978 | 0.255656 |

Native/stock elapsed spans **0.834-1.059**, with 7 stock losses;
native/previous spans **0.771-0.940**, with 0 previous losses.
Every slower control remains; prepared-function gains are distinct from complete-job gains.

All **384 focused JVM invocations** pass without skips across both released Flink lines
and JDK 11/17, targeting Java release 11. Complete 5,003-row maps/schema cover all integer
widths, signed extrema/zero, nullable/NOT NULL, modern/legacy modes, both outer cast kinds,
STRING/VARCHAR(1/3)/CHAR(4/8), arithmetic/logical producers, explicitly retained
nested-integer fallback and all-filtered batches. Real filters, CONCAT/length,
selected failures and untaken branches
also pass, with explicit retained-route assertions. Related boolean CASE, integer casts
and the original BOOLEAN formatter regressions pass. Pure claims require consumed native
work and empty evaluator descriptors.

The actual previous 64 type/mode/nullability cases compare the first CAST query's full
maps/schema, then fail only the new evaluator-free expectation. Their empty query and
later outer TRY_CAST loop are not reached; candidate tests verify those separately.
All sixteen original compound/failing/empty controls across both outer operators pass.
Four freshly executed unchanged released CastRules oracle runs pass **eight original
BOOLEAN-to-character assertions each**, separately from native work evidence. No Rust
production changes; the accepted runtime's prior 904-test result remains historical.
Other type/producer/consumer gaps stay in [#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).

Reproduce with release `integral_expression`, matching signed input/nullability/row-layout/
batch arguments and `SF_INTEGRAL_EXPRESSION=CAST(TRY_CAST(v AS BOOLEAN) AS STRING)` or
`TRY_CAST(TRY_CAST(v AS BOOLEAN) AS CHAR(8))`. Previous supplies the frozen encoder and
`SF_INTEGRAL_PREVIOUS=true` on the same release runtime. Complete jobs use
`IntegralExpressionPipelineBenchmark`, the corresponding expression, bigint input flag
and `sf.integral.previous=true` for the preceding control; other resources/data match.



## DECIMAL-to-BOOLEAN TRY_CAST {#decimal-to-boolean-try_cast}

Direct DECIMAL inputs/literals use the existing native `<>` comparison against a
zero with identical precision/scale. Every nonzero coefficient is TRUE, zero is FALSE
and NULL remains NULL in both cast modes. This follows Flink's NumericToBoolean rule
and Comet's same-type Decimal128 zero comparison. Ordinary CAST already rewrites to a
comparison when precision exceeds scale; released ordinary DECIMAL(p,p) CAST fails
during planning and remains unchanged. Computed/metadata-sensitive/failing/stateful
producers retain their previous routes and character formatting retains its separate
proof. No new Rust production kernel, setting, dependency or JNI contract is added.

The [record](results/decimal-boolean-try-cast-2026-11-03.json) retains all 50 prepared configurations and
[5,000 raw samples](results/decimal-boolean-try-cast-2026-11-03.csv.gz), including warmups, checksums and variability.
Released Flink 1.18.1/2.2.1, Oracle JDK 17.0.12, Apple M4 Pro, one thread, and release/mimalloc
are used. Five compact/noncompact input types are DECIMAL(4,2), (18,2), (19,19), (38,18)
and (38,38), each nullable/NOT NULL and GenericRowData/BinaryRowData. Matching Arrow and
Java fixtures include signed extrema, both signs and periodic zeros. The explicit
`SF_INTEGRAL_CORPUS=decimal-truth` corpus adds the same zeros to both fixtures; the earlier
nonzero-only pilot is retained separately and is not the final qualification.

Each trial consumes 131,072 rows with 16 warmups/nine measurements. The Rust timer
encloses production Calc and equivalent consumption after import/before export; Flink
starts on equal prepared RowData. Setup/perimeter costs alone are outside this function
timer; intrinsic costs remain. Actual preceding encoder `907c078c` rejects TRY_CAST and
its real Java fallback is measured, not an invented callback. Both use the exact same
unchanged accepted DSO and final fixtures. Only the benchmark executable changes to
add the explicit zero corpus. Artifact/source hashes and matching mimalloc aliases remain.

| Scope | Cells | Paired stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk (1,024 rows) | 40 | 4.245-52.674x | 4.250-45.661x | 4.222-45.962x |
| Tiny (16 rows) | 10 | 0.341-1.487x | 0.406-1.842x | 0.390-1.810x |

All bulk cells beat stock and actual previous behavior. Tiny cells retain
5 stock losses and 5 previous losses.
Independent stock ranges remain alongside paired controls.

Ten complete million-row configurations cover all five input types on both released
Flink lines. They retain bounded row input, blackhole output, startup, JNI and both
transposes, with two warmups/five measurements in alternating order. Candidate controls
require consumed NativeCalc work and empty evaluators; preceding controls require their
actual fallback and zero native substitutions. Seconds, median:

| Flink / source / target | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / DECIMAL(4,2)-BOOLEAN | 0.169193 | 0.184344 | 0.162222 | 0.157377 |
| 2.2.1 / DECIMAL(18,2)-BOOLEAN | 0.161567 | 0.177438 | 0.151877 | 0.156473 |
| 2.2.1 / DECIMAL(19,19)-BOOLEAN | 0.160679 | 0.194553 | 0.151313 | 0.161298 |
| 2.2.1 / DECIMAL(38,18)-BOOLEAN | 0.169855 | 0.193467 | 0.148120 | 0.160680 |
| 2.2.1 / DECIMAL(38,38)-BOOLEAN | 0.157564 | 0.192809 | 0.159520 | 0.148599 |
| 1.18.1 / DECIMAL(4,2)-BOOLEAN | 0.173802 | 0.178297 | 0.154189 | 0.154342 |
| 1.18.1 / DECIMAL(18,2)-BOOLEAN | 0.159733 | 0.180253 | 0.149595 | 0.154122 |
| 1.18.1 / DECIMAL(19,19)-BOOLEAN | 0.167480 | 0.186420 | 0.146684 | 0.153933 |
| 1.18.1 / DECIMAL(38,18)-BOOLEAN | 0.157908 | 0.190552 | 0.156317 | 0.147475 |
| 1.18.1 / DECIMAL(38,38)-BOOLEAN | 0.163782 | 0.187075 | 0.154887 | 0.149671 |

Native/stock elapsed spans **1.026-1.224**, retaining 10 stock losses;
native/previous spans **1.134-1.298**, retaining 10 previous losses.
Every unfavorable result remains; function qualification is distinct from full-job gains.

All **456 focused JVM invocations** pass without skips on released Flink 1.18.1/2.2.1
and JDK 11/17, targeting Java release 11. Full 5,003-row maps/schema cover eleven
precision/scale pairs from DECIMAL(1,0)/(1,1) to (38,38), nullable/NOT NULL, both cast
modes, zero, signed extrema, logical/NULL/CASE consumers and all-filtered batches.
At this qualification revision, the character control retains its generated evaluator;
the following qualification extends that proof. Filters, computed/failing producers and
lazy branches also pass. Admitted paths require actual consumed native work and empty evaluators.
The four ordinary DECIMAL(p,p) planning failures are separately verified in both modes.
Adjacent integer/BOOLEAN casts and DECIMAL projection/default regressions pass.

Actual-before runs compare 44 positive invocations' first NOT NULL/modern normal
maps/schema before the expected old-fallback assertion; later modes/nullability/empty
queries are not claimed as reached. Eight before character-consumer controls also compare
maps/schema before their fallback assertion, with later filters/negative checks unreached.
All sixteen ordinary CAST planning-failure invocations pass, each covering both modes.
Four fresh unchanged released CastRules oracles pass two original DECIMAL-to-BOOLEAN
assertions each. A fresh release core run passes **904 Rust tests**, no ignores; only the
benchmark fixture changes in Rust, and the production DSO hash stays identical.

Reproduce with release `integral_expression`, matching DECIMAL input/nullability/row-layout/
batch arguments, `SF_INTEGRAL_CORPUS=decimal-truth` and
`SF_INTEGRAL_EXPRESSION=TRY_CAST(v AS BOOLEAN)`. Actual previous uses its frozen encoder
with the same final Java fixture and explicit `sf.integral.fallbackControl=true` /
`sf.integral.fallbackReason=unsupported TRY_CAST DECIMAL→BOOLEAN` control. Complete jobs use
`IntegralExpressionPipelineBenchmark` and matching `sf.integral.decimalPrecision`/`decimalScale`;
the preceding control must keep its fallback. Remaining producers, consumers and source/
target families stay in [#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).



## Character consumers of decimal truth {#character-consumers-of-decimal-truth}

CAST/TRY_CAST of verified direct DECIMAL-to-BOOLEAN TRY_CAST to STRING/VARCHAR/CHAR
reuse the existing native BOOLEAN formatter. The producer proof permits verified logical
and NULL wrappers around that conversion. Same-precision/scale decimal zero comparison,
modern/legacy spelling, NULLs, VARCHAR truncation and CHAR padding remain unchanged.
The structure follows Flink's NumericToBoolean/BooleanToString rules and Comet's
same-type zero comparison. No native kernel, setting, dependency or JNI changes.
Computed/failing/stateful/metadata-sensitive producers retain their preceding routes.
Integer-target and bounded-position condition proofs remain separate. At this qualification
revision ordinary DECIMAL CAST uses a different comparison tree and retains its character
evaluator; the following qualification extends that direct-comparison proof. Ordinary
DECIMAL(p,p) CAST retains the released planning failure.

The [record](results/decimal-truth-character-casts-2026-11-03.json) retains 100 prepared configurations and
[10,000 raw samples](results/decimal-truth-character-casts-2026-11-03.csv.gz), including warmups, checksums and variability.
Released Flink 1.18.1/2.2.1, Oracle JDK 17.0.12, Apple M4 Pro, one thread and release/mimalloc
are used. Equal GenericRowData/BinaryRowData and Arrow contain DECIMAL(4,2), (18,2),
(19,19), (38,18) and (38,38), nullable/NOT NULL, signed extrema, zero and both signs.
`SF_INTEGRAL_CORPUS=decimal-truth` selects the same fixture. Shapes are outer CAST to
STRING and outer TRY_CAST to CHAR(8), each over the direct inner TRY_CAST.

Each trial consumes 131,072 rows, with 16 warmups/nine measurements. Rust times production
Calc and equivalent consumption after import/before export; Flink starts on equal prepared
RowData. Setup and perimeter costs alone are excluded from this function timer; intrinsic
costs remain. Actual preceding encoder `8b42f7c1` retains its character evaluator and any
production shortcuts on the exact same unchanged DSO/executable/fixture. A retained
descriptor alone is not proof of a JVM callback on every row. Hashes/mimalloc aliases match.

| Scope | Cells | Paired stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk (1,024 rows) | 80 | 2.109-13.089x | 11.307-18.114x | 1.854-11.668x |
| Tiny (16 rows) | 20 | 0.211-1.308x | 13.644-15.236x | 0.204-1.244x |

All bulk cells beat stock and actual previous behavior. Tiny cells retain
14 stock losses and 0 previous losses.
All independent measurements and unfavorable controls remain.

Twenty complete million-row configurations cover five DECIMAL types and both shapes
on both released Flink lines. They retain bounded row input, blackhole output, startup,
JNI and both transposes. Two warmups/five measurements alternate order. Candidate controls
require consumed NativeCalc work and empty evaluators; previous controls require consumed
work and their retained evaluators. Seconds, median:

| Flink / source / shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / DECIMAL(4,2)-short | 0.207693 | 0.205985 | 0.211752 | 0.238406 |
| 2.2.1 / DECIMAL(4,2)-padded | 0.250289 | 0.213052 | 0.246135 | 0.267123 |
| 2.2.1 / DECIMAL(18,2)-short | 0.210433 | 0.206001 | 0.201563 | 0.232634 |
| 2.2.1 / DECIMAL(18,2)-padded | 0.240361 | 0.207615 | 0.244654 | 0.276751 |
| 2.2.1 / DECIMAL(19,19)-short | 0.206595 | 0.201382 | 0.208084 | 0.237834 |
| 2.2.1 / DECIMAL(19,19)-padded | 0.244125 | 0.222056 | 0.254456 | 0.282303 |
| 2.2.1 / DECIMAL(38,18)-short | 0.208986 | 0.211712 | 0.208985 | 0.241863 |
| 2.2.1 / DECIMAL(38,18)-padded | 0.242238 | 0.211111 | 0.249556 | 0.281209 |
| 2.2.1 / DECIMAL(38,38)-short | 0.215642 | 0.211295 | 0.207752 | 0.243736 |
| 2.2.1 / DECIMAL(38,38)-padded | 0.247046 | 0.217456 | 0.250570 | 0.282227 |
| 1.18.1 / DECIMAL(4,2)-short | 0.207685 | 0.209153 | 0.210604 | 0.236102 |
| 1.18.1 / DECIMAL(4,2)-padded | 0.250418 | 0.211956 | 0.243160 | 0.262985 |
| 1.18.1 / DECIMAL(18,2)-short | 0.210772 | 0.206240 | 0.220348 | 0.247438 |
| 1.18.1 / DECIMAL(18,2)-padded | 0.245597 | 0.208913 | 0.243005 | 0.266683 |
| 1.18.1 / DECIMAL(19,19)-short | 0.207183 | 0.228833 | 0.210799 | 0.240593 |
| 1.18.1 / DECIMAL(19,19)-padded | 0.249136 | 0.216391 | 0.240292 | 0.270399 |
| 1.18.1 / DECIMAL(38,18)-short | 0.205375 | 0.216000 | 0.215327 | 0.232637 |
| 1.18.1 / DECIMAL(38,18)-padded | 0.246364 | 0.232057 | 0.240270 | 0.273388 |
| 1.18.1 / DECIMAL(38,38)-short | 0.218105 | 0.214802 | 0.216118 | 0.249770 |
| 1.18.1 / DECIMAL(38,38)-padded | 0.246352 | 0.215345 | 0.241897 | 0.273894 |

Native/stock elapsed spans **0.846-1.104**, retaining 4 stock losses;
native/previous spans **0.750-0.951**, retaining 0 previous losses.
Prepared function gains are distinct from complete-job gains; no slower control is removed.

All **304 focused JVM invocations** pass without skips across released Flink 1.18.1/2.2.1
and JDK 11/17, targeting Java release 11. Complete 5,003-row maps/schema cover eleven
precision/scale pairs from DECIMAL(1,0)/(1,1) to (38,38), both outer cast kinds, nullable/
NOT NULL, modern/legacy modes, STRING/VARCHAR(1/3)/CHAR(4/8), signed extrema/zero, logical/
NULL wrappers and empty filtered batches. Filter/length/CONCAT consumers, selected errors,
untaken branches and explicitly retained integer targets pass. Related direct decimal,
integer truth/character and BOOLEAN/integer regressions pass. Admitted claims require
consumed NativeCalc work and empty evaluator descriptors.

Actual-before runs compare 44 first NOT NULL/modern outer-CAST queries' complete maps/
schema before the evaluator-free assertion; their subsequent modes/nullability/empty and
outer TRY_CAST queries are not claimed as reached. All sixteen preceding compound/failing/
empty/integer-target controls pass. Four fresh unchanged released CastRules oracles pass
eight original BOOLEAN-to-character assertions each. No Rust source/artifact changes;
the immediately preceding qualification's fresh 904-test Rust result remains historical.

Reproduce with release `integral_expression`, matching DECIMAL input/nullability/row-layout/
batch arguments, `SF_INTEGRAL_CORPUS=decimal-truth` and
`SF_INTEGRAL_EXPRESSION=CAST(TRY_CAST(v AS BOOLEAN) AS STRING)` or
`TRY_CAST(TRY_CAST(v AS BOOLEAN) AS CHAR(8))`. Actual previous supplies the frozen encoder
and `SF_INTEGRAL_PREVIOUS=true` on the same runtime/fixture. Complete jobs use
`IntegralExpressionPipelineBenchmark` with matching `sf.integral.decimalPrecision`/
`decimalScale` and `sf.integral.previous=true` for previous. Other resources/data match.
Remaining type/producer/consumer gaps stay in
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).



## Character consumers of direct decimal predicates {#character-consumers-of-direct-decimal-predicates}

CAST/TRY_CAST to STRING/VARCHAR/CHAR reuse the native BOOLEAN formatter for direct
same-precision/scale DECIMAL comparisons, direct DECIMAL NULL checks and verified logical/
NULL wrappers. The NULL proof also covers released nullable-comparison rewrites. The
ordinary DECIMAL-to-BOOLEAN CAST rewrite consequently qualifies when precision exceeds
scale; ordinary DECIMAL(p,p) retains released planning failure. Computed/failing/stateful/
mixed-type operands, integer-target TRY_CAST and bounded-position proofs stay separate.
No Rust kernel, setting, dependency or JNI/transport change is introduced by this extension.

The [record](results/decimal-comparison-character-casts-2026-11-04.json) retains 60 prepared configurations and
[6,000 raw samples](results/decimal-comparison-character-casts-2026-11-04.csv.gz), including warmups, checksums and variability.
Released Flink 1.18.1/2.2.1, Oracle JDK 17.0.12, Apple M4 Pro, one thread and release/mimalloc
are used. Equal GenericRowData/BinaryRowData and Arrow contain DECIMAL(4,2), (18,2),
and (38,18), nullable/NOT NULL, signed extrema, periodic zero and both signs.
Shapes are CAST(CAST(v AS BOOLEAN) AS STRING) and
TRY_CAST(CAST(v AS BOOLEAN) AS CHAR(8)); the inner ordinary CAST produces a verified
same-type zero comparison in released planning.

Each trial consumes 131,072 rows, with 16 warmups/nine measurements. Rust times production
Calc and equivalent consumption after import/before export; stock starts on equal prepared
RowData. Setup/perimeter conversion alone is excluded; intrinsic costs remain timed.
Actual preceding encoder `6305c6f9` retains its character evaluator and any production
shortcuts on the exact same newly rebuilt DSO/executable/fixture. A descriptor alone is
not proof of a Java callback on every row. Allocator aliases and artifact hashes match.

| Scope | Cells | Paired stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk (1,024 rows) | 48 | 1.834-15.438x | 10.918-33.003x | 1.604-12.815x |
| Tiny (16 rows) | 12 | 0.169-1.127x | 13.296-14.487x | 0.167-1.185x |

All bulk cells beat paired stock and actual previous behavior. Tiny cells retain
10 paired-stock losses and 0 previous losses;
all independent measurements and unfavorable controls remain.

Eight complete million-row configurations cover compact DECIMAL(18,2) and noncompact
DECIMAL(38,18), both shapes and both released lines. Bounded row input, blackhole output,
startup, JNI and both transposes remain. Two warmups/five measurements alternate order.
Candidate needs consumed NativeCalc work and empty evaluators; preceding controls retain
consumed work and their character evaluator. Seconds, median:

| Flink / input / shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / DECIMAL(18,2)-short | 0.200580 | 0.192215 | 0.206523 | 0.233667 |
| 2.2.1 / DECIMAL(18,2)-padded | 0.246108 | 0.208744 | 0.243367 | 0.260601 |
| 2.2.1 / DECIMAL(38,18)-short | 0.201272 | 0.208937 | 0.205316 | 0.237216 |
| 2.2.1 / DECIMAL(38,18)-padded | 0.246396 | 0.215287 | 0.245370 | 0.277956 |
| 1.18.1 / DECIMAL(18,2)-short | 0.205336 | 0.203482 | 0.210707 | 0.244583 |
| 1.18.1 / DECIMAL(18,2)-padded | 0.242385 | 0.207672 | 0.233590 | 0.251610 |
| 1.18.1 / DECIMAL(38,18)-short | 0.203797 | 0.222972 | 0.194975 | 0.237246 |
| 1.18.1 / DECIMAL(38,18)-padded | 0.238009 | 0.210415 | 0.246277 | 0.265138 |

Native/stock elapsed spans **0.848-1.094**, retaining 2 stock losses;
native/previous spans **0.775-0.940**, retaining 0 previous losses.
Prepared function gains are distinct from complete-job gains; no slower control is removed.

All **260 focused JVM invocations** pass without skips on released Flink 1.18.1/2.2.1 and
JDK 11/17, targeting Java release 11. Complete 5,003-row maps/schema cover eleven precision/
scale pairs through (38,38), all six comparisons, independently varying DECIMAL columns
and NULL positions, NULL/logical wrappers, both outer cast kinds, STRING/VARCHAR(1/3)/
CHAR(4/8), modern/legacy modes and empty filtered batches. Computed operands retain their
generated controls; integer targets retain fallback. Filters/length, selected division
failures and untaken branches pass. Ordinary DECIMAL(p,p) planning failures remain covered.
Pure claims require consumed NativeCalc work and empty evaluator descriptors.

Actual-before runs compare all 44 first NOT NULL/modern outer-CAST queries' maps/schema
before failing the new evaluator-free assertion. Later modes/nullability/empty/outer-TRY_CAST
queries are not claimed as reached by those invocations. All eight retained-route controls
pass. Four fresh unchanged released DecimalTypeTest oracles each execute three original
comparison methods with their setup/teardown and original assertions. These are stock
oracles; the independent runtime-source SQL witnesses establish native parity and execution.

After rebasing onto merged main `2bb52ca2`, fresh full runtime Maven compilations for both lines,
**904 release Rust core tests** with no ignores and **304 preceding-character JVM controls**
pass on the rebuilt artifacts. A separate 260 invocations separately qualify this
comparison extension. Another **80 targeted metadata regressions** pass across the four
release/JDK pairs for NOT NULL arithmetic, narrowing CAST overflow and noncompact SIGN
character/default consumers. Eight fresh compact/noncompact direct-TRY_CAST character
prepared replays also beat stock and the actual latest-main encoder on the rebuilt runtime;
their raw samples are retained in the linked record. Earlier character measurements retain their original revision/DSO
provenance; none is relabeled as a fresh measurement of this rebuilt runtime. The
[800 fresh replay samples](results/decimal-truth-character-rebase-2026-11-04.csv.gz) are
separate prepared controls; they do not relabel the earlier twenty complete-job measurements.

Reproduce using release `integral_expression`, matching DECIMAL type/nullability/row layout/
batch, `SF_INTEGRAL_CORPUS=decimal-truth` and `SF_INTEGRAL_EXPRESSION` set to either shape.
Actual previous supplies the frozen encoder with `SF_INTEGRAL_PREVIOUS=true` on the same
runtime/fixture. Complete jobs use IntegralExpressionPipelineBenchmark with the matching
expression/precision/scale and `sf.integral.previous=true` for previous controls.
Remaining type/producer/consumer gaps stay in
[#492](https://github.com/datafusion-contrib/StreamFusion/issues/492).


