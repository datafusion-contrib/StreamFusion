# Recorded scalar workloads: printf

[Topic index and original links](scalar-functions.md). Sections retain their recorded scope, dates and evidence.

## Integer PRINTF with a literal format {#integer-printf-with-a-literal-format}

On released Flink 2.2.1, verified integer inputs/literals and failure-free arithmetic,
lossless widening or admitted rounding trees use an evaluator-free Arrow formatter with
an ASCII literal format of at most 512 characters. Conversions are `%s`/`%S`, `%d`,
`%x`/`%X`, `%o` and `%%`; width/precision are at most 256. The planner validates flags and
indexing during planning using the running JDK's `Formatter(Locale.US)`. Explicit index zero stays
on its preceding route because JDK 11 accepts it and JDK 17 rejects it. Every producer
must be failure-free, including unused arguments. Other formats/types and sensitive string
consumers retain their existing routes. Flink 1.18 has no PRINTF and retains SQL rejection.

The new path borrows the literal format, reuses an output scratch String, and appends into
one Arrow builder rather than creating an output String and materialized format per row.
The existing compatibility formatter remains shared; this does not substitute Rust format
syntax for Java Formatter. Parsing and per-conversion strings are still rowwise. Actual
preceding encoder `f910c352` is compiled independently against the shared current runtime,
fixture and allocator. Its direct compatible ASCII batches can already execute certified
Rust with a retained evaluator; computed producers retain generated evaluation. The previous
control is not assumed to perform callbacks on every batch, nor is it a whole historical
DSO replay.

The [2026-11-03 record](results/integer-literal-printf-2026-11-03.json) uses released Flink 2.2.1, Oracle JDK 17.0.12,
Apple M4 Pro, one thread and release/mimalloc-linked artifacts. Both executable and DSO
verify identical malloc/mi_malloc addresses. Prepared GenericRowData, BinaryRowData and
Arrow contain the same values, all signed integer widths, nullable/NOT NULL, extrema and
NULLs. Each trial consumes 131,072 rows, with 16 warmups and nine measurements. Rust times
production Calc and equivalent output consumption after input import and before output
export. Intrinsic callbacks remain timed; initial JNI/import/export and perimeter transposes
are excluded only from the prepared function gate. Complete jobs retain those costs.

Short formatting uses `PRINTF('%d/%s/%1$x',v,v)`; padded formatting uses
`PRINTF('%+020d:%2$-12.3S:%1$08X',v,v)`. Additional INT/BIGINT controls add one at the original
input width in the first formatter argument. Secondary input is not needed.

| Bulk cells | Paired stock / native median range | Previous / native median range | Independent stock / native |
| ---: | ---: | ---: | ---: |
| 40 | 2.532-5.180x | 1.072-5.250x | 2.462-5.094x |

All 40 bulk cells beat paired stock and the actual preceding encoder. Independent stock
has 0 lower medians than native; all independent measurements and variability remain.
The eight 16-row controls retain paired stock/native 2.174-4.008x,
including 0 controls slower than stock. The [4,800 raw trials](results/integer-literal-printf-2026-11-03.csv.gz)
include checksums and warmups.

The initial implementation merely removed the descriptor and reused certified formatting;
its previous/native pilot spans 0.968-1.033x and loses several controls. Its
[800 pilot trials](results/integer-literal-printf-2026-11-03-initial-pilot.csv.gz), source and artifact hashes remain
separate from the [800 optimized pilot trials](results/integer-literal-printf-2026-11-03-optimized-pilot.csv.gz).
For the 1,024-row nullable INT short probe, scratch/builder reuse reduces allocation requests
from 4,136/195,476 requested bytes to
3,102/91,842. Retained width-300 formats replay the saved pre-refactor
executable/DSO and current artifacts, both retaining evaluators; initial/current median
ratios span 0.987-1.005x. These [400 controls](results/integer-literal-printf-2026-11-03-retained-controls.csv.gz)
retain shared-formatter variation separately from the admitted scope.

Eight complete million-row INT/BIGINT jobs retain a bounded row source, blackhole sink,
startup, JNI and both transposes; two warmups and five measurements alternate engine order.
Candidate requires consumed NativeCalc work and empty evaluators. Previous requires its
retained evaluator, which does not prove callbacks for every compatible direct batch.
Seconds, median:

| Flink / type / format shape | Paired stock | Native | Independent stock | Previous route |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / INT-short | 0.512502 | 0.282118 | 0.528230 | 0.308422 |
| 2.2.1 / INT-padded | 0.888427 | 0.326280 | 0.970378 | 0.331879 |
| 2.2.1 / INT-short-computed | 0.511739 | 0.288623 | 0.483461 | 0.507033 |
| 2.2.1 / INT-padded-computed | 0.983899 | 0.327239 | 0.933144 | 0.967021 |
| 2.2.1 / BIGINT-short | 0.566439 | 0.293006 | 0.580639 | 0.325968 |
| 2.2.1 / BIGINT-padded | 0.962960 | 0.319097 | 0.932796 | 0.343919 |
| 2.2.1 / BIGINT-short-computed | 0.556859 | 0.298003 | 0.558454 | 0.609614 |
| 2.2.1 / BIGINT-padded-computed | 0.925704 | 0.330283 | 0.969479 | 1.019794 |

Native/stock elapsed spans **0.331-0.564**; native/previous spans
**0.324-0.983**. All trials/ranges are retained; prepared function
gains do not imply a universal complete-job speedup.

All **392 focused JVM invocations** pass without skips across released Flink 1.18.1/2.2.1
and JDK 11/17, targeting Java release 11. Runtime tests compare complete 5,003-row maps,
resolved schemas, IDs, cardinality, all signed widths/extrema, NULLs, indexing, padding,
precision, computed wrapping producers and all-filtered batches. Generated controls retain
unverified formats/producers, JDK-dependent index zero and CHAR_LENGTH representation guards.
Existing certified/PRINTF tests qualify the shared formatter on Flink 2.2. Before controls
match full maps/schema then fail all eight new pure-route assertions; the generated control
passes. Two unchanged released `StringFunctionsITCase.printfTestCases` runs pass all **47
original assertions each**, separately as stock oracles. All **902 release Rust core tests**
pass without ignores, including slices, ownership, empty arrays and rejected contracts.
Hosted CI remains separate; broader formats/types/consumers stay in
[#497](https://github.com/datafusion-contrib/StreamFusion/issues/497).

Reproduce prepared controls with the release `integral_expression` example and
`SF_INTEGRAL_EXPRESSION` above; supply independently compiled previous encoder and
`SF_INTEGRAL_PREVIOUS=true` for that control. Complete jobs use
`IntegralExpressionPipelineBenchmark`, matching `sf.integral.expression`, and
`sf.integral.bigintInput=true` for BIGINT. Other resources/settings remain identical.



## Integer boolean PRINTF formats {#integer-boolean-printf-formats}

On released Flink 2.2.1, `%b`/`%B` treats any non-NULL integer object, **including zero**,
as `true`/`TRUE`; NULL becomes `false`/`FALSE`. Bounded literal ASCII formats use the
existing evaluator-free integer formatter, including precision, space padding, explicit
indices and argument reuse. This does not admit BOOLEAN-object or other argument types.
Invalid flags and unverified producers keep Flink's NULL/error and lazy/empty behavior.
Flink 1.18.1 has no PRINTF and retains its released SQL rejection.

The [record](results/integer-boolean-printf-2026-11-03.json) uses released Flink 2.2.1, Oracle JDK 17.0.12, Apple M4 Pro,
one thread and release/mimalloc-linked artifacts. Equal prepared GenericRowData,
BinaryRowData and Arrow include all four integer widths, nullable/NOT NULL, signed
extrema and zero. Each trial consumes 131,072 rows, with 16 warmups/nine measurements.
The 48 cells cover short and padded formats, plus computed INT/BIGINT arithmetic.
Timers start/stop inside Rust after import and before export, retaining production Calc,
equivalent output consumption and intrinsic callbacks. Stock starts on prepared RowData;
only initial JNI/import/export and perimeter transposes are outside the small-function gate.
Complete jobs retain them.

The actual preceding encoder and formatter-admission helper, plus saved **whole DSO and
executable** from `5d46dde1`, run against the same final fixture/JDK/data. Both artifacts
retain verified mimalloc aliases. This preserves the original integer boolean format's
JDK route instead of accidentally giving the preceding certified path the new kernel.
Existing compatible certified formats are not described as universal Java callbacks.

The [four numeric-format controls](results/integer-boolean-printf-2026-11-03-retained-numeric-formats.csv.gz) and
[reverse-order replay](results/integer-boolean-printf-2026-11-03-retained-numeric-formats-reversed.csv.gz) retain
whole-runtime before/after differences for existing numeric formats. Both BIGINT controls
have lower preceding-runtime medians in both orders, by approximately 3-6%. The new-format
gains below do not establish an improvement for those unchanged numeric signatures.
An attempted non-inlined default helper does not recover them
([four diagnostic cells](results/integer-boolean-printf-2026-11-03-helper-experiment.csv.gz), initial/current
0.938-1.011x); that extra indirection is discarded. Its [rejected patch](results/integer-boolean-printf-2026-11-03-rejected-helper.patch), source/artifact hashes and successful
904-test Rust result remain separately recorded. The exact originally measured production
source, DSO and executable are restored, checked by hash, then requalified with the final
SQL tests including independently nullable indexing arguments. The residual numeric-format
cost differences still need attribution/profile work; no cause is inferred from a source
branch alone.

| Scope | Cells | Paired stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk (1,024 rows) | 40 | 4.947-7.411x | 5.723-8.108x | 5.346-7.519x |
| Tiny (16 rows) | 8 | 3.789-5.423x | 11.212-12.205x | 3.853-5.508x |

All bulk cells beat paired stock and the actual previous implementation. Tiny controls
retain 0 stock losses and 0 previous losses.
The [4,800 trials](results/integer-boolean-printf-2026-11-03.csv.gz) retain warmups, checksums and variability.
Four [numeric-format controls](results/integer-boolean-printf-2026-11-03-retained-numeric-formats.csv.gz) replay
initial/current artifacts on the same final encoder and fixture. Their initial/current
median ratios span 0.945-1.024x; all variation remains.

Eight complete million-row INT/BIGINT jobs include direct/computed short/padded formats.
They retain the bounded row source, blackhole sink, startup, JNI and both transposes.
Two warmups and five measurements alternate stock/native. Each native control requires
consumed NativeCalc work; new paths require empty evaluators and previous paths require
their original retained evaluators. Seconds, median:

| Flink / source / shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| 2.2.1 / INT-short | 0.621235 | 0.259344 | 0.606269 | 0.634951 |
| 2.2.1 / INT-padded | 0.750707 | 0.273240 | 0.784988 | 0.820727 |
| 2.2.1 / INT-short-computed | 0.597576 | 0.266733 | 0.607545 | 0.619766 |
| 2.2.1 / INT-padded-computed | 0.809255 | 0.270421 | 0.795140 | 0.804340 |
| 2.2.1 / BIGINT-short | 0.671868 | 0.266082 | 0.578773 | 0.597283 |
| 2.2.1 / BIGINT-padded | 0.828991 | 0.269570 | 0.783403 | 0.819152 |
| 2.2.1 / BIGINT-short-computed | 0.673516 | 0.257944 | 0.571059 | 0.568660 |
| 2.2.1 / BIGINT-padded-computed | 0.828161 | 0.274525 | 0.751099 | 0.753517 |

Native/stock elapsed spans **0.325-0.446**; native/previous spans
**0.329-0.454**. These are measured workloads; the prepared gate does
not establish universal complete-job acceleration.

All **716 focused JVM invocations** pass without skips across released Flink 1.18.1/2.2.1
and JDK 11/17, targeting Java release 11. Complete 5,003-row runtime maps/schema cover
integer widths, NULLs, signed extrema/zero, computed producers, boolean format precision/
padding/indexing with independently nullable arguments, all-filtered batches, remaining
generated controls and adjacent scalar/ARRAY CAST paths. Pure claims require consumed NativeCalc work and empty evaluators.
The actual preceding control compares full maps/schema then fails eight intended pure-route
assertions; its generated control passes. Two fresh unchanged released PRINTF oracle runs
pass **47 original assertions each**, separately as stock semantics. All **904 release Rust
core tests** pass, including sliced/empty/NULL inputs and input ownership. Hosted CI remains
separate. Remaining format/type/consumer scope stays in
[#497](https://github.com/datafusion-contrib/StreamFusion/issues/497).

Reproduce with release `integral_expression`,
`SF_INTEGRAL_EXPRESSION` set to `PRINTF('%b/%1$B/%1$.2b',v,v)`,
and matching signed type/nullability/row-layout arguments. The previous control supplies
its saved DSO/executable, separately compiled preceding encoder/helper and
`SF_INTEGRAL_PREVIOUS=true`. Complete jobs use `IntegralExpressionPipelineBenchmark` with
matching `sf.integral.expression`, `sf.integral.bigintInput=true` for BIGINT and previous
route settings. All resources and data otherwise match.



## Boolean-object and mixed integer PRINTF {#boolean-object-and-mixed-integer-printf}

On released Flink 2.2.1, verified total BOOLEAN producers and mixed signed-integer/BOOLEAN
arguments use the evaluator-free literal ASCII formatter. BOOLEAN `%b` observes true/false;
NULL is false. BOOLEAN `%s` uses true/false words and null for NULL. A non-NULL integer,
including zero, still formats as true with `%b`. Argument indices/reuse, uppercasing,
width, precision, NULLs and invalid-format routes retain Java Formatter semantics.
The running JDK validates each actual argument type at planning time. Explicit index zero
stays on the preceding route because JDK 11 and 17 disagree. Bounds remain 512 ASCII format
characters and width/precision at most 256. Other types/formats, unverified/failing/stateful
producers and representation-sensitive consumers keep their previous routes. Flink 1.18
has no PRINTF and retains released SQL rejection.

The [record](results/boolean-object-printf-2026-11-04.json) includes all final cells, initial slower versions,
source/artifact hashes and [16,000 raw trials](results/boolean-object-printf-2026-11-04.csv.gz).
Apple M4 Pro, macOS arm64, one thread, Oracle JDK 17.0.12, released Flink 2.2.1 and
release/mimalloc-linked DSO/executable with verified malloc aliases. Equal prepared
GenericRowData/BinaryRowData and Arrow cover true/false, nullable/NOT NULL and mixed
INT/BIGINT signed extrema/NULLs. Each trial consumes 131,072 rows, with 16 warmups and
nine measurements. Native timing starts/stops inside Rust after import and before export,
retaining production Calc, output consumption and intrinsic costs. Initial JNI/import/export
and perimeter transposes are excluded only from this small-function timer. Stock starts
on matching prepared RowData.

The actual preceding encoder/helper `1346ed57` runs with its saved **whole DSO/executable**
and the identical final fixture/JDK/data. It does not receive the new certified formatter.
New BOOLEAN controls retain their preceding evaluator; eight existing integer native controls
require empty evaluators on both versions. A retained descriptor alone does not prove a
Java callback on every batch. Independent stock measurements remain separate.

Shapes are `PRINTF('%b/%1$B',v)`, `PRINTF('%1$8.2S:%1$-8.3b',v)` and
`PRINTF('%1$+08d:%2$8.2B:%1$x:%2$s',n,v)` with INT or BIGINT n. Both row layouts and
NULL modes are retained, plus 16-row controls.

| Scope | Cells | Stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| New bulk, 1,024 rows | 16 | 4.930-7.627x | 5.632-8.099x | 4.849-7.460x |
| New tiny, 16 rows | 4 | 3.382-5.248x | 10.339-12.905x | 3.367-5.101x |

All new bulk cells beat paired stock and the actual preceding implementation. Retain
0 tiny stock losses and
0 tiny previous losses.
Eight retained nullable INT/BIGINT `%d`/`%+08d:%1$x` native controls have previous/current
median ratios **1.285-1.440x**, with all ranges and checksums retained.
Eight additional unchanged certified DECIMAL(4,2)/DECIMAL(38,18) short/padded controls retain
their evaluators on both whole runtimes; previous/current spans
**1.103-1.214x**. Their matching output digests and allocation probes are
retained separately. The new pure scope does not include these DECIMAL arguments.

The first shared-loop implementation passes correctness but adds a stable BIGINT-simple
slowdown in the recorded order (previous/current 0.931x with nonoverlapping ranges).
CPU samples locate format parsing and integer conversion/allocation; inclusive counts overlap
and do not attribute wall time to a source branch. Separately compiling Boolean-capable and
integer-only loops recovers that simple-format probe, but padded BIGINT retains lower preceding
medians in both orders. The final implementation also reuses one conversion String across
rows/conversions and writes decimal integer digits through the existing `itoa` buffer.
Boolean/NULL words and hexadecimal/octal/character output append into this buffer. General
certified DECIMAL formatting retains its owned result without an additional copy. Parsing
is still rowwise. The intermediate buffered version also enables Boolean dispatch in general certified
formatting; DECIMAL controls expose previous/current as low as 0.927x. The final general
formatter uses its preceding non-Boolean specialization, with Boolean-object evaluation
retained outside the planner-verified literal path. An ownership-only experiment also
retains slower padded medians and is discarded. When every format in a certified batch
is equal and non-NULL, the final evaluator borrows it once and reuses the Arrow builder/
row/conversion buffers. This removes per-row output String materialization; varying/NULL
formats retain row handling. Existing complete-batch ASCII/cache checks still precede
formatting, and a failed formatter still rejects the whole certificate. All slower stages
and reverse-order numeric probes remain recorded.
For the nullable GenericRowData BIGINT padded control, one warmed 1,024-row operation
drops from **2,489 allocation requests / 80,164 requested bytes** on the actual preceding
whole runtime to **29 / 48,687**; output buffer bytes remain 35,159. Allocation traffic
is diagnostic, not peak memory or proof of a complete-job gain.

Four million-row jobs retain the bounded row source, blackhole sink, startup, JNI and
both transposes. Two warmups and five measurements alternate stock/native. New paths require
consumed NativeCalc work and empty evaluators; previous retains its original evaluator.
Median seconds:

| Shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| truth | 0.449773 | 0.287959 | 0.459998 | 0.460954 |
| padded | 0.709251 | 0.303006 | 0.704850 | 0.753436 |
| mixed-int | 1.046717 | 0.340149 | 1.021757 | 1.049034 |
| mixed-bigint | 1.065773 | 0.329840 | 1.059488 | 1.135357 |

Native/stock elapsed spans **0.309-0.640**;
native/previous spans **0.291-0.625**. All trials and
variability remain, including any losses; prepared gains do not establish universal
complete-job acceleration.

All **112 final focused JVM invocations** pass without skips across released Flink
1.18.1/2.2.1 and JDK 11/17, targeting Java 11: 24 new BOOLEAN/absence-control invocations,
20 existing integer-PRINTF/absence controls, 12 format-helper invocations, 36 preceding
epoch invocations and 20 existing PRINTF/shared-certificate invocations on Flink 2.2.
Complete 5,003-row maps/schema cover true/false/NULL, all signed widths, logical/comparison
producers, mixed independently nullable arguments, literals, padding/index/reuse/precision,
empty filtered batches and explicit invalid/dynamic/failing/lazy/protected-consumer controls.
Pure-route witnesses require consumed native work, no fallback and empty evaluator descriptors.
The actual preceding Flink 2.2 controls each compare ten complete first-query maps/schema
before failing the intended pure-route assertions; subsequent empty-query checks in those
failing invocations are not reached. Retained routes and both 1.18 absence controls pass.

All **912 release Rust core tests** pass without ignores, including Boolean/NULL/signed-width
formatting, independently sliced/empty arrays, rejected contracts and input ownership.
Two unchanged released `StringFunctionsITCase.printfTestCases` stock-oracle runs each pass
all **47 original assertions**. These stock oracles are separate from the native SQL and
execution witnesses. Earlier decimal/epoch commit timing records retain their own runtime
provenance; this rebuilt DSO is not substituted into historical measurements. Hosted CI
remains separate.

Reproduce with release `integral_expression`, the exact expression/type/secondary-type/
NULL/layout/batch from the record and the saved previous encoder/helper plus whole runtime.
New preceding routes set `SF_INTEGRAL_PREVIOUS=true`; retained old-native integer controls
leave it false. Certified DECIMAL controls set it true on both runtimes. Complete jobs use
`IntegralExpressionPipelineBenchmark`, `sf.integral.booleanInput=true`, matching expression
and secondary type. Remaining scope stays in
[#497](https://github.com/datafusion-contrib/StreamFusion/issues/497).



## Numeric Boolean producers for PRINTF {#numeric-boolean-producers-for-printf}

The literal primitive formatter also accepts the exact numeric BOOLEAN producer proof
already used by character CAST/TRY_CAST: integer truth TRY_CAST over total integer trees,
direct DECIMAL truth TRY_CAST, same-precision/scale direct DECIMAL comparisons, and their
verified logical/NULL wrappers. This keeps `PRINTF('%b',0)` as true while
`PRINTF('%b',TRY_CAST(0 AS BOOLEAN))` is false. Ordinary DECIMAL-to-BOOLEAN CAST benefits
from its released comparison rewrite where supported; DECIMAL(p,p) retains the released
planning failure. Computed/mixed decimal predicates, failing/stateful inputs, protected
consumers and integer-target/rounding-position proofs retain their preceding scopes.
Only released Flink 2.2 has PRINTF; 1.18 retains its SQL rejection.

This is a Java admission extension with **no native source/build change**. The exact preceding
encoder/helper `46a1f487` and its saved whole runtime execute the same fixture/data/JDK.
Candidate native artifacts are byte-identical. Final prepared and SQL-test encoder/context
bytecode matches within Flink 2.2, and before-control bytecode matches the saved sources.
The [record](results/exact-numeric-boolean-printf-2026-11-04.json) retains sources, hashes, all cells and
[4,050 raw prepared trials](results/exact-numeric-boolean-printf-2026-11-04.csv.gz).

Apple M4 Pro, macOS arm64, one thread, JDK 17.0.12, Flink 2.2.1 and the same verified
release/mimalloc DSO/executable as the preceding BOOLEAN formatter qualification.
Equal prepared GenericRowData/BinaryRowData and Arrow cover nullable/NOT NULL
DECIMAL(4,2)/DECIMAL(38,18), extrema/zero, independently nullable mixed INT and signed
INT/BIGINT truth controls. Each trial consumes 131,072 rows with 16 warmups and nine
measurements. The production Calc/output-consumption timer starts/stops inside Rust after
import and before export, retaining intrinsic allocation, conversions and callbacks.
Only this prepared gate excludes initial JNI/import/export and perimeter transposes.
Stock starts on matching prepared RowData. New paths require empty evaluators; actual
preceding paths retain their original evaluator.

| Scope | Cells | Stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk, 1,024 rows | 32 | 4.646-8.479x | 5.200-8.644x | 4.495-8.516x |
| Tiny, 16 rows | 8 | 2.603-4.874x | 8.343-10.742x | 2.168-4.687x |

All bulk cells beat paired stock and actual preceding behavior. Retain
0 tiny stock losses and 0 tiny previous losses.
The initial candidate run completed 50 paired stock/native samples with matching digests,
but the local parser failed on the comma in the existing unquoted DECIMAL type field before
launching its preceding control. Those samples remain a separate `parser-attempt` stage;
they are not paired previous evidence. Normalizing that field before parsing and quoting
public CSV preserves the engine outputs without an engine/runtime modification.

Four million-row jobs retain the bounded row source, blackhole sink, job submission/startup,
JNI and both perimeter transposes. Environment setup and surrounding JVM process startup
are outside the job timer. Two warmups and five measurements alternate stock/native.
NativeCalc must consume work and the new routes require empty evaluators; preceding routes
keep their original evaluators. Median seconds:

| Shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| truth-DECIMAL(4,2) | 0.707504 | 0.258533 | 0.698759 | 0.696531 |
| mixed-DECIMAL(4,2) | 0.836147 | 0.324166 | 0.819374 | 0.819529 |
| truth-DECIMAL(38,18) | 0.691274 | 0.258629 | 0.661155 | 0.712982 |
| mixed-DECIMAL(38,18) | 0.835070 | 0.323816 | 0.816792 | 0.830866 |

Native/stock elapsed spans **0.365-0.388**
(0 stock losses); native/previous spans
**0.363-0.396** (0 previous losses).
All variability and independent stock measurements remain. These prepared and complete-job
results do not establish universal job acceleration.

All **76 fresh focused JVM invocations** pass without skips across released Flink
1.18.1/2.2.1 and JDK 11/17, targeting Java 11. Complete 5,003-row maps/schema cover eight
decimal precision/scale pairs, direct and comparison truth, independently nullable inputs,
modern/legacy modes, string precision/padding, mixed integer arguments, all-filtered
batches and retained invalid/failing/lazy/consumer controls. Signed-width cases also
compare a boxed integer's `%b` with its explicit BOOLEAN truth conversion. Admitted paths
require consumed NativeCalc work, no fallback and empty evaluator descriptors.

Actual preceding controls on each JDK compare sixteen complete first-query maps/schema
before the intended new pure-route assertion fails. Subsequent queries/modes/empty checks
in those failing invocations are not counted. Four other dynamic tests per JDK pass,
covering existing pure BOOLEAN routes and retained generated routes. The unchanged native
artifacts retain the preceding **912 release Rust core tests** and two unchanged released
PRINTF stock-oracle runs with **47 assertions** each; these are not new Rust/oracle runs.
Focused current JVM verification is separate from full hosted CI on the exact pushed head.

Reproduce with `integral_expression` using the record's expression/type, secondary type,
NULL/layout/batch and `SF_INTEGRAL_CORPUS=decimal-truth`; previous controls set
`SF_INTEGRAL_PREVIOUS=true` and use the saved actual encoder/helper and whole runtime.
Complete jobs use `IntegralExpressionPipelineBenchmark`, matching decimal precision/scale,
secondary type and expression. General STRING/DECIMAL printf arguments, remaining formats,
producers and consumers remain in [#497](https://github.com/datafusion-contrib/StreamFusion/issues/497).


## Length of primitive ASCII PRINTF {#length-of-primitive-ascii-printf}

On 2026-11-04, direct released built-in `CHAR_LENGTH`/`CHARACTER_LENGTH` over the
already-qualified primitive ASCII formatter reuse the native length kernel. ASCII has
identical byte, character and UTF-16 lengths, so this consumer does not need Java String
identity. The proof includes total integer/BOOLEAN and admitted exact numeric truth
producers. Operator identity prevents a same-name catalog UDF from being substituted.
Generic Unicode, invalid/dynamic formats, unverified producers, other consumers and
relational representation guards remain unchanged. No native runtime or transport change.

The [record](results/primitive-printf-length-2026-11-04.json) retains source/artifact hashes,
every configuration, allocation diagnostics and [3,000 raw trials](results/primitive-printf-length-2026-11-04.csv.gz).
The actual preceding encoder/helper is `bbada54b`, replayed on its saved whole runtime;
candidate DSO and prepared executable are byte-identical. Apple M4 Pro, macOS arm64,
one thread, JDK 17.0.12, released Flink 2.2.1, Java `--release 11`, release/mimalloc build.

Stock starts on prepared GenericRowData/BinaryRowData and native starts inside Rust on
equal prepared Arrow batches. The timer retains production Calc, consumption, allocation,
intrinsic conversions and callbacks after input import and before output export. Only
initial JNI/import/export and perimeter transposes are outside this small-function timer.
Each cell uses 131,072 logical rows, 16 warmups and nine alternating measured trials.
Both NULL modes and row layouts cover INT/BIGINT extrema, BOOLEAN/mixed INT, compact/wide
DECIMAL truth/comparisons, indexed formats and padding. Digests match for all engines.

| Scope | Cells | Stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk, 1,024 rows | 24 | 2.224-6.884x | 2.718-7.269x | 2.159-6.921x |
| Tiny, 16 rows | 6 | 1.352-4.703x | 8.432-10.516x | 1.406-5.079x |

Four complete million-row jobs retain job submission/startup, JNI and both RowData/Arrow
transposes, with bounded row sources and blackhole sinks. Each engine has two warmups and
five alternating measured jobs; the surrounding JVM startup and setup remain outside the
job timer. Median seconds:

| Shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| int-length | 0.319771 | 0.210095 | 0.318141 | 0.343298 |
| boolean-mixed | 0.762079 | 0.277136 | 0.779459 | 0.808125 |
| decimal-truth | 0.608789 | 0.226033 | 0.658311 | 0.658383 |
| decimal-comparison | 0.780619 | 0.298393 | 0.755788 | 0.810500 |

Native/stock elapsed spans **0.364-0.657** and native/previous **0.343-0.612**.
All measured bulk, tiny and complete-job cells improve; these bounded measurements do
not establish universal job acceleration. Raw trials retain variability and independent
stock controls.

All **114 final focused JVM invocations** pass, zero skips, across released Flink
1.18.1/2.2.1 and JDK 11/17. Full 5,003-row maps and resolved types cover all integer
widths, BOOLEAN, numeric truth, decimal extremes/NULLs, aliases, modern/legacy casts,
empty filtered batches, invalid/dynamic/non-ASCII formats and failing/lazy controls.
An executed same-name catalog UDF keeps its implementation. Pure routes require consumed
NativeCalc work, no fallback and empty evaluator descriptors. Each JDK's actual preceding
control compares fourteen complete first-query maps/schema before the intended pure-route
assertion fails; later queries in those failing invocations are not counted. Its retained
route and catalog UDF controls pass. An initial local UDF control needed quoted SQL
identifiers; final controls use the corrected test with no production-code change.

Four unchanged released `ScalarFunctionsTest.testCharLength` stock-oracle runs pass
separately, one per release/JDK pair. The byte-identical native artifacts retain their
earlier 912-core-test and PRINTF-oracle qualifications; these are not new Rust/PRINTF
oracle runs. Full hosted suites must qualify the exact pushed head separately.

Reproduce with release `integral_expression`, the record's expression/type/secondary type,
NULL/layout/batch and `SF_INTEGRAL_CORPUS=decimal-truth`. Previous controls set
`SF_INTEGRAL_PREVIOUS=true` and use the saved actual preceding encoder/helper and runtime.
Complete jobs use `IntegralExpressionPipelineBenchmark` with matching input and expression
properties. Remaining consumers/representations stay in
[#497](https://github.com/datafusion-contrib/StreamFusion/issues/497).


## Comparisons of primitive ASCII PRINTF {#comparisons-of-primitive-ascii-printf}

On 2026-11-04, direct built-in `=`, `<>`, `<`, `<=`, `>`, `>=` comparisons use native
kernels when both sides are qualified primitive ASCII formatter results or ASCII/NULL
character literals, with at least one formatter. Cached Java UTF-16 and serialized UTF-8
have identical order on this proven subset. Native filters can consume the predicates.
This Java-only admission proof preserves built-in identity and the complete existing
formatter producer proof. Arbitrary STRING inputs, non-ASCII literals, unknown/invalid
formats, other consumers and relational representation guards retain their prior paths.

The [record](results/primitive-printf-comparison-2026-11-04.json) retains every cell, source/
artifact hashes, variability, route evidence and [3,000 raw trials](results/primitive-printf-comparison-2026-11-04.csv.gz).
Apple M4 Pro, macOS arm64, one thread, JDK 17.0.12, released Flink 2.2.1, Java release 11,
release/mimalloc. The DSO and prepared executable are byte-identical to the qualified
preceding runtime; the actual previous encoder/helper is `40f26521`.

Prepared stock starts on equal GenericRowData/BinaryRowData and native starts inside Rust
on prebuilt Arrow. Each cell has 131,072 logical rows, 16 warmups and nine alternating
measurements. The Rust timer retains production Calc, consumption, allocation, intrinsic
conversions and callbacks after import and before export. Only initial JNI/import/export
and perimeter transposes are outside this small-function timer. Signed INT/BIGINT extrema,
BOOLEAN/mixed INT, compact/wide DECIMAL truth/comparisons, both NULL modes and row layouts
are covered; all engine digests match.

The previous route is verified per shape. Equality/inequality retain a native Calc with
the generated evaluator on the saved whole runtime. Ordered prepared controls verify
the actual previous whole-Calc stock fallback and exact representation reason, then time
the released generated evaluator on prepared RowData using the existing fallback fixture.
They do not substitute a hypothetical native callback for a rejected signature.

| Scope | Cells | Stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk, 1,024 rows | 24 | 1.420-6.704x | 2.192-7.800x | 1.464-7.000x |
| Tiny, 16 rows | 6 | 0.774-4.163x | 3.509-9.424x | 0.627-4.129x |

All bulk cells pass the prepared bulk-function gate. The 16-row INT equality control
loses to stock; its timing and independent stock control remain in the record. The other
five tiny controls improve. No universal small-batch improvement is claimed.

Four complete million-row jobs retain job submission/startup, JNI and both transposes,
using bounded external Row sources and blackhole sinks. The external source proves
Java-backed inputs to the preceding planner, so its ordered complete-job controls retain
the actual native Calc with generated evaluation. This differs from the conservative
prepared/internal-RowData fallback. Both runtimes must consume native work, while the new
route additionally requires empty evaluators. Each engine has two warmups and five
alternating measured jobs; surrounding JVM startup and setup are outside timing. Median seconds:

| Shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| int-order | 0.218030 | 0.197722 | 0.202463 | 0.232047 |
| boolean-mixed-order | 0.795598 | 0.299646 | 0.748803 | 0.744022 |
| decimal-truth | 0.567530 | 0.232338 | 0.595066 | 0.585824 |
| decimal-comparison-order | 0.707602 | 0.300004 | 0.710702 | 0.776674 |

Native/stock elapsed spans **0.377-0.907**, native/previous **0.386-0.852**.
All four measured complete-job medians improve; independent controls and variability
remain, and the bounded qualification does not imply every job will improve.

All **148 final focused JVM invocations** pass, zero skips, on released Flink
1.18.1/2.2.1 and JDK 11/17. Complete 5,003-row maps/schema cover all six comparisons,
operand reversal, two formatter operands, all integer widths, BOOLEAN, DECIMAL truth,
NULLs, padding, empty filtered batches and the retained Unicode/format/failing/lazy paths.
An executed filter compares every surviving ID/value with stock. Admitted paths require
consumed NativeCalc work, no fallback and empty evaluators. The previous encoder compares
fourteen complete first-query projection maps/schema plus the filter result per JDK before
fifteen expected route failures. Later queries/modes in those failing invocations are not
counted. The unverified-path control passes with its explicit fallback/generated expectations.
An initial residual test expected generated Calc where the existing guard correctly used
whole-Calc fallback; only that test expectation changed.

Four unchanged released stock-oracle invocations each pass `ScalarOperatorsTest.testCompareOperator`
and `testMoreEqualAndNonEqual`, separately from native qualification. The unchanged native
artifacts keep their earlier 912-core-test/PRINTF-oracle qualification; those are not new
Rust/PRINTF runs. Hosted suites must qualify the exact pushed head separately.

Reproduce prepared cells with release `integral_expression` and the record's type/expression,
NULL/layout/batch/secondary type and `SF_INTEGRAL_CORPUS=decimal-truth`. Previous generated
controls set `SF_INTEGRAL_PREVIOUS=true` on the saved encoder/helper/runtime. Ordered
previous controls run `IntegralExpressionBenchmarkFunctions` with `sf.integral.previous=true`,
`sf.integral.fallbackControl=true` and the recorded exact fallback reason. Complete jobs
use `IntegralExpressionPipelineBenchmark` with matching inputs and expressions. Remaining
scope stays in [#497](https://github.com/datafusion-contrib/StreamFusion/issues/497) and
[#508](https://github.com/datafusion-contrib/StreamFusion/issues/508).


## Integer text producers for PRINTF {#integer-text-producers-for-printf}

On 2026-11-04, unbounded STRING CAST/TRY_CAST of a proven total integer producer can
feed the primitive formatter without materializing intermediate text. The planner first
validates conversions against the logical STRING argument. Only then does it emit the
underlying integer: `%s`/`%S` and `%b`/`%B` have the same result for its decimal text,
including NULLs, precision, padding and indexing. Numeric conversions remain invalid for
STRING; their NULL-specific released behavior stays on the preceding route. Bounded
character targets, BOOLEAN/DECIMAL text and unverified/failing producers are excluded.
In particular, BOOLEAN false formatted with `%b` differs from its non-NULL text `false`.

This Java-only elision reuses the byte-identical native runtime and its buffered formatter.
Existing direct length/comparison consumers reuse the same ASCII proof; it does not widen
general STRING/representation admission. The [record](results/integer-text-printf-2026-11-04.json)
retains all configurations, hashes, allocation diagnostics and [3,500 raw trials](results/integer-text-printf-2026-11-04.csv.gz).
Apple M4 Pro, macOS arm64, one thread, JDK 17.0.12, released Flink 2.2.1, Java release 11,
release/mimalloc. Actual preceding encoder/helper `5e18ac8d` runs on its saved whole
DSO/executable, including its generated evaluator; measured and validated bytecode match.

Prepared stock starts on GenericRowData/BinaryRowData and native starts inside Rust on
equal prebuilt Arrow. Each cell has 131,072 logical rows, 16 warmups and nine alternating
measurements. Production Calc, output consumption, allocation, intrinsic conversions and
callbacks remain inside the Rust timer after import and before export. Initial JNI/import/
export and perimeter transposes are outside this small-function timer only. Four signed
integer widths, both NULL modes/layouts, computed wrapping, mixed BOOLEAN/integer arguments,
general string/Boolean formats, indexing, precision and padding are covered; all digests match.

| Scope | Cells | Stock / native | Actual previous / native | Independent stock / native |
| --- | ---: | ---: | ---: | ---: |
| Bulk, 1,024 rows | 28 | 2.059-7.947x | 3.023-8.367x | 2.102-8.084x |
| Tiny, 16 rows | 7 | 1.184-5.362x | 8.791-12.787x | 1.138-5.676x |

Four complete million-row jobs use bounded external Row sources and blackhole sinks,
retaining job submission/startup, JNI and both transposes. Each engine has two warmups and
five alternating measured jobs; surrounding JVM startup and setup are outside timing.
Candidate and preceding native operators must consume work; only the new route requires
empty evaluators. Median seconds:

| Shape | Paired stock | Native | Independent stock | Actual previous |
| --- | ---: | ---: | ---: | ---: |
| int-text | 0.267157 | 0.225393 | 0.268707 | 0.292513 |
| bigint-text | 0.691436 | 0.250366 | 0.622890 | 0.655572 |
| boolean-and-text | 0.816311 | 0.279250 | 0.753133 | 0.738243 |
| computed-text | 0.668061 | 0.249539 | 0.682569 | 0.675910 |

Native/stock elapsed spans **0.342-0.844** and native/previous **0.369-0.771**.
All measured cells improve, with raw variability and independent stock controls retained.
These gains qualify this bounded feature, not universal job acceleration or the earlier
16-row comparison regression, which remains in its own record.

All **172 final focused JVM invocations** pass without skips across released Flink
1.18.1/2.2.1 and JDK 11/17, targeting Java 11. Full 5,003-row maps/schema cover CAST and
TRY_CAST, signed extrema/NULLs, modern/legacy modes, arithmetic wrapping, mixed BOOLEAN,
length/comparison consumers, all-filtered batches, invalid STRING numeric conversions,
bounded/BOOLEAN/DECIMAL exclusions and failing/lazy/unused arguments. Admitted paths
require consumed NativeCalc work, no fallback and empty evaluators. On each JDK the actual
previous encoder compares eight first-query maps/schema, then fails the intended route
assertions; later legacy/empty queries in those invocations are not counted. Those broad
queries include ordered consumers and encounter the preceding internal-RowData whole-Calc
fallback. Its residual dynamic control passes; prepared direct-PRINTF controls independently
verify and time its generated native Calc route.

The unchanged DSO/executable retain their earlier 912-core-test and released PRINTF stock-
oracle qualification; these are not new Rust/oracle runs. Flink 1.18's lack of PRINTF remains
an absence control, not a native gap. Hosted suites must qualify the exact pushed head.

Reproduce prepared measurements with release `integral_expression` and the record's
type/expression, secondary type, NULL/layout/batch and `SF_INTEGRAL_CORPUS=decimal-truth`.
Previous controls set `SF_INTEGRAL_PREVIOUS=true` with the actual saved encoder/helper/runtime.
Complete jobs use `IntegralExpressionPipelineBenchmark`, matching boolean/BIGINT input,
secondary type and expression properties. Remaining direct STRING/DECIMAL arguments,
formats and consumers stay in [#497](https://github.com/datafusion-contrib/StreamFusion/issues/497).

