# Scalar function benchmarks

| Question | Start here |
| --- | --- |
| Is a function currently admitted? | [Calc coverage](../operators/calc-filter.md) |
| What is the recorded fixture and timing method? | [Inputs](scalar-functions-diagnostics.md#inputs), [reproduction](scalar-functions-diagnostics.md#reproduce), [raw data](scalar-functions-diagnostics.md#raw-data) |
| What was measured for collection and metadata extensions? | [Extended coverage](scalar-functions-diagnostics.md#extended-function-coverage) |
| Where are the SQL/JSON comparisons? | [SQL/JSON measurements](scalar-functions-json.md#sqljson-measurements), [nested boundaries](scalar-functions-json.md#nested-sqljson-batch-boundaries-2026-09-19), [string consumers](scalar-functions-json.md#json-string-consumers) |
| Which measurements are experiments or diagnostics? | [Evidence index](evidence.md), [array extrema experiment](scalar-functions-collections.md#array-extrema-admission-experiment-2026-09-28), [BINARY exit experiments](scalar-functions-integers.md#rejected-manual-binary-exit-projection) |

Each section records its own implementation and environment. Historical timings
and fallback observations remain evidence for that cohort; use the coverage page
for the current supported contract.

The original measurements below were made on 2026-09-09 against the production implementation in `69122d2b`, using Apple M4 Pro,
JDK 17, UTC, Flink 2.2.1 and DataFusion 54.0.0. The native library uses the standard Maven
`bench` profile: release mode, mimalloc, and the development connector features.

Every query evaluates one function over 2,000,000 rows at parallelism 1. Both engines run
serially in the same JVM for each scenario, alternating which engine runs first on each trial.
Each engine gets two warmups and five measured trials per case; the tables show median elapsed
seconds. Each scenario uses a fresh JVM. No other benchmark or test suite runs concurrently.

Times include SQL planning/execution, a rowwise DataStream source, and a rowwise blackhole
sink. The harness checks that every native function and identity-control plan includes
`NativeCalc`, `RowDataToArrow`, and `ArrowToRowData`. Ratios are Flink time divided by native
time: above 1 means native is faster for that workload; below 1 means it is slower. Small
differences can be run-to-run noise, and these measurements do not isolate kernel cost.

The original 48 cases cover 35 retained functions, including integer widths and literal/column
search parameters. All cases run with the 264-byte ASCII/non-null and Unicode/NULL scenarios;
the ten search cases also run with 8-byte ASCII padding, giving 106 Flink/native comparisons.
TO_TIMESTAMP and temporal FLOOR/CEIL/CEILING were outside that measurement's coverage. Their
subsequent implementation is measured in the [temporal diagnostic below](scalar-functions-temporal.md#temporal-coverage-diagnostic-2026-09-15).
See [Calc / filter](../operators/calc-filter.md) for the complete argument gates.

## Topic pages

- [Diagnostics](scalar-functions-diagnostics.md)
- [Json](scalar-functions-json.md)
- [Decimal](scalar-functions-decimal.md)
- [Strings](scalar-functions-strings.md)
- [Temporal](scalar-functions-temporal.md)
- [Collections](scalar-functions-collections.md)
- [Integers](scalar-functions-integers.md)
- [Boolean](scalar-functions-boolean.md)
- [Printf](scalar-functions-printf.md)

## Published section links

Existing bookmarks remain valid. Follow a section to its topic page.

<a id="extended-function-coverage"></a>

[Extended function coverage](scalar-functions-diagnostics.md#extended-function-coverage)

<a id="sqljson-native-first-routing-2026-09-18"></a>

[SQL/JSON native-first routing (2026-09-18)](scalar-functions-json.md#sqljson-native-first-routing-2026-09-18)

<a id="sqljson-jvm-bridge-prototype-2026-09-18"></a>

[SQL/JSON JVM bridge prototype (2026-09-18)](scalar-functions-json.md#sqljson-jvm-bridge-prototype-2026-09-18)

<a id="nested-sqljson-batch-boundaries-2026-09-19"></a>

[Nested SQL/JSON batch boundaries (2026-09-19)](scalar-functions-json.md#nested-sqljson-batch-boundaries-2026-09-19)

<a id="spaced-json-paths-diagnostic-2026-09-16"></a>

[Spaced JSON paths diagnostic (2026-09-16)](scalar-functions-json.md#spaced-json-paths-diagnostic-2026-09-16)

<a id="empty-json-member-diagnostic-2026-09-16"></a>

[Empty JSON member diagnostic (2026-09-16)](scalar-functions-json.md#empty-json-member-diagnostic-2026-09-16)

<a id="decimal-udf-consumer-diagnostic-2026-09-16"></a>

[DECIMAL UDF consumer diagnostic (2026-09-16)](scalar-functions-decimal.md#decimal-udf-consumer-diagnostic-2026-09-16)

<a id="floating-conversion-and-power-diagnostic-2026-09-16"></a>

[Floating conversion and POWER diagnostic (2026-09-16)](scalar-functions-diagnostics.md#floating-conversion-and-power-diagnostic-2026-09-16)

<a id="inputs"></a>

[Inputs](scalar-functions-diagnostics.md#inputs)

<a id="reproduce"></a>

[Reproduce](scalar-functions-diagnostics.md#reproduce)

<a id="raw-data"></a>

[Raw data](scalar-functions-diagnostics.md#raw-data)

<a id="ifnull-coverage-diagnostic-2026-09-16"></a>

[IFNULL coverage diagnostic (2026-09-16)](scalar-functions-diagnostics.md#ifnull-coverage-diagnostic-2026-09-16)

<a id="string-to-boolean-coverage-diagnostic-2026-09-16"></a>

[STRING to BOOLEAN coverage diagnostic (2026-09-16)](scalar-functions-strings.md#string-to-boolean-coverage-diagnostic-2026-09-16)

<a id="instr-overloads-diagnostic-2026-09-16"></a>

[INSTR overloads diagnostic (2026-09-16)](scalar-functions-diagnostics.md#instr-overloads-diagnostic-2026-09-16)

<a id="exact-decimal-coverage-diagnostic-2026-09-16"></a>

[Exact DECIMAL coverage diagnostic (2026-09-16)](scalar-functions-decimal.md#exact-decimal-coverage-diagnostic-2026-09-16)

<a id="startswith"></a>

[STARTSWITH](scalar-functions-strings.md#startswith)

<a id="endswith"></a>

[ENDSWITH](scalar-functions-strings.md#endswith)

<a id="instr"></a>

[INSTR](scalar-functions-strings.md#instr)

<a id="locate"></a>

[LOCATE](scalar-functions-strings.md#locate)

<a id="bin"></a>

[BIN](scalar-functions-strings.md#bin)

<a id="hex"></a>

[HEX](scalar-functions-strings.md#hex)

<a id="to_base64"></a>

[TO_BASE64](scalar-functions-strings.md#to_base64)

<a id="unhex"></a>

[UNHEX](scalar-functions-strings.md#unhex)

<a id="greatest"></a>

[GREATEST](scalar-functions-strings.md#greatest)

<a id="least"></a>

[LEAST](scalar-functions-strings.md#least)

<a id="initcap"></a>

[INITCAP](scalar-functions-strings.md#initcap)

<a id="translate"></a>

[TRANSLATE](scalar-functions-strings.md#translate)

<a id="btrim"></a>

[BTRIM](scalar-functions-strings.md#btrim)

<a id="elt"></a>

[ELT](scalar-functions-strings.md#elt)

<a id="url_encode"></a>

[URL_ENCODE](scalar-functions-strings.md#url_encode)

<a id="overlay"></a>

[OVERLAY](scalar-functions-strings.md#overlay)

<a id="url_decode"></a>

[URL_DECODE](scalar-functions-strings.md#url_decode)

<a id="encode"></a>

[ENCODE](scalar-functions-strings.md#encode)

<a id="decode"></a>

[DECODE](scalar-functions-strings.md#decode)

<a id="json_quote"></a>

[JSON_QUOTE](scalar-functions-json.md#json_quote)

<a id="json_unquote"></a>

[JSON_UNQUOTE](scalar-functions-json.md#json_unquote)

<a id="split"></a>

[SPLIT](scalar-functions-strings.md#split)

<a id="substring"></a>

[SUBSTRING](scalar-functions-strings.md#substring)

<a id="left"></a>

[LEFT](scalar-functions-strings.md#left)

<a id="right"></a>

[RIGHT](scalar-functions-strings.md#right)

<a id="lpad"></a>

[LPAD](scalar-functions-strings.md#lpad)

<a id="rpad"></a>

[RPAD](scalar-functions-strings.md#rpad)

<a id="split_index"></a>

[SPLIT_INDEX](scalar-functions-strings.md#split_index)

<a id="integer-delimiter-split-index"></a>

[Integer-Delimiter Split Index](scalar-functions-strings.md#integer-delimiter-split-index)

<a id="to_date"></a>

[TO_DATE](scalar-functions-temporal.md#to_date)

<a id="quarter"></a>

[QUARTER](scalar-functions-temporal.md#quarter)

<a id="week"></a>

[WEEK](scalar-functions-temporal.md#week)

<a id="dayofyear"></a>

[DAYOFYEAR](scalar-functions-temporal.md#dayofyear)

<a id="dayofweek"></a>

[DAYOFWEEK](scalar-functions-temporal.md#dayofweek)

<a id="runtime-sha2-widths"></a>

[Runtime SHA2 Widths](scalar-functions-strings.md#runtime-sha2-widths)

<a id="ltrim"></a>

[LTRIM](scalar-functions-strings.md#ltrim)

<a id="rtrim"></a>

[RTRIM](scalar-functions-strings.md#rtrim)

<a id="sqljson-measurements"></a>

[SQL/JSON measurements](scalar-functions-json.md#sqljson-measurements)

<a id="json_value"></a>

[JSON_VALUE](scalar-functions-json.md#json_value)

<a id="json_exists"></a>

[JSON_EXISTS](scalar-functions-json.md#json_exists)

<a id="to_base64-binary-input"></a>

[TO_BASE64 binary input](scalar-functions-strings.md#to_base64-binary-input)

<a id="encode-utf-16-charsets"></a>

[ENCODE UTF-16 charsets](scalar-functions-strings.md#encode-utf-16-charsets)

<a id="decode-utf-16-charsets"></a>

[DECODE UTF-16 charsets](scalar-functions-strings.md#decode-utf-16-charsets)

<a id="json_string-scalars"></a>

[JSON_STRING scalars](scalar-functions-json.md#json_string-scalars)

<a id="json_object-scalar-values"></a>

[JSON_OBJECT scalar values](scalar-functions-json.md#json_object-scalar-values)

<a id="trim-directions-and-literal-sets"></a>

[TRIM directions and literal sets](scalar-functions-strings.md#trim-directions-and-literal-sets)

<a id="sha1"></a>

[SHA1](scalar-functions-strings.md#sha1)

<a id="is-json-predicates"></a>

[IS JSON predicates](scalar-functions-json.md#is-json-predicates)

<a id="json_value-returning"></a>

[JSON_VALUE RETURNING](scalar-functions-json.md#json_value-returning)

<a id="temporal-coverage-diagnostic-2026-09-15"></a>

[Temporal coverage diagnostic (2026-09-15)](scalar-functions-temporal.md#temporal-coverage-diagnostic-2026-09-15)

<a id="byte-characters-and-json-extensions-2026-09-15"></a>

[Byte characters and JSON extensions (2026-09-15)](scalar-functions-json.md#byte-characters-and-json-extensions-2026-09-15)

<a id="ascii"></a>

[ASCII](scalar-functions-json.md#ascii)

<a id="chr"></a>

[CHR](scalar-functions-json.md#chr)

<a id="json_string-decimal"></a>

[JSON_STRING DECIMAL](scalar-functions-json.md#json_string-decimal)

<a id="json_object-decimal"></a>

[JSON_OBJECT DECIMAL](scalar-functions-json.md#json_object-decimal)

<a id="unicode-json-member-paths"></a>

[Unicode JSON member paths](scalar-functions-json.md#unicode-json-member-paths)

<a id="decimal-and-varbinary-scalar-udfs"></a>

[DECIMAL and VARBINARY scalar UDFs](scalar-functions-json.md#decimal-and-varbinary-scalar-udfs)

<a id="from_unixtime-literal-formats"></a>

[FROM_UNIXTIME literal formats](scalar-functions-temporal.md#from_unixtime-literal-formats)

<a id="json-string-consumers"></a>

[JSON string consumers](scalar-functions-json.md#json-string-consumers)

<a id="negative-json-array-indexes"></a>

[Negative JSON array indexes](scalar-functions-json.md#negative-json-array-indexes)

<a id="json_value-exception-parity-2026-09-19"></a>

[JSON_VALUE exception parity (2026-09-19)](scalar-functions-json.md#json_value-exception-parity-2026-09-19)

<a id="integer-array_distinct-2026-09-28"></a>

[Integer ARRAY_DISTINCT (2026-09-28)](scalar-functions-collections.md#integer-array_distinct-2026-09-28)

<a id="array-extrema-admission-experiment-2026-09-28"></a>

[Array extrema admission experiment (2026-09-28)](scalar-functions-collections.md#array-extrema-admission-experiment-2026-09-28)

<a id="double-truncate-consumer-and-nullable-workloads"></a>

[DOUBLE TRUNCATE consumer and nullable workloads](scalar-functions-integers.md#double-truncate-consumer-and-nullable-workloads)

<a id="rejected-manual-binary-exit-projection"></a>

[Rejected manual BINARY exit projection](scalar-functions-integers.md#rejected-manual-binary-exit-projection)

<a id="rejected-generated-binary-segment-projection"></a>

[Rejected generated BINARY segment projection](scalar-functions-integers.md#rejected-generated-binary-segment-projection)

<a id="map-sqljson-batch-boundaries-2026-09-19"></a>

[MAP SQL/JSON batch boundaries (2026-09-19)](scalar-functions-json.md#map-sqljson-batch-boundaries-2026-09-19)

<a id="map-scalar-callback-costs"></a>

[MAP scalar callback costs](scalar-functions-collections.md#map-scalar-callback-costs)

<a id="final-json_value-and-union-all-2026-10-22"></a>

[Final JSON_VALUE and UNION ALL (2026-10-22)](scalar-functions-json.md#final-json_value-and-union-all-2026-10-22)

<a id="exact-numeric-array-casts"></a>

[Exact numeric ARRAY casts](scalar-functions-collections.md#exact-numeric-array-casts)

<a id="standalone-collection-programs"></a>

[Standalone collection programs](scalar-functions-collections.md#standalone-collection-programs)

<a id="collection-character-casts"></a>

[Collection character casts](scalar-functions-collections.md#collection-character-casts)

<a id="fixed-binary-map-values"></a>

[Fixed BINARY map values](scalar-functions-collections.md#fixed-binary-map-values)

<a id="fixed-binary-map-keys"></a>

[Fixed BINARY map keys](scalar-functions-collections.md#fixed-binary-map-keys)

<a id="multiset-casts"></a>

[MULTISET casts](scalar-functions-collections.md#multiset-casts)

<a id="fixed-binary-multiset-elements"></a>

[Fixed BINARY MULTISET elements](scalar-functions-collections.md#fixed-binary-multiset-elements)

<a id="not-null-fixed-binary-map-leaves"></a>

[NOT NULL fixed BINARY map leaves](scalar-functions-collections.md#not-null-fixed-binary-map-leaves)

<a id="runtime-array_sort-options-2026-10-27"></a>

[Runtime ARRAY_SORT options (2026-10-27)](scalar-functions-collections.md#runtime-array_sort-options-2026-10-27)

<a id="integral-sign"></a>

[Integral SIGN](scalar-functions-integers.md#integral-sign)

<a id="integral-abs"></a>

[Integral ABS](scalar-functions-integers.md#integral-abs)

<a id="integral-unary-minus"></a>

[Integral unary minus](scalar-functions-integers.md#integral-unary-minus)

<a id="integer-floor"></a>

[Integer FLOOR](scalar-functions-integers.md#integer-floor)

<a id="integer-ceil-ceiling"></a>

[Integer CEIL / CEILING](scalar-functions-integers.md#integer-ceil-ceiling)

<a id="single-argument-integer-round"></a>

[Single-argument integer ROUND](scalar-functions-integers.md#single-argument-integer-round)

<a id="single-argument-integer-truncate"></a>

[Single-argument integer TRUNCATE](scalar-functions-integers.md#single-argument-integer-truncate)

<a id="decimal-abs"></a>

[DECIMAL ABS](scalar-functions-decimal.md#decimal-abs)

<a id="decimal-unary-minus"></a>

[DECIMAL unary minus](scalar-functions-decimal.md#decimal-unary-minus)

<a id="external-structured-collection-inputs"></a>

[External STRUCTURED collection inputs](scalar-functions-collections.md#external-structured-collection-inputs)

<a id="decimal-sign"></a>

[DECIMAL SIGN](scalar-functions-decimal.md#decimal-sign)

<a id="prepared-operation"></a>

[Prepared operation](scalar-functions-decimal.md#prepared-operation)

<a id="complete-row-source-job"></a>

[Complete row-source job](scalar-functions-decimal.md#complete-row-source-job)

<a id="validation-and-reproduction"></a>

[Validation and reproduction](scalar-functions-decimal.md#validation-and-reproduction)

<a id="fixed-binary-base64-encoding"></a>

[Fixed binary Base64 encoding](scalar-functions-strings.md#fixed-binary-base64-encoding)

<a id="from_base64"></a>

[FROM_BASE64](scalar-functions-strings.md#from_base64)

<a id="runtime-sql-trim"></a>

[Runtime SQL TRIM](scalar-functions-strings.md#runtime-sql-trim)

<a id="integer-round-with-a-literal-position"></a>

[Integer ROUND with a literal position](scalar-functions-integers.md#integer-round-with-a-literal-position)

<a id="integer-truncate-with-a-literal-position"></a>

[Integer TRUNCATE with a literal position](scalar-functions-integers.md#integer-truncate-with-a-literal-position)

<a id="decimal-to-string"></a>

[DECIMAL to STRING](scalar-functions-decimal.md#decimal-to-string)

<a id="binary-startswith"></a>

[Binary STARTSWITH](scalar-functions-strings.md#binary-startswith)

<a id="binary-endswith"></a>

[Binary ENDSWITH](scalar-functions-strings.md#binary-endswith)

<a id="binary-to-string"></a>

[Binary to STRING](scalar-functions-strings.md#binary-to-string)

<a id="complete-row-source-jobs"></a>

[Complete row-source jobs](scalar-functions-strings.md#complete-row-source-jobs)

<a id="profiling-and-validation"></a>

[Profiling and validation](scalar-functions-strings.md#profiling-and-validation)

<a id="reproduce_1"></a>

[Reproduce](scalar-functions-strings.md#reproduce_1)

<a id="modern-bounded-charvarchar-targets"></a>

[Modern bounded CHAR/VARCHAR targets](scalar-functions-strings.md#modern-bounded-charvarchar-targets)

<a id="latest-main-bounded-cast-requalification-2026-11-03"></a>

[Latest-main bounded-cast requalification (2026-11-03)](scalar-functions-strings.md#latest-main-bounded-cast-requalification-2026-11-03)

<a id="decimal-to-string-try_cast"></a>

[DECIMAL to STRING TRY_CAST](scalar-functions-decimal.md#decimal-to-string-try_cast)

<a id="string-is_alpha"></a>

[STRING IS_ALPHA](scalar-functions-strings.md#string-is_alpha)

<a id="string-is_digit"></a>

[STRING IS_DIGIT](scalar-functions-strings.md#string-is_digit)

<a id="string-is_decimal"></a>

[STRING IS_DECIMAL](scalar-functions-strings.md#string-is_decimal)

<a id="declared-not-null-classification-controls"></a>

[Declared NOT NULL classification controls](scalar-functions-strings.md#declared-not-null-classification-controls)

<a id="integer-abs-over-arithmetic"></a>

[Integer ABS over arithmetic](scalar-functions-integers.md#integer-abs-over-arithmetic)

<a id="integer-unary-minus-over-arithmetic"></a>

[Integer unary minus over arithmetic](scalar-functions-integers.md#integer-unary-minus-over-arithmetic)

<a id="integer-sign-over-arithmetic"></a>

[Integer SIGN over arithmetic](scalar-functions-integers.md#integer-sign-over-arithmetic)

<a id="integer-floor-over-arithmetic"></a>

[Integer FLOOR over arithmetic](scalar-functions-integers.md#integer-floor-over-arithmetic)

<a id="integer-ceil-over-arithmetic"></a>

[Integer CEIL over arithmetic](scalar-functions-integers.md#integer-ceil-over-arithmetic)

<a id="integer-round-over-arithmetic"></a>

[Integer ROUND over arithmetic](scalar-functions-integers.md#integer-round-over-arithmetic)

<a id="integer-truncate-over-arithmetic"></a>

[Integer TRUNCATE over arithmetic](scalar-functions-integers.md#integer-truncate-over-arithmetic)

<a id="numeric-is_alpha"></a>

[Numeric IS_ALPHA](scalar-functions-strings.md#numeric-is_alpha)

<a id="numeric-is_digit"></a>

[Numeric IS_DIGIT](scalar-functions-strings.md#numeric-is_digit)

<a id="numeric-is_decimal"></a>

[Numeric IS_DECIMAL](scalar-functions-decimal.md#numeric-is_decimal)

<a id="boolean-cast-to-character"></a>

[BOOLEAN CAST to character](scalar-functions-boolean.md#boolean-cast-to-character)

<a id="boolean-try_cast-to-character"></a>

[BOOLEAN TRY_CAST to character](scalar-functions-boolean.md#boolean-try_cast-to-character)

<a id="boolean-cast-over-logical-operands"></a>

[BOOLEAN CAST over logical operands](scalar-functions-boolean.md#boolean-cast-over-logical-operands)

<a id="boolean-try_cast-over-logical-operands"></a>

[BOOLEAN TRY_CAST over logical operands](scalar-functions-boolean.md#boolean-try_cast-over-logical-operands)

<a id="bounded-decimal-cast-to-character"></a>

[Bounded DECIMAL CAST to character](scalar-functions-decimal.md#bounded-decimal-cast-to-character)

<a id="bounded-decimal-try_cast-to-character"></a>

[Bounded DECIMAL TRY_CAST to character](scalar-functions-decimal.md#bounded-decimal-try_cast-to-character)

<a id="boolean-cast-over-integer-comparisons"></a>

[BOOLEAN CAST over integer comparisons](scalar-functions-boolean.md#boolean-cast-over-integer-comparisons)

<a id="boolean-try_cast-over-integer-comparisons"></a>

[BOOLEAN TRY_CAST over integer comparisons](scalar-functions-boolean.md#boolean-try_cast-over-integer-comparisons)

<a id="decimal-unary-cast-to-character"></a>

[DECIMAL unary CAST to character](scalar-functions-decimal.md#decimal-unary-cast-to-character)

<a id="decimal-unary-try_cast-to-character"></a>

[DECIMAL unary TRY_CAST to character](scalar-functions-decimal.md#decimal-unary-try_cast-to-character)

<a id="nullable-decimal-arithmetic-cast-to-character"></a>

[Nullable DECIMAL arithmetic CAST to character](scalar-functions-decimal.md#nullable-decimal-arithmetic-cast-to-character)

<a id="decimal-arithmetic-try_cast-to-character"></a>

[DECIMAL arithmetic TRY_CAST to character](scalar-functions-decimal.md#decimal-arithmetic-try_cast-to-character)

<a id="decimal-abs-over-arithmetic"></a>

[DECIMAL ABS over arithmetic](scalar-functions-decimal.md#decimal-abs-over-arithmetic)

<a id="decimal-negation-over-arithmetic"></a>

[DECIMAL negation over arithmetic](scalar-functions-decimal.md#decimal-negation-over-arithmetic)

<a id="decimal-sign-over-arithmetic"></a>

[DECIMAL SIGN over arithmetic](scalar-functions-decimal.md#decimal-sign-over-arithmetic)

<a id="mixed-exact-greatest"></a>

[Mixed exact GREATEST](scalar-functions-decimal.md#mixed-exact-greatest)

<a id="mixed-exact-least"></a>

[Mixed exact LEAST](scalar-functions-decimal.md#mixed-exact-least)

<a id="mixed-exact-if"></a>

[Mixed exact IF](scalar-functions-decimal.md#mixed-exact-if)

<a id="mixed-selection-latest-main-regression"></a>

[Mixed selection latest-main regression](scalar-functions-integers.md#mixed-selection-latest-main-regression)

<a id="utc-ltz-calendar-floor"></a>

[UTC LTZ calendar FLOOR](scalar-functions-temporal.md#utc-ltz-calendar-floor)

<a id="utc-ltz-calendar-ceil"></a>

[UTC LTZ calendar CEIL](scalar-functions-temporal.md#utc-ltz-calendar-ceil)

<a id="bigint-epoch-millisecond-conversion"></a>

[BIGINT epoch millisecond conversion](scalar-functions-temporal.md#bigint-epoch-millisecond-conversion)

<a id="function-allocator-provenance-and-matched-if-controls"></a>

[Function allocator provenance and matched IF controls](scalar-functions-integers.md#function-allocator-provenance-and-matched-if-controls)

<a id="bigint-epoch-second-conversion"></a>

[BIGINT epoch second conversion](scalar-functions-temporal.md#bigint-epoch-second-conversion)

<a id="retained-millisecond-controls"></a>

[Retained millisecond controls](scalar-functions-temporal.md#retained-millisecond-controls)

<a id="narrow-integer-epoch-conversion"></a>

[Narrow integer epoch conversion](scalar-functions-temporal.md#narrow-integer-epoch-conversion)

<a id="utc-ltz-fixed-unit-floor"></a>

[UTC LTZ fixed-unit FLOOR](scalar-functions-temporal.md#utc-ltz-fixed-unit-floor)

<a id="utc-ltz-fixed-unit-ceil"></a>

[UTC LTZ fixed-unit CEIL](scalar-functions-temporal.md#utc-ltz-fixed-unit-ceil)

<a id="computed-integer-epoch-conversion"></a>

[Computed integer epoch conversion](scalar-functions-temporal.md#computed-integer-epoch-conversion)

<a id="integer-round-with-a-bounded-runtime-position"></a>

[Integer ROUND with a bounded runtime position](scalar-functions-integers.md#integer-round-with-a-bounded-runtime-position)

<a id="integer-truncate-with-a-bounded-runtime-position"></a>

[Integer TRUNCATE with a bounded runtime position](scalar-functions-integers.md#integer-truncate-with-a-bounded-runtime-position)

<a id="integer-printf-with-a-literal-format"></a>

[Integer PRINTF with a literal format](scalar-functions-printf.md#integer-printf-with-a-literal-format)

<a id="integer-try_cast"></a>

[Integer TRY_CAST](scalar-functions-integers.md#integer-try_cast)

<a id="integer-boolean-printf-formats"></a>

[Integer boolean PRINTF formats](scalar-functions-printf.md#integer-boolean-printf-formats)

<a id="boolean-to-integer-try_cast"></a>

[BOOLEAN-to-integer TRY_CAST](scalar-functions-boolean.md#boolean-to-integer-try_cast)

<a id="integer-to-boolean-try_cast"></a>

[Integer-to-BOOLEAN TRY_CAST](scalar-functions-boolean.md#integer-to-boolean-try_cast)

<a id="character-consumers-of-integer-truth"></a>

[Character consumers of integer truth](scalar-functions-boolean.md#character-consumers-of-integer-truth)

<a id="decimal-to-boolean-try_cast"></a>

[DECIMAL-to-BOOLEAN TRY_CAST](scalar-functions-boolean.md#decimal-to-boolean-try_cast)

<a id="character-consumers-of-decimal-truth"></a>

[Character consumers of decimal truth](scalar-functions-boolean.md#character-consumers-of-decimal-truth)

<a id="character-consumers-of-direct-decimal-predicates"></a>

[Character consumers of direct decimal predicates](scalar-functions-boolean.md#character-consumers-of-direct-decimal-predicates)

<a id="runtime-integer-epoch-precision"></a>

[Runtime integer epoch precision](scalar-functions-temporal.md#runtime-integer-epoch-precision)

<a id="boolean-object-and-mixed-integer-printf"></a>

[Boolean-object and mixed integer PRINTF](scalar-functions-printf.md#boolean-object-and-mixed-integer-printf)

<a id="numeric-boolean-producers-for-printf"></a>

[Numeric Boolean producers for PRINTF](scalar-functions-printf.md#numeric-boolean-producers-for-printf)

<a id="length-of-primitive-ascii-printf"></a>

[Length of primitive ASCII PRINTF](scalar-functions-printf.md#length-of-primitive-ascii-printf)

<a id="comparisons-of-primitive-ascii-printf"></a>

[Comparisons of primitive ASCII PRINTF](scalar-functions-printf.md#comparisons-of-primitive-ascii-printf)

<a id="integer-text-producers-for-printf"></a>

[Integer text producers for PRINTF](scalar-functions-printf.md#integer-text-producers-for-printf)

<a id="primitive-text-upper"></a>

[Primitive text UPPER](scalar-functions-strings.md#primitive-text-upper)

<a id="primitive-text-lower"></a>

[Primitive text LOWER](scalar-functions-strings.md#primitive-text-lower)

<a id="integer-round-with-clamped-runtime-positions"></a>

[Integer ROUND with clamped runtime positions](scalar-functions-integers.md#integer-round-with-clamped-runtime-positions)

<a id="prepared-operation_1"></a>

[Prepared operation](scalar-functions-integers.md#prepared-operation_1)

<a id="complete-jobs-and-correctness"></a>

[Complete jobs and correctness](scalar-functions-integers.md#complete-jobs-and-correctness)

<a id="integer-truncate-with-clamped-runtime-positions"></a>

[Integer TRUNCATE with clamped runtime positions](scalar-functions-integers.md#integer-truncate-with-clamped-runtime-positions)

<a id="prepared-operation_2"></a>

[Prepared operation](scalar-functions-integers.md#prepared-operation_2)

<a id="complete-jobs-and-correctness_1"></a>

[Complete jobs and correctness](scalar-functions-integers.md#complete-jobs-and-correctness_1)

<a id="length-of-proven-primitive-case-results"></a>

[Length of proven primitive case results](scalar-functions-strings.md#length-of-proven-primitive-case-results)

<a id="functionality-gate"></a>

[Functionality gate](scalar-functions-strings.md#functionality-gate)

<a id="complete-jobs-and-correctness_2"></a>

[Complete jobs and correctness](scalar-functions-strings.md#complete-jobs-and-correctness_2)

<a id="utc-floor-over-integer-epochs"></a>

[UTC FLOOR over integer epochs](scalar-functions-temporal.md#utc-floor-over-integer-epochs)

<a id="utc-ceil-over-integer-epochs"></a>

[UTC CEIL over integer epochs](scalar-functions-temporal.md#utc-ceil-over-integer-epochs)

<a id="primitive-text-greatest"></a>

[Primitive-text GREATEST](scalar-functions-strings.md#primitive-text-greatest)

<a id="primitive-text-least"></a>

[Primitive-text LEAST](scalar-functions-strings.md#primitive-text-least)

<a id="shared-boolean-kernel-controls"></a>

[Shared Boolean kernel controls](scalar-functions-strings.md#shared-boolean-kernel-controls)

<a id="decimal-epoch-conversion"></a>

[DECIMAL epoch conversion](scalar-functions-temporal.md#decimal-epoch-conversion)

<a id="decimal-unary-epoch-producers"></a>

[DECIMAL unary epoch producers](scalar-functions-temporal.md#decimal-unary-epoch-producers)

<a id="decimal-from-proven-integer-text"></a>

[DECIMAL from proven integer text](scalar-functions-decimal.md#decimal-from-proven-integer-text)

<a id="default-numeric-epoch-precision"></a>

[Default numeric epoch precision](scalar-functions-temporal.md#default-numeric-epoch-precision)

<a id="numeric-date_format-calendar-correctness-2026-11-05"></a>

[Numeric DATE_FORMAT calendar correctness (2026-11-05)](scalar-functions-temporal.md#numeric-date_format-calendar-correctness-2026-11-05)

<a id="java-date_format-literal-syntax-2026-11-05"></a>

[Java DATE_FORMAT literal syntax (2026-11-05)](scalar-functions-temporal.md#java-date_format-literal-syntax-2026-11-05)

<a id="exact-utc-date_format-over-local-zoned-timestamps-2026-11-05"></a>

[Exact UTC DATE_FORMAT over local-zoned timestamps (2026-11-05)](scalar-functions-temporal.md#exact-utc-date_format-over-local-zoned-timestamps-2026-11-05)

<a id="decimal-abs-over-negative-producers-2026-11-05"></a>

[DECIMAL ABS over negative producers (2026-11-05)](scalar-functions-decimal.md#decimal-abs-over-negative-producers-2026-11-05)

<a id="decimal-negation-over-absolute-values-2026-11-05"></a>

[DECIMAL negation over absolute values (2026-11-05)](scalar-functions-decimal.md#decimal-negation-over-absolute-values-2026-11-05)

<a id="decimal-unary-arithmetic-try_cast-to-character-2026-11-05"></a>

[DECIMAL unary arithmetic TRY_CAST to character (2026-11-05)](scalar-functions-decimal.md#decimal-unary-arithmetic-try_cast-to-character-2026-11-05)

<a id="utc-date_format-over-verified-integer-epochs-2026-11-05"></a>

[UTC DATE_FORMAT over verified integer epochs (2026-11-05)](scalar-functions-temporal.md#utc-date_format-over-verified-integer-epochs-2026-11-05)

<a id="decimal-sign-over-verified-unary-producers-2026-11-05"></a>

[DECIMAL SIGN over verified unary producers (2026-11-05)](scalar-functions-decimal.md#decimal-sign-over-verified-unary-producers-2026-11-05)

<a id="utc-date_format-over-verified-decimal-epochs-2026-11-05"></a>

[UTC DATE_FORMAT over verified DECIMAL epochs (2026-11-05)](scalar-functions-temporal.md#utc-date_format-over-verified-decimal-epochs-2026-11-05)

<a id="utc-floor-over-verified-decimal-epochs-2026-11-05"></a>

[UTC FLOOR over verified DECIMAL epochs (2026-11-05)](scalar-functions-temporal.md#utc-floor-over-verified-decimal-epochs-2026-11-05)

<a id="utc-ceil-over-verified-decimal-epochs-2026-11-06"></a>

[UTC CEIL over verified DECIMAL epochs (2026-11-06)](scalar-functions-temporal.md#utc-ceil-over-verified-decimal-epochs-2026-11-06)

<a id="flat-scalar-json_array-2026-11-06"></a>

[Flat scalar JSON_ARRAY (2026-11-06)](scalar-functions-json.md#flat-scalar-json_array-2026-11-06)

<a id="definite-json_query-2026-11-06"></a>

[Definite JSON_QUERY (2026-11-06)](scalar-functions-json.md#definite-json_query-2026-11-06)

<a id="shared-json-reader-history-2026-11-06"></a>

[Shared JSON reader history (2026-11-06)](scalar-functions-json.md#shared-json-reader-history-2026-11-06)

<a id="arrayrow-json_string-2026-11-06"></a>

[ARRAY/ROW JSON_STRING (2026-11-06)](scalar-functions-json.md#arrayrow-json_string-2026-11-06)

<a id="byte-preserving-concat"></a>

[Byte-preserving CONCAT](scalar-functions-strings.md#byte-preserving-concat)

<a id="byte-preserving-concat_ws"></a>

[Byte-preserving CONCAT_WS](scalar-functions-strings.md#byte-preserving-concat_ws)

<a id="byte-preserving-digest-functions"></a>

[Byte-preserving digest functions](scalar-functions-strings.md#byte-preserving-digest-functions)

<a id="byte-preserving-to_base64"></a>

[Byte-preserving TO_BASE64](scalar-functions-strings.md#byte-preserving-to_base64)

<a id="byte-preserving-ascii"></a>

[Byte-preserving ASCII](scalar-functions-strings.md#byte-preserving-ascii)

<a id="literal-width-sha2-over-original-bytes"></a>

[Literal-width SHA2 over original bytes](scalar-functions-strings.md#literal-width-sha2-over-original-bytes)

<a id="byte-backed-strings-through-a-streaming-watermark"></a>

[Byte-backed strings through a streaming watermark](scalar-functions-strings.md#byte-backed-strings-through-a-streaming-watermark)
