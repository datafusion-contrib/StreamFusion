# Calc / filter

**Status:** native, with gaps — the largest admission surface in the project.

This page describes streaming expression coverage. [Batch SQL](batch.md) reuses
the encoder with batch-specific type and plan-shape compatibility checks.

Use this page for the current admission rules. For a specific question, start at:

| Topic | Sections |
| --- | --- |
| Routing and failure semantics | [Overview](calc-filter-routing.md#admission-and-routing), [failure order](calc-filter-routing.md#failure-order-across-rows), [declared-type guard](calc-filter-routing.md#declared-type-guard) |
| Basic types | [Casts](calc-filter-numeric.md#casts), [decimal arithmetic](calc-filter-numeric.md#decimal-arithmetic), [integer division](calc-filter-numeric.md#integer-division) |
| Strings and JSON | [String functions](calc-filter-strings.md#string-and-calendar-functions), [SQL/JSON](calc-filter-strings.md#sqljson-evaluation), [case folding and regex](calc-filter-strings.md#case-folding-regex) |
| User functions and collections | [Scalar UDFs](calc-filter-jvm-collections.md#user-scalar-functions), [collection expressions](calc-filter-strings.md#collection-expressions), [subscripts](calc-filter-jvm-collections.md#collection-subscripts) |
| Time and version compatibility | [Temporal functions](temporal-functions.md), [Flink 1.18](calc-filter-temporal.md#flink-118-compatibility) |

The [evidence index](../benchmarks/evidence.md) separates recorded performance
comparisons and experiments from these current coverage rules.

## Topic pages

- [Routing](calc-filter-routing.md)
- [Jvm collections](calc-filter-jvm-collections.md)
- [Temporal](calc-filter-temporal.md)
- [Strings](calc-filter-strings.md)
- [Numeric](calc-filter-numeric.md)

## Published section links

Existing bookmarks remain valid. Follow a section to its topic page.

<a id="admission-and-routing"></a>

[Admission and routing](calc-filter-routing.md#admission-and-routing)

<a id="nested-java-scalar-results"></a>

[Nested Java scalar results](calc-filter-jvm-collections.md#nested-java-scalar-results)

<a id="recursive-external-scalar-conversions"></a>

[Recursive external scalar conversions](calc-filter-jvm-collections.md#recursive-external-scalar-conversions)

<a id="237-stays-open"></a>

[237 stays open.](calc-filter-jvm-collections.md#237-stays-open)

<a id="failure-order-across-rows"></a>

[Failure order across rows](calc-filter-routing.md#failure-order-across-rows)

<a id="from_unixtime-literal-formats"></a>

[FROM_UNIXTIME literal formats](calc-filter-temporal.md#from_unixtime-literal-formats)

<a id="like-escape-and-similar-to"></a>

[LIKE ESCAPE and SIMILAR TO](calc-filter-strings.md#like-escape-and-similar-to)

<a id="generated-processing-time"></a>

[Generated processing time](calc-filter-temporal.md#generated-processing-time)

<a id="coalesce"></a>

[COALESCE](calc-filter-strings.md#coalesce)

<a id="if"></a>

[IF](calc-filter-strings.md#if)

<a id="ifnull"></a>

[IFNULL](calc-filter-strings.md#ifnull)

<a id="uuid"></a>

[UUID](calc-filter-strings.md#uuid)

<a id="rand-and-rand_integer"></a>

[RAND and RAND_INTEGER](calc-filter-strings.md#rand-and-rand_integer)

<a id="type-and-database-metadata"></a>

[Type and database metadata](calc-filter-strings.md#type-and-database-metadata)

<a id="user-scalar-functions"></a>

[User scalar functions](calc-filter-jvm-collections.md#user-scalar-functions)

<a id="temporal-scalar-functions"></a>

[Temporal scalar functions](calc-filter-jvm-collections.md#temporal-scalar-functions)

<a id="temporal-scalar-conversion"></a>

[Temporal scalar conversion](calc-filter-jvm-collections.md#temporal-scalar-conversion)

<a id="string-ordering"></a>

[String ordering](calc-filter-strings.md#string-ordering)

<a id="string-concatenation-and-hashes"></a>

[String concatenation and hashes](calc-filter-strings.md#string-concatenation-and-hashes)

<a id="byte-preserving-string-input"></a>

[Byte-preserving string input](calc-filter-strings.md#byte-preserving-string-input)

<a id="utf8-concatenation-and-hash-diagnostics"></a>

[Utf8 concatenation and hash diagnostics](calc-filter-strings.md#utf8-concatenation-and-hash-diagnostics)

<a id="hash_code"></a>

[HASH_CODE](calc-filter-strings.md#hash_code)

<a id="declared-type-guard"></a>

[Declared-type guard](calc-filter-routing.md#declared-type-guard)

<a id="collection-subscripts"></a>

[Collection subscripts](calc-filter-jvm-collections.md#collection-subscripts)

<a id="integer-division"></a>

[Integer division](calc-filter-numeric.md#integer-division)

<a id="sign"></a>

[SIGN](calc-filter-numeric.md#sign)

<a id="floating-comparisons"></a>

[Floating comparisons](calc-filter-numeric.md#floating-comparisons)

<a id="casts"></a>

[Casts](calc-filter-numeric.md#casts)

<a id="stringvarcharchar-to-boolean"></a>

[STRING/VARCHAR/CHAR to BOOLEAN](calc-filter-numeric.md#stringvarcharchar-to-boolean)

<a id="integer-try_cast"></a>

[Integer TRY_CAST](calc-filter-numeric.md#integer-try_cast)

<a id="boolean-to-integer-try_cast"></a>

[BOOLEAN-to-integer TRY_CAST](calc-filter-numeric.md#boolean-to-integer-try_cast)

<a id="integer-to-boolean-try_cast"></a>

[Integer-to-BOOLEAN TRY_CAST](calc-filter-numeric.md#integer-to-boolean-try_cast)

<a id="character-consumers-of-integer-truth"></a>

[Character consumers of integer truth](calc-filter-numeric.md#character-consumers-of-integer-truth)

<a id="decimal-to-boolean-try_cast"></a>

[DECIMAL-to-BOOLEAN TRY_CAST](calc-filter-numeric.md#decimal-to-boolean-try_cast)

<a id="character-consumers-of-decimal-truth"></a>

[Character consumers of decimal truth](calc-filter-numeric.md#character-consumers-of-decimal-truth)

<a id="character-consumers-of-direct-decimal-predicates"></a>

[Character consumers of direct decimal predicates](calc-filter-numeric.md#character-consumers-of-direct-decimal-predicates)

<a id="integerstring-casts"></a>

[Integer/string casts](calc-filter-numeric.md#integerstring-casts)

<a id="temporal-try_cast"></a>

[Temporal TRY_CAST](calc-filter-numeric.md#temporal-try_cast)

<a id="binary-casts-and-fixed-width-generated-expressions"></a>

[Binary casts and fixed-width generated expressions](calc-filter-numeric.md#binary-casts-and-fixed-width-generated-expressions)

<a id="decimal-from-proven-integer-text"></a>

[DECIMAL from proven integer text](calc-filter-numeric.md#decimal-from-proven-integer-text)

<a id="decimal-try_cast"></a>

[DECIMAL TRY_CAST](calc-filter-numeric.md#decimal-try_cast)

<a id="the-host-exact-jvm-upcall"></a>

[The host-exact JVM upcall](calc-filter-numeric.md#the-host-exact-jvm-upcall)

<a id="still-falling-back"></a>

[Still falling back](calc-filter-numeric.md#still-falling-back)

<a id="decimal-arithmetic"></a>

[Decimal arithmetic](calc-filter-numeric.md#decimal-arithmetic)

<a id="integer-floor-ceil-ceiling"></a>

[Integer FLOOR / CEIL / CEILING](calc-filter-numeric.md#integer-floor-ceil-ceiling)

<a id="single-argument-integer-round"></a>

[Single-argument integer ROUND](calc-filter-numeric.md#single-argument-integer-round)

<a id="integer-round-with-a-literal-position"></a>

[Integer ROUND with a literal position](calc-filter-numeric.md#integer-round-with-a-literal-position)

<a id="integer-round-with-a-bounded-runtime-position"></a>

[Integer ROUND with a bounded runtime position](calc-filter-numeric.md#integer-round-with-a-bounded-runtime-position)

<a id="single-argument-integer-truncate"></a>

[Single-argument integer TRUNCATE](calc-filter-numeric.md#single-argument-integer-truncate)

<a id="integer-truncate-with-a-literal-position"></a>

[Integer TRUNCATE with a literal position](calc-filter-numeric.md#integer-truncate-with-a-literal-position)

<a id="integer-truncate-with-a-bounded-runtime-position"></a>

[Integer TRUNCATE with a bounded runtime position](calc-filter-numeric.md#integer-truncate-with-a-bounded-runtime-position)

<a id="exact-unary-and-integral-functions"></a>

[Exact unary and integral functions](calc-filter-numeric.md#exact-unary-and-integral-functions)

<a id="decimal-round-truncate-and-literals"></a>

[Decimal ROUND, TRUNCATE and literals](calc-filter-numeric.md#decimal-round-truncate-and-literals)

<a id="arithmetic-operators"></a>

[Arithmetic operators](calc-filter-numeric.md#arithmetic-operators)

<a id="string-and-calendar-functions"></a>

[String and calendar functions](calc-filter-strings.md#string-and-calendar-functions)

<a id="startswith"></a>

[STARTSWITH](calc-filter-strings.md#startswith)

<a id="endswith"></a>

[ENDSWITH](calc-filter-strings.md#endswith)

<a id="instr"></a>

[INSTR](calc-filter-strings.md#instr)

<a id="locate"></a>

[LOCATE](calc-filter-strings.md#locate)

<a id="ascii"></a>

[ASCII](calc-filter-strings.md#ascii)

<a id="chr"></a>

[CHR](calc-filter-strings.md#chr)

<a id="bin"></a>

[BIN](calc-filter-strings.md#bin)

<a id="hex"></a>

[HEX](calc-filter-strings.md#hex)

<a id="to_base64"></a>

[TO_BASE64](calc-filter-strings.md#to_base64)

<a id="from_base64"></a>

[FROM_BASE64](calc-filter-strings.md#from_base64)

<a id="is_decimal-is_digit-and-is_alpha"></a>

[IS_DECIMAL, IS_DIGIT and IS_ALPHA](calc-filter-strings.md#is_decimal-is_digit-and-is_alpha)

<a id="unhex"></a>

[UNHEX](calc-filter-strings.md#unhex)

<a id="greatest"></a>

[GREATEST](calc-filter-strings.md#greatest)

<a id="least"></a>

[LEAST](calc-filter-strings.md#least)

<a id="initcap"></a>

[INITCAP](calc-filter-strings.md#initcap)

<a id="translate"></a>

[TRANSLATE](calc-filter-strings.md#translate)

<a id="btrim"></a>

[BTRIM](calc-filter-strings.md#btrim)

<a id="trim"></a>

[TRIM](calc-filter-strings.md#trim)

<a id="elt"></a>

[ELT](calc-filter-strings.md#elt)

<a id="url_encode"></a>

[URL_ENCODE](calc-filter-strings.md#url_encode)

<a id="overlay"></a>

[OVERLAY](calc-filter-strings.md#overlay)

<a id="parse_url"></a>

[PARSE_URL](calc-filter-strings.md#parse_url)

<a id="printf"></a>

[PRINTF](calc-filter-strings.md#printf)

<a id="url_decode"></a>

[URL_DECODE](calc-filter-strings.md#url_decode)

<a id="encode"></a>

[ENCODE](calc-filter-strings.md#encode)

<a id="decode"></a>

[DECODE](calc-filter-strings.md#decode)

<a id="sqljson-evaluation"></a>

[SQL/JSON evaluation](calc-filter-strings.md#sqljson-evaluation)

<a id="json_query"></a>

[JSON_QUERY](calc-filter-strings.md#json_query)

<a id="json_quote"></a>

[JSON_QUOTE](calc-filter-strings.md#json_quote)

<a id="json_unquote"></a>

[JSON_UNQUOTE](calc-filter-strings.md#json_unquote)

<a id="json_string"></a>

[JSON_STRING](calc-filter-strings.md#json_string)

<a id="json_object"></a>

[JSON_OBJECT](calc-filter-strings.md#json_object)

<a id="json_array"></a>

[JSON_ARRAY](calc-filter-strings.md#json_array)

<a id="is-json"></a>

[IS JSON](calc-filter-strings.md#is-json)

<a id="json_value"></a>

[JSON_VALUE](calc-filter-strings.md#json_value)

<a id="json_exists"></a>

[JSON_EXISTS](calc-filter-strings.md#json_exists)

<a id="collection-expressions"></a>

[Collection expressions](calc-filter-strings.md#collection-expressions)

<a id="exact-numeric-array-cast-and-try_cast"></a>

[Exact numeric ARRAY CAST and TRY_CAST](calc-filter-strings.md#exact-numeric-array-cast-and-try_cast)

<a id="collection-casts-to-character-types"></a>

[Collection casts to character types](calc-filter-strings.md#collection-casts-to-character-types)

<a id="array_distinct"></a>

[ARRAY_DISTINCT](calc-filter-strings.md#array_distinct)

<a id="split"></a>

[SPLIT](calc-filter-strings.md#split)

<a id="substring"></a>

[SUBSTRING](calc-filter-strings.md#substring)

<a id="left"></a>

[LEFT](calc-filter-strings.md#left)

<a id="right"></a>

[RIGHT](calc-filter-strings.md#right)

<a id="lpad"></a>

[LPAD](calc-filter-strings.md#lpad)

<a id="rpad"></a>

[RPAD](calc-filter-strings.md#rpad)

<a id="split_index"></a>

[SPLIT_INDEX](calc-filter-strings.md#split_index)

<a id="temporal-parsing-extraction-and-rounding"></a>

[Temporal parsing, extraction and rounding](calc-filter-strings.md#temporal-parsing-extraction-and-rounding)

<a id="ltrim"></a>

[LTRIM](calc-filter-strings.md#ltrim)

<a id="rtrim"></a>

[RTRIM](calc-filter-strings.md#rtrim)

<a id="case-folding-regex"></a>

[Case folding &amp; regex](calc-filter-strings.md#case-folding-regex)

<a id="datetime"></a>

[Date/time](calc-filter-temporal.md#datetime)

<a id="power-and-sqrt"></a>

[POWER and SQRT](calc-filter-numeric.md#power-and-sqrt)

<a id="exact-floating-math"></a>

[Exact floating math](calc-filter-numeric.md#exact-floating-math)

<a id="literalarity-guards"></a>

[Literal/arity guards](calc-filter-numeric.md#literalarity-guards)

<a id="generated-helper-performance"></a>

[Generated helper performance](calc-filter-jvm-collections.md#generated-helper-performance)

<a id="flink-118-compatibility"></a>

[Flink 1.18 compatibility](calc-filter-temporal.md#flink-118-compatibility)

<a id="legacy-encode-result-representation"></a>

[Legacy ENCODE result representation](calc-filter-temporal.md#legacy-encode-result-representation)

<a id="flink-118-mixed-timestamp-comparison-compatibility"></a>

[Flink 1.18 mixed timestamp comparison compatibility](calc-filter-temporal.md#flink-118-mixed-timestamp-comparison-compatibility)

<a id="exact-double-truncate"></a>

[Exact DOUBLE TRUNCATE](calc-filter-temporal.md#exact-double-truncate)

<a id="binary-elt-and-raw-payload-boundaries"></a>

[Binary ELT and raw payload boundaries](calc-filter-temporal.md#binary-elt-and-raw-payload-boundaries)

<a id="operator-format-performance-admission-2026-10-19"></a>

[Operator-format performance admission, 2026-10-19](calc-filter-temporal.md#operator-format-performance-admission-2026-10-19)

<a id="binary-elt-default-admission-2026-10-20"></a>

[Binary ELT default admission, 2026-10-20](calc-filter-temporal.md#binary-elt-default-admission-2026-10-20)

<a id="map-lookup-after-retained-row-operators"></a>

[Map lookup after retained-row operators](calc-filter-temporal.md#map-lookup-after-retained-row-operators)

<a id="java-array-scalar-signatures"></a>

[Java ARRAY scalar signatures](calc-filter-temporal.md#java-array-scalar-signatures)

<a id="fixed-binary-multiset-elements"></a>

[Fixed BINARY MULTISET elements](calc-filter-temporal.md#fixed-binary-multiset-elements)

<a id="not-null-fixed-binary-map-leaves"></a>

[NOT NULL fixed BINARY map leaves](calc-filter-temporal.md#not-null-fixed-binary-map-leaves)

<a id="java-map-scalar-signatures"></a>

[Java MAP scalar signatures](calc-filter-temporal.md#java-map-scalar-signatures)

<a id="verified-native-map-keys"></a>

[Verified native map keys](calc-filter-temporal.md#verified-native-map-keys)

<a id="native-container-cast-bounds"></a>

[Native container cast bounds](calc-filter-temporal.md#native-container-cast-bounds)

<a id="temporal-collection-copy-categories"></a>

[Temporal collection copy categories](calc-filter-temporal.md#temporal-collection-copy-categories)

<a id="external-structured-collection-inputs"></a>

[External STRUCTURED collection inputs](calc-filter-temporal.md#external-structured-collection-inputs)

<a id="scalar-arrays-across-native-expansion"></a>

[Scalar arrays across native expansion](calc-filter-temporal.md#scalar-arrays-across-native-expansion)

<a id="character-search-endpoints"></a>

[Character search endpoints](calc-filter-temporal.md#character-search-endpoints)

<a id="collection-references-beside-native-scalar-expressions"></a>

[Collection references beside native scalar expressions](calc-filter-temporal.md#collection-references-beside-native-scalar-expressions)
