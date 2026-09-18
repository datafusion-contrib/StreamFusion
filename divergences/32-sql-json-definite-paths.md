# Flink SQL/JSON paths

Comet's `native/spark-expr/src/string_funcs/get_json_object.rs` separates scalar paths from
column inputs and streams through each document instead of building a DOM on its common
path. StreamFusion follows that structure: the path and policies remain scalar, a
path is parsed once per batch, and unescaped selected strings borrow their input span until
appended to the Arrow result. Every field is validated, including fields outside the selected
path. JSON_VALUE, JSON_EXISTS and IS JSON share this parser under the scalar-function registry.

Signed array indexes follow Flink's released Jayway evaluator: negative indexes add the array
length, and negative zero is zero. The streaming path counts a negatively indexed array using
a copied parser cursor, then selects while retaining complete-document validation. This pays
an extra scan of that array instead of allocating Jackson's full object tree or retaining each
element. The SIMD tape already records array lengths and needs no counting pass. This extends
the shared definite-path reader; it does not use Comet's Spark-specific wildcard semantics.

For documents with many short members, a shared `simd-json` reader reuses its input scratch,
structural buffers and tape across rows. It validates the whole document before selecting a
path, and scans all members of each selected object so the last duplicate wins. This differs
from `datafusion-functions-json`'s jiter early lookup, which can stop before an invalid later
field or a duplicate ancestor. The tape's decoded strings are borrowed until the Arrow builder
copies the selected result; no DOM or per-row result String is created.

The SIMD path is deliberately narrower than function admission: it requires fewer than 50,000
input bytes, at most 1000 tape nodes, no Unicode escapes and no floating-point nodes. These
bounds preserve Jackson's string/name/depth constraints and avoid replacing BigDecimal or
surrogate semantics with the SIMD library's conversions. Selected numbers, rejected SIMD
documents and all other inputs use the existing native parser. The first 256 input bytes must
contain at least eight colons before SIMD is attempted; this inexpensive workload heuristic
keeps long padding strings on the streaming path. It does not change accepted input or results.

The semantic reference is Flink **release-2.2.1**, specifically `SqlJsonUtils`,
`JsonValueCallGen`, and its released Jackson 2.18.2/Jayway implementation. Spark's function
does not implement SQL/JSON strict/lax modes, independent EMPTY/ERROR policies, or Flink's
JSON-null behavior. Neither Comet's serde visitor nor StreamFusion's connector JSON decoder
can be substituted without changing results. Connector decoding also has different input
validation and malformed-string contracts, so this change does not alter that parser.

String token validation reuses serde_json's public streaming `IgnoredAny` interface. Its
string scanner validates escapes and control bytes in word-sized chunks without allocating
a decoded String or rejecting lone escaped UTF-16 surrogates. Only the selected string is
unescaped under Flink's rules. The existing serde dependency is therefore also needed by
the connector-free core; no new crate or version is added.

The scanner retains scalar spans and the last matching object member. This reproduces
Jackson's duplicate-member replacement even when a later duplicate ancestor removes an
earlier match. It validates the entire first JSON value, then preserves Jackson's handling
of trailing content. Escaped strings retain UTF-16 surrogate semantics; numbers preserve
integer precision and BigDecimal's scale, zero and exponent formatting. Jackson switches
from the JDK BigDecimal constructor to FastDoubleParser at 500 characters. JDK 17 bounds
the exponent itself to 32 bits; JDK 21/24/25 and FastDoubleParser accept a wider exponent
when the resulting BigDecimal scale fits. The same runtime profile selects that rule.

Root true/false/null token termination calls `Character.isJavaIdentifierPart(char)` in
Jackson. The planner passes the runtime's Unicode version as a scalar literal (JDK 17:
13.0, JDK 21: 15.0, JDK 24/25: 16.0). Native character categories are intersected with that
Unicode age, and supplementary code points are tested as UTF-16 surrogate units, as in
Jackson. This avoids treating a newly assigned character as a token suffix on an older JDK.
Unmapped JDK versions are declined rather than silently using the build machine's rules.

## Resource-limit boundary

Jackson's published limits are 1000 number digits, 1000 nesting levels, 20 million UTF-16
string units and 50,000 member-name units. Those limits are checked even outside the selected
subtree. There is a host implementation quirk beyond the documented number limit:
`ReaderBasedJsonParser._parseNumber2` represents an absent fraction/exponent length as -1,
while its fast path uses 0. This can admit a 1001-digit floating-point token only on the
slow path. Leading zeroes and end-of-input select that path predictably; buffer boundaries
also depend on Jackson's thread-local recycled buffer and prior unrelated parser calls.

The reader acquires the task thread's actual token buffer from shaded Jackson 2.18.2's
`JsonFactory._getBufferRecycler()` and `BufferRecycler` before evaluating a batch. The
underscore-prefixed accessor is internal despite being public, so it is a versioned
compatibility dependency. Both factories must use `JsonRecyclerPools.ThreadLocalPool`,
the default in 2.18.2 used by Flink's `SqlJsonUtils`. Jackson 2.17.0 changed the default
pool, and 2.17.1 reverted it; another default cannot be assumed to share task-thread state.

Before encoding JSON_VALUE, JSON_EXISTS or IS JSON, the planner checks a result cached once
per class loader. The probe requires version 2.18.2 and the thread-local pool, constructs a
runtime, returns its buffer and verifies that a second default factory acquires the same
recycler and buffer. Construction/release failures, missing classes/methods and other linkage
errors decline the expressions during planning. Factory initialization is lazy so those
failures are caught by the probe rather than escaping static initialization. An upgrade
requires re-verifying this contract and the parity fixtures before widening the version gate.
Planning and task JVMs must carry the same verified shaded Jackson classes and pool strategy.

Inputs of at most 32768 UTF-16 units grow
the capacity before parsing, including invalid input and rows handled by SIMD; larger inputs
use the existing capacity as `StringReader` does. The native number scanner uses that actual
capacity for boundary checks. At batch completion, including an EMPTY/ERROR policy failure,
the buffer is returned with the capacity reached by the reader. No JSON document or selected
value crosses JNI, and neither the operator nor the row/Arrow converters change.

Comet's `native/spark-expr/src/jvm_udf/mod.rs` and `native/jni-bridge/src/comet_udf_bridge.rs`
provide the reference for calling back on the driving task thread. This use is narrower than
an expression callback: only parser state is exchanged, and a JNI local frame bounds the
Java object's lifetime. The streaming and SIMD kernels continue to execute in Rust.

Both functions are enabled by default. The regression tests compare native JNI evaluation
with `SqlJsonUtils` under initial capacities of 4000, 8000, 16000 and 32768 characters, and
exercise growth within and across batches, Unicode length, malformed input and SIMD input.
They assert the fixture really produces different Flink results under different buffer
histories; matching a fresh-buffer run alone would miss the original bug.

IS JSON uses the same validated first-document result before applying Jayway's path policy.
This is essential for root JSON null: Jackson accepts it as a scalar, while Jayway refuses
to construct a path context from Java null. Its VALUE/OBJECT/ARRAY/SCALAR predicates and
their negations are enabled by default. SQL NULL and invalid documents produce FALSE,
with no SQL NULL result. Neither DataFusion's general JSON readers nor Comet's Spark JSON
extraction implements this SQL predicate contract, so the wrapper reuses our verified parser.

SQL harness validation uses JDK 17. The ten JNI buffer-history tests also pass on JDK 24
with `-Dtest=NativeJsonBufferHistoryTest -Djunit.jupiter.extensions.autodetection.enabled=false`.
That disables the automatic MiniCluster extension, which these direct comparisons do not
need. The full SQL harness cannot start on JDK 24 because the current Hadoop dependency
calls the removed `Subject.getSubject` API before any query is executed.

## Admission

Constant member/index paths and wildcards are admitted. Recursion, predicates, slices,
multi-selectors, functions and dynamic paths remain on Flink. Unicode dot members
and single/double-quoted bracket members follow Flink's released Jayway parser. Quotes delimit a literal member:
punctuation such as `.` or `*` inside them is part of the key. Empty quoted names are ordinary
object keys, distinct from names containing spaces. The existing streaming and SIMD member
selectors handle them, including last-duplicate replacement and nested empty ancestors;
only the planner and compact path grammar need to admit them. Verified Jayway bracket escapes
are decoded and canonically JSON-quoted by the planner; dot-member backslashes and controls
remain literal. Invalid Unicode escapes and unpaired-surrogate path names are excluded before
crossing JNI. The native grammar retains borrowed member slices, with no per-row path parsing or change to JSON validation.

The planner normalizes ASCII spaces around bracket members/indexes and at the end of the
literal path before passing it to the compact native grammar. Quoted member content and
leading-zero index spelling are preserved. No new native parsing or per-row allocation is
needed. Arroyo registers `datafusion-functions-json`; its general JSON lookup contract does
not supply Flink's SQL/JSON mode, error-policy or Jayway whitespace semantics, so the existing
Flink-specific parser remains necessary.

General whitespace trimming is deliberately excluded. Jayway trims ASCII spaces but can
interpret a trailing tab as part of a dot member, ignore a single character after a bracket,
or reject a longer trailing sequence. Only verified forms are admitted: trailing U+0000–U+0020
controls inside array-index brackets are trimmed, while controls within dot names remain literal. Other whitespace forms stay outside
native admission, including leading whitespace without an explicit mode. Whitespace
around the optional strict/lax mode continues to follow Flink's mode regex. SQL regressions
compare compact/spaced selectors, quoted member spaces, missing/null/scalar/container values,
duplicate members, invalid unselected fields, independent paths and INTEGER defaults.
JSON_EXISTS ERROR diagnostics retain exact host comparison; JSON_VALUE failures still use
the existing native wrapper, with diagnostic parity tracked in
[#108](https://github.com/datafusion-contrib/StreamFusion/issues/108).

JSON_VALUE returns VARCHAR, BOOLEAN, INTEGER or
DOUBLE. The latter three conversions in Flink are Java object casts outside ON ERROR, so
reusing SQL CAST would be incorrect. Native extraction writes directly into the matching
Arrow primitive builder, requiring a JSON boolean, a signed 32-bit integer token, or a
decimal/exponent token respectively. Type mismatch fails after EMPTY/ERROR policies.
The DOUBLE conversion parses the validated decimal spelling with correctly rounded binary
conversion, removing a minus sign only from a mathematically zero decimal. Underflow of a
negative nonzero value retains negative zero, as BigDecimal.doubleValue does. JNI tests
compare 2,010 deterministic boundary/random samples against Flink's BigDecimal output bits.

Non-null character, BOOLEAN and INTEGER literal defaults match their corresponding return
types. DOUBLE defaults are declined because generated Flink Double/DecimalData objects cannot
be cast to BigDecimal. Null defaults participate in Flink's generated whole-call null guard
and are declined, as are non-literal or mismatched-type defaults.

There is a separate Flink code-generation edge for boxed boolean NULLs: Calc/filter,
truth predicates and CASE conditions may unbox the result without consulting its null flag.
Nullable BOOLEAN JSON_VALUE forms therefore require a direct projection. Supplying non-null
DEFAULT or ERROR for both EMPTY and ERROR permits native composition. This is an expression
admission check, not a change to operators or row/Arrow conversion.
Typed JSON_VALUE under AND/OR is also declined: DataFusion 54's batch short-circuiting
can still evaluate the right side for rows Flink skips. SQL probes pin both an OR and an
AND with an invalid scalar type on the skipped row; CASE result selection remains native.
The same short-circuit gate covers VARCHAR JSON_VALUE with an ERROR policy and JSON_EXISTS
with ERROR ON ERROR. JSON_EXISTS UNKNOWN ON ERROR requires a direct projection because
Flink's MethodCallGen also returns a boxed Boolean, with the same unsafe consumers. Default
FALSE and TRUE ON ERROR remain composable. Regression probes verify the host failures and
the successful short-circuited results that would otherwise diverge in native execution.
JSON_EXISTS supports all four ON ERROR behaviors. Both functions are ordinary scalar
expressions; no operator or converter changes are required.

## Wildcards

Arroyo's `arroyo-planner/src/functions.rs::extract_json` parses a scalar `serde_json_path` path
once and emits an Arrow list of serialized matches. Flink's JSON_VALUE/JSON_EXISTS have a
different observable contract: every indefinite result is a collection, even with zero or one
matches, and neither function returns its elements. We retain Arroyo's scalar-path/column-input
structure while carrying only the collection marker through our existing readers. No result list,
new JNI call or operator is needed; every JSON field is still validated.

The reference is released Jayway 2.9.0's `WildcardPathToken`, `PathToken`, and `ArrayPathToken`,
plus Flink 2.2.1's `SqlJsonUtils`. A wildcard over a scalar or nested null produces an
empty collection. Strict mode rejects missing properties and wrong-type selectors in the definite
prefix, but skips an out-of-range array index and returns an empty collection. Lax mode suppresses
path failures into an empty collection while malformed JSON remains a null context; root JSON null
still fails context construction. Both the streaming cursor and SIMD tape preserve these distinct
outcomes, including last-duplicate replacement.

After the first wildcard, Jayway skips missing or wrong-type member/index branches and always
returns a collection. We validate the entire path grammar, then retain only the definite prefix
and first wildcard for evaluation. This also covers repeated wildcards and member/index
continuations without enumerating matches. It is specific to JSON_VALUE and JSON_EXISTS;
JSON_QUERY would need the actual collection, and functions can change its result shape, so
neither is admitted by this rule. The released-Flink matrix checks these continuations against
scalar/null/container inputs, empty/multiple matches and conflicting duplicate ancestors.
