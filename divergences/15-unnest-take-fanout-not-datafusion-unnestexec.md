# 15 — UNNEST as a take-based fan-out, not DataFusion's `UnnestExec`

Arroyo implements `UNNEST` by rewriting to a DataFusion `LogicalPlan::Unnest` and letting DataFusion's
`UnnestExec` run it. We do **not** mirror that, and instead fan rows out with an Arrow `take` (the same
shape as our windowing TVF `assign_windows` and `Expand`). Reasons:

1. **Arroyo owns its whole plan; we don't.** Arroyo rewrites to a DataFusion plan and lets DataFusion
   *define* the unnest's output schema. We must match Flink's fixed `Correlate` output —
   `[input cols.., element]`, with the source array column **preserved** — and thread our `$row_kind$`
   changelog meta-column through. DataFusion's `UnnestExec` replaces the unnested column *in place*,
   maps the surviving columns *by name*, and has no notion of `$row_kind$`. Bending it to Flink's
   append-the-element-and-keep-the-array contract would mean duplicating the array column, building a
   name-matched output schema, threading `$row_kind$` as a non-unnested column, and reordering — more
   machinery than the fan-out itself.

2. **DataFusion's batch kernel is private.** Only the `UnnestExec` `ExecutionPlan` is public
   (`unnest_list_array`/`build_batch` are private), so "call DataFusion's unnest on a batch" isn't
   available — we'd have to stand up a `MemorySource` + `collect` per batch (a tokio round-trip) the
   way we do for the hash joins, purely to get at the kernel.

3. **It's the established in-repo pattern.** Our other stateless per-row fan-outs — the windowing TVF
   and `Expand` — already fan out with `ListArray`/`take`. UNNEST is the same shape: take the child
   values by element index and repeat selected input columns by row index, without plan construction.

Scope today: **INNER/LEFT `UNNEST` of one `ARRAY`, `MAP`, or `MULTISET` operand**, including encodable
computed ARRAYs and nested array fields. A scalar `ARRAY` element appends
one column; an `ARRAY<ROW>` element is flattened into one column per struct field; a `MAP` appends a
key and a value column; a `MULTISET` (carried as a MAP<element, count> at the Arrow boundary) appends
the element repeated by its count (Flink's behavior — a MAP/MULTISET is read as an Arrow `MapArray`). `WITH ORDINALITY` appends a trailing 1-based INTEGER ordinal (the element's position
in its collection). A null `ROW` element is dropped without ordinality. With ordinality, a nullable
one-field ROW retains its NULL row and position; parent validity is folded into the child before
take. Other nullable ROW shapes retain the released host's failure behavior through fallback.
Component timestamp structs are scalars and never enter ROW expansion.
A filter pushed into the `Correlate` as a `condition`
(`… WHERE element > x`) is applied as a native filter over the unnest output: Flink's correlate
condition indexes the table-function output, so its refs are shifted by the input arity to index the
`[input.., element]` output, then encoded by the expression engine and run as a `StreamPhysicalNativeFilter`
on top of the unnest (it composes — no new operator). A LEFT (outer) unnest is supported: a row whose
collection produces no element emits one row with the appended columns null (a null take index makes
`take` emit null; the appended fields are relaxed to nullable). A condition the expression engine
cannot encode, or a pushed LEFT condition, uses the released Flink correlate generator when its
types and representation boundary are verified, as described below.

The physical node carries an optional logical-output projection. Pure projections and INNER
filter projections compose this mapping, retaining predicate dependencies. Expansion builds row
and element indices once, takes only selected columns, and shares the output array for repeated
columns. This avoids copying discarded source arrays once per element, an otherwise quadratic
cost in array width. The changelog sidecar is selected independently; an empty SQL projection
retains the expanded row count. The old unprojected JNI entry point delegates to the same kernel.

A computed operand is materialized once per input batch by the existing Calc and appended as a
hidden column; the expansion projection removes it. Homogeneous scalar ARRAY construction uses
released DataFusion's `make_array` kernel only at this immediate consumer, where Flink's generic
versus binary container identity cannot escape. Compact timestamp construction still clears hidden
nanoseconds to reproduce Flink's binary writer. Generated evaluation remains available for other
expressions. Numeric epoch conversion and
TIMESTAMP_LTZ DATE/TIME extraction retain the released SQL range, signed Julian-date arithmetic
and per-version TIME precision policy. Non-UTC casts use JDK ZoneRules offsets through the existing
batch bridge, cached across transition intervals; arithmetic and array construction stay in Arrow.
The total offset lookup is exempt from whole-Calc error-order fusion. Arbitrary JVM functions and
fallible native expressions retain the existing scheduling safeguards.

Representation-sensitive computed MAPs and ARRAYs use the released Flink correlate generator in
an Arrow-input/Arrow-output wrapper. This is a second deliberate divergence from Arroyo's
DataFusion-only expansion: Flink's generated constructors expose binary/generic container classes,
map iteration order, duplicate-key behavior and nested serializer rules that a generic Arrow
flatten cannot reproduce. The same generator preserves LEFT null padding after element predicates.
Unused source fields and output columns are pruned; simple collections retain the take kernel.
User function admission is unchanged. Sensitive complex values cannot cross unverified state,
ordering, exchange or native-sink boundaries.

Generated nested binary ROW output carries an owned byte payload alongside its ordinary Arrow
children and representation tag. Comet's columnar-to-row boundary similarly uses an owned binary
row representation for efficient handover; ours retains the Arrow columns for columnar consumers
and IPC. An immediate row exit uses the cache only after checking the whole batch; generic rows
and missing caches keep their existing representation-preserving readers. No borrowed native
pointer escapes the input batch.

Preserved scalar arrays now reuse the same native take kernel across verified collection Calcs
and collection aggregate outputs. A represented string struct remains one scalar, with its
UTF-16 cache and copy metadata; it is never a logical ROW expansion. NOT NULL-element getters
and cached-string value consumers retain the existing generated boundary where necessary.
Computed projections prune unused input columns before fan-out. Native string ARRAY construction
uses the existing binary-element snapshot contract, matching Flink's BinaryArrayWriter rather
than retaining source Java caches. Comet's import/release pattern remains unchanged.
