# Preserve typed grouped accumulators and shared DISTINCT views

Arroyo separates incremental accumulator state from retained membership in its
`crates/arroyo-worker/src/arrow/incremental_aggregator.rs`. StreamFusion keeps that separation,
but canonical interchange must preserve the released Flink generated layout. Its membership
counters are signed Java longs with wrapping arithmetic, whereas Arroyo's batch membership count
uses saturating unsigned decrement. Stock-bound COUNT DISTINCT uses Flink's exact zero/one
transition rules; the ordinary native membership policy remains separate.

The binding derives physical schemas from released aggregate information and verifies the actual
running function's accumulator types. It uses original aggregate calls for DISTINCT/filter policy
because Flink's derived inner calls strip those flags. Actual key/view serializers and stock
identities accompany the private native execution graph; the portable plan stays stock Flink.

A shared stock view becomes one native membership table per logical count, preserving shared bit
or signed-counter slots on export. COUNT's scalar remains independent of membership. A NULL view
key is retained as a membership counter and exported to the released null-marker state; it does
not increment COUNT. Native checkpoints use the existing preserved-scalar representation so
recovery neither counts null markers nor reconstructs an independent scalar from cardinality.
The generated RAW placeholder remains a non-null value with Flink's one-byte NullSerializer payload.

The JNI handoff follows Comet's moved Arrow C Data ownership and the existing guarded callback
pattern. Direct raw RocksDB header/companion visitors and memory visitors emit bounded frames,
without writing an intermediate native snapshot. Full preflight precedes mutation and source
entries remain available until import succeeds. Entry TTL for DISTINCT is deliberately unbound
until independent view ages have a complete projection.

The exact supported and tested subsets remain on [the operator coverage page](../docs/operators/group-by.md).
Other views, phases, schemas, connector guarantees and failure/version matrices remain in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.

Append MIN/MAX additionally accepts the existing DATE/Time32/BOOLEAN scalar representation.
The native fold, Arrow ownership and bounded visitors are unchanged; only the stock type binding
admits those carriers for append extrema. Other aggregate kinds and retraction layouts remain
rejected. Released scalar serializers retain logical precision and NULL/false distinctions;
no extrema are reconstructed from outputs or membership. Comet's existing C Data ownership and
Arroyo's accumulator/retained-entry separation continue to apply without another bridge format.


Append-only string extrema reuse the existing owned string accumulator and Arrow Java-string
companions. Released Flink compares two Java-backed values in UTF-16 order and otherwise uses
UTF-8 byte order. The captured stock input graph therefore remains upstream of these native
aggregates until native producers preserve the same representation. This differs from simply
choosing one columnar string comparator: neither universal UTF-8 nor universal UTF-16 matches
Flink's mixed-representation behavior. The aggregate itself remains Arrow-in/Arrow-out, with
Comet's existing moved C Data ownership and accounted owned UTF-8/UTF-16 payloads.

The native persistent path follows released RocksDB's per-record deserialization boundary even
inside one Arrow batch. Canonical and ordinary checkpoint restore retain only encoded strings,
like released stock; no StreamFusion cache serializer enters the savepoint. Stock-only controls
prove that canonical restore itself can alter later heap Unicode comparison choices. Continuation
therefore compares with independently restored stock state at identical cuts, retaining exact
serialized state and changelog assertions. This transient-cache handling changes neither the
canonical state schema nor the ordinary native checkpoint format. Retracting string views and
wider phase/failure/version coverage remain open under #569; benchmarking stays deferred.


Scalar mini-batches follow released Flink's bundle protocol rather than treating each partial
as a direct retained-state update. A global bundle has an independent transient accumulator
which is merged into durable state once at drain. This preserves both floating-point rounding
and the nontransitive mixed Java/binary string comparator. The local AVG sum uses the actual
stock widened accumulator carrier; input type alone cannot define its partial schema.

Arroyo's incremental accumulator/retained-entry separation remains the starting structure.
The extra global buffer is a required Flink protocol projection, not another persisted native
snapshot. Buffers drain before barriers; direct stock export visits live durable state. Comet's
existing moved C Data ownership applies to partials and their optional string companions.
An empty local barrier before any key schema has been observed exports no rows. Ordinary
native checkpoints retain their existing format and rebind the stock merge mode on restore.


Shared append SUM DISTINCT extends the same raw-accumulator/membership separation. The sum is
restored as a nullable scalar; membership import never refolds values into it. Stock COUNT
cardinality validation is retained for COUNT calls, while SUM keeps an independent scalar. Each
aggregate's filter bit and the shared NULL marker remain part of the released view contract.
This follows Arroyo's accumulator/membership separation and Comet's existing moved Arrow C Data
ownership without adding a wire format or an intermediate native snapshot. The bound mode
preserves raw sums and presence through ordinary native checkpoints. Local/global DISTINCT
merge, retractions and independently aged views still require their own qualified projections.


## Append local/global DISTINCT handoffs

Released Flink merges each local DISTINCT MapView into a temporary heap map, then into the
retained state map. It invokes the inner accumulator only for newly present membership bits;
local DISTINCT scalar partials are not additive. Keep that two-stage union in native state and
retain raw sums independently when canonical state is imported or exported. Native local views
remain split by argument/filter pair inside the private Arrow chain; the global canonical
projection combines their independent memberships into the released shared bit layout.

Arroyo's incremental aggregation separates retained grouped state from changed-key processing;
we retain that structure. Stock Flink's generated merge semantics additionally require retained
NULL markers and Java map order for floating-point folds. Reuse the existing map-order model
and preserve both filter alignment and initialization presence. Local policy is explicitly bound
only for captured append stock contracts; ordinary native execution keeps its existing policy.
No native snapshot intermediate or persisted format is introduced. The added JNI entry point
configures a transient local owner and follows the existing guarded-handle ownership pattern.


## Companion-table bounds

The existing native grouped RocksDB format stores the aggregate ordinal plus one in a table
byte; zero is the main table. Reject positions above 254 before binding a canonical DISTINCT
view, and reject an unencodable codec before opening a database. Checked conversion prevents
wrapping into unrelated state. Keep wide scalar-only schemas admissible because they have no
companion IDs. SQL/compiled fallback uses the captured stock template and never advertises an
unbound native envelope as a stock-compatible handoff. This preserves current native checkpoint
bytes for supported layouts; expanding the encoding remains part of #569.


## Captured JVM accumulator handlers

Arroyo's incremental aggregate structure remains the native aggregate model, but arbitrary
Flink user accumulators and managed ListView/MapView contracts require their actual released
generated handler. Retain the captured synchronous stock function and key selector behind the
existing Arrow UDF operator instead of regenerating a different accumulator schema. This is an
authorized UDF execution path, not a Rust acceleration claim. Stock manages its state and
serializers directly; the optional cache drains before barriers and is disabled for TTL.
Canonical execution proof enumerates the opened owner's restored accState keys and credits
those imports only when that same object successfully consumes new input. Stock fallback and
unrelated native Calc work do not qualify. Broader mini-batch/view/failure coverage remains #569;
benchmarking is explicitly deferred.


## Captured one-phase bundle semantics

Keep Flink's actual one-phase MapBundleFunction and BundleTrigger behind the Arrow UDF boundary.
The local per-key buffer follows the released addInput/finishBundle sequence and is transient:
count triggers, watermarks and barriers drain it into original managed accumulators and views.
This preserves generated bookkeeping and avoids replacing a mini-batch UDF with immediate
row-at-a-time evaluation. The optional JVM cache flushes before capture and remains disabled
for TTL. Conversion benchmarks are deferred; local/global partial transport remains separate.
