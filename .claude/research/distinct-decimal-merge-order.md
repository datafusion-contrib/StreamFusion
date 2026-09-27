# Wide-decimal DISTINCT merge ordering

Investigation for #231: two-phase SUM/AVG DISTINCT over DECIMAL precision 20–38.
The ordering, buffering, retention and recovery contracts below now support wider admission;
`docs/operators/group-by.md` records current coverage and benchmark results.

## Released-host evidence

`FlinkDistinctDecimalMergeOrderTest` uses released Flink's `MapView`, `NullAwareMapSerializer`,
`DecimalDataSerializer`, and `DecimalDataUtils.add`. On JDK 17, both Flink 2.2.1 and 1.18.1
exhibit these two independent effects:

* Three equal membership maps can iterate differently after `NullAwareMapSerializer.copy` or a
  serialize/deserialize round trip. With `L = 9 * 10^37`, the default map containing
  `[L, L - 3, -L]` folds SUM to `-L`; the copied/deserialized maps fold to `L - 3`.
  AVG's running sum over the original map overflows to sticky NULL, while the other maps
  retain `L - 3`. The membership maps themselves compare equal.
* Folding singleton local views in arrival order `[L, -L, L - 3]` retains `L - 3`, but first
  combining those views in a default `MapView` produces `-L` for SUM and NULL for AVG's sum.

The test isolates membership ordering and decimal addition. It is not an end-to-end claim
that any particular physical SQL route always selects one of those results.

## Relevant Flink structure

Reference files in the canonical Flink tree:

* `flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/codegen/agg/DistinctAggCodeGen.scala`:
  creates a fresh MapView and merges by iterating `otherAcc.entries()`; shared filters use
  a common membership view with per-aggregate flags/counts.
* `flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/aggregate/MiniBatchLocalGroupAggFunction.java`:
  creates a fresh accumulator per buffered key, accumulates input rows, and emits it at flush.
* `flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/aggregate/MiniBatchGlobalGroupAggFunction.java`:
  `addInput` merges incoming partials into a temporary local accumulator; `finishBundle`
  merges that accumulator into durable global state. The released 2.2.1 bytecode confirms
  this extra buffering stage.
* `flink-table/flink-table-common/src/main/java/org/apache/flink/table/dataview/NullAwareMapSerializer.java`:
  copy/deserialization creates a HashMap sized for the entry count, while a fresh MapView
  starts with a default HashMap.
* `flink-table/flink-table-common/src/main/java/org/apache/flink/table/data/DecimalData.java`:
  decimal keys use BigDecimal hash codes and numerical comparison.

## Consequences for implementation

The native global currently merges each incoming membership view immediately and coalesces
output changes at flush. Exact wrapping integer sums and precision-at-most-19 decimal sums
are independent of this iteration order; wide decimal sums are not.

Sorting each native local view by decimal hash is insufficient. A complete implementation
must establish the actual copy/serialization route, preserve the temporary global union
and its logical bundle boundaries, reproduce HashMap iteration (including collisions,
resizes and tree bins), and preserve shared-filter membership ordering. Durable snapshot
restore must retain the existing running sum rather than refold it in arbitrary map order.

Keep the planner fallback until runtime-source SQL tests exercise those contracts on both
released Flink lines. The issue's precision-20 example remains open; a small non-overflowing
fixture does not justify admitting all values of that type.

## Native ordering model

`native/engine/src/group_agg/decimal_map_order.rs` supplies decimal hashing and delegates
insert-only membership order to the reusable `java_map_order.rs` core, including BigDecimal-compatible hashing, signed hash comparisons,
collision chains, red/black tree insertion, root placement, resize splits, tree-to-list
transitions, and serializer-sized copies. It uses indexed Rust nodes and reports owned
vector capacity for later memory-accounting integration. The local aggregate now uses it for wide-decimal views; the planner admission gate has not
changed, so this path remains inaccessible from admitted SQL until final admission validation is complete.

The shared `src/test/resources/decimal-map-order.json` fixtures contain 55 default/copy
scenarios. They cover random precision-38 keys at scales 0/2/18/37, duplicates, full-hash
collisions, bucket collisions, resize boundaries and tree-to-list transitions. The Java
regression verifies each fixture with released Flink DecimalData keys and NullAwareMapSerializer;
the Rust regression verifies the same order and checks chain/tree invariants after every
insertion. The fixtures were initially generated with JDK 17 BigDecimal HashMap keys, and
are checked against Flink's actual key class to avoid assuming identical tree behavior.

Local integration retains one order per original decimal argument, shared across filters.
Each emitted filtered view includes the full common key order with zero counts for inactive
members, including NULL. NULL does not contribute to SUM/AVG, but it occupies a map entry
and can affect capacity, tree shape and iteration. The null-aware serializer-sized copy
models the local view's transport representation. Ten additional fixtures cover NULL keys
inserted before/inside/after zero-hash collision trees and duplicate NULL insertion.

The native local regression checks shared filters, NULLs, duplicate multiplicity, scalar
and Arrow key paths, physical batch splits, flush/reset, and tracked memory release. The
local integration passed 570 native tests with one ignored.

The global stage now unions wide-decimal views per logical bundle and folds the resulting
map order into durable state at flush. Zero-count and NULL keys affect temporary order but
never enter durable DISTINCT membership. The direct regression uses singleton views whose
arrival order avoids overflow but whose union order overflows: SUM becomes -9e37 and AVG
stays NULL. Repeated bundles preserve membership and release their accounted buffer memory;
a shared-filter regression also verifies COUNT ignores ordering-only entries.

Wide SUM raw checkpoints now preserve the running value and live count, marked by Arrow
field metadata `streamfusion.distinct-running-state=1`. Legacy unmarked SUM frames retain
their old refold behavior; current AVG snapshots remain compatible. A regression permutes
membership side-table rows to prove recovery does not refold them, including NULL overflow
and duplicate retractions. The shared persistent test now covers SUM as well as AVG through
raw import, RocksDB checkpoint/reopen, canonical export and subsequent retractions. The
native suite passes 572 tests with one ignored.

Local flush now retains state accounting until its input containers are dropped, and reserves
a conservative scratch bound before draining wide-decimal bundles. The bound covers one
serializer-order copy (including resize working space) and simultaneous scalar/Arrow view
output. Direct membership lookup removes the extra copied membership map. Reservation failure
leaves the original bundle intact and follows the existing JNI memory-error path. This follows
Comet's fallible reservation pattern (`native/core/src/execution/memory_pools/unified_pool.rs`).
A tight-budget regression verifies failure before draining and successful retry with a larger
budget. With this change, 573 native tests pass (one ignored). The actual transport route
must still be validated end to end; local integration alone
is not sufficient for native decimal parity.

For multi-key SQL validation, also check local group emission order: Flink's bundle holds
keys in a HashMap and emits that map's entries, while the native local currently emits
first-appearance order. This can change which partials share a count-triggered global
bundle. A single-key fixture cannot prove the complete buffered contract for multiple keys.

## End-to-end group emission probe

A temporary widening of the two local admission predicates to precision 38 ran separate
SUM DISTINCT and AVG DISTINCT queries with COUNT(*) through both native stages on released
Flink 2.2.1. Unshuffled arrivals passed for (key count, bundle size) pairs (1,3), (1,5),
(1,64), (7,3), (7,5), (7,64), (2,5), (3,5), (7,17). Each group received three repetitions
of [9e37, 9e37-3, -9e37, NULL]; latency was one hour to isolate count-triggered bundles.

Shuffling the rows with Java Random seeds 0 through 7 for (2,5), (3,5), (7,17), (17,31)
failed 14 of 32 parameter cases, comparing collapsed final results, not just output order.
For example, (2,5,1) returned SUM 9e37-3 for one host group but -9e37 natively.

A diagnostic native flush sorted the first-appearance INT groups by their released-compatible
BinaryRow hash spread (`h ^ (h >>> 16)`) and final HashMap bucket index, retaining arrival order
within a bucket. Capacity started at 16 and doubled at 75% occupancy. With only that change,
all 32 shuffled cases passed, each checking SUM and AVG separately. This establishes local
group emission as an observable part of the global decimal merge contract. The diagnostic
sort and widened gate were then removed: sorting final buckets is insufficient for collision
trees and has not established the full key-type contract. Implement the complete ordering
model before admission; do not replace the native Arrow key encoding with BinaryRow bytes.

`FlinkWideDecimalGroupOrderSqlHarnessTest` retains four failing configurations as fallback
regressions with runtime input and explicit local/global host-plan assertions. They can become
native parity regressions when the ordering implementation is complete. A combined SUM+AVG
query over the same decimal argument rewrites to DISTINCT SUM0 in this host and was excluded
from the isolated transport probe because that aggregate form already falls back.

## Shared group-order model and exact-hash ties

The bucket/list/tree machinery now accepts caller-supplied spread hashes and tie ordering,
so group emission and decimal membership can share the same rotations and resize semantics.
Eighteen `group-map-order.json` fixtures cover 1,231 BinaryRowData BIGINT/NULL insertions,
duplicates, collision trees and resize splits. They verify released Flink hashes and map
iteration in Java and the shared core in Rust. The 55 decimal fixtures still pass unchanged.
The group fixtures intentionally have distinct complete hashes for unequal keys: BinaryRowData
is not Comparable, so equal-hash tree ties use JVM object identity, unlike DecimalData.

A separate released-host probe built sixteen distinct BIGINT BinaryRowData keys with complete
hash zero. Reconstructing the same key sequence 64 times produced 64 map orders in JDK 17.
With those keys, three repetitions of [9e37, 9e37-3, -9e37, NULL], Random(0) shuffle, count
bundles of 31 and one-hour latency, eight stock Flink 2.2.1 SQL jobs produced four different
final result sets. No native operator was installed. This is actual aggregate nondeterminism
when overflow makes JVM identity-dependent group order observable, not a native discrepancy.

`FlinkGroupIdentityOrderingTest` retains those exact keys and input. It asserts the two-phase
plan, colliding hashes, final key/count multiplicities, and that each SUM is one of the valid
host results (-9e37 or 9e37-3). It deliberately does not require a particular number of distinct
outputs across runs. Deterministic cases retain exact host/native parity. For this identity-tie
case a deterministic native tie order can select a valid host ordering; it cannot reproduce an
independent host run's object identities.

The local aggregate now records BinaryRow hashes on each group's first appearance and permutes
its emitted groups through the shared ordering model. It keeps native Arrow/scalar state keys;
BinaryRow is used only for the host-compatible hash. Declared timestamp precision travels through
the local planner/operator/JNI constructor, as it already does for the global state boundary.
Insertion ranks supply a deterministic valid identity ordering for exact-hash binary-key ties.

Flink clears its bundle HashMap without releasing the bucket array. The native group-order model
therefore clears nodes but preserves bucket capacity across flushes, and retains the corresponding
memory reservation. Three new fixtures cover large-to-small bundles after clear. The native local
integration checks every fixture through both physical key paths and accounts permutation scratch
space before draining. A direct operator regression compares TIMESTAMP/LTZ precision 3/9 group
emission with released BinaryRow HashMaps, including NULLs, duplicates and retained capacity.

With the planner gate temporarily widened for verification, all 32 shuffled SUM/AVG configurations,
three timestamp-group configurations (SUM and AVG each), and the exact-hash valid-outcome probe
pass on both released Flink versions. After these probes, positive-retention reads and writes
were deferred to the bundle-flush clock, with memory/RocksDB checkpoint regressions. Wider
admission now passes the combined 58-case SQL suite on Flink 2.2.1 and 57 cases with one host
capability skip on 1.18.1. The permanent identity-collision test also checks native results.
