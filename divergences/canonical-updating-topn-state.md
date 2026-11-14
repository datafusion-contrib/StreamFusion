# Stock updating Top-N state projection

Arroyo's `crates/arroyo-datastream/src/lib.rs` declares windowed Top-N but provides no equivalent
unbounded Flink unique-key tuple interchange. Retain the existing columnar update-fast ranker and
project its complete retained rows into released Flink 2.2.1 state descriptors and stock serializers.
The transient Flink rank-row helper is not the serialized value: state contains a stock row and its
integer position within an equal-sort group. A fast Top-1 instead stores one stock row per partition.

Comet's `native/core/src/execution/utils.rs` supplies the moved Arrow C Data ownership pattern.
Transfer owners drop before JNI errors are installed, and the existing bridge preserves the original
receiver exception after releasing FFI owners. Raw canonical RocksDB reads bypass expiry and visit
one decoded partition at a time, emitting at most 1,024 rows per callback. No native snapshot or
processing replay is an intermediate interchange format.

Use actual partition/unique/sort selectors and validate source descriptors, retained payloads and
keys before copying non-null primitives. Validate the whole source before applying any transfer;
then native frames check their own metadata, actual BinaryRow encodings and persisted earlier chunks.
A resource failure can partially populate its target, which must be closed; stock source state is
cleared only after full success, allowing a fresh target to retry. Read-only validation releases
read-through resident charges even on failure.

Automatic binding covers fixed start-one ranges without TTL, one/two sort fields and the verified
INT/BIGINT/VARCHAR payloads. Resolve the actual released strategy/function and selectors, identities,
maximum parallelism and before-image flag. An ordinary graph with unqualified native state owners
is rebuilt through the captured stock template: qualified roles accelerate while other owners
retain stock state. This currently keeps upstream grouped aggregation on Flink. Stock-bound rank
retains the per-record changelog under table mini-batching. The common node exposes only whether
it owns unique-key state; stock-template selection remains in the Flink 2.2 planner layer.

The adapter currently requires consecutive tied positions. Numeric
inner-rank gaps may be legal after Flink TTL restoration: its initializer preserves them despite
logging a warning. Do not silently replace those values with dense ordinals. Their representation,
TTL write ages and independent variable bounds require further implementation and actual-operator
qualification. The selected released two-sort fixture and automatic binding are qualified; the
broader process, native-graph, TTL, connector and version gates remain open in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.


Raw checkpoint restore intentionally stamps legacy/TTL-disabled rows for a possible enable-TTL
migration. A TTL-disabled canonical projection must omit that internal clock, since the stock
serializer has no age envelope. Preserve raw restore semantics and emit zero only in the qualified
canonical wire contract. The initial process matrix caught four failures from forwarding that
clock; focused memory/Rocks restore tests and the corrected full 120-job cohort pass.


The pending #569 extension admits constant updating OFFSET on Flink 1.18 and 2.2, reusing this
projection without adding metadata: stock and native retain the hidden prefix in the same map.
Exact changelog, tie, checkpoint and rescale controls are written but not yet executed. Keep TTL
and variable bounds gated; consecutive tie positions are still required. This extension changes
canonical binding of an existing native range, not its ordinary execution algorithm.


A further pending extension carries the existing first-observed variable bound in an explicit
transient header rather than deriving it from retained rows. The header has inner-rank and kind
sentinels of -1, an empty unique-key payload, NULL row columns, and the signed bound in the
otherwise unused TTL-disabled metadata word. Ordinary row records remain unchanged. The stock
projection writes the released `rankEnd` VALUE descriptor with LongSerializer; raw native
checkpoints already contain this bound. This introduces no new JNI entry point or private
state format. Comet's C Data ownership pattern remains unchanged, with all imported roots
scoped before callback failures propagate. New direct/stock/process controls await execution.
