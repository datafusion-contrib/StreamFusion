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

The initial foundation covers fixed ranges without TTL and consecutive tied positions. Numeric
inner-rank gaps may be legal after Flink TTL restoration: its initializer preserves them despite
logging a warning. Do not silently replace those values with dense ordinals. Their representation,
TTL write ages and independent variable bounds require further implementation and actual-operator
qualification. Automatic binding, process and released fixture gates remain open in
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569). Benchmarking is deferred.
