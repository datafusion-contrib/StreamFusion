# Released OVER DISTINCT membership projection

Arroyo does not provide Flink's independent DISTINCT MapView schema. Reuse StreamFusion's existing
unbounded native DISTINCT fold and membership visitors, and bind them to the released Flink
accumulator, map view and external serializers. Comet's execution utilities and ArrowImporter
supply the moved C Data ownership pattern; imported/exported roots are scoped before errors
propagate. No new JNI entry point or native checkpoint format is needed.

The Java arithmetic binding covers TINYINT/SMALLINT/INT/BIGINT COUNT/SUM/SUM0/AVG DISTINCT in
TTL-disabled unbounded processing-time ROWS/RANGE frames. COUNT additionally uses the scalar
member codecs described below. Nullable arguments retain stock's NULL
membership marker independently of the scalar count/sum. Memory holds a presence flag alongside
the non-NULL set; RocksDB stores a typed NULL in its existing membership table. Existing raw
checkpoint member lists already admit typed NULLs, so no record field is added. Live NULL tracking
is enabled only for this bound processing contract. TTL still needs independent scalar/view lifetimes. Those existing
native protocols and the wider event/bounded contracts require separate qualification under
[#569](https://github.com/datafusion-contrib/StreamFusion/issues/569).

The stock scalar and membership views validate together before any native insertion, including
membership-bit domains, cardinality, required NULL markers and wrapping integral sums. Narrow SUM
and wide AVG fields are checked independently. This retains
source state for retry. Canonical capture visits live/frozen native state and does not create an
intermediate native checkpoint. The new both-release state/planner/process controls are authored
but remain unexecuted until the final local testing window; benchmarking is deferred.

NULL flags do not count as distinct numeric members. Native completion validation excludes them
from cardinality, and Java preflight checks their bit domain and scalar owner before either table
changes. Required arguments reject any NULL marker. The released non-null RAW view placeholder
uses the same NullSerializer byte representation as the grouped-state adapter. This extension
does not reconstruct NULL history absent from older unbound native checkpoints.

ROW partition columns reuse the fold's selective original-byte schema in the membership view.
Only those columns receive binary ROW metadata; scalar companion keys and membership values
keep their existing carriers. Import normalizes the leading key columns before matching the
native fold's Arrow codec. This preserves reserved bytes and nested kinds across generic/live and
binary/restored forms without changing stock serializers or adding a replay phase. Scalar/ROW
descendant and processing/TTL gates remain shared with the fold binding.

COUNT also reuses existing decimal, BOOLEAN and date/time/timestamp membership codecs. Captured
internal key data types keep their released serializers (including DecimalData and TimestampData);
preflight checks typed members, exact cardinality and nullable-marker ownership. Arithmetic views
keep the integral restriction, so no decimal arithmetic history is reconstructed from membership.
CHAR/VARCHAR COUNT uses the existing UTF-8 member codec. Before insertion, text preflight rejects
malformed UTF-8 and Java caches inconsistent with their serialized bytes. Valid caches are
equivalent to UTF-8 equality; invalid caches can survive in heap state but have already been
materialized by RocksDB. The source remains intact on rejection, and no text arithmetic history
is reconstructed. Floating/complex membership and wider clock/TTL contracts remain unbound. Unqualified
ordinary native processing is not counted as canonical qualification.
