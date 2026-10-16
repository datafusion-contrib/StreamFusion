# Nested Flink ROW kinds

Comet's Arrow struct vectors expose logical children directly. Flink's Java `Row` and `RowData`
also carry a `RowKind`, including when a ROW is a SQL field or an ARRAY element. A Java scalar
function can inspect that kind. Carrying only the SQL fields changes observable callback results:
the nested-kind fixture computed different BIGINT values from stock Flink for UPDATE_BEFORE,
UPDATE_AFTER and DELETE inputs.

The default-off `streamfusion.experimental.nestedScalarUdfs` investigation appends a nullable Int8 child
tagged `streamfusion.nested-row-kind=1` to each SQL ROW's Arrow struct. Readers/writers keep it
outside logical field ordinals. The field name is made unique when user fields use the internal
prefix. NULL parents remain NULL, and the marker follows nested structs and array elements through
ordinary Arrow ownership and serialization. No borrowed Arrow value is retained by a callback;
released Flink converters materialize the external Java ROW, and results are written immediately.

The generated Calc's outer struct is a projection container rather than a SQL ROW value, so it
has no extra kind child. ROW-valued fields inside it do. Computed ROW-valued programs and filters, including SQL ROW constructors and comparisons,
use Flink whole-Calc generation. Pure input-reference projections keep the native path. Missing or NULL markers read as INSERT. Arrow type compatibility compares logical
children separately from the marked child.

Native key encoding excludes metadata from logical type descriptors and writes its value into
the released Flink BinaryRow header. ARRAY-of-ROW UNNEST excludes the tagged child from output
columns. Sort-key preparation compares a constant kind child while preserving the physical schema
and the original payload: Flink SQL ROW ordering ignores RowKind. This transport does not replace
Flink's released array converters; generic Java-backed array elements changed Unicode ordering
in an ARRAY_SORT regression and were rejected.

This differs from Comet because Spark structs have no corresponding per-value changelog kind.
The existing outer record RowKind column is unchanged. The experiment must be configured
consistently in the planner and task JVMs because the flag also selects this physical struct
schema. Default schemas remain unchanged. Distributed configuration and performance admission
remain part of the investigation before enabling the feature by default.

## Binary payload lengths

The same experimental flag maps SQL BINARY to Arrow's variable binary representation. A released
Flink ROW callback declared `ROW<payload BINARY(4)>` retains byte arrays of lengths zero, one,
five and 4,097 without padding or truncation. FixedSizeBinary storage would change or reject
those values. Keeping the logical SQL width separate from physical storage preserves them through
ROW/ARRAY boundaries and subsequent field projections. Native fixed-size outputs remain readable
as bytes, and default schemas/admission stay unchanged when the flag is disabled.

This follows [Comet's binary reader](https://github.com/apache/datafusion-comet/blob/4897161704b7b8b7dfa909f4bf897c6508b11117/spark/src/main/java/org/apache/comet/vector/CometPlainVector.java#L169):
the physical vector supplies its actual byte extent, and the Java result owns its bytes. Released
Flink converters still normalize callback inputs/results; the experiment changes neither their
SQL types nor evaluation order. The key-byte/hash oracle compares the resulting nested ROW/ARRAY
values directly with released Flink serialization, including NULL payloads and all inner kinds.
Nested binary recovery and whole-job performance admission remain pending in #235/#237.


The binary-ELT opt-in independently enables the same length-delimited BINARY transport.
This preserves released Flink raw-byte extents at existing native selection boundaries,
while the logical SQL width and nested-scalar admission flag remain independent. Comet's
`getBinary` reads the actual vector extent into an owned result; the existing released
Arrow readers and writers retain that ownership pattern. No new JNI/memory handoff or
Rust selection kernel is introduced. The maintainer accepted deferred throughput work.
