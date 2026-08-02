package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.jordepic.streamfusion.format.EncodeFormat;
import io.github.jordepic.streamfusion.format.LogicalTypeDescriptors;
import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.serialize.PbRowDataSerializationSchema;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Referees every native protobuf sink byte against Flink's {@code PbRowDataSerializationSchema}:
 * null columns leave fields unset, nulls inside containers become type defaults (strings the
 * {@code write-null-string-literal} value), and an all-unset row is Flink's empty {@code byte[]}.
 * Maps stay single-entry — multi-entry wire order is HashMap iteration order on Flink's side and
 * therefore not byte-defined.
 */
@Tag("streamfusion-kafka")
class NativeKafkaProtobufEncoderTest {

  private static final String PKG = "io.github.jordepic.streamfusion.proto";

  @Test
  void matchesFlinkForScalarRows() throws Exception {
    RowType rowType =
        RowType.of(
            new LogicalType[] {
              new IntType(),
              new BigIntType(),
              new BooleanType(),
              new FloatType(),
              new DoubleType(),
              new VarCharType(VarCharType.MAX_LENGTH),
              new IntType(),
              new BigIntType()
            },
            new String[] {"i32", "i64", "flag", "f32", "f64", "text", "si32", "si64"});
    List<RowData> rows =
        List.of(
            GenericRowData.of(
                7, 9_876_543_210L, true, 2.5f, -3.25, StringData.fromString("plain"), -1, -2L),
            GenericRowData.of(
                0, 0L, false, 0.0f, 0.0, StringData.fromString(""), 0, 0L),
            GenericRowData.of(null, null, null, null, null, null, null, null),
            GenericRowData.of(
                null, 5L, null, null, 1.5, StringData.fromString("holes"), -100, null));

    assertMatchesFlink(rows, rowType, PKG + ".Scalars", Map.of());
  }

  @Test
  void matchesFlinkForNestedRows() throws Exception {
    RowType nested =
        RowType.of(
            new LogicalType[] {
              new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH), new DoubleType()
            },
            new String[] {"id", "name", "score"});
    RowType rowType =
        RowType.of(new LogicalType[] {new BigIntType(), nested}, new String[] {"id", "nested"});
    List<RowData> rows =
        List.of(
            GenericRowData.of(
                1L, GenericRowData.of(2L, StringData.fromString("inner"), 0.5)),
            // Null fields inside a non-null nested row leave the nested builder's fields unset.
            GenericRowData.of(3L, GenericRowData.of(null, null, 1.25)),
            // A null nested row leaves the message field unset entirely.
            GenericRowData.of(4L, null),
            GenericRowData.of(null, null));

    assertMatchesFlink(rows, rowType, PKG + ".WithNested", Map.of());
  }

  @Test
  void matchesFlinkForContainersAndNullDefaults() throws Exception {
    RowType nested =
        RowType.of(
            new LogicalType[] {
              new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH), new DoubleType()
            },
            new String[] {"id", "name", "score"});
    RowType rowType =
        RowType.of(
            new LogicalType[] {
              new BigIntType(),
              new ArrayType(new BigIntType()),
              new MapType(new VarCharType(VarCharType.MAX_LENGTH), new BigIntType()),
              nested
            },
            new String[] {"id", "nums", "tags", "nested"});
    List<RowData> rows =
        List.of(
            GenericRowData.of(
                1L,
                new GenericArrayData(new Long[] {10L, 20L, 30L}),
                new GenericMapData(Map.of(StringData.fromString("a"), 5L)),
                GenericRowData.of(2L, StringData.fromString("x"), 1.5)),
            // Null array elements and map values become proto defaults on both engines.
            GenericRowData.of(
                2L,
                new GenericArrayData(new Long[] {null, 40L}),
                singleEntryMap(StringData.fromString("k"), null),
                null),
            // Null containers leave the repeated/map fields unset.
            GenericRowData.of(3L, null, null, null));

    for (Map<String, String> options :
        List.of(Map.<String, String>of(), Map.of("write-null-string-literal", "-"))) {
      assertMatchesFlink(rows, rowType, PKG + ".Complex", options);
    }
  }

  @Test
  void gatesShapesFlinkWouldRejectOrDivergeOn() {
    RowType matching =
        RowType.of(new LogicalType[] {new BigIntType()}, new String[] {"id"});
    assertNotNull(
        EncodeFormat.of("protobuf", Map.of("message-class-name", PKG + ".Row"), matching));
    // A column type that mismatches the proto field falls back so Flink raises its own error.
    RowType mismatched =
        RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});
    assertNull(
        EncodeFormat.of("protobuf", Map.of("message-class-name", PKG + ".Row"), mismatched));
    // A column naming no proto field would panic the native encoder — it must never plan.
    RowType unknown =
        RowType.of(new LogicalType[] {new BigIntType()}, new String[] {"nope"});
    assertNull(
        EncodeFormat.of("protobuf", Map.of("message-class-name", PKG + ".Row"), unknown));
    // Presence shapes the decode gate rejects stay rejected for encode (shared shape gate).
    assertNull(
        EncodeFormat.of(
            "protobuf",
            Map.of("message-class-name", PKG + ".WithOptionalScalar"),
            RowType.of(new LogicalType[] {new BigIntType()}, new String[] {"id"})));
    // No message class, or a literal that cannot ride the line-encoded carrier.
    assertNull(EncodeFormat.of("protobuf", Map.of(), matching));
    assertNull(
        EncodeFormat.of(
            "protobuf",
            Map.of(
                "message-class-name", PKG + ".Row",
                "write-null-string-literal", "line\nbreak"),
            matching));
  }

  /** GenericMapData over Map.of cannot hold null values; build the backing map by hand. */
  private static GenericMapData singleEntryMap(Object key, Object value) {
    java.util.HashMap<Object, Object> entries = new java.util.HashMap<>();
    entries.put(key, value);
    return new GenericMapData(entries);
  }

  /**
   * The plan often feeds the sink generated expression names ({@code EXPR$0}, ...). Columns map
   * to proto fields by the declared sink names carried across the boundary, so the encode must
   * rename the batch first instead of failing on a generated name.
   */
  @Test
  void matchesFlinkWhenThePlanRenamesColumns() throws Exception {
    RowType declared =
        RowType.of(
            new LogicalType[] {
              new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH), new DoubleType()
            },
            new String[] {"id", "name", "score"});
    RowType generated =
        RowType.of(
            declared.getChildren().toArray(LogicalType[]::new),
            new String[] {"EXPR$0", "EXPR$1", "EXPR$2"});
    List<RowData> rows =
        List.of(
            GenericRowData.of(7L, StringData.fromString("renamed"), 1.5),
            GenericRowData.of(null, null, null));

    assertMatchesFlink(rows, declared, generated, PKG + ".Row", Map.of());
  }

  private static void assertMatchesFlink(
      List<RowData> rows, RowType rowType, String messageClass, Map<String, String> options)
      throws Exception {
    assertMatchesFlink(rows, rowType, rowType, messageClass, options);
  }

  private static void assertMatchesFlink(
      List<RowData> rows,
      RowType rowType,
      RowType batchRowType,
      String messageClass,
      Map<String, String> options)
      throws Exception {
    PbFormatConfig config =
        new PbFormatConfig(
            messageClass, false, false, options.getOrDefault("write-null-string-literal", ""));
    PbRowDataSerializationSchema flink = new PbRowDataSerializationSchema(rowType, config);
    flink.open(null); // Janino codegen happens at open; the context is unused

    java.util.HashMap<String, String> formatOptions = new java.util.HashMap<>(options);
    formatOptions.put("message-class-name", messageClass);
    EncodeFormat format = EncodeFormat.of("protobuf", formatOptions, rowType);
    assertNotNull(format, options::toString);

    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, batchRowType, allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      byte[][] actual =
          NativeKafka.encodeKafkaBatch(
              array.memoryAddress(),
              schema.memoryAddress(),
              format.format,
              format.options,
              LogicalTypeDescriptors.of(rowType),
              rowType.getFieldNames().toArray(String[]::new));

      assertEquals(rows.size(), actual.length);
      for (int i = 0; i < rows.size(); i++) {
        byte[] expected = flink.serialize(rows.get(i));
        assertArrayEquals(
            expected,
            actual[i],
            "options "
                + options
                + ", row "
                + i
                + ": expected "
                + new String(expected, StandardCharsets.UTF_8)
                + ", actual "
                + new String(actual[i], StandardCharsets.UTF_8));
      }
    }
  }
}
