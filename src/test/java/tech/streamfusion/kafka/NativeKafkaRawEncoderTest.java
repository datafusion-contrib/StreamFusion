package tech.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import tech.streamfusion.format.EncodeFormat;
import tech.streamfusion.format.LogicalTypeDescriptors;
import tech.streamfusion.operator.RowDataArrowConverter;
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
import org.apache.flink.formats.raw.RawFormatSerializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Referees every native raw sink byte against Flink's {@code RawFormatSerializationSchema}: the
 * single NOT NULL column's value is the whole message — strings as UTF-8, bytes verbatim, BOOLEAN
 * one byte, fixed-width numerics in the table's {@code raw.endianness}. Nullable columns fall back
 * at plan time because Flink writes a null field as a Kafka tombstone, which the sink's value path
 * does not produce.
 */
@Tag("streamfusion-kafka")
class NativeKafkaRawEncoderTest {

  @Test
  void matchesFlinkForEveryTypeAndEndianness() throws Exception {
    assertMatchesFlink(
        new VarCharType(false, VarCharType.MAX_LENGTH),
        List.of(StringData.fromString("plain"), StringData.fromString("雪 and 😀"), StringData.fromString("")));
    assertMatchesFlink(
        new VarBinaryType(false, VarBinaryType.MAX_LENGTH),
        List.of(new byte[] {1, 2, 3}, new byte[0], new byte[] {-1, 0, 127}));
    assertMatchesFlink(new BooleanType(false), List.of(true, false));
    assertMatchesFlink(new TinyIntType(false), List.of((byte) -1, (byte) 0, (byte) 127));
    assertMatchesFlink(new SmallIntType(false), List.of((short) -300, (short) 0, (short) 300));
    assertMatchesFlink(new IntType(false), List.of(-123456, 0, 123456));
    assertMatchesFlink(new BigIntType(false), List.of(-9_876_543_210L, 0L, 9_876_543_210L));
    assertMatchesFlink(new FloatType(false), List.of(-2.5f, 0.0f, 3.25f));
    assertMatchesFlink(new DoubleType(false), List.of(-2.5, 0.0, 1e300));
  }

  @Test
  void gatesShapesTheValuePathCannotCarry() {
    RowType notNull = RowType.of(new LogicalType[] {new BigIntType(false)}, new String[] {"v"});
    assertNotNull(EncodeFormat.of("raw", Map.of(), notNull));
    assertNotNull(EncodeFormat.of("raw", Map.of("charset", "utf8"), notNull));
    assertNotNull(EncodeFormat.of("raw", Map.of("endianness", "little-endian"), notNull));
    // A nullable column writes Kafka tombstones for null values — falls back.
    RowType nullable = RowType.of(new LogicalType[] {new BigIntType(true)}, new String[] {"v"});
    assertNull(EncodeFormat.of("raw", Map.of(), nullable));
    // A non-UTF-8 charset, an invalid endianness, and multi-column schemas stay on Flink.
    assertNull(EncodeFormat.of("raw", Map.of("charset", "UTF-16"), notNull));
    assertNull(EncodeFormat.of("raw", Map.of("endianness", "middle-endian"), notNull));
    assertNull(
        EncodeFormat.of(
            "raw",
            Map.of(),
            RowType.of(
                new LogicalType[] {new BigIntType(false), new BigIntType(false)},
                new String[] {"a", "b"})));
  }

  private static void assertMatchesFlink(LogicalType type, List<Object> values) throws Exception {
    for (String endianness : new String[] {"big-endian", "little-endian"}) {
      RowType rowType = RowType.of(new LogicalType[] {type}, new String[] {"v"});
      RawFormatSerializationSchema flink =
          new RawFormatSerializationSchema(type, "UTF-8", "big-endian".equals(endianness));
      flink.open(null);
      Map<String, String> options =
          "big-endian".equals(endianness) ? Map.of() : Map.of("endianness", endianness);
      EncodeFormat format = EncodeFormat.of("raw", options, rowType);
      assertNotNull(format, () -> type + " with " + endianness);

      List<RowData> rows = values.stream().map(GenericRowData::of).map(RowData.class::cast).toList();
      try (BufferAllocator allocator = new RootAllocator();
          CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
          VectorSchemaRoot root = RowDataArrowConverter.write(rows, rowType, allocator);
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
              type + " " + endianness + " row " + i + ": "
                  + new String(actual[i], StandardCharsets.UTF_8));
        }
      }
    }
  }
}
