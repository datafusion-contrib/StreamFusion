package tech.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.format.EncodeFormat;
import tech.streamfusion.format.LogicalTypeDescriptors;
import tech.streamfusion.operator.RowDataArrowConverter;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.formats.avro.AvroFormatOptions.AvroEncoding;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Byte-level referee against Flink's own Avro serializer: the native encode must produce the exact
 * datum Flink writes, including its deliberate quirks (epoch-millisecond longs in micros schemas,
 * HashMap-ordered map entries).
 */
@Tag("streamfusion-kafka")
class NativeKafkaAvroEncoderTest {

  private static final RowType FULL_TYPE =
      RowType.of(
          false,
          new LogicalType[] {
            new BooleanType(),
            new TinyIntType(),
            new SmallIntType(),
            new IntType(),
            new BigIntType(),
            new FloatType(),
            new DoubleType(),
            new VarCharType(VarCharType.MAX_LENGTH),
            new BinaryType(3),
            new VarBinaryType(VarBinaryType.MAX_LENGTH),
            new DecimalType(5, 2),
            new DateType(),
            new TimeType(3),
            new TimestampType(3),
            RowType.of(new IntType(), new VarCharType(VarCharType.MAX_LENGTH)),
            new ArrayType(new IntType()),
            new MapType(new VarCharType(VarCharType.MAX_LENGTH), new BigIntType()),
            new MultisetType(new VarCharType(VarCharType.MAX_LENGTH))
          },
          new String[] {
            "flag", "ti", "si", "i", "bi", "f", "d", "s", "bin", "varbin", "dec", "day", "tod",
            "ts", "nested", "arr", "m", "bag"
          });

  private static GenericRowData populatedRow() {
    GenericRowData row = new GenericRowData(18);
    row.setField(0, true);
    row.setField(1, (byte) -2);
    row.setField(2, (short) 300);
    row.setField(3, 42);
    row.setField(4, 9_876_543_210L);
    row.setField(5, 1.5f);
    row.setField(6, -2.25d);
    row.setField(7, StringData.fromString("snow 雪"));
    row.setField(8, new byte[] {1, 2, 3});
    row.setField(9, new byte[] {9, 8});
    row.setField(10, DecimalData.fromBigDecimal(new BigDecimal("123.45"), 5, 2));
    row.setField(11, 19_000);
    row.setField(12, 45_296_789);
    row.setField(13, TimestampData.fromEpochMillis(1_500L, 123_456));
    row.setField(14, GenericRowData.of(7, StringData.fromString("inner")));
    row.setField(15, new GenericArrayData(new Integer[] {1, null, 3}));
    Map<StringData, Long> m = new LinkedHashMap<>();
    m.put(StringData.fromString("zebra"), 1L);
    m.put(StringData.fromString("apple"), null);
    m.put(StringData.fromString("mango"), 3L);
    m.put(StringData.fromString("kiwi"), 4L);
    row.setField(16, new GenericMapData(m));
    Map<StringData, Integer> bag = new LinkedHashMap<>();
    for (int i = 0; i < 13; i++) {
      bag.put(StringData.fromString("key" + (12 - i)), i + 1);
    }
    row.setField(17, new GenericMapData(bag));
    return row;
  }

  private static GenericRowData nullRow() {
    return new GenericRowData(18);
  }

  @Test
  void matchesFlinkAcrossTheEncodableTypeFamily() throws Exception {
    assertMatchesFlink(List.of(populatedRow(), nullRow()), FULL_TYPE, Map.of());
  }

  /**
   * The corrected timestamp mapping derives micros schemas for precision 4..6 and admits
   * TIMESTAMP_LTZ — but Flink still writes epoch-millisecond longs into them (its converter calls
   * toEpochMilli in every branch), values 1000x smaller than the schema claims. Parity means
   * reproducing that, sub-millisecond digits dropped, pre-epoch values floored.
   */
  @Test
  void reproducesFlinksMillisecondLongsUnderTheCorrectedMapping() throws Exception {
    RowType rowType =
        RowType.of(
            false,
            new LogicalType[] {
              new TimestampType(3),
              new TimestampType(6),
              new LocalZonedTimestampType(3),
              new LocalZonedTimestampType(6)
            },
            new String[] {"ts3", "ts6", "ltz3", "ltz6"});
    GenericRowData row = new GenericRowData(4);
    row.setField(0, TimestampData.fromEpochMillis(1_500L, 123_456));
    row.setField(1, TimestampData.fromEpochMillis(1_500L, 123_456));
    row.setField(2, TimestampData.fromEpochMillis(-1L, 999_999));
    row.setField(3, TimestampData.fromEpochMillis(-7L, 5_000));
    assertMatchesFlink(
        List.of(row, new GenericRowData(4)), rowType, Map.of("timestamp_mapping.legacy", "false"));
  }

  /** Flink NPEs on a null map key; the native encode fails the batch with an explicit error. */
  @Test
  void nullMapKeysFailOnBothEngines() throws Exception {
    RowType rowType =
        RowType.of(
            false,
            new LogicalType[] {
              new MapType(new VarCharType(VarCharType.MAX_LENGTH), new BigIntType())
            },
            new String[] {"m"});
    Map<StringData, Long> m = new LinkedHashMap<>();
    m.put(StringData.fromString("a"), 1L);
    m.put(null, 2L);
    GenericRowData row = GenericRowData.of(new GenericMapData(m));

    AvroRowDataSerializationSchema flink = referee(rowType, true);
    assertThrows(RuntimeException.class, () -> flink.serialize(row));

    Exception failure =
        assertThrows(Exception.class, () -> encodeNatively(List.of(row), rowType, Map.of()));
    assertTrue(failure.getMessage().contains("NULL map key"), failure.getMessage());
  }

  /** Confluent framing is the magic byte plus the registered id ahead of the same datum. */
  @Test
  void framesConfluentMessagesAroundTheLegacyDatum() throws Exception {
    RowType rowType =
        RowType.of(
            false,
            new LogicalType[] {new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH)},
            new String[] {"id", "name"});
    GenericRowData row = GenericRowData.of(42L, StringData.fromString("hi"));
    AvroRowDataSerializationSchema flink = referee(rowType, true);
    byte[] datum = flink.serialize(row);
    byte[] expected = new byte[datum.length + 5];
    expected[0] = 0;
    expected[4] = 39; // big-endian id 39
    System.arraycopy(datum, 0, expected, 5, datum.length);

    EncodeFormat format =
        EncodeFormat.of(
            "avro-confluent",
            Map.of("url", "http://registry:8081", "subject", "orders-value"),
            rowType);
    assertNotNull(format);
    byte[][] actual =
        encode(List.of(row), rowType, format.format, format.options + "schema-id=39\n");
    assertArrayEquals(expected, actual[0]);
  }

  /** Upsert avro records: the key format serializes the PK projection; deletes are tombstones. */
  @Test
  void serializesUpsertKeysAndTombstonesWithAvroFormats() throws Exception {
    RowType rowType =
        RowType.of(
            false,
            new LogicalType[] {new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH)},
            new String[] {"id", "name"});
    RowType keyType = RowType.of(false, new LogicalType[] {new BigIntType()}, new String[] {"id"});
    GenericRowData insert = GenericRowData.of(1L, StringData.fromString("a"));
    GenericRowData delete = GenericRowData.of(2L, StringData.fromString("b"));
    delete.setRowKind(org.apache.flink.types.RowKind.DELETE);

    EncodeFormat format = EncodeFormat.of("avro", Map.of(), rowType);
    EncodeFormat keyFormat = EncodeFormat.of("avro", Map.of(), keyType);
    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        VectorSchemaRoot root =
            RowDataArrowConverter.write(List.of(insert, delete), rowType, allocator, true);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      byte[][][] records =
          NativeKafka.encodeKafkaRecords(
              array.memoryAddress(),
              schema.memoryAddress(),
              format.format,
              format.options,
              keyFormat.format,
              keyFormat.options,
              LogicalTypeDescriptors.of(rowType),
              rowType.getFieldNames().toArray(String[]::new),
              new int[] {0},
              new int[] {0, 1},
              true);

      AvroRowDataSerializationSchema valueReferee = referee(rowType, true);
      AvroRowDataSerializationSchema keyReferee = referee(keyType, true);
      assertArrayEquals(keyReferee.serialize(GenericRowData.of(1L)), records[0][0]);
      assertArrayEquals(valueReferee.serialize(insert), records[1][0]);
      assertArrayEquals(keyReferee.serialize(GenericRowData.of(2L)), records[0][1]);
      assertNull(records[1][1]);
    }
  }

  private static void assertMatchesFlink(
      List<RowData> rows, RowType rowType, Map<String, String> options) throws Exception {
    boolean legacy = !"false".equalsIgnoreCase(options.get("timestamp_mapping.legacy"));
    AvroRowDataSerializationSchema flink = referee(rowType, legacy);
    byte[][] actual = encodeNatively(rows, rowType, options);
    assertEquals(rows.size(), actual.length);
    for (int i = 0; i < rows.size(); i++) {
      assertArrayEquals(flink.serialize(rows.get(i)), actual[i], "row " + i);
    }
  }

  private static byte[][] encodeNatively(
      List<RowData> rows, RowType rowType, Map<String, String> options) throws Exception {
    EncodeFormat format = EncodeFormat.of("avro", options, rowType);
    assertNotNull(format);
    return encode(rows, rowType, format.format, format.openOptions());
  }

  private static byte[][] encode(
      List<RowData> rows, RowType rowType, int format, String formatOptions) {
    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, rowType, allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      return NativeKafka.encodeKafkaBatch(
          array.memoryAddress(),
          schema.memoryAddress(),
          format,
          formatOptions,
          LogicalTypeDescriptors.of(rowType),
          rowType.getFieldNames().toArray(String[]::new));
    }
  }

  private static AvroRowDataSerializationSchema referee(RowType rowType, boolean legacy)
      throws Exception {
    AvroRowDataSerializationSchema referee =
        new AvroRowDataSerializationSchema(rowType, AvroEncoding.BINARY, legacy);
    referee.open(null);
    return referee;
  }
}
