package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.RowDataArrowConverter;

class FlinkBinaryRowHashParityTest {

  @Test
  @org.junit.jupiter.api.condition.EnabledIfSystemProperty(
      named = "streamfusion.experimental.nestedScalarUdfs",
      matches = "true")
  void nestedFixedBinaryUsesExactRuntimeBytesAndHashes() {
    RowType child = RowType.of(new BinaryType(4));
    RowType type = RowType.of(child, new ArrayType(new BinaryType(4)));
    List<RowData> rows = new ArrayList<>();
    for (int width : new int[] {0, 1, 4, 8, 4097}) {
      byte[] bytes = new byte[width];
      for (int i = 0; i < width; i++) bytes[i] = (byte) (128 + i * 73);
      for (org.apache.flink.types.RowKind kind : org.apache.flink.types.RowKind.values()) {
        GenericRowData nested = GenericRowData.of((Object) bytes);
        nested.setRowKind(kind);
        rows.add(
            GenericRowData.of(
                nested, new GenericArrayData(new Object[] {bytes, null, new byte[0]})));
      }
    }
    GenericRowData nullPayload = GenericRowData.of((Object) null);
    nullPayload.setRowKind(org.apache.flink.types.RowKind.DELETE);
    rows.add(GenericRowData.of(nullPayload, new GenericArrayData(new Object[] {null})));
    rows.add(GenericRowData.of(null, new GenericArrayData(new Object[0])));
    rows.add(GenericRowData.of(null, null));
    int[] descriptors = {-1, -1, -1, -1};
    RowDataSerializer serializer = new RowDataSerializer(type);
    assertArrayEquals(
        rows.stream().mapToInt(row -> serializer.toBinaryRow(row).hashCode()).toArray(),
        nativeHashes(rows, type, new int[] {0, 1}, descriptors));
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, type, allocator);
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      byte[][] actual =
          Native.flinkBinaryRows(
              array.memoryAddress(),
              schema.memoryAddress(),
              new int[] {0, 1},
              descriptors,
              java.util.stream.IntStream.range(0, rows.size()).toArray());
      for (int i = 0; i < rows.size(); i++) {
        BinaryRowData binary = serializer.toBinaryRow(rows.get(i));
        byte[] expected = new byte[binary.getSizeInBytes()];
        binary.getSegments()[0].get(binary.getOffset(), expected, 0, expected.length);
        assertArrayEquals(expected, actual[i], "row=" + i);
      }
    }
  }

  @Test
  @org.junit.jupiter.api.condition.EnabledIfSystemProperty(
      named = "streamfusion.experimental.nestedScalarUdfs",
      matches = "true")
  void nestedRowKindsMatchReleasedBinaryBytesAndHashes() {
    RowType child = RowType.of(new BigIntType(), new TimestampType(9));
    RowType type = RowType.of(child, new ArrayType(child));
    List<RowData> rows = new ArrayList<>();
    for (org.apache.flink.types.RowKind kind : org.apache.flink.types.RowKind.values()) {
      GenericRowData nested = GenericRowData.of(7L, TimestampData.fromEpochMillis(-1, 123456));
      nested.setRowKind(kind);
      rows.add(GenericRowData.of(nested, new GenericArrayData(new Object[] {nested, null})));
    }
    rows.add(GenericRowData.of(null, new GenericArrayData(new Object[0])));
    rows.add(GenericRowData.of(null, null));
    int[] descriptors = {-1, -1, 9, -1, -1, -1, 9};
    RowDataSerializer serializer = new RowDataSerializer(type);
    assertArrayEquals(
        rows.stream().mapToInt(row -> serializer.toBinaryRow(row).hashCode()).toArray(),
        nativeHashes(rows, type, new int[] {0, 1}, descriptors));
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, type, allocator);
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      byte[][] actual =
          Native.flinkBinaryRows(
              array.memoryAddress(),
              schema.memoryAddress(),
              new int[] {0, 1},
              descriptors,
              new int[] {0, 1, 2, 3, 4, 5});
      for (int i = 0; i < rows.size(); i++) {
        BinaryRowData binary = serializer.toBinaryRow(rows.get(i));
        byte[] expected = new byte[binary.getSizeInBytes()];
        binary.getSegments()[0].get(binary.getOffset(), expected, 0, expected.length);
        assertArrayEquals(expected, actual[i], "nested kind row " + i);
      }
    }
  }

  private static final RowType KEY_SCHEMA =
      RowType.of(
          new LogicalType[] {
            new BooleanType(),
            new IntType(),
            new BigIntType(),
            new FloatType(),
            new DoubleType(),
            new VarCharType(VarCharType.MAX_LENGTH),
            new BinaryType(3),
            new VarBinaryType(VarBinaryType.MAX_LENGTH),
            new DecimalType(10, 2),
            new DecimalType(30, 5),
            new DateType(),
            new TimeType(9),
            new TimestampType(3),
            new TimestampType(9),
            new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH),
            new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND)
          },
          new String[] {
            "bool",
            "int",
            "bigint",
            "float",
            "double",
            "string",
            "fixed_binary",
            "binary",
            "compact_decimal",
            "wide_decimal",
            "date",
            "time",
            "compact_timestamp",
            "wide_timestamp",
            "months",
            "millis"
          });

  private static List<RowData> scalarRows() {
    GenericRowData values = new GenericRowData(KEY_SCHEMA.getFieldCount());
    values.setField(0, true);
    values.setField(1, -123);
    values.setField(2, 9_876_543_210L);
    values.setField(3, -0.0f);
    values.setField(4, Double.longBitsToDouble(0x7ff8_0000_0000_0001L));
    values.setField(5, StringData.fromString("Flink \uD83E\uDD80"));
    values.setField(6, new byte[] {0, -1, 7});
    values.setField(7, new byte[] {9, 8, 7, 6, 5, 4, 3, 2});
    values.setField(8, DecimalData.fromBigDecimal(new BigDecimal("-12345.67"), 10, 2));
    values.setField(
        9, DecimalData.fromBigDecimal(new BigDecimal("1234567890123456789012345.67890"), 30, 5));
    values.setField(10, -18_000);
    values.setField(11, 86_399_123);
    values.setField(12, TimestampData.fromEpochMillis(-1_234));
    values.setField(13, TimestampData.fromEpochMillis(-1_234, 567_000));
    values.setField(14, -14);
    values.setField(15, -86_399_123L);

    GenericRowData nulls = new GenericRowData(KEY_SCHEMA.getFieldCount());
    return List.of(values, nulls);
  }

  @Test
  void nativeHashMatchesFlinksBinaryRowForScalarKeys() {
    List<RowData> rows = scalarRows();
    int[] keyColumns = java.util.stream.IntStream.range(0, KEY_SCHEMA.getFieldCount()).toArray();
    int[] timestampPrecisions = new int[KEY_SCHEMA.getFieldCount()];
    java.util.Arrays.fill(timestampPrecisions, -1);
    timestampPrecisions[12] = 3;
    timestampPrecisions[13] = 9;

    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, KEY_SCHEMA, allocator);
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      int[] actual =
          Native.flinkBinaryRowHashes(
              array.memoryAddress(), schema.memoryAddress(), keyColumns, timestampPrecisions);
      RowDataSerializer serializer = new RowDataSerializer(KEY_SCHEMA);
      int[] expected =
          rows.stream().mapToInt(row -> serializer.toBinaryRow(row).hashCode()).toArray();
      assertArrayEquals(expected, actual);
    }
  }

  @Test
  void nativeBinaryRowsMatchFlinksSerializedBytes() {
    List<RowData> rows = scalarRows();
    int[] keyColumns = {5, 2, 12};
    int[] timestampPrecisions = {-1, -1, 3};
    RowType keyType =
        RowType.of(new VarCharType(VarCharType.MAX_LENGTH), new BigIntType(), new TimestampType(3));
    RowDataSerializer serializer = new RowDataSerializer(keyType);
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, KEY_SCHEMA, allocator);
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      byte[][] actual =
          Native.flinkBinaryRows(
              array.memoryAddress(),
              schema.memoryAddress(),
              keyColumns,
              timestampPrecisions,
              new int[] {1, 0});
      for (int i = 0; i < 2; i++) {
        RowData row = rows.get(1 - i);
        GenericRowData key = new GenericRowData(3);
        key.setField(0, row.isNullAt(5) ? null : row.getString(5));
        key.setField(1, row.isNullAt(2) ? null : row.getLong(2));
        key.setField(2, row.isNullAt(12) ? null : row.getTimestamp(12, 3));
        BinaryRowData binary = serializer.toBinaryRow(key);
        byte[] expected = new byte[binary.getSizeInBytes()];
        binary.getSegments()[0].get(binary.getOffset(), expected, 0, expected.length);
        assertArrayEquals(expected, actual[i]);
      }
    }
  }

  @Test
  void nativeHashMatchesFlinksBinaryRowForNestedKeys() {
    RowType schema =
        RowType.of(
            new LogicalType[] {
              new ArrayType(new IntType()),
              new MapType(new VarCharType(VarCharType.MAX_LENGTH), new ArrayType(new IntType())),
              RowType.of(
                  new LogicalType[] {
                    new VarCharType(VarCharType.MAX_LENGTH), new ArrayType(new TimestampType(9))
                  },
                  new String[] {"name", "times"}),
              new MultisetType(new VarCharType(VarCharType.MAX_LENGTH))
            },
            new String[] {"numbers", "map_of_arrays", "nested_row", "multiset"});

    LinkedHashMap<StringData, Object> map = new LinkedHashMap<>();
    map.put(StringData.fromString("a"), new GenericArrayData(new Integer[] {3, null, -4}));
    map.put(StringData.fromString("b"), new GenericArrayData(new Integer[] {}));
    LinkedHashMap<StringData, Integer> multiset = new LinkedHashMap<>();
    multiset.put(StringData.fromString("red"), 2);
    multiset.put(StringData.fromString("blue"), 1);
    GenericRowData nested =
        GenericRowData.of(
            StringData.fromString("nest"),
            new GenericArrayData(
                new TimestampData[] {
                  TimestampData.fromEpochMillis(-1, 999_999),
                  null,
                  TimestampData.fromEpochMillis(7, 42)
                }));
    GenericRowData values =
        GenericRowData.of(
            new GenericArrayData(new Integer[] {1, null, -2}),
            new GenericMapData(map),
            nested,
            new GenericMapData(multiset));
    GenericRowData nulls = new GenericRowData(schema.getFieldCount());
    List<RowData> rows = List.of(values, nulls);
    int[] keyColumns = {0, 1, 2, 3};
    // Pre-order logical type stream: ARRAY<int>, MAP<string, ARRAY<int>>, ROW<string,
    // ARRAY<timestamp(9)>>, MULTISET<string> (Arrow MAP<string, int>).
    int[] typeDescriptors = {-1, -1, -1, -1, -1, -1, -1, -1, -1, 9, -1, -1, -1};
    int[][] perFieldDescriptors = {{-1, -1}, {-1, -1, -1, -1}, {-1, -1, -1, 9}, {-1, -1, -1}};
    Object[] fieldValues = {
      values.getField(0), values.getField(1), values.getField(2), values.getField(3)
    };

    int[] actual = nativeHashes(rows, schema, keyColumns, typeDescriptors);
    RowDataSerializer serializer = new RowDataSerializer(schema);
    int[] expected =
        rows.stream().mapToInt(row -> serializer.toBinaryRow(row).hashCode()).toArray();
    for (int field = 0; field < keyColumns.length; field++) {
      GenericRowData projected = new GenericRowData(1);
      projected.setField(0, fieldValues[field]);
      GenericRowData projectedNull = new GenericRowData(1);
      int[] actualField = nativeHashes(rows, schema, new int[] {field}, perFieldDescriptors[field]);
      RowDataSerializer fieldSerializer = new RowDataSerializer(schema.getTypeAt(field));
      int[] expectedField = {
        fieldSerializer.toBinaryRow(projected).hashCode(),
        fieldSerializer.toBinaryRow(projectedNull).hashCode()
      };
      assertArrayEquals(expectedField, actualField, "nested field " + field);
    }
    assertArrayEquals(expected, actual);
  }

  @Test
  void nativeHashMatchesFlinksBinaryRowForDeepCompositeKeys() {
    ArrayType nestedStrings = new ArrayType(new ArrayType(new VarCharType(VarCharType.MAX_LENGTH)));
    RowType mapValue =
        RowType.of(
            new LogicalType[] {
              new DecimalType(30, 4), new ArrayType(new TimeType(9)), new BinaryType(3)
            },
            new String[] {"amount", "times", "tag"});
    MapType binaryToRow = new MapType(new VarBinaryType(VarBinaryType.MAX_LENGTH), mapValue);
    MapType stringToDecimals =
        new MapType(new VarCharType(VarCharType.MAX_LENGTH), new ArrayType(new DecimalType(28, 3)));
    RowType schema =
        RowType.of(
            new LogicalType[] {
              nestedStrings,
              binaryToRow,
              RowType.of(
                  new LogicalType[] {
                    new ArrayType(new ArrayType(new TimestampType(9))), stringToDecimals
                  },
                  new String[] {"nested_times", "decimal_map"}),
              new MultisetType(new ArrayType(new VarCharType(VarCharType.MAX_LENGTH)))
            },
            new String[] {"nested_strings", "binary_to_row", "deep_row", "array_multiset"});

    LinkedHashMap<byte[], Object> binaryMap = new LinkedHashMap<>();
    binaryMap.put(
        new byte[] {1, 2},
        GenericRowData.of(
            DecimalData.fromBigDecimal(new BigDecimal("12345678901234567890.1234"), 30, 4),
            new GenericArrayData(new Integer[] {0, null, 86_399_999}),
            new byte[] {7, 8, 9}));
    binaryMap.put(
        new byte[] {10, 11, 12, 13, 14, 15, 16, 17},
        GenericRowData.of(
            DecimalData.fromBigDecimal(new BigDecimal("-0.0001"), 30, 4),
            new GenericArrayData(new Integer[] {}),
            new byte[] {0, 0, 1}));

    LinkedHashMap<StringData, Object> decimalMap = new LinkedHashMap<>();
    decimalMap.put(
        StringData.fromString("short"),
        new GenericArrayData(
            new DecimalData[] {DecimalData.fromBigDecimal(new BigDecimal("1.001"), 28, 3), null}));
    decimalMap.put(
        StringData.fromString("a-key-longer-than-seven-bytes"),
        new GenericArrayData(new DecimalData[] {}));

    GenericArrayData blue = new GenericArrayData(new StringData[] {StringData.fromString("blue")});
    GenericArrayData red =
        new GenericArrayData(
            new StringData[] {
              StringData.fromString("red"), StringData.fromString("a long element")
            });
    LinkedHashMap<GenericArrayData, Integer> multiset = new LinkedHashMap<>();
    multiset.put(red, 3);
    multiset.put(blue, 1);

    GenericRowData values =
        GenericRowData.of(
            new GenericArrayData(
                new GenericArrayData[] {
                  new GenericArrayData(
                      new StringData[] {
                        StringData.fromString("tiny"), null, StringData.fromString("long-string")
                      }),
                  new GenericArrayData(new StringData[] {})
                }),
            new GenericMapData(binaryMap),
            GenericRowData.of(
                new GenericArrayData(
                    new GenericArrayData[] {
                      new GenericArrayData(
                          new TimestampData[] {TimestampData.fromEpochMillis(-1, 999_999), null}),
                      new GenericArrayData(
                          new TimestampData[] {TimestampData.fromEpochMillis(1, 1)})
                    }),
                new GenericMapData(decimalMap)),
            new GenericMapData(multiset));
    GenericRowData nulls = new GenericRowData(schema.getFieldCount());
    List<RowData> rows = List.of(values, nulls);
    int[] keyColumns = {0, 1, 2, 3};
    int[] typeDescriptors = typeDescriptors(schema.getChildren());
    Object[] fieldValues = {
      values.getField(0), values.getField(1), values.getField(2), values.getField(3)
    };

    assertNativeHashParity(rows, schema, keyColumns, typeDescriptors, fieldValues);
  }

  private static void assertNativeHashParity(
      List<RowData> rows,
      RowType schema,
      int[] keyColumns,
      int[] typeDescriptors,
      Object[] fieldValues) {
    int[] actual = nativeHashes(rows, schema, keyColumns, typeDescriptors);
    RowDataSerializer serializer = new RowDataSerializer(schema);
    int[] expected =
        rows.stream().mapToInt(row -> serializer.toBinaryRow(row).hashCode()).toArray();
    for (int field = 0; field < keyColumns.length; field++) {
      GenericRowData projected = new GenericRowData(1);
      projected.setField(0, fieldValues[field]);
      GenericRowData projectedNull = new GenericRowData(1);
      int[] actualField =
          nativeHashes(rows, schema, new int[] {field}, typeDescriptors(schema.getTypeAt(field)));
      RowDataSerializer fieldSerializer = new RowDataSerializer(schema.getTypeAt(field));
      int[] expectedField = {
        fieldSerializer.toBinaryRow(projected).hashCode(),
        fieldSerializer.toBinaryRow(projectedNull).hashCode()
      };
      assertArrayEquals(expectedField, actualField, "nested field " + field);
    }
    assertArrayEquals(expected, actual);
  }

  private static int[] typeDescriptors(List<LogicalType> types) {
    List<Integer> descriptors = new ArrayList<>();
    for (LogicalType type : types) {
      appendTypeDescriptors(type, descriptors);
    }
    return descriptors.stream().mapToInt(Integer::intValue).toArray();
  }

  private static int[] typeDescriptors(LogicalType type) {
    return typeDescriptors(List.of(type));
  }

  private static void appendTypeDescriptors(LogicalType type, List<Integer> descriptors) {
    descriptors.add(type instanceof TimestampType ? ((TimestampType) type).getPrecision() : -1);
    switch (type.getTypeRoot()) {
      case ARRAY:
        appendTypeDescriptors(((ArrayType) type).getElementType(), descriptors);
        break;
      case MAP:
        MapType map = (MapType) type;
        appendTypeDescriptors(map.getKeyType(), descriptors);
        appendTypeDescriptors(map.getValueType(), descriptors);
        break;
      case MULTISET:
        appendTypeDescriptors(((MultisetType) type).getElementType(), descriptors);
        descriptors.add(-1); // Arrow represents the occurrence count as an internal INT.
        break;
      case ROW:
        for (LogicalType field : ((RowType) type).getChildren()) {
          appendTypeDescriptors(field, descriptors);
        }
        break;
      default:
        break;
    }
  }

  private static int[] nativeHashes(
      List<RowData> rows, RowType schema, int[] keyColumns, int[] typeDescriptors) {
    // C Data export transfers the ArrowArray/ArrowSchema ownership to Rust. Every oracle call
    // therefore exports a fresh pair instead of reusing released FFI structures.
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, schema, allocator);
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, arrowSchema);
      return Native.flinkBinaryRowHashes(
          array.memoryAddress(), arrowSchema.memoryAddress(), keyColumns, typeDescriptors);
    }
  }
}
