package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.format.EncodeFormat;
import io.github.jordepic.streamfusion.format.LogicalTypeDescriptors;
import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
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
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("streamfusion-kafka")
class NativeKafkaJsonEncoderTest {

  private static final RowType ROW_TYPE =
      RowType.of(
          new LogicalType[] {
            new IntType(),
            new VarCharType(VarCharType.MAX_LENGTH),
            new BooleanType(),
            new BigIntType()
          },
          new String[] {"id", "name", "enabled", "score"});

  @Test
  void matchesFlinkForWholeBatchesWithAndWithoutNullFields() throws Exception {
    GenericRowData first = GenericRowData.of(1, StringData.fromString("quote: \" and 雪"), true, 25L);
    GenericRowData nulls = GenericRowData.of(2, null, false, null);
    List<RowData> rows = List.of(first, nulls);

    assertMatchesFlink(rows, false);
    assertMatchesFlink(rows, true);
  }

  @Test
  void matchesFlinkTimestampFormatting() throws Exception {
    RowType timestamps =
        RowType.of(
            new LogicalType[] {new TimestampType(3), new TimestampType(9)},
            new String[] {"millis", "nanos"});
    List<RowData> rows =
        List.of(
            GenericRowData.of(
                TimestampData.fromEpochMillis(1_577_934_245_678L),
                TimestampData.fromEpochMillis(1_577_934_245_678L, 123_456)));

    assertMatchesFlink(rows, timestamps, TimestampFormat.SQL, false);
    assertMatchesFlink(rows, timestamps, TimestampFormat.ISO_8601, false);
  }

  /**
   * Flink's timestamp formatters trim the fraction to its shortest spelling (and drop it entirely
   * at .000) for plain TIMESTAMP and TIMESTAMP_LTZ alike, regardless of the declared precision.
   */
  @Test
  void matchesFlinkTimestampFractionTrimming() throws Exception {
    RowType timestamps =
        RowType.of(
            new LogicalType[] {
              new TimestampType(3),
              new TimestampType(9),
              new LocalZonedTimestampType(3),
              new LocalZonedTimestampType(9)
            },
            new String[] {"ts3", "ts9", "ltz3", "ltz9"});
    long base = 1_577_934_245_000L;
    List<RowData> rows =
        List.of(
            timestampRow(TimestampData.fromEpochMillis(base + 500)), // .5, not .500
            timestampRow(TimestampData.fromEpochMillis(base)), // no fraction at all
            timestampRow(TimestampData.fromEpochMillis(base + 123, 456_789)), // .123456789
            timestampRow(TimestampData.fromEpochMillis(base + 120)), // .12
            timestampRow(TimestampData.fromEpochMillis(base + 100, 230_000))); // .10023

    assertMatchesFlink(rows, timestamps, TimestampFormat.SQL, false);
    assertMatchesFlink(rows, timestamps, TimestampFormat.ISO_8601, false);
  }

  private static RowData timestampRow(TimestampData value) {
    return GenericRowData.of(value, value, value, value);
  }

  /**
   * Both of Flink's decimal spellings: the default is {@code stripTrailingZeros().toString()},
   * which turns {@code 100.00} into {@code 1E+2}, while {@code encode.decimal-as-plain-number}
   * keeps the column scale intact ({@code 100.00}). The helper referees each row against both
   * serializer configurations.
   */
  @Test
  void matchesFlinkDecimalSpellingsInBothModes() throws Exception {
    RowType decimals =
        RowType.of(
            new LogicalType[] {new DecimalType(10, 2), new DecimalType(12, 3), new DecimalType(38, 10)},
            new String[] {"low", "mid", "huge"});
    List<RowData> rows =
        List.of(
            row(decimals, "100.00", "123.450", "12345678901234567890123456.7890123456"),
            row(decimals, "1.00", "0.000", "0.0000000010"),
            row(decimals, "0.00", "-0.010", "-9999999999999999999999999999.9999999999"),
            row(decimals, "-0.01", "1000000.000", "0.0000001000"),
            row(decimals, "12345678.90", "-120.000", "1.0000000000"));

    assertMatchesFlink(rows, decimals, TimestampFormat.SQL, false);
  }

  private static RowData row(RowType decimals, String... values) {
    Object[] fields = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      DecimalType type = (DecimalType) decimals.getTypeAt(i);
      fields[i] =
          DecimalData.fromBigDecimal(
              new BigDecimal(values[i]), type.getPrecision(), type.getScale());
    }
    return GenericRowData.of(fields);
  }

  /**
   * FLOAT/DOUBLE spell through the legacy {@code Double.toString} port: raw digits for finite
   * values — including values whose legacy spelling is not the shortest representation — and
   * quoted {@code "NaN"}/{@code "Infinity"}/{@code "-Infinity"} for non-finite ones (Jackson's
   * default {@code QUOTE_NON_NUMERIC_NUMBERS}). FLOAT keeps {@code Float.toString}'s
   * single-precision digits (Flink builds a {@code FloatNode}); a double promotion would spell
   * 0.1f as 0.10000000149011612.
   */
  @Test
  void matchesFlinkFloatAndDoubleSpellings() throws Exception {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new DoubleType(), new FloatType()}, new String[] {"d", "f"});
    List<RowData> rows = new ArrayList<>();
    double[] doubles = FloatingPointCorpus.edgeDoubles();
    float[] floats = FloatingPointCorpus.edgeFloats();
    for (int i = 0; i < Math.max(doubles.length, floats.length); i++) {
      rows.add(GenericRowData.of(doubles[i % doubles.length], floats[i % floats.length]));
    }
    rows.add(GenericRowData.of(null, null));

    assertMatchesFlink(rows, rowType, TimestampFormat.SQL, false);
    assertMatchesFlink(rows, rowType, TimestampFormat.SQL, true);
  }

  @Test
  void matchesFlinkFloatSpellingsOnSeededRandomSweep() throws Exception {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new DoubleType(), new FloatType()}, new String[] {"d", "f"});
    double[] doubles = FloatingPointCorpus.randomDoubles(10_000, 0x0DDB175D0B1EL);
    float[] floats = FloatingPointCorpus.randomFloats(10_000, 0xF10A7C0DEL);
    List<RowData> rows = new ArrayList<>();
    for (int i = 0; i < doubles.length; i++) {
      rows.add(GenericRowData.of(doubles[i], floats[i]));
    }

    assertMatchesFlink(rows, rowType, TimestampFormat.SQL, false, false);
  }

  /** The float encoders apply inside containers too (arrays, nested rows). */
  @Test
  void matchesFlinkFloatSpellingsInsideContainers() throws Exception {
    RowType nested =
        RowType.of(new LogicalType[] {new FloatType()}, new String[] {"ratio"});
    RowType rowType =
        RowType.of(
            new LogicalType[] {new ArrayType(new DoubleType()), nested},
            new String[] {"scores", "inner"});
    List<RowData> rows =
        List.of(
            GenericRowData.of(
                new GenericArrayData(
                    new Object[] {0.1d, Double.NaN, Double.NEGATIVE_INFINITY, null, 1e8d}),
                GenericRowData.of(0.1f)),
            GenericRowData.of(new GenericArrayData(new Object[0]), GenericRowData.of((Object) null)));

    assertMatchesFlink(rows, rowType, TimestampFormat.SQL, false);
  }

  @Test
  void matchesFlinkForRemainingScalarTypes() throws Exception {
    RowType scalars =
        RowType.of(
            new LogicalType[] {
              new DecimalType(10, 2),
              new VarBinaryType(VarBinaryType.MAX_LENGTH),
              new DateType(),
              new TimeType(3),
              new LocalZonedTimestampType(3)
            },
            new String[] {"amount", "payload", "day", "time", "instant"});
    List<RowData> rows =
        List.of(
            GenericRowData.of(
                DecimalData.fromBigDecimal(new BigDecimal("12345678.90"), 10, 2),
                new byte[] {0, 1, 2, -1},
                (int) LocalDate.of(2020, 2, 29).toEpochDay(),
                45_296_789,
                TimestampData.fromEpochMillis(1_577_934_245_678L)),
            GenericRowData.of(
                DecimalData.fromBigDecimal(new BigDecimal("1.00"), 10, 2),
                new byte[0],
                (int) LocalDate.of(1970, 1, 1).toEpochDay(),
                0,
                TimestampData.fromEpochMillis(0)),
            GenericRowData.of(
                DecimalData.fromBigDecimal(new BigDecimal("-0.01"), 10, 2),
                new byte[] {-128, 127},
                (int) LocalDate.of(1969, 12, 31).toEpochDay(),
                86_399_999,
                TimestampData.fromEpochMillis(-1)));

    assertMatchesFlink(rows, scalars, TimestampFormat.SQL, false);
    assertMatchesFlink(rows, scalars, TimestampFormat.ISO_8601, false);
  }

  /**
   * DATE years outside [0, 9999]: Flink's {@code ISO_LOCAL_DATE} uses {@code
   * SignStyle.EXCEEDS_PAD} — a {@code +} past four digits, a {@code -} for negative years, year 0
   * spelled {@code 0000} — which arrow-json's stock chrono rendering does not reproduce.
   */
  @Test
  void matchesFlinkForExtremeDateYears() throws Exception {
    RowType dates = RowType.of(new LogicalType[] {new DateType()}, new String[] {"day"});
    List<RowData> rows =
        List.of(
            GenericRowData.of((int) LocalDate.of(10_000, 1, 1).toEpochDay()),
            GenericRowData.of((int) LocalDate.of(275_760, 9, 13).toEpochDay()),
            GenericRowData.of((int) LocalDate.of(9_999, 12, 31).toEpochDay()),
            GenericRowData.of((int) LocalDate.of(999, 6, 15).toEpochDay()),
            GenericRowData.of((int) LocalDate.of(0, 1, 1).toEpochDay()),
            GenericRowData.of((int) LocalDate.of(-1, 12, 31).toEpochDay()),
            GenericRowData.of((int) LocalDate.of(-9_999, 1, 1).toEpochDay()));

    assertMatchesFlink(rows, dates, TimestampFormat.SQL, false);
  }

  /**
   * Nested rows must reproduce Flink's recursive converter byte for byte: a null field inside a
   * nested row follows the same {@code encode.ignore-null-fields} choice as the top level, a null
   * nested row is a single null (or omitted), and the scalar encoders (timestamps, time, strings)
   * apply unchanged inside the container.
   */
  @Test
  void matchesFlinkForNestedRows() throws Exception {
    RowType inner =
        RowType.of(
            new LogicalType[] {
              new IntType(),
              new VarCharType(VarCharType.MAX_LENGTH),
              new LocalZonedTimestampType(3),
              new TimestampType(9),
              new TimeType(3)
            },
            new String[] {"a", "b", "instant", "ts", "tod"});
    RowType rowType =
        RowType.of(new LogicalType[] {inner, new IntType()}, new String[] {"nested", "x"});
    TimestampData ts = TimestampData.fromEpochMillis(1_577_934_245_500L);
    List<RowData> rows =
        List.of(
            GenericRowData.of(
                GenericRowData.of(1, StringData.fromString("quote: \" and 雪"), ts, ts, 45_296_789),
                7),
            GenericRowData.of(GenericRowData.of(null, null, null, null, null), null),
            GenericRowData.of(null, 9));

    assertMatchesFlink(rows, rowType, TimestampFormat.SQL, false);
    assertMatchesFlink(rows, rowType, TimestampFormat.SQL, true);
    assertMatchesFlink(rows, rowType, TimestampFormat.ISO_8601, false);
    assertMatchesFlink(rows, rowType, TimestampFormat.ISO_8601, true);
  }

  /**
   * Arrays keep explicit nulls for their elements regardless of {@code encode.ignore-null-fields}
   * (Flink's array converter always renders a null element), while rows inside arrays and arrays
   * inside arrays recurse through the same converters — including the Jackson decimal spellings
   * and base64 binary, which proves the encoder factory applies inside containers.
   */
  @Test
  void matchesFlinkForArrays() throws Exception {
    RowType element =
        RowType.of(
            new LogicalType[] {
              new DecimalType(10, 2),
              new VarBinaryType(VarBinaryType.MAX_LENGTH),
              new LocalZonedTimestampType(9)
            },
            new String[] {"amount", "payload", "at"});
    RowType rowType =
        RowType.of(
            new LogicalType[] {
              new ArrayType(new IntType()),
              new ArrayType(element),
              new ArrayType(new ArrayType(new VarCharType(VarCharType.MAX_LENGTH))),
              new ArrayType(new DateType())
            },
            new String[] {"ints", "rows", "matrix", "days"});
    GenericRowData full =
        GenericRowData.of(
            new GenericArrayData(new Object[] {1, null, 3}),
            new GenericArrayData(
                new Object[] {
                  GenericRowData.of(
                      DecimalData.fromBigDecimal(new BigDecimal("100.00"), 10, 2),
                      new byte[] {0, -1, 7},
                      TimestampData.fromEpochMillis(1_577_934_245_123L, 456_789)),
                  null,
                  GenericRowData.of(null, null, null)
                }),
            new GenericArrayData(
                new Object[] {
                  new GenericArrayData(
                      new Object[] {StringData.fromString("esc\"aped"), null}),
                  null
                }),
            new GenericArrayData(new Object[] {(int) LocalDate.of(2020, 2, 29).toEpochDay()}));
    GenericRowData empty =
        GenericRowData.of(
            new GenericArrayData(new Object[0]),
            null,
            new GenericArrayData(new Object[] {new GenericArrayData(new Object[0])}),
            null);
    List<RowData> rows = List.of(full, empty);

    assertMatchesFlink(rows, rowType, TimestampFormat.SQL, false);
    assertMatchesFlink(rows, rowType, TimestampFormat.SQL, true);
    assertMatchesFlink(rows, rowType, TimestampFormat.ISO_8601, false);
  }

  /**
   * Maps and multisets serialize as JSON objects with Flink's null rules: null map values are
   * always written (only row fields honor {@code encode.ignore-null-fields}), keys are
   * Jackson-escaped field names, and the value converters recurse — a map of rows of arrays
   * proves the deep path. A MULTISET is its element-to-count map.
   */
  @Test
  void matchesFlinkForMapsAndMultisets() throws Exception {
    RowType valueRow =
        RowType.of(
            new LogicalType[] {
              new LocalZonedTimestampType(3), new ArrayType(new DecimalType(10, 2))
            },
            new String[] {"at", "amounts"});
    RowType rowType =
        RowType.of(
            new LogicalType[] {
              new MapType(new VarCharType(VarCharType.MAX_LENGTH), new IntType()),
              new MapType(new VarCharType(VarCharType.MAX_LENGTH), valueRow),
              new MultisetType(new VarCharType(VarCharType.MAX_LENGTH))
            },
            new String[] {"counts", "rows", "bag"});
    java.util.LinkedHashMap<Object, Object> counts = new java.util.LinkedHashMap<>();
    counts.put(StringData.fromString("plain"), 1);
    counts.put(StringData.fromString("esc\"aped\nkey"), null);
    counts.put(StringData.fromString("統一碼"), 3);
    java.util.LinkedHashMap<Object, Object> rowsByKey = new java.util.LinkedHashMap<>();
    rowsByKey.put(
        StringData.fromString("full"),
        GenericRowData.of(
            TimestampData.fromEpochMillis(1_577_934_245_500L),
            new GenericArrayData(
                new Object[] {DecimalData.fromBigDecimal(new BigDecimal("100.00"), 10, 2), null})));
    rowsByKey.put(StringData.fromString("holes"), GenericRowData.of(null, null));
    rowsByKey.put(StringData.fromString("missing"), null);
    java.util.LinkedHashMap<Object, Object> bag = new java.util.LinkedHashMap<>();
    bag.put(StringData.fromString("twice"), 2);
    bag.put(StringData.fromString("once"), 1);
    List<RowData> rows =
        List.of(
            GenericRowData.of(
                new GenericMapData(counts), new GenericMapData(rowsByKey), new GenericMapData(bag)),
            GenericRowData.of(
                new GenericMapData(new java.util.LinkedHashMap<>()), null, null));

    assertMatchesFlink(rows, rowType, TimestampFormat.SQL, false);
    assertMatchesFlink(rows, rowType, TimestampFormat.SQL, true);
    assertMatchesFlink(rows, rowType, TimestampFormat.ISO_8601, false);
  }

  /**
   * Null map keys follow {@code json.map-null-key.mode}: DROP skips the entry, LITERAL writes the
   * configured literal as the field name (escaped like any other key). Several null keys — and
   * duplicate keys generally, which a {@code MapData} can carry — collapse the way Jackson's
   * ObjectNode does: the first occurrence keeps its position, the last value wins.
   */
  @Test
  void matchesFlinkMapNullKeyModes() throws Exception {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new MapType(new VarCharType(VarCharType.MAX_LENGTH), new IntType())},
            new String[] {"m"});
    MapData withNullAndDuplicateKeys =
        new ArrayBackedMapData(
            new GenericArrayData(
                new Object[] {
                  StringData.fromString("a"), null, StringData.fromString("a"), null
                }),
            new GenericArrayData(new Object[] {1, 2, 3, null}));
    List<RowData> rows = List.of(GenericRowData.of(withNullAndDuplicateKeys));

    assertMatchesFlink(
        rows,
        rowType,
        TimestampFormat.SQL,
        false,
        false,
        JsonFormatOptions.MapNullKeyMode.DROP,
        "null");
    assertMatchesFlink(
        rows,
        rowType,
        TimestampFormat.SQL,
        false,
        false,
        JsonFormatOptions.MapNullKeyMode.LITERAL,
        "esc\"aped literal");
  }

  /**
   * FAIL mode is data-dependent, so it cannot gate at plan time: like Flink, the native encoder
   * fails the record at runtime, and its message points at {@code json.map-null-key.mode} the way
   * Flink's does.
   */
  @Test
  void failsLikeFlinkOnNullMapKeys() throws Exception {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new MapType(new VarCharType(VarCharType.MAX_LENGTH), new IntType())},
            new String[] {"m"});
    java.util.LinkedHashMap<Object, Object> data = new java.util.LinkedHashMap<>();
    data.put(null, 1);
    List<RowData> rows = List.of(GenericRowData.of(new GenericMapData(data)));

    JsonRowDataSerializationSchema flink =
        new JsonRowDataSerializationSchema(
            rowType,
            TimestampFormat.SQL,
            JsonFormatOptions.MapNullKeyMode.FAIL,
            "null",
            false,
            false);
    flink.open(initializationContext());
    assertThrows(RuntimeException.class, () -> flink.serialize(rows.get(0)));

    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, rowType, allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      EncodeFormat format = EncodeFormat.json(Map.of());
      Exception failure =
          assertThrows(
              Exception.class,
              () ->
                  NativeKafka.encodeKafkaBatch(
                      array.memoryAddress(),
                      schema.memoryAddress(),
                      format.format,
                      format.options,
                      LogicalTypeDescriptors.of(rowType),
                      rowType.getFieldNames().toArray(String[]::new)));
      assertTrue(
          failure.getMessage().contains("json.map-null-key.mode"), failure.getMessage());
    }
  }

  /** A {@link MapData} view over two parallel arrays — the shape duplicate keys arrive in. */
  private record ArrayBackedMapData(GenericArrayData keys, GenericArrayData values)
      implements MapData {
    @Override
    public int size() {
      return keys.size();
    }

    @Override
    public ArrayData keyArray() {
      return keys;
    }

    @Override
    public ArrayData valueArray() {
      return values;
    }
  }

  /**
   * The upsert key format is its own format instance in Flink, configured solely from {@code
   * key.json.*} (or that format's defaults) — never from the value's settings. The referee builds
   * Flink's key and value serializers the way the upsert-kafka factory would, with deliberately
   * conflicting options, and diffs both byte streams.
   */
  @Test
  void honorsKeyFormatOptionsIndependentlyOfValueOptions() throws Exception {
    RowType rowType =
        RowType.of(
            new LogicalType[] {
              new DecimalType(10, 2), new TimestampType(3), new VarCharType(VarCharType.MAX_LENGTH)
            },
            new String[] {"amount", "ts", "name"});
    RowType keyType =
        RowType.of(
            new LogicalType[] {new DecimalType(10, 2), new TimestampType(3)},
            new String[] {"amount", "ts"});
    DecimalData amount = DecimalData.fromBigDecimal(new BigDecimal("100.00"), 10, 2);
    TimestampData ts = TimestampData.fromEpochMillis(1_577_934_245_500L);
    GenericRowData insert = GenericRowData.of(amount, ts, StringData.fromString("one"));
    GenericRowData delete = GenericRowData.of(amount, ts, StringData.fromString("one"));
    delete.setRowKind(RowKind.DELETE);
    List<RowData> rows = List.of(insert, delete);

    JsonRowDataSerializationSchema flinkKey =
        new JsonRowDataSerializationSchema(
            keyType,
            TimestampFormat.ISO_8601,
            JsonFormatOptions.MapNullKeyMode.LITERAL,
            "null",
            true,
            false);
    flinkKey.open(initializationContext());
    JsonRowDataSerializationSchema flinkValue =
        new JsonRowDataSerializationSchema(
            rowType,
            TimestampFormat.SQL,
            JsonFormatOptions.MapNullKeyMode.LITERAL,
            "null",
            false,
            false);
    flinkValue.open(initializationContext());

    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, rowType, allocator, true);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      EncodeFormat valueFormat = EncodeFormat.json(Map.of());
      EncodeFormat keyFormat =
          EncodeFormat.json(
              Map.of(
                  "timestamp-format.standard", "ISO-8601",
                  "encode.decimal-as-plain-number", "true"));
      byte[][][] records =
          NativeKafka.encodeKafkaRecords(
              array.memoryAddress(),
              schema.memoryAddress(),
              valueFormat.format,
              valueFormat.options,
              keyFormat.format,
              keyFormat.options,
              LogicalTypeDescriptors.of(rowType),
              rowType.getFieldNames().toArray(String[]::new),
              new int[] {0, 1},
              new int[] {0, 1, 2},
              true);

      byte[] expectedKey = flinkKey.serialize(GenericRowData.of(amount, ts));
      assertArrayEquals(expectedKey, records[0][0]);
      assertArrayEquals(expectedKey, records[0][1]);
      assertArrayEquals(flinkValue.serialize(insert), records[1][0]);
      assertNull(records[1][1]);
    }
  }

  private static void assertMatchesFlink(List<RowData> rows, boolean ignoreNullFields)
      throws Exception {
    assertMatchesFlink(rows, ROW_TYPE, TimestampFormat.SQL, ignoreNullFields, false);
  }

  private static void assertMatchesFlink(
      List<RowData> rows,
      RowType rowType,
      TimestampFormat timestampFormat,
      boolean ignoreNullFields)
      throws Exception {
    assertMatchesFlink(rows, rowType, timestampFormat, ignoreNullFields, false);
    assertMatchesFlink(rows, rowType, timestampFormat, ignoreNullFields, true);
  }

  private static void assertMatchesFlink(
      List<RowData> rows,
      RowType rowType,
      TimestampFormat timestampFormat,
      boolean ignoreNullFields,
      boolean decimalAsPlainNumber)
      throws Exception {
    assertMatchesFlink(
        rows,
        rowType,
        timestampFormat,
        ignoreNullFields,
        decimalAsPlainNumber,
        JsonFormatOptions.MapNullKeyMode.FAIL,
        "null");
  }

  private static void assertMatchesFlink(
      List<RowData> rows,
      RowType rowType,
      TimestampFormat timestampFormat,
      boolean ignoreNullFields,
      boolean decimalAsPlainNumber,
      JsonFormatOptions.MapNullKeyMode mapNullKeyMode,
      String mapNullKeyLiteral)
      throws Exception {
    JsonRowDataSerializationSchema flink =
        new JsonRowDataSerializationSchema(
            rowType,
            timestampFormat,
            mapNullKeyMode,
            mapNullKeyLiteral,
            decimalAsPlainNumber,
            ignoreNullFields);
    flink.open(initializationContext());

    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, rowType, allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      EncodeFormat format =
          EncodeFormat.json(
              Map.of(
                  "timestamp-format.standard",
                  timestampFormat == TimestampFormat.SQL ? "SQL" : "ISO-8601",
                  "encode.ignore-null-fields",
                  String.valueOf(ignoreNullFields),
                  "encode.decimal-as-plain-number",
                  String.valueOf(decimalAsPlainNumber),
                  "map-null-key.mode",
                  mapNullKeyMode.name(),
                  "map-null-key.literal",
                  mapNullKeyLiteral));
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
            "row "
                + i
                + ": expected "
                + new String(expected, StandardCharsets.UTF_8)
                + ", actual "
                + new String(actual[i], StandardCharsets.UTF_8));
      }
    }
  }

  private static SerializationSchema.InitializationContext initializationContext() {
    return new SerializationSchema.InitializationContext() {
      @Override
      public MetricGroup getMetricGroup() {
        return new UnregisteredMetricsGroup();
      }

      @Override
      public UserCodeClassLoader getUserCodeClassLoader() {
        return SimpleUserCodeClassLoader.create(NativeKafkaJsonEncoderTest.class.getClassLoader());
      }
    };
  }
}
