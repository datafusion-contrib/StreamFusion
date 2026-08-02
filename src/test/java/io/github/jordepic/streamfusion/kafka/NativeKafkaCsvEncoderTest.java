package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import org.apache.flink.formats.csv.CsvRowDataSerializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
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
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Referees every native CSV sink byte against Flink's {@code CsvRowDataSerializationSchema}
 * (Jackson CSV underneath) — the quote decision, escaping, spellings, and option handling must be
 * indistinguishable.
 */
@Tag("streamfusion-kafka")
class NativeKafkaCsvEncoderTest {

  /**
   * The option matrices a test can run one corpus across. Values are the raw table option strings
   * (post prefix-stripping), exactly what {@link EncodeFormat#csv} receives.
   */
  private static final List<Map<String, String>> OPTION_MATRIX =
      List.of(
          Map.of(),
          Map.of("field-delimiter", "\\t"),
          Map.of("field-delimiter", "|"),
          Map.of("quote-character", "'"),
          Map.of("disable-quote-character", "true"),
          Map.of("escape-character", "\\"),
          Map.of("escape-character", "|", "null-literal", "N/A"),
          Map.of("null-literal", "n,a"),
          Map.of("array-element-delimiter", "|", "null-literal", "N/A"),
          Map.of("write-bigdecimal-in-scientific-notation", "true"),
          Map.of("write-bigdecimal-in-scientific-notation", "false"));

  @Test
  void matchesFlinkForScalarRows() throws Exception {
    RowType rowType =
        RowType.of(
            new LogicalType[] {
              new BooleanType(),
              new TinyIntType(),
              new SmallIntType(),
              new IntType(),
              new BigIntType(),
              new VarCharType(VarCharType.MAX_LENGTH),
              new VarBinaryType(VarBinaryType.MAX_LENGTH)
            },
            new String[] {"ok", "b", "s", "i", "l", "name", "payload"});
    List<RowData> rows =
        List.of(
            GenericRowData.of(
                true,
                (byte) -1,
                (short) 300,
                -12345,
                9_876_543_210L,
                StringData.fromString("plain-text"),
                new byte[] {1, 2, 3}),
            GenericRowData.of(
                false,
                (byte) 0,
                (short) 0,
                0,
                0L,
                StringData.fromString("with,comma and \"quote\""),
                // Long enough for base64 to pass 24 chars, with bytes that produce '+' and '/'.
                new byte[] {-5, -17, 62, 63, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}),
            GenericRowData.of(null, null, null, null, null, null, null));

    for (Map<String, String> options : OPTION_MATRIX) {
      assertMatchesFlink(rows, rowType, options);
    }
  }

  /**
   * The Jackson quote decision under the default "loose" check: anything at or below the
   * delimiter/quote threshold quotes (a space, '!', '#', '+'), a backslash quotes only while no
   * escape character is configured, and 25 UTF-16 units force quotes regardless of content —
   * surrogate pairs counting as two. A value spelled exactly like the null literal writes the
   * same bytes as null. Everything raw once the quote character is disabled.
   */
  @Test
  void matchesFlinkQuotingEdges() throws Exception {
    RowType rowType =
        RowType.of(
            new LogicalType[] {
              new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH)
            },
            new String[] {"a", "b"});
    String[] values = {
      "plain-text",
      "with space",
      "bang!",
      "#leading-hash",
      "plus+sign",
      "slash/dot.dash-",
      "back\\slash",
      "has\"quote",
      "'single'",
      "line\nbreak",
      "carriage\rreturn",
      "tab\there",
      "abcdefghijklmnopqrstuvwx",
      "abcdefghijklmnopqrstuvwxy",
      "雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪",
      "雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪雪",
      "😀😀😀😀😀😀😀😀😀😀😀😀",
      "😀😀😀😀😀😀😀😀😀😀😀😀a",
      "N/A",
      "n,a",
      "",
    };
    for (String value : values) {
      List<RowData> rows =
          List.of(
              GenericRowData.of(StringData.fromString(value), StringData.fromString(value)),
              GenericRowData.of(StringData.fromString(value), null));
      for (Map<String, String> options : OPTION_MATRIX) {
        assertMatchesFlink(rows, rowType, options);
      }
    }
  }

  /**
   * Temporal spellings: DATE is ISO with EXCEEDS_PAD years, TIME keeps its seconds and trims the
   * millisecond fraction, TIMESTAMP is the SQL spelling with a value-trimmed fraction, and
   * TIMESTAMP_LTZ adds Flink's 'Z'. The fraction length also drives the value across Jackson's
   * 24-char always-quote threshold, so both sides of that boundary are pinned.
   */
  @Test
  void matchesFlinkTemporalSpellings() throws Exception {
    RowType rowType =
        RowType.of(
            new LogicalType[] {
              new DateType(),
              new TimeType(3),
              new TimestampType(9),
              new LocalZonedTimestampType(9)
            },
            new String[] {"day", "tod", "ts", "instant"});
    long base = 1_577_934_245_000L;
    List<RowData> rows =
        List.of(
            temporalRow(18321, 45_240_000, TimestampData.fromEpochMillis(base)),
            temporalRow(-1, 45_296_789, TimestampData.fromEpochMillis(base + 500)),
            temporalRow(0, 500, TimestampData.fromEpochMillis(base + 120)),
            temporalRow(2_932_896, 86_399_999, TimestampData.fromEpochMillis(base + 123)),
            // Fraction widths 4 and 5 straddle the 24-char quote threshold (25 with the 'Z').
            temporalRow(2_932_897, 0, TimestampData.fromEpochMillis(base + 123, 400_000)),
            temporalRow(-719_162, 1, TimestampData.fromEpochMillis(base + 123, 450_000)),
            temporalRow(-719_529, 60_000, TimestampData.fromEpochMillis(base + 123, 456_789)),
            GenericRowData.of(null, null, null, null));

    for (Map<String, String> options : OPTION_MATRIX) {
      assertMatchesFlink(rows, rowType, options);
    }
  }

  private static RowData temporalRow(int days, int millisOfDay, TimestampData timestamp) {
    return GenericRowData.of(days, millisOfDay, timestamp, timestamp);
  }

  /**
   * Both decimal modes. Unset and explicit-false keep the column scale via {@code toPlainString()}
   * (the option's declared default of true never reaches Flink's builder — the factory reads it
   * through {@code getOptional}); explicit true strips zeros into Java's {@code toString()},
   * scientific notation included. Numbers are never quoted, whatever they contain.
   */
  @Test
  void matchesFlinkDecimalSpellings() throws Exception {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new DecimalType(10, 2), new DecimalType(38, 10)},
            new String[] {"low", "huge"});
    List<RowData> rows =
        List.of(
            decimalRow(rowType, "100.00", "12345678901234567890123456.7890123456"),
            decimalRow(rowType, "1.00", "0.0000000010"),
            decimalRow(rowType, "0.00", "-9999999999999999999999999999.9999999999"),
            decimalRow(rowType, "-0.01", "0.0000001000"),
            decimalRow(rowType, "12345678.90", "1.0000000000"),
            GenericRowData.of(null, null));

    for (Map<String, String> options : OPTION_MATRIX) {
      assertMatchesFlink(rows, rowType, options);
    }
  }

  private static RowData decimalRow(RowType rowType, String... values) {
    Object[] fields = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      DecimalType type = (DecimalType) rowType.getTypeAt(i);
      fields[i] =
          DecimalData.fromBigDecimal(
              new BigDecimal(values[i]), type.getPrecision(), type.getScale());
    }
    return GenericRowData.of(fields);
  }

  /**
   * A nested ROW or ARRAY is one CSV field: elements join on the array-element delimiter with no
   * per-element escaping (a delimiter inside an element rides raw), nulls spell the null literal,
   * and only the joined whole goes through the quote decision.
   */
  /**
   * BINARY(n) crosses the boundary as Arrow fixed-size bytes; Flink's CSV converter base64-encodes
   * it exactly like VARBINARY, at the top level and as a depth-one array element.
   */
  @Test
  void matchesFlinkFixedLengthBinary() throws Exception {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new BinaryType(3), new ArrayType(new BinaryType(2))},
            new String[] {"payload", "chunks"});
    List<RowData> rows =
        List.of(
            GenericRowData.of(
                new byte[] {-5, -17, 62},
                new GenericArrayData(new Object[] {new byte[] {0, 1}, null, new byte[] {-1, 127}})),
            GenericRowData.of(null, new GenericArrayData(new Object[0])),
            GenericRowData.of(new byte[] {0, 0, 0}, null));

    for (Map<String, String> options : OPTION_MATRIX) {
      assertMatchesFlink(rows, rowType, options);
    }
  }

  @Test
  void matchesFlinkForArraysAndNestedRows() throws Exception {
    RowType nested =
        RowType.of(
            new LogicalType[] {
              new IntType(),
              new VarCharType(VarCharType.MAX_LENGTH),
              new DecimalType(10, 2),
              new LocalZonedTimestampType(3)
            },
            new String[] {"x", "y", "amount", "at"});
    RowType rowType =
        RowType.of(
            new LogicalType[] {
              new ArrayType(new IntType()),
              new ArrayType(new VarCharType(VarCharType.MAX_LENGTH)),
              nested
            },
            new String[] {"ints", "words", "pair"});
    TimestampData ts = TimestampData.fromEpochMillis(1_577_934_245_500L);
    List<RowData> rows =
        List.of(
            GenericRowData.of(
                new GenericArrayData(new Object[] {1, null, 3}),
                new GenericArrayData(
                    new Object[] {
                      StringData.fromString("a;b"),
                      StringData.fromString("c,d"),
                      StringData.fromString("q\"uote"),
                      null
                    }),
                GenericRowData.of(
                    7,
                    StringData.fromString("ok"),
                    DecimalData.fromBigDecimal(new BigDecimal("100.00"), 10, 2),
                    ts)),
            GenericRowData.of(
                new GenericArrayData(new Object[0]),
                null,
                GenericRowData.of(null, null, null, null)),
            GenericRowData.of(null, new GenericArrayData(new Object[] {null}), null));

    for (Map<String, String> options : OPTION_MATRIX) {
      assertMatchesFlink(rows, rowType, options);
    }
  }

  /**
   * FLOAT/DOUBLE are Jackson numbers: written raw and never quoted — NaN and the infinities
   * included — spelled with the legacy {@code Double.toString}/{@code Float.toString} digits (the
   * edge corpus carries values whose legacy spelling is not the shortest representation). Inside
   * an array they join like any other raw element.
   */
  @Test
  void matchesFlinkFloatAndDoubleSpellings() throws Exception {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new DoubleType(), new FloatType(), new ArrayType(new DoubleType())},
            new String[] {"d", "f", "ds"});
    List<RowData> rows = new ArrayList<>();
    double[] doubles = FloatingPointCorpus.edgeDoubles();
    float[] floats = FloatingPointCorpus.edgeFloats();
    for (int i = 0; i < Math.max(doubles.length, floats.length); i++) {
      rows.add(
          GenericRowData.of(
              doubles[i % doubles.length],
              floats[i % floats.length],
              new GenericArrayData(
                  new Object[] {doubles[i % doubles.length], null, Double.NaN})));
    }
    rows.add(GenericRowData.of(null, null, null));

    for (Map<String, String> options : OPTION_MATRIX) {
      assertMatchesFlink(rows, rowType, options);
    }
  }

  @Test
  void matchesFlinkFloatSpellingsOnSeededRandomSweep() throws Exception {
    RowType rowType =
        RowType.of(
            new LogicalType[] {new DoubleType(), new FloatType()}, new String[] {"d", "f"});
    double[] doubles = FloatingPointCorpus.randomDoubles(10_000, 0x0DDC5BB17EL);
    float[] floats = FloatingPointCorpus.randomFloats(10_000, 0xF10C5BB17EL);
    List<RowData> rows = new ArrayList<>();
    for (int i = 0; i < doubles.length; i++) {
      rows.add(GenericRowData.of(doubles[i], floats[i]));
    }

    assertMatchesFlink(rows, rowType, Map.of());
  }

  /**
   * The upsert key format is its own format instance: here the key encodes CSV under its own
   * options while the value stays CSV with conflicting ones, and DELETE rows become tombstones.
   */
  @Test
  void encodesUpsertKeysAndTombstonesUnderCsv() throws Exception {
    RowType rowType =
        RowType.of(
            new LogicalType[] {
              new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH), new DecimalType(10, 2)
            },
            new String[] {"id", "name", "amount"});
    RowType keyType =
        RowType.of(
            new LogicalType[] {new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH)},
            new String[] {"id", "name"});
    DecimalData amount = DecimalData.fromBigDecimal(new BigDecimal("100.00"), 10, 2);
    GenericRowData insert =
        GenericRowData.of(7L, StringData.fromString("a,b"), amount);
    GenericRowData delete =
        GenericRowData.of(7L, StringData.fromString("a,b"), amount);
    delete.setRowKind(RowKind.DELETE);
    List<RowData> rows = List.of(insert, delete);

    Map<String, String> valueOptions = Map.of("write-bigdecimal-in-scientific-notation", "true");
    Map<String, String> keyOptions = Map.of("field-delimiter", ";", "quote-character", "'");
    CsvRowDataSerializationSchema flinkKey = flinkSchema(keyType, keyOptions);
    CsvRowDataSerializationSchema flinkValue = flinkSchema(rowType, valueOptions);

    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, rowType, allocator, true);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      EncodeFormat valueFormat = EncodeFormat.csv(valueOptions);
      EncodeFormat keyFormat = EncodeFormat.csv(keyOptions);
      assertNotNull(valueFormat);
      assertNotNull(keyFormat);
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

      byte[] expectedKey = flinkKey.serialize(GenericRowData.of(7L, StringData.fromString("a,b")));
      assertArrayEquals(expectedKey, records[0][0]);
      assertArrayEquals(expectedKey, records[0][1]);
      assertArrayEquals(flinkValue.serialize(insert), records[1][0]);
      assertNull(records[1][1]);
    }
  }

  private static void assertMatchesFlink(
      List<RowData> rows, RowType rowType, Map<String, String> options) throws Exception {
    CsvRowDataSerializationSchema flink = flinkSchema(rowType, options);
    EncodeFormat format = EncodeFormat.csv(options);
    assertNotNull(format, options::toString);

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

  /** Configures Flink's serializer builder the way {@code CsvFormatFactory} would. */
  private static CsvRowDataSerializationSchema flinkSchema(
      RowType rowType, Map<String, String> options) throws Exception {
    CsvRowDataSerializationSchema.Builder builder =
        new CsvRowDataSerializationSchema.Builder(rowType);
    String delimiter = options.get("field-delimiter");
    if (delimiter != null) {
      builder.setFieldDelimiter("\\t".equals(delimiter) ? '\t' : delimiter.charAt(0));
    }
    if (Boolean.parseBoolean(options.getOrDefault("disable-quote-character", "false"))) {
      builder.disableQuoteCharacter();
    }
    String quote = options.get("quote-character");
    if (quote != null) {
      builder.setQuoteCharacter(quote.charAt(0));
    }
    String arrayDelimiter = options.get("array-element-delimiter");
    if (arrayDelimiter != null) {
      builder.setArrayElementDelimiter(arrayDelimiter);
    }
    String escape = options.get("escape-character");
    if (escape != null) {
      builder.setEscapeCharacter(escape.charAt(0));
    }
    String nullLiteral = options.get("null-literal");
    if (nullLiteral != null) {
      builder.setNullLiteral(nullLiteral);
    }
    String scientific = options.get("write-bigdecimal-in-scientific-notation");
    if (scientific != null) {
      builder.setWriteBigDecimalInScientificNotation(Boolean.parseBoolean(scientific));
    }
    CsvRowDataSerializationSchema schema = builder.build();
    schema.open(initializationContext());
    return schema;
  }

  private static SerializationSchema.InitializationContext initializationContext() {
    return new SerializationSchema.InitializationContext() {
      @Override
      public MetricGroup getMetricGroup() {
        return new UnregisteredMetricsGroup();
      }

      @Override
      public UserCodeClassLoader getUserCodeClassLoader() {
        return SimpleUserCodeClassLoader.create(NativeKafkaCsvEncoderTest.class.getClassLoader());
      }
    };
  }

  @Test
  void unusableOptionValuesStayOnFlink() {
    assertNull(EncodeFormat.csv(Map.of("quote-character", "'", "disable-quote-character", "true")));
    assertNull(EncodeFormat.csv(Map.of("field-delimiter", "||")));
    assertNull(EncodeFormat.csv(Map.of("field-delimiter", "\\n")));
    assertNull(EncodeFormat.csv(Map.of("quote-character", "€")));
    assertNull(EncodeFormat.csv(Map.of("escape-character", "ab")));
    assertNull(EncodeFormat.csv(Map.of("array-element-delimiter", "€")));
    assertNull(EncodeFormat.csv(Map.of("null-literal", "line\nbreak")));
    assertNull(EncodeFormat.csv(Map.of("allow-comments", "banana")));
    assertNull(EncodeFormat.csv(Map.of("ignore-parse-errors", "banana")));
    assertNull(EncodeFormat.csv(Map.of("disable-quote-character", "banana")));
    assertNull(EncodeFormat.csv(Map.of("write-bigdecimal-in-scientific-notation", "banana")));
    // Valid deser-only options are ignored, not a fallback.
    assertNotNull(EncodeFormat.csv(Map.of("allow-comments", "true", "ignore-parse-errors", "true")));
  }
}
