package tech.streamfusion.starrocks;

import static org.junit.jupiter.api.Assertions.*;

import com.starrocks.connector.flink.row.sink.StarRocksSerializerFactory;
import com.starrocks.connector.flink.row.sink.StarRocksTableRowTransformer;
import com.starrocks.connector.flink.table.StarRocksDataType;
import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.v2.RowDataSerializationSchema;
import com.starrocks.data.load.stream.StreamLoadUtils;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.RowDataArrowConverter;

/**
 * The released connector's own row serialization schema is the referee: every row the native
 * encoder hands the stream-load SDK must equal, byte for byte, the row the stock writer would hand
 * it, in the same order.
 */
class StarRocksEncoderRefereeTest {
  private List<String> expected;
  private List<String> actual;

  private static final long MIN_TIMESTAMP_MILLIS = -62_167_219_200_000L;
  private static final long MAX_TIMESTAMP_MILLIS = 253_402_300_799_999L;
  private static final int MIN_DATE = -719_528;
  private static final int MAX_DATE = 2_932_896;
  private static final String[] TEXT_PIECES = {
    "a", "Z", "0", " ", "\t", "\n", "\r", ",", "|", "\\", "\\N", "\"", "'", "/", "{", "[", "}", "]",
    ":", "é", "ß", "中文", "😀", "\u0000", "\u0001", "\u0008", "\u000B", "\u000C", "\u001F", "\u007F",
    "\u0080", "\u009F", "\u00A0", "\u2027", "\u2028", "\u2029", "\uFEFF", "null"
  };

  private static TimeZone previousZone;

  @BeforeAll
  static void pinZone() {
    previousZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  @AfterAll
  static void restoreZone() {
    TimeZone.setDefault(previousZone);
  }

  @Test
  void everyScalarTypeWithNullsAndExtremesMatchesStockCsvAndJson() throws Exception {
    RowType type = allTypes();
    List<RowData> rows = new ArrayList<>();
    rows.add(
        GenericRowData.of(
            true,
            (byte) -128,
            (short) -32768,
            Integer.MIN_VALUE,
            Long.MIN_VALUE,
            Float.NEGATIVE_INFINITY,
            -Double.MAX_VALUE,
            DecimalData.fromBigDecimal(new BigDecimal("-99999999.99"), 10, 2),
            DecimalData.fromBigDecimal(new BigDecimal("0E-18"), 38, 18),
            StringData.fromString("ab"),
            StringData.fromString("tab\there"),
            StringData.fromString("line\nbreak"),
            MIN_DATE,
            TimestampData.fromEpochMillis(MIN_TIMESTAMP_MILLIS),
            TimestampData.fromEpochMillis(-1, 999_999),
            TimestampData.fromEpochMillis(MIN_TIMESTAMP_MILLIS, 1),
            new byte[] {1, 2, 3, 4}));
    rows.add(GenericRowData.of(new Object[type.getFieldCount()]));
    rows.add(
        GenericRowData.of(
            false,
            (byte) 127,
            (short) 32767,
            Integer.MAX_VALUE,
            Long.MAX_VALUE,
            Float.NaN,
            Double.MIN_VALUE,
            DecimalData.fromBigDecimal(new BigDecimal("0.01"), 10, 2),
            DecimalData.fromBigDecimal(new BigDecimal("1E-18"), 38, 18),
            StringData.fromString(""),
            StringData.fromString("\\N"),
            StringData.fromString("{\"json\":[1,2]}"),
            MAX_DATE,
            TimestampData.fromEpochMillis(MAX_TIMESTAMP_MILLIS, 999_999),
            TimestampData.fromEpochMillis(0),
            TimestampData.fromEpochMillis(MAX_TIMESTAMP_MILLIS),
            new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff}));
    for (String format : List.of("csv", "json")) {
      StarRocksSinkOptions options = options(format, Map.of());
      expected = stock(type, rows, options);
      actual = encoded(type, rows, options);
      assertEquals(expected, actual, difference(expected, actual, format));
    }
  }

  @Test
  void rowKindsFollowTheConnectorsUpsertAndUpdateBeforePolicies() throws Exception {
    RowType type =
        RowType.of(
            new LogicalType[] {new IntType(false), new VarCharType(8)}, new String[] {"k", "v"});
    List<RowData> rows = new ArrayList<>();
    RowKind[] kinds = RowKind.values();
    for (int i = 0; i < 16; i++) {
      GenericRowData row = GenericRowData.of(i, i % 3 == 0 ? null : StringData.fromString("v" + i));
      row.setRowKind(kinds[i % kinds.length]);
      rows.add(row);
    }
    for (String format : List.of("csv", "json")) {
      for (boolean primaryKey : new boolean[] {false, true}) {
        for (String ignoreUpdateBefore : List.of("true", "false")) {
          StarRocksSinkOptions options =
              options(format, Map.of("sink.ignore.update-before", ignoreUpdateBefore));
          if (primaryKey) options.enableUpsertDelete();
          expected = stock(type, rows, options);
          actual = encoded(type, rows, options);
          assertEquals(
              expected,
              actual,
              difference(
                  expected,
                  actual,
                  format + " primaryKey=" + primaryKey + " ignore=" + ignoreUpdateBefore));
        }
      }
    }
  }

  @Test
  void configuredSeparatorsIncludingSignExtendedHexBytesMatchStock() throws Exception {
    RowType type =
        RowType.of(new IntType(), new VarCharType(VarCharType.MAX_LENGTH), new DoubleType());
    List<RowData> rows =
        List.of(
            GenericRowData.of(1, StringData.fromString("x"), 1.5d),
            GenericRowData.of(null, null, null),
            GenericRowData.of(-7, StringData.fromString("ünï\u0001"), -0.0d));
    String[][] separators = {
      {"|", "\n"},
      {"\\x01", "\\x02"},
      {"||", "\r\n"},
      {"\\x80", "\\xFF"},
      {"\\X7c", "\\x0a"},
      {",", ";"}
    };
    for (String[] separator : separators) {
      StarRocksSinkOptions options =
          options(
              "csv",
              Map.of(
                  "sink.properties.column_separator", separator[0],
                  "sink.properties.row_delimiter", separator[1]));
      expected = stock(type, rows, options);
      actual = encoded(type, rows, options);
      assertEquals(
          expected, actual, difference(expected, actual, separator[0] + " " + separator[1]));
    }
  }

  @Test
  void seededRandomRowsMatchStockForEveryFormatAndChangelogPolicy() throws Exception {
    RowType type = allTypes();
    for (long seed = 1; seed <= 6; seed++) {
      Random random = new Random(seed);
      List<RowData> rows = new ArrayList<>();
      for (int i = 0; i < 2_000; i++) rows.add(randomRow(type, random, true));
      for (String format : List.of("csv", "json")) {
        StarRocksSinkOptions options =
            options(format, Map.of("sink.ignore.update-before", seed % 2 == 0 ? "true" : "false"));
        if (seed % 3 != 0) options.enableUpsertDelete();
        expected = stock(type, rows, options);
        actual = encoded(type, rows, options);
        assertEquals(expected, actual, difference(expected, actual, "seed " + seed + " " + format));
      }
    }
  }

  @Test
  void aTaskWhoseZoneShiftsTheStockDatesWritesTheConnectorsOwnCsvRows() throws Exception {
    RowType type =
        RowType.of(
            new LogicalType[] {new IntType(false), new DateType()}, new String[] {"id", "day"});
    TableSchema schema =
        TableSchema.builder()
            .field("id", DataTypes.INT().notNull())
            .field("day", DataTypes.DATE())
            .build();
    Map<String, StarRocksDataType> targets =
        Map.of("id", StarRocksDataType.INT, "day", StarRocksDataType.DATE);
    StarRocksSinkOptions options = options("csv", Map.of("sink.ignore.update-before", "false"));
    options.enableUpsertDelete();
    List<RowData> rows =
        List.of(
            GenericRowData.ofKind(RowKind.INSERT, 1, 0),
            GenericRowData.ofKind(RowKind.UPDATE_BEFORE, 1, 0),
            GenericRowData.ofKind(RowKind.UPDATE_AFTER, 1, 19_000),
            GenericRowData.ofKind(RowKind.DELETE, 2, null),
            GenericRowData.ofKind(RowKind.INSERT, 3, -719_528));
    StarRocksEncoding planned;
    RowDataSerializationSchema stockPlanned;
    TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
    try {
      planned = StarRocksEncoding.of(options, schema, type, targets);
      var converter = new StarRocksTableRowTransformer(InternalTypeInfo.of(type));
      converter.setStarRocksColumns(targets);
      converter.setTableSchema(schema);
      stockPlanned =
          new RowDataSerializationSchema(
              "db",
              "t",
              true,
              false,
              StarRocksSerializerFactory.createSerializer(options, schema.getFieldNames()),
              converter);
    } finally {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }
    RowDataSerializationSchema stock = InstantiationUtil.clone(stockPlanned);
    stock.open(null, null);
    List<String> stockRows = new ArrayList<>();
    for (RowData row : rows) {
      StarRocksRowData serialized = stock.serialize(row);
      if (serialized != null) stockRows.add(serialized.getRow());
    }

    List<String> taskRows = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator();
        StarRocksRowSource source =
            StarRocksRowSource.open(
                InstantiationUtil.clone(planned), new UnregisteredMetricsGroup())) {
      assertInstanceOf(ConnectorStarRocksRows.class, source);
      source.begin(new ArrowBatch(RowDataArrowConverter.write(rows, type, allocator, true)));
      try {
        for (StarRocksRowData row; (row = source.next()) != null; ) taskRows.add(row.getRow());
      } finally {
        source.end();
      }
    }

    assertEquals(stockRows, taskRows);
    assertEquals("1\t1969-12-31\t0", taskRows.get(0));
    assertEquals("2\t\\N\t1", taskRows.get(3));
    assertEquals("", NativeStarRocks.liveNativeHandles());
  }

  @Test
  void aTaskWhoseZoneShiftsTheStockDatesWritesTheConnectorsOwnJsonRows() throws Exception {
    RowType type =
        RowType.of(new LogicalType[] {new IntType(), new DateType()}, new String[] {"id", "day"});
    TableSchema schema =
        TableSchema.builder().field("id", DataTypes.INT()).field("day", DataTypes.DATE()).build();
    Map<String, StarRocksDataType> targets =
        Map.of("id", StarRocksDataType.INT, "day", StarRocksDataType.DATE);
    StarRocksSinkOptions options = options("json", Map.of());
    List<RowData> rows =
        List.of(
            GenericRowData.of(1, 0),
            GenericRowData.of(2, null),
            GenericRowData.of(null, 2_932_896),
            GenericRowData.ofKind(RowKind.DELETE, 4, 7));
    StarRocksEncoding planned;
    RowDataSerializationSchema stockPlanned;
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
    try {
      planned = StarRocksEncoding.of(options, schema, type, targets);
      var converter = new StarRocksTableRowTransformer(InternalTypeInfo.of(type));
      converter.setStarRocksColumns(targets);
      converter.setTableSchema(schema);
      stockPlanned =
          new RowDataSerializationSchema(
              "db",
              "t",
              false,
              true,
              StarRocksSerializerFactory.createSerializer(options, schema.getFieldNames()),
              converter);
    } finally {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }
    RowDataSerializationSchema stock = InstantiationUtil.clone(stockPlanned);
    stock.open(null, null);
    List<String> stockRows = new ArrayList<>();
    for (RowData row : rows) {
      StarRocksRowData serialized = stock.serialize(row);
      if (serialized != null) stockRows.add(serialized.getRow());
    }

    List<String> taskRows = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator();
        StarRocksRowSource source =
            StarRocksRowSource.open(
                InstantiationUtil.clone(planned), new UnregisteredMetricsGroup())) {
      assertInstanceOf(ConnectorStarRocksRows.class, source);
      source.begin(new ArrowBatch(RowDataArrowConverter.write(rows, type, allocator, true)));
      try {
        for (StarRocksRowData row; (row = source.next()) != null; ) taskRows.add(row.getRow());
      } finally {
        source.end();
      }
    }

    assertEquals(stockRows, taskRows);
    assertEquals(3, taskRows.size());
    assertTrue(taskRows.get(0).contains("\"1969-12-31\""), taskRows.get(0));
    assertEquals("", NativeStarRocks.liveNativeHandles());
  }

  @Test
  void aTaskWhoseZoneKeepsTheStockDatesStaysNative() throws Exception {
    RowType type = RowType.of(new LogicalType[] {new DateType()}, new String[] {"day"});
    TableSchema schema = TableSchema.builder().field("day", DataTypes.DATE()).build();
    Map<String, StarRocksDataType> targets = Map.of("day", StarRocksDataType.DATE);
    StarRocksSinkOptions options = options("csv", Map.of());
    List<RowData> rows =
        List.of(
            GenericRowData.of(-719_528),
            GenericRowData.of(-141_427),
            GenericRowData.of(0),
            GenericRowData.of(2_932_896));
    StarRocksEncoding planned;
    RowDataSerializationSchema stockPlanned;
    TimeZone.setDefault(TimeZone.getTimeZone("Asia/Kolkata"));
    try {
      planned = StarRocksEncoding.of(options, schema, type, targets);
      var converter = new StarRocksTableRowTransformer(InternalTypeInfo.of(type));
      converter.setStarRocksColumns(targets);
      converter.setTableSchema(schema);
      stockPlanned =
          new RowDataSerializationSchema(
              "db",
              "t",
              false,
              true,
              StarRocksSerializerFactory.createSerializer(options, schema.getFieldNames()),
              converter);
    } finally {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }
    RowDataSerializationSchema stock = InstantiationUtil.clone(stockPlanned);
    stock.open(null, null);
    List<String> stockRows = new ArrayList<>();
    for (RowData row : rows) stockRows.add(stock.serialize(row).getRow());

    List<String> taskRows = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator();
        StarRocksRowSource source =
            StarRocksRowSource.open(
                InstantiationUtil.clone(planned), new UnregisteredMetricsGroup())) {
      assertInstanceOf(NativeStarRocksEncoder.class, source);
      source.begin(new ArrowBatch(RowDataArrowConverter.write(rows, type, allocator, true)));
      try {
        for (StarRocksRowData row; (row = source.next()) != null; ) taskRows.add(row.getRow());
      } finally {
        source.end();
      }
    }

    assertEquals(stockRows, taskRows);
    assertEquals("1970-01-01", taskRows.get(2));
    assertEquals("", NativeStarRocks.liveNativeHandles());
  }

  @Test
  void everyFlinkDateMatchesStockInUtcNewYorkAndKolkata() throws Exception {
    RowType type = RowType.of(new DateType());
    List<RowData> rows = new ArrayList<>(MAX_DATE - MIN_DATE + 1);
    for (int day = MIN_DATE; day <= MAX_DATE; day++) rows.add(GenericRowData.of(day));
    for (String zone : List.of("UTC", "America/New_York", "Asia/Kolkata")) {
      TimeZone.setDefault(TimeZone.getTimeZone(zone));
      try {
        StarRocksSinkOptions options = options("csv", Map.of());
        expected = stock(type, rows, options);
        actual = encoded(type, rows, options);
        assertEquals(expected, actual, difference(expected, actual, zone));
      } finally {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      }
    }
  }

  @Test
  void floatingPointSpellingsMatchLegacyJdkCsvAndFastjsonJson() throws Exception {
    RowType type = RowType.of(new FloatType(), new DoubleType());
    Random random = new Random(0x51A7);
    List<RowData> rows = new ArrayList<>();
    float[] floatEdges = {
      0f,
      -0f,
      Float.MIN_VALUE,
      -Float.MIN_VALUE,
      Float.MIN_NORMAL,
      Float.MAX_VALUE,
      1e-3f,
      1e7f,
      9999999f,
      0.001f,
      1e-4f,
      3.4028235e38f,
      1.17549435e-38f,
      Float.NaN,
      Float.POSITIVE_INFINITY
    };
    double[] doubleEdges = {
      0d,
      -0d,
      Double.MIN_VALUE,
      Double.MIN_NORMAL,
      Double.MAX_VALUE,
      1e-3,
      1e7,
      9999999.999,
      1e23,
      5e-324,
      2.2250738585072014e-308,
      0.1 + 0.2,
      1d / 3,
      Double.NaN,
      Double.NEGATIVE_INFINITY
    };
    for (int i = 0; i < floatEdges.length; i++)
      rows.add(GenericRowData.of(floatEdges[i], doubleEdges[i]));
    for (int i = 0; i < 200_000; i++)
      rows.add(
          GenericRowData.of(
              Float.intBitsToFloat(random.nextInt()), Double.longBitsToDouble(random.nextLong())));
    for (int exponent = -45; exponent <= 308; exponent++)
      rows.add(GenericRowData.of((float) Math.pow(10, exponent), Math.pow(10, exponent)));
    for (String format : List.of("csv", "json")) {
      StarRocksSinkOptions options = options(format, Map.of());
      expected = stock(type, rows, options);
      actual = encoded(type, rows, options);
      assertEquals(expected, actual, difference(expected, actual, format));
    }
  }

  @Test
  void everyBasicMultilingualCharacterAndSurrogatePairMatchesStock() throws Exception {
    RowType type = RowType.of(new VarCharType(VarCharType.MAX_LENGTH));
    List<RowData> rows = new ArrayList<>();
    for (int code = 0; code <= 0xFFFF; code++) {
      if (Character.isSurrogate((char) code)) continue;
      String text = new String(Character.toChars(code));
      rows.add(GenericRowData.of(StringData.fromString(text)));
      rows.add(GenericRowData.of(StringData.fromString("x" + text + "\"" + text + "y")));
    }
    for (int code = 0x10000; code <= 0x10FFFF; code += 0x101)
      rows.add(
          GenericRowData.of(StringData.fromString("\t" + new String(Character.toChars(code)))));
    for (String format : List.of("csv", "json")) {
      StarRocksSinkOptions options = options(format, Map.of());
      expected = stock(type, rows, options);
      actual = encoded(type, rows, options);
      assertEquals(expected, actual, difference(expected, actual, format));
    }
  }

  @Test
  void malformedUtf8IsReplacedLikeJavaDecoding() throws Exception {
    RowType type = RowType.of(new VarCharType(VarCharType.MAX_LENGTH));
    Random random = new Random(0xBAD);
    List<RowData> rows = new ArrayList<>();
    byte[][] fixed = {
      {(byte) 0xff},
      {(byte) 0xc0, (byte) 0x80},
      {(byte) 0xe0, (byte) 0x80, (byte) 0x80},
      {(byte) 0xed, (byte) 0xa0, (byte) 0x80},
      {(byte) 0xf0, (byte) 0x80, (byte) 0x80, (byte) 0x80},
      {(byte) 0xf4, (byte) 0x90, (byte) 0x80, (byte) 0x80},
      {(byte) 0xe2, (byte) 0x82},
      {'a', (byte) 0xc3},
      {(byte) 0xc3, 'a'},
      {(byte) 0xf0, (byte) 0x9f, (byte) 0x98}
    };
    for (byte[] bytes : fixed) rows.add(GenericRowData.of(StringData.fromBytes(bytes)));
    for (int i = 0; i < 20_000; i++) {
      byte[] bytes = new byte[1 + random.nextInt(8)];
      random.nextBytes(bytes);
      rows.add(GenericRowData.of(StringData.fromBytes(bytes)));
    }
    for (String format : List.of("csv", "json")) {
      StarRocksSinkOptions options = options(format, Map.of());
      expected = stock(type, rows, options);
      actual = encoded(type, rows, options);
      assertEquals(expected, actual, difference(expected, actual, format));
    }
  }

  @Test
  void decimalsAcrossEveryPrecisionAndScaleMatchStock() throws Exception {
    Random random = new Random(38);
    for (int precision = 1; precision <= 38; precision++) {
      for (int scale = 0; scale <= precision; scale++) {
        RowType type = RowType.of(new DecimalType(precision, scale));
        BigInteger limit = BigInteger.TEN.pow(precision);
        List<RowData> rows = new ArrayList<>();
        for (BigInteger unscaled :
            List.of(
                BigInteger.ZERO,
                BigInteger.ONE,
                BigInteger.ONE.negate(),
                limit.subtract(BigInteger.ONE),
                limit.subtract(BigInteger.ONE).negate()))
          rows.add(GenericRowData.of(decimal(unscaled, precision, scale)));
        for (int i = 0; i < 20; i++)
          rows.add(
              GenericRowData.of(
                  decimal(
                      new BigInteger(precision * 3, random)
                          .mod(limit)
                          .multiply(BigInteger.valueOf(random.nextBoolean() ? 1 : -1)),
                      precision,
                      scale)));
        for (String format : List.of("csv", "json")) {
          StarRocksSinkOptions options = options(format, Map.of());
          expected = stock(type, rows, options);
          actual = encoded(type, rows, options);
          assertEquals(
              expected,
              actual,
              difference(expected, actual, format + " DECIMAL(" + precision + "," + scale + ")"));
        }
      }
    }
  }

  @Test
  void timestampsAtEveryPrecisionMatchStock() throws Exception {
    Random random = new Random(9);
    for (int precision = 0; precision <= 9; precision++) {
      RowType type =
          RowType.of(new TimestampType(precision), new LocalZonedTimestampType(precision));
      List<RowData> rows = new ArrayList<>();
      for (int i = 0; i < 5_000; i++) {
        TimestampData value = randomTimestamp(random);
        rows.add(GenericRowData.of(value, value));
      }
      for (String format : List.of("csv", "json")) {
        StarRocksSinkOptions options = options(format, Map.of());
        expected = stock(type, rows, options);
        actual = encoded(type, rows, options);
        assertEquals(
            expected, actual, difference(expected, actual, format + " precision " + precision));
      }
    }
  }

  @Test
  void binaryFoldsMatchStockForEveryLength() throws Exception {
    Random random = new Random(17);
    for (int length = 1; length <= 17; length++) {
      RowType type = RowType.of(new BinaryType(length));
      List<RowData> rows = new ArrayList<>();
      for (int i = 0; i < 200; i++) {
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        rows.add(GenericRowData.of((Object) bytes));
      }
      StarRocksSinkOptions options = options("csv", Map.of());
      expected = stock(type, rows, options);
      actual = encoded(type, rows, options);
      assertEquals(expected, actual, difference(expected, actual, ""));
    }
  }

  @Test
  void jsonKeysFollowTheConnectorsHashMapOrderForWideAndCollidingSchemas() throws Exception {
    List<String> names = new ArrayList<>(List.of("Aa", "BB", "AaAa", "BBBB", "AaBB", "BBAa"));
    for (int i = 0; i < 60; i++) names.add("column_" + Integer.toString(i * 7919, 36));
    LogicalType[] types = new LogicalType[names.size()];
    Object[] values = new Object[names.size()];
    for (int i = 0; i < types.length; i++) {
      types[i] = new IntType();
      values[i] = i % 5 == 0 ? null : i;
    }
    RowType type = RowType.of(types, names.toArray(String[]::new));
    List<RowData> rows =
        List.of(GenericRowData.of(values), GenericRowData.ofKind(RowKind.DELETE, values));
    for (boolean primaryKey : new boolean[] {false, true}) {
      StarRocksSinkOptions options = options("json", Map.of());
      if (primaryKey) options.enableUpsertDelete();
      expected = stock(type, rows, options);
      actual = encoded(type, rows, options);
      assertEquals(expected, actual, difference(expected, actual, ""));
    }
  }

  @Test
  void batchesWhoseRowsAreAllFilteredSendNothing() throws Exception {
    RowType type = RowType.of(new IntType());
    List<RowData> rows =
        List.of(
            GenericRowData.ofKind(RowKind.UPDATE_BEFORE, 1),
            GenericRowData.ofKind(RowKind.DELETE, 2));
    StarRocksSinkOptions options = options("csv", Map.of());
    assertEquals(List.of(), stock(type, rows, options));
    assertEquals(List.of(), encoded(type, rows, options));
  }

  @Test
  void rowsCarryTheTableKeyTheStreamLoadSdkDerivesAsOneSharedString() throws Exception {
    RowType type = RowType.of(new IntType());
    StarRocksSinkOptions options = options("csv", Map.of());
    StarRocksEncoding encoding =
        StarRocksEncoding.of(
            options,
            TableSchema.builder().field("f0", DataTypes.INT()).build(),
            type,
            Map.of("f0", StarRocksDataType.INT));
    try (BufferAllocator allocator = new RootAllocator();
        NativeStarRocksEncoder encoder =
            new NativeStarRocksEncoder(encoding, new UnregisteredMetricsGroup())) {
      encoder.begin(
          new ArrowBatch(
              RowDataArrowConverter.write(
                  List.of(GenericRowData.of(1), GenericRowData.of(2)), type, allocator, true)));
      try {
        StarRocksRowData first = encoder.next();
        String key = first.getUniqueKey();
        assertEquals("1", first.getRow());
        assertEquals("db", first.getDatabase());
        assertEquals("t", first.getTable());
        assertEquals(StreamLoadUtils.getTableUniqueKey("db", "t"), key);
        StarRocksRowData second = encoder.next();
        assertEquals("2", second.getRow());
        assertSame(key, second.getUniqueKey());
        assertNull(encoder.next());
      } finally {
        encoder.end();
      }
    }
  }

  @Test
  void rowsLargerThanASlabAndRowsAcrossSlabBoundariesArriveWhole() throws Exception {
    RowType type = RowType.of(new IntType(), new VarCharType(VarCharType.MAX_LENGTH));
    List<RowData> rows = new ArrayList<>();
    for (int i = 0; i < 1_200; i++)
      rows.add(GenericRowData.of(i, StringData.fromString("ü".repeat(i % 7 == 0 ? 6_000 : 3))));
    rows.add(GenericRowData.of(-1, StringData.fromString("€".repeat(1_500_000))));
    rows.add(GenericRowData.of(-2, StringData.fromString("")));
    rows.add(GenericRowData.of(-3, StringData.fromString("x".repeat(3_000_000))));
    for (String format : List.of("csv", "json")) {
      StarRocksSinkOptions options = options(format, Map.of());
      expected = stock(type, rows, options);
      actual = encoded(type, rows, options);
      assertEquals(1_203, actual.size());
      assertEquals(expected, actual, difference(expected, actual, format));
    }
  }

  @Test
  void textForVarcharTargetsIsNeverReparsedAsJson() throws Exception {
    RowType type = RowType.of(new VarCharType(VarCharType.MAX_LENGTH), new CharType(3));
    List<RowData> rows =
        List.of(
            GenericRowData.of(
                StringData.fromString("{\"b\": 1, \"a\": [2.50]}"), StringData.fromString("[1]")),
            GenericRowData.of(StringData.fromString("[ \"x\" ]"), StringData.fromString("{}")));
    Map<String, StarRocksDataType> targets =
        Map.of("f0", StarRocksDataType.VARCHAR, "f1", StarRocksDataType.CHAR);
    for (String format : List.of("csv", "json")) {
      StarRocksSinkOptions options = options(format, Map.of());
      expected = stock(type, rows, options, targets);
      actual = encoded(type, rows, options);
      assertEquals(expected, actual, difference(expected, actual, format));
    }
  }

  /** The first differing row, escaped, since rows can be long and carry control characters. */
  private static String difference(List<String> expected, List<String> actual, String label) {
    int row = 0;
    while (row < Math.min(expected.size(), actual.size())
        && expected.get(row).equals(actual.get(row))) row++;
    return label
        + " differs at row "
        + row
        + " of "
        + expected.size()
        + " stock and "
        + actual.size()
        + " native rows\n stock:  "
        + (row < expected.size() ? escape(clip(expected.get(row))) : "<none>")
        + "\n native: "
        + (row < actual.size() ? escape(clip(actual.get(row))) : "<none>");
  }

  private static String clip(String row) {
    return row.length() <= 400 ? row : row.substring(0, 400) + "...";
  }

  private static String escape(String text) {
    StringBuilder escaped = new StringBuilder();
    text.codePoints()
        .forEach(
            code ->
                escaped.append(
                    code < 0x20 || code >= 0x7f
                        ? String.format("\\u{%x}", code)
                        : Character.toString(code)));
    return escaped.toString();
  }

  private static RowType allTypes() {
    return RowType.of(
        new LogicalType[] {
          new BooleanType(),
          new TinyIntType(),
          new SmallIntType(),
          new IntType(),
          new BigIntType(),
          new FloatType(),
          new DoubleType(),
          new DecimalType(10, 2),
          new DecimalType(38, 18),
          new CharType(2),
          new VarCharType(16),
          new VarCharType(VarCharType.MAX_LENGTH),
          new DateType(),
          new TimestampType(3),
          new TimestampType(6),
          new LocalZonedTimestampType(9),
          new BinaryType(4)
        },
        new String[] {
          "b", "t", "s", "i", "l", "f", "d", "dec", "wide", "c", "v", "text", "day", "ts3", "ts6",
          "ltz9", "bin"
        });
  }

  private static RowData randomRow(RowType type, Random random, boolean withKinds) {
    Object[] values = new Object[type.getFieldCount()];
    for (int i = 0; i < values.length; i++) {
      if (random.nextInt(7) == 0) continue;
      LogicalType column = type.getTypeAt(i);
      switch (column.getTypeRoot()) {
        case BOOLEAN:
          values[i] = random.nextBoolean();
          break;
        case TINYINT:
          values[i] = (byte) random.nextInt();
          break;
        case SMALLINT:
          values[i] = (short) random.nextInt();
          break;
        case INTEGER:
          values[i] = random.nextInt();
          break;
        case BIGINT:
          values[i] = random.nextLong();
          break;
        case FLOAT:
          values[i] = Float.intBitsToFloat(random.nextInt());
          break;
        case DOUBLE:
          values[i] =
              random.nextBoolean()
                  ? Double.longBitsToDouble(random.nextLong())
                  : random.nextGaussian() * 1e6;
          break;
        case DECIMAL:
          DecimalType decimal = (DecimalType) column;
          values[i] =
              decimal(
                  new BigInteger(decimal.getPrecision() * 3, random)
                      .mod(BigInteger.TEN.pow(decimal.getPrecision()))
                      .multiply(BigInteger.valueOf(random.nextBoolean() ? 1 : -1)),
                  decimal.getPrecision(),
                  decimal.getScale());
          break;
        case CHAR:
        case VARCHAR:
          values[i] = StringData.fromString(randomText(random));
          break;
        case DATE:
          values[i] = MIN_DATE + random.nextInt(MAX_DATE - MIN_DATE + 1);
          break;
        case TIMESTAMP_WITHOUT_TIME_ZONE:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          values[i] = randomTimestamp(random);
          break;
        case BINARY:
          byte[] bytes = new byte[((BinaryType) column).getLength()];
          random.nextBytes(bytes);
          values[i] = bytes;
          break;
        default:
          throw new IllegalArgumentException(column.toString());
      }
    }
    GenericRowData row = GenericRowData.of(values);
    if (withKinds) row.setRowKind(RowKind.values()[random.nextInt(4)]);
    return row;
  }

  private static String randomText(Random random) {
    StringBuilder text = new StringBuilder();
    for (int piece = random.nextInt(6); piece > 0; piece--)
      text.append(TEXT_PIECES[random.nextInt(TEXT_PIECES.length)]);
    return text.toString();
  }

  private static TimestampData randomTimestamp(Random random) {
    long millis =
        MIN_TIMESTAMP_MILLIS
            + (long) (random.nextDouble() * (MAX_TIMESTAMP_MILLIS - MIN_TIMESTAMP_MILLIS));
    switch (random.nextInt(4)) {
      case 0:
        return TimestampData.fromEpochMillis(millis - Math.floorMod(millis, 60_000));
      case 1:
        return TimestampData.fromEpochMillis(millis);
      case 2:
        return TimestampData.fromEpochMillis(millis, random.nextInt(1000) * 1000);
      default:
        return TimestampData.fromEpochMillis(millis, random.nextInt(1_000_000));
    }
  }

  private static DecimalData decimal(BigInteger unscaled, int precision, int scale) {
    return DecimalData.fromBigDecimal(new BigDecimal(unscaled, scale), precision, scale);
  }

  private static StarRocksSinkOptions options(String format, Map<String, String> extra) {
    var builder =
        StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9030")
            .withProperty("load-url", "127.0.0.1:8030")
            .withProperty("database-name", "db")
            .withProperty("table-name", "t")
            .withProperty("username", "root")
            .withProperty("password", "")
            .withProperty("sink.properties.format", format);
    extra.forEach(builder::withProperty);
    return builder.build();
  }

  /** Stock rows for targets the planner admits: every text column lands in a VARCHAR column. */
  private static List<String> stock(
      RowType type, List<RowData> rows, StarRocksSinkOptions options) {
    Map<String, StarRocksDataType> targets = new HashMap<>();
    for (String name : type.getFieldNames()) targets.put(name, StarRocksDataType.VARCHAR);
    return stock(type, rows, options, targets);
  }

  /** The stock unified-sink serializer's rows as the stream-load SDK encodes them. */
  private static List<String> stock(
      RowType type,
      List<RowData> rows,
      StarRocksSinkOptions options,
      Map<String, StarRocksDataType> targets) {
    String[] names = type.getFieldNames().toArray(String[]::new);
    TableSchema.Builder schema = TableSchema.builder();
    for (int i = 0; i < names.length; i++)
      schema.field(names[i], TypeConversions.fromLogicalToDataType(type.getTypeAt(i)));
    var transformer = new StarRocksTableRowTransformer(InternalTypeInfo.of(type));
    transformer.setStarRocksColumns(targets);
    transformer.setTableSchema(schema.build());
    var serialization =
        new RowDataSerializationSchema(
            "db",
            "t",
            options.supportUpsertDelete(),
            options.getIgnoreUpdateBefore(),
            StarRocksSerializerFactory.createSerializer(options, names),
            transformer);
    serialization.open(null, null);
    List<String> serializedRows = new ArrayList<>();
    for (RowData row : rows) {
      StarRocksRowData serialized = serialization.serialize(row);
      if (serialized != null) serializedRows.add(sdkText(serialized.getRow()));
    }
    return serializedRows;
  }

  /** The text of the UTF-8 bytes the stream-load SDK writes for a row. */
  private static String sdkText(String row) {
    return new String(row.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
  }

  private static List<String> encoded(
      RowType type, List<RowData> rows, StarRocksSinkOptions options) throws Exception {
    TableSchema.Builder schema = TableSchema.builder();
    Map<String, StarRocksDataType> targets = new HashMap<>();
    for (int i = 0; i < type.getFieldCount(); i++) {
      String name = type.getFieldNames().get(i);
      schema.field(name, TypeConversions.fromLogicalToDataType(type.getTypeAt(i)));
      targets.put(name, StarRocksDataType.VARCHAR);
    }
    StarRocksEncoding encoding = StarRocksEncoding.of(options, schema.build(), type, targets);
    List<String> encodedRows = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator();
        NativeStarRocksEncoder encoder =
            new NativeStarRocksEncoder(encoding, new UnregisteredMetricsGroup())) {
      encoder.begin(new ArrowBatch(RowDataArrowConverter.write(rows, type, allocator, true)));
      try {
        for (StarRocksRowData row; (row = encoder.next()) != null; )
          encodedRows.add(sdkText(row.getRow()));
      } finally {
        encoder.end();
      }
    }
    return encodedRows;
  }
}
