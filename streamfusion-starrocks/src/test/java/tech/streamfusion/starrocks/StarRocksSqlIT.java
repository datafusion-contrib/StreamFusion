package tech.streamfusion.starrocks;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.ExceptionUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;
import tech.streamfusion.planner.NativePlanner;

/**
 * Runs each workload through the stock StarRocks writer and through the Arrow-fed native encoder
 * into twin tables of a real StarRocks server, then compares what the server stored.
 */
@Timeout(300)
class StarRocksSqlIT {
  private static final String HOST = System.getProperty("sf.starrocks.host", "127.0.0.1");
  private static final String HTTP = System.getProperty("sf.starrocks.http-port", "8030");
  private static final String MYSQL = System.getProperty("sf.starrocks.mysql-port", "9030");
  private static Connection connection;
  private static String database;

  @BeforeAll
  static void start() throws Exception {
    TaskOffHeapMemory.initialize(
        new Configuration()
            .set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(256)));
    connection =
        DriverManager.getConnection(
            "jdbc:mysql://" + HOST + ":" + MYSQL + "/?useSSL=false", "root", "");
    database = "sf_" + UUID.randomUUID().toString().replace("-", "");
    sql("CREATE DATABASE " + database);
  }

  @AfterAll
  static void stop() throws Exception {
    if (connection != null) {
      try {
        sql("DROP DATABASE " + database + " FORCE");
      } finally {
        connection.close();
      }
    }
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (NativeAllocator.SHARED.getAllocatedMemory() != 0 && System.nanoTime() < deadline)
      Thread.sleep(10);
    assertEquals(
        0,
        NativeAllocator.SHARED.getAllocatedMemory(),
        "Completed, failed and restarted StarRocks jobs must release their Arrow batches");
    assertEquals("", NativeStarRocks.liveNativeHandles(), "Every task closes its encoder");
  }

  @ParameterizedTest(name = "{0}, unified sink API {1}")
  @CsvSource({"csv,false", "csv,true", "json,false", "json,true"})
  void everyEncodedTypeLoadsLikeStock(String format, boolean unified) throws Exception {
    String stockTable = "types_stock_" + format + "_" + unified;
    String nativeTable = "types_native_" + format + "_" + unified;
    for (String table : List.of(stockTable, nativeTable))
      sql(
          "CREATE TABLE "
              + database
              + "."
              + table
              + " (id INT, b BOOLEAN, t TINYINT, s SMALLINT, i INT, l BIGINT, f FLOAT, d DOUBLE,"
              + " big DECIMAL(38,10), whole DECIMAL(18,0), c CHAR(8), v STRING, dt DATE,"
              + " ts0 DATETIME, ts3 DATETIME, ts6 DATETIME, ts9 DATETIME, ltz DATETIME, bin BIGINT)"
              + " DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 2"
              + " PROPERTIES('replication_num'='1')");
    DataType type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("b", DataTypes.BOOLEAN()),
            DataTypes.FIELD("t", DataTypes.TINYINT()),
            DataTypes.FIELD("s", DataTypes.SMALLINT()),
            DataTypes.FIELD("i", DataTypes.INT()),
            DataTypes.FIELD("l", DataTypes.BIGINT()),
            DataTypes.FIELD("f", DataTypes.FLOAT()),
            DataTypes.FIELD("d", DataTypes.DOUBLE()),
            DataTypes.FIELD("big", DataTypes.DECIMAL(38, 10)),
            DataTypes.FIELD("whole", DataTypes.DECIMAL(18, 0)),
            DataTypes.FIELD("c", DataTypes.CHAR(8)),
            DataTypes.FIELD("v", DataTypes.STRING()),
            DataTypes.FIELD("dt", DataTypes.DATE()),
            DataTypes.FIELD("ts0", DataTypes.TIMESTAMP(0)),
            DataTypes.FIELD("ts3", DataTypes.TIMESTAMP(3)),
            DataTypes.FIELD("ts6", DataTypes.TIMESTAMP(6)),
            DataTypes.FIELD("ts9", DataTypes.TIMESTAMP(9)),
            DataTypes.FIELD("ltz", DataTypes.TIMESTAMP_LTZ(6)),
            DataTypes.FIELD("bin", DataTypes.BINARY(8)));
    var rows = new ArrayList<Row>();
    rows.add(
        Row.of(
            1, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
            null, null, null, null));
    rows.add(
        Row.of(
            2,
            false,
            Byte.MIN_VALUE,
            Short.MIN_VALUE,
            Integer.MIN_VALUE,
            Long.MIN_VALUE,
            -Float.MAX_VALUE,
            -Double.MAX_VALUE,
            new BigDecimal("-9999999999999999999999999999.9999999999"),
            new BigDecimal("-999999999999999999"),
            "",
            "",
            LocalDate.of(1, 1, 1),
            LocalDateTime.of(1, 1, 1, 0, 0),
            LocalDateTime.of(1, 1, 1, 0, 0, 0, 1_000_000),
            LocalDateTime.of(1, 1, 1, 0, 0, 0, 1_000),
            LocalDateTime.of(1, 1, 1, 0, 0, 0, 1),
            Instant.EPOCH,
            new byte[] {(byte) 0x80, 0, 0, 0, 0, 0, 0, 0}));
    rows.add(
        Row.of(
            3,
            true,
            Byte.MAX_VALUE,
            Short.MAX_VALUE,
            Integer.MAX_VALUE,
            Long.MAX_VALUE,
            Float.MAX_VALUE,
            Double.MAX_VALUE,
            new BigDecimal("9999999999999999999999999999.9999999999"),
            new BigDecimal("999999999999999999"),
            "zzzzzzzz",
            "z".repeat(1000),
            LocalDate.of(9999, 12, 31),
            LocalDateTime.of(9999, 12, 31, 23, 59, 59),
            LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_000_000),
            LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_000),
            LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_999),
            Instant.parse("2262-04-11T23:47:16.854775Z"),
            new byte[] {0x7f, -1, -1, -1, -1, -1, -1, -1}));
    rows.add(
        Row.of(
            4,
            false,
            (byte) 0,
            (short) 0,
            0,
            0L,
            -0.0f,
            -0.0d,
            BigDecimal.ZERO,
            BigDecimal.ZERO,
            " ",
            " ",
            LocalDate.EPOCH,
            LocalDateTime.of(1970, 1, 1, 0, 0),
            LocalDateTime.of(1970, 1, 1, 0, 0),
            LocalDateTime.of(1970, 1, 1, 0, 0),
            LocalDateTime.of(1970, 1, 1, 0, 0),
            Instant.EPOCH,
            new byte[8]));
    rows.add(
        Row.of(
            5,
            true,
            (byte) 1,
            (short) 1,
            1,
            1L,
            Float.NaN,
            Double.POSITIVE_INFINITY,
            new BigDecimal("0.0000000001"),
            BigDecimal.ONE,
            "ab",
            "héllo wörld",
            LocalDate.of(1582, 10, 4),
            LocalDateTime.of(1582, 10, 4, 23, 59, 59),
            LocalDateTime.of(1582, 10, 4, 12, 0, 0, 100_000_000),
            LocalDateTime.of(1582, 10, 4, 12, 0, 0, 120_000),
            LocalDateTime.of(1582, 10, 4, 12, 0, 0, 123_000_000),
            Instant.parse("1582-10-04T12:00:00Z"),
            new byte[] {1, 2, 3, 4, 5, 6, 7, 8}));
    rows.add(
        Row.of(
            6,
            false,
            (byte) -1,
            (short) -1,
            -1,
            -1L,
            Float.NEGATIVE_INFINITY,
            Double.NaN,
            new BigDecimal("0.0000001"),
            new BigDecimal("-1"),
            "é",
            "😀 emoji 中文 Ελληνικά",
            LocalDate.of(1582, 10, 5),
            LocalDateTime.of(1582, 10, 5, 0, 0),
            LocalDateTime.of(1582, 10, 10, 1, 2, 3, 4_000_000),
            LocalDateTime.of(1582, 10, 14, 1, 2, 3, 4_000),
            LocalDateTime.of(1582, 10, 15, 1, 2, 3, 4),
            Instant.parse("1969-12-31T23:59:59.999999Z"),
            new byte[] {-1, -1, -1, -1, -1, -1, -1, -1}));
    rows.add(
        Row.of(
            7,
            true,
            (byte) 42,
            (short) 4242,
            424242,
            42424242424242L,
            Float.MIN_VALUE,
            Double.MIN_VALUE,
            new BigDecimal("-0.000001"),
            new BigDecimal("123456789012345678"),
            "{x}",
            "{\"not\":\"json\"}",
            LocalDate.of(1582, 10, 14),
            LocalDateTime.of(1900, 1, 1, 0, 0, 1),
            LocalDateTime.of(1900, 1, 1, 0, 0, 1, 10_000_000),
            LocalDateTime.of(1900, 1, 1, 0, 0, 1, 10_000),
            LocalDateTime.of(1900, 1, 1, 0, 0, 1, 10),
            Instant.parse("1900-01-01T00:00:00Z"),
            new byte[] {0, 0, 0, 0, 0, 0, 0, 1}));
    rows.add(
        Row.of(
            8,
            false,
            (byte) 7,
            (short) 7,
            7,
            7L,
            1.0E7f,
            9999999.999d,
            new BigDecimal("12345678901234567890.1234567891"),
            new BigDecimal("-123456789012345678"),
            "[1]",
            "[1,2]",
            LocalDate.of(1582, 10, 15),
            LocalDateTime.of(2000, 2, 29, 12, 0),
            LocalDateTime.of(2000, 2, 29, 12, 0, 0, 1_000_000),
            LocalDateTime.of(2000, 2, 29, 12, 0, 0, 1_000),
            LocalDateTime.of(2000, 2, 29, 12, 0, 0, 999),
            Instant.parse("2000-02-29T12:00:00.000001Z"),
            new byte[] {0, 0, 0, 0, 0, 1, 0, 0}));
    rows.add(
        Row.of(
            9,
            true,
            (byte) -7,
            (short) -7,
            -7,
            -7L,
            9999999.0f,
            0.001d,
            new BigDecimal("0.5"),
            new BigDecimal("10"),
            "\\N",
            "\\N",
            LocalDate.of(1900, 2, 28),
            LocalDateTime.of(2024, 3, 10, 2, 30),
            LocalDateTime.of(2024, 3, 10, 2, 30, 0, 500_000_000),
            LocalDateTime.of(2024, 3, 10, 2, 30, 0, 500_000),
            LocalDateTime.of(2024, 3, 10, 2, 30, 0, 500_000_000),
            Instant.parse("2024-03-10T02:30:00Z"),
            new byte[] {1, 0, 0, 0, 0, 0, 0, 0}));
    rows.add(
        Row.of(
            10,
            false,
            (byte) 100,
            (short) 1000,
            100000,
            10000000000L,
            0.001f,
            9.999E-4d,
            new BigDecimal("-0.5"),
            new BigDecimal("-10"),
            "q\"b\\s/",
            "quote \" backslash \\ slash /",
            LocalDate.of(2000, 2, 29),
            LocalDateTime.of(1969, 12, 31, 23, 59, 59),
            LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_000_000),
            LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_999_000),
            LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_999_999),
            Instant.parse("1969-12-31T23:59:59Z"),
            new byte[] {0, 1, 0, 1, 0, 1, 0, 1}));
    rows.add(
        Row.of(
            11,
            true,
            (byte) 3,
            (short) 3,
            3,
            3L,
            0.1f,
            0.1 + 0.2,
            new BigDecimal("1E+20"),
            new BigDecimal("1E+17"),
            "\u0001\u007f",
            "\u0001\u007f\u0080\u009f\u2028\u2029",
            LocalDate.of(1969, 12, 31),
            LocalDateTime.of(2024, 11, 3, 1, 30),
            LocalDateTime.of(2024, 11, 3, 1, 30, 0, 250_000_000),
            LocalDateTime.of(2024, 11, 3, 1, 30, 0, 250_000),
            LocalDateTime.of(2024, 11, 3, 1, 30, 0, 250_000_001),
            Instant.parse("2024-11-03T05:30:00Z"),
            new byte[] {
              0x12, 0x34, 0x56, 0x78, (byte) 0x9a, (byte) 0xbc, (byte) 0xde, (byte) 0xf0
            }));
    rows.add(
        Row.of(
            12,
            false,
            (byte) 9,
            (short) 9,
            9,
            9L,
            3.3333333f,
            1.0E21d,
            new BigDecimal("-1E-10"),
            new BigDecimal("5"),
            "a,b;c|d",
            "a,b;c|d",
            LocalDate.of(2024, 3, 10),
            LocalDateTime.of(1582, 10, 15, 0, 0),
            LocalDateTime.of(1582, 10, 15, 0, 0, 0, 1_000_000),
            LocalDateTime.of(1582, 10, 15, 0, 0, 0, 1_000),
            LocalDateTime.of(1582, 10, 15, 0, 0, 0, 1),
            Instant.parse("1582-10-15T00:00:00Z"),
            new byte[] {0, 0, 0, 0, 0, 0, 1, 0}));
    rows.add(
        Row.of(
            13,
            true,
            (byte) 13,
            (short) 13,
            13,
            13L,
            16777216.0f,
            9007199254740993.0d,
            new BigDecimal("3.1415926536"),
            new BigDecimal("31415926535897932"),
            "\uFEFFbom",
            "\uFEFFbom",
            LocalDate.of(100, 3, 1),
            LocalDateTime.of(100, 3, 1, 0, 0),
            LocalDateTime.of(100, 3, 1, 0, 0, 0, 7_000_000),
            LocalDateTime.of(100, 3, 1, 0, 0, 0, 7_000),
            LocalDateTime.of(100, 3, 1, 0, 0, 0, 7),
            Instant.parse("0100-03-01T00:00:00Z"),
            new byte[] {0, 0, 0, 0, 0, 0, 0, 0}));
    rows.add(
        Row.of(
            14,
            false,
            (byte) 14,
            (short) 14,
            14,
            14L,
            Float.MIN_NORMAL,
            2.82879384806159E17d,
            new BigDecimal("0.0000000009"),
            new BigDecimal("0"),
            "null",
            "null",
            LocalDate.of(1600, 2, 29),
            LocalDateTime.of(1600, 2, 29, 23, 59),
            LocalDateTime.of(1600, 2, 29, 23, 59, 0, 990_000_000),
            LocalDateTime.of(1600, 2, 29, 23, 59, 0, 999_990_000),
            LocalDateTime.of(1600, 2, 29, 23, 59, 0, 999_999_990),
            Instant.parse("1600-02-29T23:59:00Z"),
            new byte[] {(byte) 0xff, 0, 0, 0, 0, 0, 0, 0}));
    rows.add(
        Row.of(
            15,
            true,
            (byte) 15,
            (short) 15,
            15,
            15L,
            1.0E-36f,
            1.18575755E-316d,
            new BigDecimal("-9999999999.9999999999"),
            new BigDecimal("1000000000000000000").subtract(BigDecimal.ONE),
            "true",
            "true",
            LocalDate.of(2262, 4, 11),
            LocalDateTime.of(2262, 4, 11, 23, 47, 16),
            LocalDateTime.of(2262, 4, 11, 23, 47, 16, 854_000_000),
            LocalDateTime.of(2262, 4, 11, 23, 47, 16, 854_775_000),
            LocalDateTime.of(2262, 4, 11, 23, 47, 16, 854_775_807),
            Instant.parse("2262-04-11T23:47:16.854775Z"),
            new byte[] {0, (byte) 0x80, 0, 0, 0, 0, 0, 0}));
    // CSV bodies do not escape separators, so only JSON can carry tabs and line breaks.
    if (format.equals("json"))
      rows.add(
          Row.of(
              16,
              true,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              "\t\n",
              "tab\there\nnewline\r\b\f end",
              null,
              null,
              null,
              null,
              null,
              null,
              null));
    String sink =
        "CREATE TABLE dst (id INT, b BOOLEAN, t TINYINT, s SMALLINT, i INT, l BIGINT, f FLOAT,"
            + " d DOUBLE, big DECIMAL(38,10), whole DECIMAL(18,0), c CHAR(8), v STRING, dt DATE,"
            + " ts0 TIMESTAMP(0), ts3 TIMESTAMP(3), ts6 TIMESTAMP(6), ts9 TIMESTAMP(9),"
            + " ltz TIMESTAMP_LTZ(6), bin BINARY(8)) WITH (%s, 'sink.properties.format'='"
            + format
            + "', 'sink.use.new-sink-api'='"
            + unified
            + "')";

    var stockEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    stockEnvironment.setParallelism(2);
    var stock = StreamTableEnvironment.create(stockEnvironment);
    stock.createTemporaryView(
        "src",
        stock.fromDataStream(stockEnvironment.fromCollection(rows, ExternalTypeInfo.of(type))));
    stock.executeSql(String.format(sink, starrocks(stockTable)));
    stock.executeSql("INSERT INTO dst SELECT * FROM src").await();

    var nativeEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    nativeEnvironment.setParallelism(2);
    var accelerated = StreamTableEnvironment.create(nativeEnvironment);
    NativePlanner.install(accelerated);
    accelerated.createTemporaryView(
        "src",
        accelerated.fromDataStream(
            nativeEnvironment.fromCollection(rows, ExternalTypeInfo.of(type))));
    accelerated.executeSql(String.format(sink, starrocks(nativeTable)));
    String plan = NativePlanner.explain(accelerated, "INSERT INTO dst SELECT * FROM src");
    assertTrue(plan.contains("NativeStarRocksSink"), plan);
    assertFalse(plan.contains("ArrowToRowData"), plan);
    accelerated.executeSql("INSERT INTO dst SELECT * FROM src").await();

    awaitCount(stockTable, rows.size());
    awaitCount(nativeTable, rows.size());
    assertEquals(rows(stockTable), rows(nativeTable));
  }

  @ParameterizedTest(name = "{0} table, {1}")
  @CsvSource({
    "DUPLICATE,csv", "DUPLICATE,json", "AGGREGATE,csv", "AGGREGATE,json",
    "UNIQUE,csv", "UNIQUE,json", "PRIMARY,csv", "PRIMARY,json"
  })
  void tableModelsMergeRowsLikeStock(String model, String format) throws Exception {
    String stockTable = "model_stock_" + model + "_" + format;
    String nativeTable = "model_native_" + model + "_" + format;
    String columns =
        model.equals("AGGREGATE")
            ? "(k INT, total BIGINT SUM, label STRING REPLACE) AGGREGATE KEY(k)"
            : model.equals("PRIMARY")
                ? "(k INT NOT NULL, total BIGINT, label STRING) PRIMARY KEY(k)"
                : "(k INT, total BIGINT, label STRING) " + model + " KEY(k)";
    for (String table : List.of(stockTable, nativeTable))
      sql(
          "CREATE TABLE "
              + database
              + "."
              + table
              + " "
              + columns
              + " DISTRIBUTED BY HASH(k) BUCKETS 2 PROPERTIES('replication_num'='1')");
    DataType type =
        DataTypes.ROW(
            DataTypes.FIELD("k", DataTypes.INT()),
            DataTypes.FIELD("total", DataTypes.BIGINT()),
            DataTypes.FIELD("label", DataTypes.STRING()));
    List<Row> rows =
        List.of(
            Row.of(1, 10L, "a"),
            Row.of(2, 20L, "b"),
            Row.of(1, 5L, "a2"),
            Row.of(3, 30L, null),
            Row.of(2, -1L, "b2"),
            Row.of(1, 1L, "a3"));
    String sink =
        "CREATE TABLE dst (k INT, total BIGINT, label STRING"
            + (model.equals("PRIMARY") ? ", PRIMARY KEY (k) NOT ENFORCED" : "")
            + ") WITH (%s, 'sink.properties.format'='"
            + format
            + "')";

    var stockEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    stockEnvironment.setParallelism(1);
    var stock = StreamTableEnvironment.create(stockEnvironment);
    stock.createTemporaryView(
        "src",
        stock.fromDataStream(stockEnvironment.fromCollection(rows, ExternalTypeInfo.of(type))));
    stock.executeSql(String.format(sink, starrocks(stockTable)));
    stock.executeSql("INSERT INTO dst SELECT * FROM src").await();

    var nativeEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    nativeEnvironment.setParallelism(1);
    var accelerated = StreamTableEnvironment.create(nativeEnvironment);
    NativePlanner.install(accelerated);
    accelerated.createTemporaryView(
        "src",
        accelerated.fromDataStream(
            nativeEnvironment.fromCollection(rows, ExternalTypeInfo.of(type))));
    accelerated.executeSql(String.format(sink, starrocks(nativeTable)));
    String plan = NativePlanner.explain(accelerated, "INSERT INTO dst SELECT * FROM src");
    assertTrue(plan.contains("NativeStarRocksSink"), plan);
    accelerated.executeSql("INSERT INTO dst SELECT * FROM src").await();

    int expected = model.equals("DUPLICATE") ? 6 : 3;
    awaitCount(stockTable, expected);
    awaitCount(nativeTable, expected);
    assertEquals(rows(stockTable), rows(nativeTable));
    if (model.equals("AGGREGATE"))
      assertEquals(List.of("1|16|a3", "2|19|b2", "3|30|null"), rows(nativeTable));
    if (model.equals("UNIQUE") || model.equals("PRIMARY"))
      assertEquals(List.of("1|1|a3", "2|-1|b2", "3|30|null"), rows(nativeTable));
  }

  @ParameterizedTest(name = "{0}, ignore update-before {1}, unified sink API {2}")
  @CsvSource({
    "csv,true,false", "csv,false,false", "csv,false,true",
    "json,true,true", "json,false,true", "json,true,false"
  })
  void primaryKeyChangelogsUpsertAndDeleteLikeStock(
      String format, boolean ignoreUpdateBefore, boolean unified) throws Exception {
    String stockTable = "changelog_stock_" + format + "_" + ignoreUpdateBefore + "_" + unified;
    String nativeTable = "changelog_native_" + format + "_" + ignoreUpdateBefore + "_" + unified;
    for (String table : List.of(stockTable, nativeTable))
      sql(
          "CREATE TABLE "
              + database
              + "."
              + table
              + " (id INT NOT NULL, name STRING, score INT) PRIMARY KEY(id)"
              + " DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES('replication_num'='1')");
    DataType type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT().notNull()),
            DataTypes.FIELD("name", DataTypes.STRING()),
            DataTypes.FIELD("score", DataTypes.INT()));
    Schema schema = Schema.newBuilder().fromRowDataType(type).primaryKey("id").build();
    List<Row> changes =
        List.of(
            Row.ofKind(RowKind.INSERT, 1, "a", 10),
            Row.ofKind(RowKind.INSERT, 2, "b", 20),
            Row.ofKind(RowKind.UPDATE_BEFORE, 1, "a", 10),
            Row.ofKind(RowKind.UPDATE_AFTER, 1, "a2", 11),
            Row.ofKind(RowKind.DELETE, 2, "b", 20),
            Row.ofKind(RowKind.INSERT, 3, "c", 30),
            Row.ofKind(RowKind.UPDATE_BEFORE, 3, "c", 30),
            Row.ofKind(RowKind.UPDATE_AFTER, 3, "c2", 31),
            Row.ofKind(RowKind.INSERT, 2, "b2", 21),
            Row.ofKind(RowKind.DELETE, 4, "never", 0),
            Row.ofKind(RowKind.INSERT, 5, "e", 50),
            Row.ofKind(RowKind.UPDATE_BEFORE, 5, "e", 50));
    String sink =
        "CREATE TABLE dst (id INT NOT NULL, name STRING, score INT, PRIMARY KEY (id) NOT ENFORCED)"
            + " WITH (%s, 'sink.properties.format'='"
            + format
            + "', 'sink.ignore.update-before'='"
            + ignoreUpdateBefore
            + "', 'sink.use.new-sink-api'='"
            + unified
            + "')";

    var stockEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    stockEnvironment.setParallelism(1);
    var stock = StreamTableEnvironment.create(stockEnvironment);
    stock.createTemporaryView(
        "src",
        stock.fromChangelogStream(
            stockEnvironment.fromCollection(changes, ExternalTypeInfo.of(type)),
            schema,
            ChangelogMode.all()));
    stock.executeSql(String.format(sink, starrocks(stockTable)));
    stock.executeSql("INSERT INTO dst SELECT * FROM src").await();

    var nativeEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    nativeEnvironment.setParallelism(1);
    var accelerated = StreamTableEnvironment.create(nativeEnvironment);
    NativePlanner.install(accelerated);
    accelerated.createTemporaryView(
        "src",
        accelerated.fromChangelogStream(
            nativeEnvironment.fromCollection(changes, ExternalTypeInfo.of(type)),
            schema,
            ChangelogMode.all()));
    accelerated.executeSql(String.format(sink, starrocks(nativeTable)));
    String plan = NativePlanner.explain(accelerated, "INSERT INTO dst SELECT * FROM src");
    assertTrue(plan.contains("NativeStarRocksSink"), plan);
    accelerated.executeSql("INSERT INTO dst SELECT * FROM src").await();

    List<String> expected =
        ignoreUpdateBefore
            ? List.of("1|a2|11", "2|b2|21", "3|c2|31", "5|e|50")
            : List.of("1|a2|11", "2|b2|21", "3|c2|31");
    awaitCount(stockTable, expected.size());
    awaitCount(nativeTable, expected.size());
    assertEquals(rows(stockTable), rows(nativeTable));
    assertEquals(expected, rows(nativeTable));
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = {"csv", "json"})
  void changelogsIntoTablesWithoutPrimaryKeysAppendLikeStock(String format) throws Exception {
    String stockTable = "append_changelog_stock_" + format;
    String nativeTable = "append_changelog_native_" + format;
    for (String table : List.of(stockTable, nativeTable))
      sql(
          "CREATE TABLE "
              + database
              + "."
              + table
              + " (id INT, name STRING) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 2"
              + " PROPERTIES('replication_num'='1')");
    DataType type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("name", DataTypes.STRING()));
    List<Row> changes =
        List.of(
            Row.ofKind(RowKind.INSERT, 1, "a"),
            Row.ofKind(RowKind.UPDATE_BEFORE, 1, "a"),
            Row.ofKind(RowKind.UPDATE_AFTER, 1, "a2"),
            Row.ofKind(RowKind.DELETE, 1, "a2"),
            Row.ofKind(RowKind.INSERT, 2, "b"));
    String sink =
        "CREATE TABLE dst (id INT, name STRING) WITH (%s, 'sink.properties.format'='"
            + format
            + "')";

    var stockEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    stockEnvironment.setParallelism(1);
    var stock = StreamTableEnvironment.create(stockEnvironment);
    stock.createTemporaryView(
        "src",
        stock.fromChangelogStream(
            stockEnvironment.fromCollection(changes, ExternalTypeInfo.of(type))));
    stock.executeSql(String.format(sink, starrocks(stockTable)));
    stock.executeSql("INSERT INTO dst SELECT * FROM src").await();

    var nativeEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    nativeEnvironment.setParallelism(1);
    var accelerated = StreamTableEnvironment.create(nativeEnvironment);
    NativePlanner.install(accelerated);
    accelerated.createTemporaryView(
        "src",
        accelerated.fromChangelogStream(
            nativeEnvironment.fromCollection(changes, ExternalTypeInfo.of(type))));
    accelerated.executeSql(String.format(sink, starrocks(nativeTable)));
    String plan = NativePlanner.explain(accelerated, "INSERT INTO dst SELECT * FROM src");
    assertTrue(plan.contains("NativeStarRocksSink"), plan);
    accelerated.executeSql("INSERT INTO dst SELECT * FROM src").await();

    awaitCount(stockTable, 3);
    awaitCount(nativeTable, 3);
    assertEquals(rows(stockTable), rows(nativeTable));
    assertEquals(List.of("1|a", "1|a2", "2|b"), rows(nativeTable));
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = {"csv", "json"})
  void nativeAggregateUpsertsReachTheWriterWithoutRowConversion(String format) throws Exception {
    String stockTable = "aggregate_stock_" + format;
    String nativeTable = "aggregate_native_" + format;
    for (String table : List.of(stockTable, nativeTable))
      sql(
          "CREATE TABLE "
              + database
              + "."
              + table
              + " (k BIGINT NOT NULL, cnt BIGINT, total BIGINT, top BIGINT) PRIMARY KEY(k)"
              + " DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES('replication_num'='1')");
    String source =
        "CREATE TABLE src (id BIGINT) WITH ('connector'='datagen', 'fields.id.kind'='sequence',"
            + " 'fields.id.start'='1', 'fields.id.end'='20000', 'rows-per-second'='1000000')";
    String sink =
        "CREATE TABLE dst (k BIGINT NOT NULL, cnt BIGINT, total BIGINT, top BIGINT,"
            + " PRIMARY KEY (k) NOT ENFORCED) WITH (%s, 'sink.properties.format'='"
            + format
            + "')";
    String insert =
        "INSERT INTO dst SELECT id % 97, COUNT(*), SUM(id), MAX(id) FROM src GROUP BY id % 97";

    var stockEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    stockEnvironment.setParallelism(2);
    var stock = StreamTableEnvironment.create(stockEnvironment);
    stock.executeSql(source);
    stock.executeSql(String.format(sink, starrocks(stockTable)));
    stock.executeSql(insert).await();

    var nativeEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    nativeEnvironment.setParallelism(2);
    var accelerated = StreamTableEnvironment.create(nativeEnvironment);
    NativePlanner.install(accelerated);
    accelerated.executeSql(source);
    accelerated.executeSql(String.format(sink, starrocks(nativeTable)));
    String plan = NativePlanner.explain(accelerated, insert);
    assertTrue(plan.contains("NativeStarRocksSink"), plan);
    assertFalse(plan.contains("ArrowToRowData"), plan);
    accelerated.executeSql(insert).await();

    awaitCount(stockTable, 97);
    awaitCount(nativeTable, 97);
    assertEquals(rows(stockTable), rows(nativeTable));
    try (var jdbc = connection.createStatement();
        var result =
            jdbc.executeQuery(
                "SELECT SUM(cnt), SUM(total), MAX(top) FROM " + database + "." + nativeTable)) {
      assertTrue(result.next());
      assertEquals(20000, result.getLong(1));
      assertEquals(200010000L, result.getLong(2));
      assertEquals(20000, result.getLong(3));
    }
  }

  @ParameterizedTest(name = "upsert materialize {0}, sink parallelism {1}")
  @CsvSource({"AUTO,3", "FORCE,3", "AUTO,2"})
  void parallelWritersKeepPerKeyOrderLikeStock(String materialize, int sinkParallelism)
      throws Exception {
    String stockTable = "parallel_stock_" + materialize + "_" + sinkParallelism;
    String nativeTable = "parallel_native_" + materialize + "_" + sinkParallelism;
    for (String table : List.of(stockTable, nativeTable))
      sql(
          "CREATE TABLE "
              + database
              + "."
              + table
              + " (k BIGINT NOT NULL, cnt BIGINT, total BIGINT) PRIMARY KEY(k)"
              + " DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES('replication_num'='1')");
    String source =
        "CREATE TABLE src (id BIGINT) WITH ('connector'='datagen', 'fields.id.kind'='sequence',"
            + " 'fields.id.start'='1', 'fields.id.end'='50000', 'rows-per-second'='1000000')";
    String sink =
        "CREATE TABLE dst (k BIGINT NOT NULL, cnt BIGINT, total BIGINT,"
            + " PRIMARY KEY (k) NOT ENFORCED) WITH (%s, 'sink.parallelism'='"
            + sinkParallelism
            + "')";
    String insert = "INSERT INTO dst SELECT id % 101, COUNT(*), SUM(id) FROM src GROUP BY id % 101";

    var stockEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    stockEnvironment.setParallelism(2);
    var stock = StreamTableEnvironment.create(stockEnvironment);
    stock.getConfig().set("table.exec.sink.upsert-materialize", materialize);
    stock.executeSql(source);
    stock.executeSql(String.format(sink, starrocks(stockTable)));
    stock.executeSql(insert).await();

    var nativeEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    nativeEnvironment.setParallelism(2);
    var accelerated = StreamTableEnvironment.create(nativeEnvironment);
    accelerated.getConfig().set("table.exec.sink.upsert-materialize", materialize);
    NativePlanner.install(accelerated);
    accelerated.executeSql(source);
    accelerated.executeSql(String.format(sink, starrocks(nativeTable)));
    String plan = NativePlanner.explain(accelerated, insert);
    assertTrue(plan.contains("NativeStarRocksSink"), plan);
    assertEquals(materialize.equals("FORCE"), plan.contains("upsertMaterialize=[true]"), plan);
    // Flink keys a configured writer above a hash exchange even at the same parallelism.
    String job = accelerated.explainSql(insert, ExplainDetail.JSON_EXECUTION_PLAN);
    assertTrue(job.contains("starrocks-primary-key-split"), job);
    accelerated.executeSql(insert).await();

    awaitCount(stockTable, 101);
    awaitCount(nativeTable, 101);
    assertEquals(rows(stockTable), rows(nativeTable));
  }

  @ParameterizedTest(name = "sink parallelism {0}")
  @ValueSource(ints = {2, 3})
  void changelogsWithoutAPrimaryKeyRejectAConfiguredParallelismLikeStock(int sinkParallelism)
      throws Exception {
    String source =
        "CREATE TABLE src (id BIGINT) WITH ('connector'='datagen', 'fields.id.kind'='sequence',"
            + " 'fields.id.start'='1', 'fields.id.end'='10')";
    String sink =
        "CREATE TABLE dst (k BIGINT, cnt BIGINT) WITH (%s, 'sink.parallelism'='"
            + sinkParallelism
            + "')";
    String insert = "INSERT INTO dst SELECT id % 2, COUNT(*) FROM src GROUP BY id % 2";
    sql(
        "CREATE TABLE IF NOT EXISTS "
            + database
            + ".unkeyed_changelog (k BIGINT, cnt BIGINT) DUPLICATE KEY(k)"
            + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");

    var stockEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    stockEnvironment.setParallelism(2);
    var stock = StreamTableEnvironment.create(stockEnvironment);
    stock.executeSql(source);
    stock.executeSql(String.format(sink, starrocks("unkeyed_changelog")));
    var stockFailure = assertThrows(TableException.class, () -> stock.executeSql(insert));

    var nativeEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    nativeEnvironment.setParallelism(2);
    var accelerated = StreamTableEnvironment.create(nativeEnvironment);
    NativePlanner.install(accelerated);
    accelerated.executeSql(source);
    accelerated.executeSql(String.format(sink, starrocks("unkeyed_changelog")));
    var nativeFailure = assertThrows(TableException.class, () -> accelerated.executeSql(insert));

    assertEquals(stockFailure.getMessage(), nativeFailure.getMessage());
    assertTrue(
        java.util.Arrays.stream(nativeFailure.getStackTrace())
            .anyMatch(frame -> frame.getClassName().endsWith("StarRocksSinkRuntime")),
        ExceptionUtils.stringifyException(nativeFailure));
    assertEquals(List.of(), rows("unkeyed_changelog"));
  }

  @ParameterizedTest(name = "native {0}, {1}, unified sink API {2}")
  @CsvSource({
    "false,exactly-once,false", "true,exactly-once,false",
    "false,exactly-once,true", "true,exactly-once,true",
    "false,at-least-once,false", "true,at-least-once,false",
    "false,at-least-once,true", "true,at-least-once,true"
  })
  void checkpointedWritersRecoverAfterAFailure(boolean nativeRun, String semantic, boolean unified)
      throws Exception {
    String table = "recovery_" + nativeRun + "_" + semantic.replace('-', '_') + "_" + unified;
    sql(
        "CREATE TABLE "
            + database
            + "."
            + table
            + " (id BIGINT, n INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1"
            + " PROPERTIES('replication_num'='1')");
    var config = new Configuration();
    config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
    config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
    var environment = StreamExecutionEnvironment.getExecutionEnvironment(config);
    environment.setParallelism(1);
    environment.enableCheckpointing(100);
    var tables = StreamTableEnvironment.create(environment);
    if (nativeRun) NativePlanner.install(tables);
    RestartingRows.failures.set(0);
    tables.createTemporaryView(
        "src",
        environment
            .addSource(new RestartingRows())
            .returns(Types.ROW_NAMED(new String[] {"id", "n"}, Types.LONG, Types.INT)));
    tables.executeSql(
        "CREATE TABLE dst (id BIGINT, n INT) WITH ("
            + starrocks(table)
            + ", 'sink.semantic'='"
            + semantic
            + "', 'sink.use.new-sink-api'='"
            + unified
            + "', 'sink.buffer-flush.interval-ms'='100')");
    String insert = "INSERT INTO dst SELECT id, n + 1 FROM src";
    String plan = nativeRun ? NativePlanner.explain(tables, insert) : tables.explainSql(insert);
    assertEquals(nativeRun, plan.contains("NativeStarRocksSink"), plan);

    tables.executeSql(insert).await();

    assertEquals(1, RestartingRows.failures.get());
    if (semantic.equals("exactly-once")) awaitCount(table, 2000);
    try (var jdbc = connection.createStatement();
        var result =
            jdbc.executeQuery(
                "SELECT COUNT(*), COUNT(DISTINCT id), SUM(DISTINCT n) FROM "
                    + database
                    + "."
                    + table)) {
      assertTrue(result.next());
      if (semantic.equals("exactly-once")) assertEquals(2000, result.getInt(1));
      else assertTrue(result.getInt(1) >= 2000, "at-least-once may replay but never loses rows");
      assertEquals(2000, result.getInt(2));
      assertEquals(2001000L, result.getLong(3));
    }
  }

  @Test
  void unifiedExactlyOnceRestoresFailLikeTheReleasedConnector() throws Exception {
    // Connector 1.2.16 reads labelled unified writer state back as the legacy snapshot type:
    // https://github.com/StarRocks/starrocks-connector-for-apache-flink/issues/518
    var messages = new ArrayList<String>();
    for (boolean nativeRun : List.of(false, true)) {
      String table = "unified_exactly_once_" + nativeRun;
      sql(
          "CREATE TABLE "
              + database
              + "."
              + table
              + " (id BIGINT, n INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1"
              + " PROPERTIES('replication_num'='1')");
      var config = new Configuration();
      config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
      config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
      config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
      var environment = StreamExecutionEnvironment.getExecutionEnvironment(config);
      environment.setParallelism(1);
      environment.enableCheckpointing(100);
      var tables = StreamTableEnvironment.create(environment);
      if (nativeRun) NativePlanner.install(tables);
      RestartingRows.failures.set(0);
      tables.createTemporaryView(
          "src",
          environment
              .addSource(new RestartingRows())
              .returns(Types.ROW_NAMED(new String[] {"id", "n"}, Types.LONG, Types.INT)));
      tables.executeSql(
          "CREATE TABLE dst (id BIGINT, n INT) WITH ("
              + starrocks(table)
              + ", 'sink.semantic'='exactly-once', 'sink.use.new-sink-api'='true',"
              + " 'sink.label-prefix'='sf"
              + UUID.randomUUID().toString().replace("-", "")
              + "')");
      String insert = "INSERT INTO dst SELECT id, n + 1 FROM src";
      String plan = nativeRun ? NativePlanner.explain(tables, insert) : tables.explainSql(insert);
      assertEquals(nativeRun, plan.contains("NativeStarRocksSink"), plan);

      var failure = assertThrows(Exception.class, () -> tables.executeSql(insert).await());

      assertEquals(1, RestartingRows.failures.get());
      messages.add(
          ExceptionUtils.findThrowable(failure, ClassCastException.class)
              .orElseThrow()
              .getMessage());
      assertTrue(rows(table).size() <= 1000, "only checkpointed rows were committed");
    }
    assertEquals(messages.get(0), messages.get(1));
    assertTrue(messages.get(1).contains("StarRocksWriterState"), messages.get(1));
  }

  public static final class RestartingRows extends RichParallelSourceFunction<Row>
      implements CheckpointedFunction, CheckpointListener {
    static final AtomicInteger failures = new AtomicInteger();
    private transient ListState<Integer> state;
    private int next;
    private volatile boolean completed;
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Row> context) throws Exception {
      boolean first = getRuntimeContext().getAttemptNumber() == 0;
      int end = first ? 1000 : 2000;
      while (running && next < end) {
        synchronized (context.getCheckpointLock()) {
          context.collect(Row.of((long) next, next));
          next++;
        }
      }
      if (first) {
        while (running && !completed) Thread.sleep(10);
        if (running) {
          failures.incrementAndGet();
          throw new java.io.IOException(
              "StarRocks recovery test: fail after a completed checkpoint");
        }
      }
    }

    @Override
    public void cancel() {
      running = false;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      completed = true;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      state.update(List.of(next));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      state =
          context
              .getOperatorStateStore()
              .getListState(new ListStateDescriptor<>("next", Integer.class));
      if (context.isRestored()) for (int offset : state.get()) next = offset;
    }
  }

  @ParameterizedTest(name = "unified sink API {0}")
  @ValueSource(booleans = {false, true})
  void notNullViolationsFailLikeStock(boolean unified) throws Exception {
    String stockTable = "not_null_stock_" + unified;
    String nativeTable = "not_null_native_" + unified;
    for (String table : List.of(stockTable, nativeTable))
      sql(
          "CREATE TABLE "
              + database
              + "."
              + table
              + " (id INT, name STRING) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1"
              + " PROPERTIES('replication_num'='1')");
    DataType type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("name", DataTypes.STRING()));
    List<Row> rows = List.of(Row.of(1, "a"), Row.of(2, null), Row.of(3, "c"));
    String sink =
        "CREATE TABLE dst (id INT, name STRING NOT NULL) WITH (%s,"
            + " 'sink.semantic'='exactly-once', 'sink.use.new-sink-api'='"
            + unified
            + "')";

    var stockEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    stockEnvironment.setParallelism(1);
    var stock = StreamTableEnvironment.create(stockEnvironment);
    stock.createTemporaryView(
        "src",
        stock.fromDataStream(stockEnvironment.fromCollection(rows, ExternalTypeInfo.of(type))));
    stock.executeSql(String.format(sink, starrocks(stockTable)));
    var stockFailure =
        assertThrows(
            Exception.class, () -> stock.executeSql("INSERT INTO dst SELECT * FROM src").await());

    var nativeEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    nativeEnvironment.setParallelism(1);
    var accelerated = StreamTableEnvironment.create(nativeEnvironment);
    NativePlanner.install(accelerated);
    accelerated.createTemporaryView(
        "src",
        accelerated.fromDataStream(
            nativeEnvironment.fromCollection(rows, ExternalTypeInfo.of(type))));
    accelerated.executeSql(String.format(sink, starrocks(nativeTable)));
    assertTrue(
        NativePlanner.explain(accelerated, "INSERT INTO dst SELECT * FROM src")
            .contains("NativeStarRocksSink"));
    var nativeFailure =
        assertThrows(
            Exception.class,
            () -> accelerated.executeSql("INSERT INTO dst SELECT * FROM src").await());

    String stockMessage =
        ExceptionUtils.findThrowableWithMessage(stockFailure, "is NOT NULL")
            .orElseThrow()
            .getMessage();
    String nativeMessage =
        ExceptionUtils.findThrowableWithMessage(nativeFailure, "is NOT NULL")
            .orElseThrow()
            .getMessage();
    assertEquals(stockMessage, nativeMessage);
    assertTrue(nativeMessage.startsWith("Column 'name' is NOT NULL"), nativeMessage);
    assertEquals(List.of(), rows(stockTable));
    assertEquals(List.of(), rows(nativeTable));
  }

  @ParameterizedTest(name = "{0}, unified sink API {1}")
  @CsvSource({"csv,false", "json,true"})
  void rowsTheServerFiltersFailTheLoadLikeStock(String format, boolean unified) throws Exception {
    String stockTable = "filtered_stock_" + format;
    String nativeTable = "filtered_native_" + format;
    for (String table : List.of(stockTable, nativeTable))
      sql(
          "CREATE TABLE "
              + database
              + "."
              + table
              + " (id INT, code TINYINT NOT NULL) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS"
              + " 1 PROPERTIES('replication_num'='1')");
    DataType type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("code", DataTypes.INT()));
    List<Row> rows = List.of(Row.of(1, 7), Row.of(2, 1000));
    String sink =
        "CREATE TABLE dst (id INT, code INT) WITH (%s, 'sink.properties.format'='"
            + format
            + "', 'sink.use.new-sink-api'='"
            + unified
            + "')";

    var stockEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    stockEnvironment.setParallelism(1);
    var stock = StreamTableEnvironment.create(stockEnvironment);
    stock.createTemporaryView(
        "src",
        stock.fromDataStream(stockEnvironment.fromCollection(rows, ExternalTypeInfo.of(type))));
    stock.executeSql(String.format(sink, starrocks(stockTable)));
    var stockFailure =
        assertThrows(
            Exception.class, () -> stock.executeSql("INSERT INTO dst SELECT * FROM src").await());

    var nativeEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    nativeEnvironment.setParallelism(1);
    var accelerated = StreamTableEnvironment.create(nativeEnvironment);
    NativePlanner.install(accelerated);
    accelerated.createTemporaryView(
        "src",
        accelerated.fromDataStream(
            nativeEnvironment.fromCollection(rows, ExternalTypeInfo.of(type))));
    accelerated.executeSql(String.format(sink, starrocks(nativeTable)));
    assertTrue(
        NativePlanner.explain(accelerated, "INSERT INTO dst SELECT * FROM src")
            .contains("NativeStarRocksSink"));
    var nativeFailure =
        assertThrows(
            Exception.class,
            () -> accelerated.executeSql("INSERT INTO dst SELECT * FROM src").await());

    assertTrue(
        ExceptionUtils.findThrowableWithMessage(stockFailure, "too many filtered rows").isPresent(),
        ExceptionUtils.stringifyException(stockFailure));
    assertTrue(
        ExceptionUtils.findThrowableWithMessage(nativeFailure, "too many filtered rows")
            .isPresent(),
        ExceptionUtils.stringifyException(nativeFailure));
    assertEquals(List.of(), rows(stockTable));
    assertEquals(List.of(), rows(nativeTable));
  }

  @ParameterizedTest(name = "{0}, columns from Flink schema {1}")
  @CsvSource({"csv,false", "json,true"})
  void partialColumnUpdatesMatchStock(String format, boolean columnsFromSchema) throws Exception {
    String stockTable = "partial_stock_" + format;
    String nativeTable = "partial_native_" + format;
    for (String table : List.of(stockTable, nativeTable)) {
      sql(
          "CREATE TABLE "
              + database
              + "."
              + table
              + " (id INT NOT NULL, name STRING, score INT, note STRING) PRIMARY KEY(id)"
              + " DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES('replication_num'='1')");
      sql("INSERT INTO " + database + "." + table + " VALUES (1,'a',10,'n1'), (2,'b',20,'n2')");
    }
    DataType type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT().notNull()),
            DataTypes.FIELD("score", DataTypes.INT()));
    List<Row> rows = List.of(Row.of(1, 11), Row.of(3, 30), Row.of(2, null));
    String sink =
        "CREATE TABLE dst (id INT NOT NULL, score INT, PRIMARY KEY (id) NOT ENFORCED) WITH (%s,"
            + " 'sink.properties.partial_update'='true', 'sink.properties.format'='"
            + format
            + "', 'sink.json.columns-from-flink-schema'='"
            + columnsFromSchema
            + "')";

    var stockEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    stockEnvironment.setParallelism(1);
    var stock = StreamTableEnvironment.create(stockEnvironment);
    stock.createTemporaryView(
        "src",
        stock.fromDataStream(stockEnvironment.fromCollection(rows, ExternalTypeInfo.of(type))));
    stock.executeSql(String.format(sink, starrocks(stockTable)));
    stock.executeSql("INSERT INTO dst SELECT * FROM src").await();

    var nativeEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    nativeEnvironment.setParallelism(1);
    var accelerated = StreamTableEnvironment.create(nativeEnvironment);
    NativePlanner.install(accelerated);
    accelerated.createTemporaryView(
        "src",
        accelerated.fromDataStream(
            nativeEnvironment.fromCollection(rows, ExternalTypeInfo.of(type))));
    accelerated.executeSql(String.format(sink, starrocks(nativeTable)));
    String plan = NativePlanner.explain(accelerated, "INSERT INTO dst SELECT * FROM src");
    assertTrue(plan.contains("NativeStarRocksSink"), plan);
    accelerated.executeSql("INSERT INTO dst SELECT * FROM src").await();

    awaitCount(stockTable, 3);
    awaitCount(nativeTable, 3);
    assertEquals(rows(stockTable), rows(nativeTable));
    assertEquals(List.of("1|a|11|n1", "2|b|null|n2", "3|null|30|null"), rows(nativeTable));
  }

  @Test
  void explicitColumnMappingsMatchStock() throws Exception {
    for (String table : List.of("mapped_stock", "mapped_native"))
      sql(
          "CREATE TABLE "
              + database
              + "."
              + table
              + " (id INT, label STRING, doubled BIGINT) DUPLICATE KEY(id)"
              + " DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
    DataType type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("amount", DataTypes.BIGINT()),
            DataTypes.FIELD("label", DataTypes.STRING()));
    List<Row> rows = List.of(Row.of(1, 21L, "x"), Row.of(2, null, "y"), Row.of(3, -4L, null));
    String sink =
        "CREATE TABLE dst (id INT, amount BIGINT, label STRING) WITH (%s,"
            + " 'sink.properties.columns'='id,amount,label,doubled=amount*2')";

    var stockEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    stockEnvironment.setParallelism(1);
    var stock = StreamTableEnvironment.create(stockEnvironment);
    stock.createTemporaryView(
        "src",
        stock.fromDataStream(stockEnvironment.fromCollection(rows, ExternalTypeInfo.of(type))));
    stock.executeSql(String.format(sink, starrocks("mapped_stock")));
    stock.executeSql("INSERT INTO dst SELECT * FROM src").await();

    var nativeEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    nativeEnvironment.setParallelism(1);
    var accelerated = StreamTableEnvironment.create(nativeEnvironment);
    NativePlanner.install(accelerated);
    accelerated.createTemporaryView(
        "src",
        accelerated.fromDataStream(
            nativeEnvironment.fromCollection(rows, ExternalTypeInfo.of(type))));
    accelerated.executeSql(String.format(sink, starrocks("mapped_native")));
    String plan = NativePlanner.explain(accelerated, "INSERT INTO dst SELECT * FROM src");
    assertTrue(plan.contains("NativeStarRocksSink"), plan);
    accelerated.executeSql("INSERT INTO dst SELECT * FROM src").await();

    awaitCount("mapped_stock", 3);
    awaitCount("mapped_native", 3);
    assertEquals(rows("mapped_stock"), rows("mapped_native"));
    assertEquals(List.of("1|x|42", "2|y|null", "3|null|-8"), rows("mapped_native"));
  }

  @Test
  void textThatTheConnectorReparsesKeepsTheStockWriter() throws Exception {
    sql(
        "CREATE TABLE "
            + database
            + ".json_target (id INT, doc JSON) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)"
            + " BUCKETS 1 PROPERTIES('replication_num'='1')");
    sql(
        "CREATE TABLE "
            + database
            + ".cased_target (id INT, name STRING) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)"
            + " BUCKETS 1 PROPERTIES('replication_num'='1')");
    DataType type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("doc", DataTypes.STRING()));
    List<Row> rows =
        List.of(Row.of(1, "{\"b\": 2, \"a\": [1, 2]}"), Row.of(2, "[ ]"), Row.of(3, "7"));

    var environment = StreamExecutionEnvironment.getExecutionEnvironment();
    environment.setParallelism(1);
    var tables = StreamTableEnvironment.create(environment);
    var scan = NativePlanner.install(tables);
    tables.createTemporaryView(
        "src", tables.fromDataStream(environment.fromCollection(rows, ExternalTypeInfo.of(type))));
    tables.executeSql(
        "CREATE TABLE json_dst (id INT, doc STRING) WITH (" + starrocks("json_target") + ")");
    tables.executeSql(
        "CREATE TABLE cased_dst (ID INT, NAME STRING) WITH (" + starrocks("cased_target") + ")");
    String jsonPlan = tables.explainSql("INSERT INTO json_dst SELECT * FROM src");
    List<String> jsonReasons = scan.fallbackReasons();
    String casedPlan = tables.explainSql("INSERT INTO cased_dst SELECT * FROM src");
    List<String> casedReasons = scan.fallbackReasons();
    tables.executeSql("INSERT INTO json_dst SELECT * FROM src").await();
    tables.executeSql("INSERT INTO cased_dst SELECT * FROM src").await();

    assertFalse(jsonPlan.contains("NativeStarRocksSink"), jsonPlan);
    assertFalse(casedPlan.contains("NativeStarRocksSink"), casedPlan);
    assertTrue(
        jsonReasons.contains(
            "starrocks sink: text column doc targets a StarRocks JSON column, which the connector"
                + " re-serializes through fastjson"),
        jsonReasons.toString());
    assertTrue(
        casedReasons.contains(
            "starrocks sink: text column NAME targets a StarRocks unrecognized or differently cased"
                + " column, which the connector re-serializes through fastjson"),
        casedReasons.toString());
    awaitCount("json_target", 3);
    awaitCount("cased_target", 3);
    assertEquals(List.of("1|{\"a\": [1, 2], \"b\": 2}", "2|[]", "3|7"), rows("json_target"));
    assertEquals(List.of("1|{\"a\":[1,2],\"b\":2}", "2|[]", "3|7"), rows("cased_target"));
  }

  @ParameterizedTest(name = "column separator {0}, row delimiter {1}")
  @CsvSource({"\\x01,\\x02", "||,@@"})
  void customSeparatorsCarryTabsAndLineBreaksLikeStock(String column, String row) throws Exception {
    String suffix = column.equals("||") ? "multi" : "control";
    for (String table : List.of("separated_stock_" + suffix, "separated_native_" + suffix))
      sql(
          "CREATE TABLE "
              + database
              + "."
              + table
              + " (id INT, v STRING) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1"
              + " PROPERTIES('replication_num'='1')");
    DataType type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("v", DataTypes.STRING()));
    List<Row> rows =
        List.of(
            Row.of(1, "tab\there"), Row.of(2, "line\nbreak\r\n"), Row.of(3, null), Row.of(4, ""));
    String sink =
        "CREATE TABLE dst (id INT, v STRING) WITH (%s, 'sink.properties.column_separator'='"
            + column
            + "', 'sink.properties.row_delimiter'='"
            + row
            + "')";

    var stockEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    stockEnvironment.setParallelism(1);
    var stock = StreamTableEnvironment.create(stockEnvironment);
    stock.createTemporaryView(
        "src",
        stock.fromDataStream(stockEnvironment.fromCollection(rows, ExternalTypeInfo.of(type))));
    stock.executeSql(String.format(sink, starrocks("separated_stock_" + suffix)));
    stock.executeSql("INSERT INTO dst SELECT * FROM src").await();

    var nativeEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    nativeEnvironment.setParallelism(1);
    var accelerated = StreamTableEnvironment.create(nativeEnvironment);
    NativePlanner.install(accelerated);
    accelerated.createTemporaryView(
        "src",
        accelerated.fromDataStream(
            nativeEnvironment.fromCollection(rows, ExternalTypeInfo.of(type))));
    accelerated.executeSql(String.format(sink, starrocks("separated_native_" + suffix)));
    String plan = NativePlanner.explain(accelerated, "INSERT INTO dst SELECT * FROM src");
    assertTrue(plan.contains("NativeStarRocksSink"), plan);
    accelerated.executeSql("INSERT INTO dst SELECT * FROM src").await();

    awaitCount("separated_stock_" + suffix, 4);
    awaitCount("separated_native_" + suffix, 4);
    assertEquals(rows("separated_stock_" + suffix), rows("separated_native_" + suffix));
    assertEquals(
        List.of("1|tab\there", "2|line\nbreak\r\n", "3|null", "4|"),
        rows("separated_native_" + suffix));
  }

  @ParameterizedTest(name = "{0}, unified sink API {1}")
  @CsvSource({"csv,false", "csv,true", "json,false", "json,true"})
  void compressedLoadsAcrossManyFlushesMatchStock(String format, boolean unified) throws Exception {
    String stockTable = "flushes_stock_" + format + "_" + unified;
    String nativeTable = "flushes_native_" + format + "_" + unified;
    for (String table : List.of(stockTable, nativeTable))
      sql(
          "CREATE TABLE "
              + database
              + "."
              + table
              + " (id BIGINT, label STRING, half DOUBLE) DUPLICATE KEY(id)"
              + " DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES('replication_num'='1')");
    String source =
        "CREATE TABLE src (id BIGINT) WITH ('connector'='datagen', 'fields.id.kind'='sequence',"
            + " 'fields.id.start'='1', 'fields.id.end'='200000', 'rows-per-second'='100000')";
    String sink =
        "CREATE TABLE dst (id BIGINT, label STRING, half DOUBLE) WITH (%s,"
            + " 'sink.buffer-flush.interval-ms'='50', 'sink.properties.compression'='lz4_frame',"
            + " 'sink.properties.format'='"
            + format
            + "', 'sink.use.new-sink-api'='"
            + unified
            + "')";
    String insert =
        "INSERT INTO dst SELECT id, CONCAT('row-', CAST(id AS STRING)), id / 2.0 FROM src";

    var stockEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    stockEnvironment.setParallelism(2);
    stockEnvironment.enableCheckpointing(200);
    var stock = StreamTableEnvironment.create(stockEnvironment);
    stock.executeSql(source);
    stock.executeSql(String.format(sink, starrocks(stockTable)));
    stock.executeSql(insert).await();

    var nativeEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    nativeEnvironment.setParallelism(2);
    nativeEnvironment.enableCheckpointing(200);
    var accelerated = StreamTableEnvironment.create(nativeEnvironment);
    NativePlanner.install(accelerated);
    accelerated.executeSql(source);
    accelerated.executeSql(String.format(sink, starrocks(nativeTable)));
    String plan = NativePlanner.explain(accelerated, insert);
    assertTrue(plan.contains("NativeStarRocksSink"), plan);
    accelerated.executeSql(insert).await();

    awaitCount(stockTable, 200000);
    awaitCount(nativeTable, 200000);
    assertEquals(rows(stockTable), rows(nativeTable));
    try (var jdbc = connection.createStatement();
        var result =
            jdbc.executeQuery(
                "SELECT COUNT(*) FROM information_schema.loads WHERE DB_NAME = '"
                    + database
                    + "' AND TABLE_NAME = '"
                    + nativeTable
                    + "' AND STATE = 'FINISHED'")) {
      assertTrue(result.next());
      assertTrue(result.getInt(1) > 2, "The native writer must commit several loads");
    }
  }

  @Test
  void zoneDependentDatesAndLocalTimestampsMatchStock() throws Exception {
    for (String table : List.of("zoned_stock", "zoned_native"))
      sql(
          "CREATE TABLE "
              + database
              + "."
              + table
              + " (id INT, dt DATE, ltz DATETIME) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id)"
              + " BUCKETS 1 PROPERTIES('replication_num'='1')");
    DataType type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("dt", DataTypes.DATE()),
            DataTypes.FIELD("ltz", DataTypes.TIMESTAMP_LTZ(3)));
    List<Row> rows =
        List.of(
            Row.of(1, LocalDate.of(1582, 10, 4), Instant.parse("1582-10-04T23:00:00Z")),
            Row.of(2, LocalDate.of(1582, 10, 9), Instant.parse("1582-10-09T23:00:00Z")),
            Row.of(3, LocalDate.of(1883, 11, 18), Instant.parse("1883-11-18T16:59:59.999Z")),
            Row.of(4, LocalDate.of(1918, 3, 31), Instant.parse("1918-03-31T07:00:00Z")),
            Row.of(5, LocalDate.of(1969, 12, 31), Instant.parse("1969-12-31T23:59:59.999Z")),
            Row.of(6, LocalDate.of(2024, 3, 10), Instant.parse("2024-03-10T07:00:00Z")),
            Row.of(7, LocalDate.of(2024, 11, 3), Instant.parse("2024-11-03T06:00:00Z")),
            Row.of(8, LocalDate.of(1, 1, 1), Instant.parse("0001-01-01T00:00:00Z")));
    String sink = "CREATE TABLE dst (id INT, dt DATE, ltz TIMESTAMP_LTZ(3)) WITH (%s)";
    TimeZone previous = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
    try {
      var stockEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
      stockEnvironment.setParallelism(1);
      var stock = StreamTableEnvironment.create(stockEnvironment);
      stock.getConfig().set("table.local-time-zone", "Asia/Kolkata");
      stock.createTemporaryView(
          "src",
          stock.fromDataStream(stockEnvironment.fromCollection(rows, ExternalTypeInfo.of(type))));
      stock.executeSql(String.format(sink, starrocks("zoned_stock")));
      stock.executeSql("INSERT INTO dst SELECT * FROM src").await();

      var nativeEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
      nativeEnvironment.setParallelism(1);
      var accelerated = StreamTableEnvironment.create(nativeEnvironment);
      accelerated.getConfig().set("table.local-time-zone", "Asia/Kolkata");
      NativePlanner.install(accelerated);
      accelerated.createTemporaryView(
          "src",
          accelerated.fromDataStream(
              nativeEnvironment.fromCollection(rows, ExternalTypeInfo.of(type))));
      accelerated.executeSql(String.format(sink, starrocks("zoned_native")));
      String plan = NativePlanner.explain(accelerated, "INSERT INTO dst SELECT * FROM src");
      assertTrue(plan.contains("NativeStarRocksSink"), plan);
      accelerated.executeSql("INSERT INTO dst SELECT * FROM src").await();
    } finally {
      TimeZone.setDefault(previous);
    }

    awaitCount("zoned_stock", rows.size());
    awaitCount("zoned_native", rows.size());
    assertEquals(rows("zoned_stock"), rows("zoned_native"));
    assertTrue(
        rows("zoned_native").contains("2|1582-10-19|1582-10-09 23:00:00"),
        rows("zoned_native").toString());
  }

  @ParameterizedTest(name = "unified sink API {0}")
  @ValueSource(booleans = {false, true})
  void datesPlannedInAnotherZoneThanTheirTasksMatchStock(boolean unified) throws Exception {
    String stockTable = "split_zone_stock_" + unified;
    String nativeTable = "split_zone_native_" + unified;
    for (String table : List.of(stockTable, nativeTable))
      sql(
          "CREATE TABLE "
              + database
              + "."
              + table
              + " (id INT, dt DATE) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1"
              + " PROPERTIES('replication_num'='1')");
    DataType type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("dt", DataTypes.DATE()));
    List<Row> rows =
        List.of(
            Row.of(1, LocalDate.of(1970, 1, 1)),
            Row.of(2, LocalDate.of(2024, 3, 10)),
            Row.of(3, LocalDate.of(1582, 10, 15)),
            Row.of(4, LocalDate.of(1, 1, 1)),
            Row.of(5, null));
    String sink =
        "CREATE TABLE dst (id INT, dt DATE) WITH (%s, 'sink.use.new-sink-api'='" + unified + "')";
    String insert = "INSERT INTO dst SELECT * FROM src";
    TimeZone previous = TimeZone.getDefault();
    try {
      // The connector creates its date formatter while planning; the tasks run in another zone.
      TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
      var stockEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
      stockEnvironment.setParallelism(1);
      var stock = StreamTableEnvironment.create(stockEnvironment);
      stock.createTemporaryView(
          "src",
          stock.fromDataStream(stockEnvironment.fromCollection(rows, ExternalTypeInfo.of(type))));
      stock.executeSql(String.format(sink, starrocks(stockTable)));
      stock.createStatementSet().addInsertSql(insert).attachAsDataStream();

      var nativeEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
      nativeEnvironment.setParallelism(1);
      var accelerated = StreamTableEnvironment.create(nativeEnvironment);
      NativePlanner.install(accelerated);
      accelerated.createTemporaryView(
          "src",
          accelerated.fromDataStream(
              nativeEnvironment.fromCollection(rows, ExternalTypeInfo.of(type))));
      accelerated.executeSql(String.format(sink, starrocks(nativeTable)));
      accelerated.createStatementSet().addInsertSql(insert).attachAsDataStream();
      assertTrue(
          nativeEnvironment.getTransformations().stream()
              .anyMatch(transformation -> transformation.getName().contains("NativeSink")),
          nativeEnvironment.getTransformations().toString());

      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      stockEnvironment.execute();
      nativeEnvironment.execute();
    } finally {
      TimeZone.setDefault(previous);
    }

    awaitCount(stockTable, rows.size());
    awaitCount(nativeTable, rows.size());
    assertEquals(rows(stockTable), rows(nativeTable));
    assertTrue(rows(nativeTable).contains("1|1969-12-31"), rows(nativeTable).toString());
  }

  @Test
  void nativeOperatorsFeedTheWriterWithoutRowConversion() throws Exception {
    for (String table : List.of("pipeline_stock", "pipeline_native"))
      sql(
          "CREATE TABLE "
              + database
              + "."
              + table
              + " (id BIGINT, tag STRING, tripled BIGINT) DUPLICATE KEY(id)"
              + " DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES('replication_num'='1')");
    String source =
        "CREATE TABLE src (id BIGINT) WITH ('connector'='datagen', 'fields.id.kind'='sequence',"
            + " 'fields.id.start'='1', 'fields.id.end'='30000', 'rows-per-second'='1000000')";
    String sink = "CREATE TABLE dst (id BIGINT, tag STRING, tripled BIGINT) WITH (%s)";
    String insert =
        "INSERT INTO dst SELECT id, UPPER(CONCAT('t', CAST(id % 13 AS STRING))), id * 3 FROM src"
            + " WHERE MOD(id, 7) <> 0";

    var stockEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    stockEnvironment.setParallelism(2);
    var stock = StreamTableEnvironment.create(stockEnvironment);
    stock.executeSql(source);
    stock.executeSql(String.format(sink, starrocks("pipeline_stock")));
    stock.executeSql(insert).await();

    var nativeEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    nativeEnvironment.setParallelism(2);
    var accelerated = StreamTableEnvironment.create(nativeEnvironment);
    NativePlanner.install(accelerated);
    accelerated.executeSql(source);
    accelerated.executeSql(String.format(sink, starrocks("pipeline_native")));
    String plan = NativePlanner.explain(accelerated, insert);
    assertTrue(plan.contains("NativeCalc"), plan);
    assertTrue(plan.contains("NativeStarRocksSink"), plan);
    assertFalse(plan.contains("ArrowToRowData"), plan);
    accelerated.executeSql(insert).await();

    awaitCount("pipeline_stock", 30000 - 30000 / 7);
    awaitCount("pipeline_native", 30000 - 30000 / 7);
    assertEquals(rows("pipeline_stock"), rows("pipeline_native"));
  }

  @Test
  void twoNativeWritersInOneJobMatchStock() throws Exception {
    for (String table :
        List.of("set_csv_stock", "set_csv_native", "set_json_stock", "set_json_native"))
      sql(
          "CREATE TABLE "
              + database
              + "."
              + table
              + " (id BIGINT, v STRING) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1"
              + " PROPERTIES('replication_num'='1')");
    String source =
        "CREATE TABLE src (id BIGINT) WITH ('connector'='datagen', 'fields.id.kind'='sequence',"
            + " 'fields.id.start'='1', 'fields.id.end'='5000', 'rows-per-second'='1000000')";

    var stockEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    stockEnvironment.setParallelism(1);
    var stock = StreamTableEnvironment.create(stockEnvironment);
    stock.executeSql(source);
    stock.executeSql(
        "CREATE TABLE csv_dst (id BIGINT, v STRING) WITH (" + starrocks("set_csv_stock") + ")");
    stock.executeSql(
        "CREATE TABLE json_dst (id BIGINT, v STRING) WITH ("
            + starrocks("set_json_stock")
            + ", 'sink.properties.format'='json')");
    var stockSet = stock.createStatementSet();
    stockSet.addInsertSql("INSERT INTO csv_dst SELECT id, CAST(id * 2 AS STRING) FROM src");
    stockSet.addInsertSql(
        "INSERT INTO json_dst SELECT id, CAST(id * 3 AS STRING) FROM src WHERE id > 10");
    stockSet.execute().await();

    var nativeEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    nativeEnvironment.setParallelism(1);
    var accelerated = StreamTableEnvironment.create(nativeEnvironment);
    NativePlanner.install(accelerated);
    accelerated.executeSql(source);
    accelerated.executeSql(
        "CREATE TABLE csv_dst (id BIGINT, v STRING) WITH (" + starrocks("set_csv_native") + ")");
    accelerated.executeSql(
        "CREATE TABLE json_dst (id BIGINT, v STRING) WITH ("
            + starrocks("set_json_native")
            + ", 'sink.properties.format'='json')");
    var nativeSet = accelerated.createStatementSet();
    nativeSet.addInsertSql("INSERT INTO csv_dst SELECT id, CAST(id * 2 AS STRING) FROM src");
    nativeSet.addInsertSql(
        "INSERT INTO json_dst SELECT id, CAST(id * 3 AS STRING) FROM src WHERE id > 10");
    String plan = nativeSet.explain();
    assertTrue(
        plan.contains("table=[default_catalog.default_database.csv_dst], format=[csv]"), plan);
    assertTrue(
        plan.contains("table=[default_catalog.default_database.json_dst], format=[json]"), plan);
    nativeSet.execute().await();

    awaitCount("set_csv_stock", 5000);
    awaitCount("set_csv_native", 5000);
    awaitCount("set_json_stock", 4990);
    awaitCount("set_json_native", 4990);
    assertEquals(rows("set_csv_stock"), rows("set_csv_native"));
    assertEquals(rows("set_json_stock"), rows("set_json_native"));
  }

  @ParameterizedTest(name = "unified sink API {0}")
  @ValueSource(booleans = {false, true})
  void emptyInputsLoadNothingLikeStock(boolean unified) throws Exception {
    String stockTable = "empty_stock_" + unified;
    String nativeTable = "empty_native_" + unified;
    for (String table : List.of(stockTable, nativeTable))
      sql(
          "CREATE TABLE "
              + database
              + "."
              + table
              + " (id BIGINT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1"
              + " PROPERTIES('replication_num'='1')");
    String source =
        "CREATE TABLE src (id BIGINT) WITH ('connector'='datagen', 'fields.id.kind'='sequence',"
            + " 'fields.id.start'='1', 'fields.id.end'='100')";
    String sink =
        "CREATE TABLE dst (id BIGINT) WITH (%s, 'sink.use.new-sink-api'='" + unified + "')";
    String insert = "INSERT INTO dst SELECT id FROM src WHERE id < 0";

    var stockEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    stockEnvironment.setParallelism(2);
    var stock = StreamTableEnvironment.create(stockEnvironment);
    stock.executeSql(source);
    stock.executeSql(String.format(sink, starrocks(stockTable)));
    stock.executeSql(insert).await();

    var nativeEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    nativeEnvironment.setParallelism(2);
    var accelerated = StreamTableEnvironment.create(nativeEnvironment);
    NativePlanner.install(accelerated);
    accelerated.executeSql(source);
    accelerated.executeSql(String.format(sink, starrocks(nativeTable)));
    String plan = NativePlanner.explain(accelerated, insert);
    assertTrue(plan.contains("NativeStarRocksSink"), plan);
    accelerated.executeSql(insert).await();

    assertEquals(List.of(), rows(stockTable));
    assertEquals(List.of(), rows(nativeTable));
  }

  private static String starrocks(String table) {
    return "'connector'='starrocks', 'jdbc-url'='jdbc:mysql://"
        + HOST
        + ":"
        + MYSQL
        + "', 'load-url'='"
        + HOST
        + ":"
        + HTTP
        + "', 'database-name'='"
        + database
        + "', 'table-name'='"
        + table
        + "', 'username'='root', 'password'=''";
  }

  private static void sql(String statement) throws Exception {
    try (var jdbc = connection.createStatement()) {
      jdbc.execute(statement);
    }
  }

  /**
   * The table's rows as sorted, pipe-joined server spellings. Casting server side keeps the JDBC
   * driver's hybrid-calendar date conversions out of the comparison.
   */
  private static List<String> rows(String table) throws Exception {
    var casts = new StringJoiner(", ");
    try (var jdbc = connection.createStatement();
        var columns = jdbc.executeQuery("SELECT * FROM " + database + "." + table + " LIMIT 0")) {
      for (int column = 1; column <= columns.getMetaData().getColumnCount(); column++)
        casts.add("CAST(`" + columns.getMetaData().getColumnName(column) + "` AS STRING)");
    }
    var rows = new ArrayList<String>();
    try (var jdbc = connection.createStatement();
        var result = jdbc.executeQuery("SELECT " + casts + " FROM " + database + "." + table)) {
      int columns = result.getMetaData().getColumnCount();
      while (result.next()) {
        var row = new StringJoiner("|");
        for (int column = 1; column <= columns; column++) row.add(result.getString(column));
        rows.add(row.toString());
      }
    }
    Collections.sort(rows);
    return rows;
  }

  /** Waits until a committed load is visible, failing if the table settles at another size. */
  private static void awaitCount(String table, int expected) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
    int count;
    do {
      try (var jdbc = connection.createStatement();
          var result = jdbc.executeQuery("SELECT COUNT(*) FROM " + database + "." + table)) {
        assertTrue(result.next());
        count = result.getInt(1);
      }
      if (count == expected) return;
      Thread.sleep(50);
    } while (System.nanoTime() < deadline);
    assertEquals(expected, count, table + " must hold every committed row exactly once");
  }
}
