package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkRetractingWindowSqlHarnessTest {
  private static InMemoryReporter reporter;
  private static MiniClusterResource cluster;
  private static final Map<String, RecoveryProof> RECOVERY = new ConcurrentHashMap<>();

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void updatingWindowsWithNonzeroOffsetStayOnFlink(String phase) throws Exception {
    String sql =
        "SELECT k, window_end, SUM(v) FROM TABLE(HOP(TABLE ranked, DESCRIPTOR(rt),"
            + " INTERVAL '5' SECOND, INTERVAL '10' SECOND, INTERVAL '1' SECOND))"
            + " GROUP BY k, window_start, window_end";
    var host = collect(environment(phase), sql);
    var table = environment(phase);
    var scan = NativePlanner.install(table);
    assertEquals(host.rows(), collect(table, sql).rows());
    assertEquals(0, scan.substitutions());
    assertTrue(
        scan.fallbackReasons().stream().anyMatch(reason -> reason.contains("aligned event-time")),
        scan.fallbackReasons().toString());
  }

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TUMBLE,false,true", "TWO_PHASE,TUMBLE,false,true",
    "ONE_PHASE,HOP,false,true", "TWO_PHASE,HOP,false,true",
    "ONE_PHASE,CUMULATE,false,true", "TWO_PHASE,CUMULATE,false,true",
    "ONE_PHASE,TUMBLE,false,false", "TWO_PHASE,TUMBLE,false,false",
    "ONE_PHASE,HOP,false,false", "TWO_PHASE,HOP,false,false",
    "ONE_PHASE,CUMULATE,false,false", "TWO_PHASE,CUMULATE,false,false",
    "ONE_PHASE,TUMBLE,true,false", "TWO_PHASE,TUMBLE,true,false",
    "ONE_PHASE,HOP,true,false", "TWO_PHASE,HOP,true,false",
    "ONE_PHASE,CUMULATE,true,false", "TWO_PHASE,CUMULATE,true,false"
  })
  void lateRetractionsChangeOnlyUnfiredWindows(
      String phase, String shape, boolean groupingOnly, boolean filtered) throws Exception {
    String window =
        switch (shape) {
          case "TUMBLE" -> "TUMBLE(TABLE changes, DESCRIPTOR(rt), INTERVAL '5' SECOND)";
          case "HOP" ->
              "HOP(TABLE changes, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '10' SECOND)";
          default ->
              "CUMULATE(TABLE changes, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '15' SECOND)";
        };
    String sql =
        "SELECT k, window_end"
            + (groupingOnly
                ? ""
                : ", SUM(v), COUNT(*), AVG(v), AVG(CAST(v AS FLOAT)), AVG(CAST(v AS DOUBLE)),"
                    + " SUM(CAST(v AS FLOAT)), SUM(CAST(v AS DOUBLE)), SUM(CAST(v AS"
                    + " DECIMAL(38,2))), AVG(CAST(v AS DECIMAL(38,3)))")
            + " FROM TABLE("
            + window
            + ") GROUP BY k, window_start, window_end";
    if (filtered) {
      for (String aggregate :
          List.of(
              "SUM(v)",
              "COUNT(*)",
              "AVG(v)",
              "AVG(CAST(v AS FLOAT))",
              "AVG(CAST(v AS DOUBLE))",
              "SUM(CAST(v AS FLOAT))",
              "SUM(CAST(v AS DOUBLE))",
              "SUM(CAST(v AS DECIMAL(38,2)))",
              "AVG(CAST(v AS DECIMAL(38,3)))")) {
        sql = sql.replace(aggregate, aggregate + " FILTER (WHERE v > 10)");
      }
    }
    var expected = new ArrayList<Row>();
    expected.add(Row.of(1, LocalDateTime.ofEpochSecond(5, 0, ZoneOffset.UTC), 10L, 1L));
    if (!shape.equals("TUMBLE")) {
      expected.add(Row.of(1, LocalDateTime.ofEpochSecond(10, 0, ZoneOffset.UTC), 20L, 1L));
      expected.add(Row.of(2, LocalDateTime.ofEpochSecond(10, 0, ZoneOffset.UTC), null, 1L));
    }
    if (shape.equals("CUMULATE")) {
      expected.add(Row.of(2, LocalDateTime.ofEpochSecond(15, 0, ZoneOffset.UTC), null, 1L));
    }
    if (filtered) {
      expected.replaceAll(
          row ->
              row.getField(2) == null || (Long) row.getField(2) <= 10
                  ? Row.of(row.getField(0), row.getField(1), null, 0L)
                  : row);
    }
    if (groupingOnly) {
      expected.replaceAll(row -> Row.project(row, new int[] {0, 1}));
    } else {
      expected.replaceAll(
          row -> {
            Long value = (Long) row.getField(2);
            return Row.join(
                row,
                Row.of(
                    value,
                    value == null ? null : value.floatValue(),
                    value == null ? null : value.doubleValue(),
                    value == null ? null : value.floatValue(),
                    value == null ? null : value.doubleValue(),
                    value == null ? null : BigDecimal.valueOf(value).setScale(2),
                    value == null ? null : BigDecimal.valueOf(value).setScale(6)));
          });
    }
    expected.sort(Comparator.comparing(Row::toString));
    for (boolean nativeEnabled : new boolean[] {false, true}) {
      var env = new TestStreamEnvironment(cluster.getMiniCluster(), 1);
      var table = StreamTableEnvironment.create(env);
      table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
      table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
      var source =
          env.fromData(
                  Types.ROW_NAMED(
                      new String[] {"k", "millis", "v", "wm"},
                      Types.INT,
                      Types.LONG,
                      Types.LONG,
                      Types.LONG),
                  Row.ofKind(RowKind.INSERT, 1, 4000L, 10L, 5000L),
                  Row.ofKind(RowKind.UPDATE_BEFORE, 1, 4000L, 10L, 6000L),
                  Row.ofKind(RowKind.UPDATE_AFTER, 1, 4000L, 20L, 7000L),
                  Row.ofKind(RowKind.INSERT, 2, 4000L, null, 10000L),
                  Row.ofKind(RowKind.DELETE, 1, 4000L, 20L, 15000L),
                  Row.ofKind(RowKind.INSERT, 1, 4000L, 99L, 20000L))
              .assignTimestampsAndWatermarks(
                  WatermarkStrategy.<Row>forGenerator(context -> new ExplicitWatermarks())
                      .withTimestampAssigner((row, previous) -> (Long) row.getField(1)));
      table.createTemporaryView(
          "changes",
          table.fromChangelogStream(
              source,
              Schema.newBuilder()
                  .column("k", DataTypes.INT())
                  .column("millis", DataTypes.BIGINT())
                  .column("v", DataTypes.BIGINT())
                  .column("wm", DataTypes.BIGINT())
                  .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
                  .watermark("rt", "SOURCE_WATERMARK()")
                  .build()));
      var scan = nativeEnabled ? NativePlanner.install(table) : null;
      Result result = collect(table, sql);
      assertEquals(expected, result.rows());
      if (nativeEnabled) {
        assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
        assertWindowRows(result.job(), phase);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TINYINT", "TWO_PHASE,TINYINT",
    "ONE_PHASE,SMALLINT", "TWO_PHASE,SMALLINT",
    "ONE_PHASE,INT", "TWO_PHASE,INT",
    "ONE_PHASE,BIGINT", "TWO_PHASE,BIGINT"
  })
  void retractingBuffersPreserveIntegerWidthsAndNumericCountTypes(String phase, String type)
      throws Exception {
    String sql =
        "SELECT k, SUM(CAST(v AS "
            + type
            + ")), COUNT(CAST(v AS FLOAT)),"
            + " COUNT(CAST(v AS DOUBLE)), COUNT(CAST(v AS DECIMAL(12,2)))"
            + " FROM TABLE(TUMBLE(TABLE ranked, DESCRIPTOR(rt), INTERVAL '5' SECOND))"
            + " GROUP BY k, window_start, window_end";
    List<Row> host = collect(environment(phase), sql).rows();
    var table = environment(phase);
    var scan = NativePlanner.install(table);
    Result nativeResult = collect(table, sql);
    assertEquals(host, nativeResult.rows());
    assertEquals(3, host.size());
    assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
    assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
    assertWindowRows(nativeResult.job(), phase);
  }

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TINYINT", "TWO_PHASE,TINYINT",
    "ONE_PHASE,SMALLINT", "TWO_PHASE,SMALLINT",
    "ONE_PHASE,INT", "TWO_PHASE,INT",
    "ONE_PHASE,BIGINT", "TWO_PHASE,BIGINT",
    "ONE_PHASE,FLOAT", "TWO_PHASE,FLOAT",
    "ONE_PHASE,DOUBLE", "TWO_PHASE,DOUBLE",
    "ONE_PHASE,'DECIMAL(12,2)'", "TWO_PHASE,'DECIMAL(12,2)'"
  })
  void filteredNumericWindowsMatchFlinkForAppendAndUpdatingInput(String phase, String type)
      throws Exception {
    for (String input : List.of("src", "ranked")) {
      String sql =
          "SELECT k, SUM(CAST(v AS "
              + type
              + ")) FILTER (WHERE v > 10),"
              + " AVG(CAST(v AS "
              + type
              + ")) FILTER (WHERE v < 20),"
              + " COUNT(v) FILTER (WHERE v IS NULL), COUNT(*) FILTER (WHERE v >= 0)"
              + " FROM TABLE(HOP(TABLE "
              + input
              + ", DESCRIPTOR(rt),"
              + " INTERVAL '5' SECOND, INTERVAL '10' SECOND))"
              + " GROUP BY k, window_start, window_end";
      List<Row> host = collect(environment(phase), sql).rows();
      var table = environment(phase);
      var scan = NativePlanner.install(table);
      Result result = collect(table, sql);
      assertEquals(host, result.rows());
      assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
      assertWindowRows(result.job(), phase);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TINYINT", "TWO_PHASE,TINYINT",
    "ONE_PHASE,SMALLINT", "TWO_PHASE,SMALLINT",
    "ONE_PHASE,INT", "TWO_PHASE,INT",
    "ONE_PHASE,BIGINT", "TWO_PHASE,BIGINT",
    "ONE_PHASE,FLOAT", "TWO_PHASE,FLOAT",
    "ONE_PHASE,DOUBLE", "TWO_PHASE,DOUBLE",
    "ONE_PHASE,'DECIMAL(12,2)'", "TWO_PHASE,'DECIMAL(12,2)'"
  })
  void filteredExtremaKeepNullAndRejectedGroups(String phase, String type) throws Exception {
    for (String window :
        List.of(
            "TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '5' SECOND)",
            "HOP(TABLE src, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '10' SECOND)",
            "CUMULATE(TABLE src, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '15' SECOND)")) {
      String sql =
          "SELECT k, window_end, MIN(CAST(v AS "
              + type
              + ")) FILTER (WHERE v > 10), "
              + "MAX(CAST(v AS "
              + type
              + ")) FILTER (WHERE v < 20), "
              + "MIN(CAST(v AS "
              + type
              + ")) FILTER (WHERE v < 0) "
              + "FROM TABLE("
              + window
              + ") GROUP BY k, window_start, window_end";
      var host = collect(environment(phase), sql).rows();
      assertTrue(
          host.stream()
              .anyMatch(
                  row ->
                      (Integer) row.getField(0) == 3
                          && row.getField(2) == null
                          && row.getField(3) == null));
      assertTrue(host.stream().allMatch(row -> row.getField(4) == null));
      var table = environment(phase);
      var scan = NativePlanner.install(table);
      var result = collect(table, sql);
      assertEquals(host, result.rows());
      assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
      assertWindowRows(result.job(), phase);
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void unsupportedFilteredWindowsRemainOnFlink(String phase) throws Exception {
    for (String sql :
        List.of(
            "SELECT k, MIN(v) FILTER (WHERE v > 10) FROM TABLE(TUMBLE(TABLE ranked,"
                + " DESCRIPTOR(rt), INTERVAL '5' SECOND)) GROUP BY k, window_start, window_end",
            "SELECT k, MAX(v) FILTER (WHERE v > 10) FROM TABLE(TUMBLE(TABLE ranked,"
                + " DESCRIPTOR(rt), INTERVAL '5' SECOND)) GROUP BY k, window_start, window_end",
            "SELECT k, COUNT(*) FILTER (WHERE v > 10) FROM TABLE(SESSION(TABLE src PARTITION BY k,"
                + " DESCRIPTOR(rt), INTERVAL '5' SECOND)) GROUP BY k, window_start, window_end",
            "SELECT k, SUM(v) FILTER (WHERE v > 10) FROM src"
                + " GROUP BY k, TUMBLE(rt, INTERVAL '5' SECOND)")) {
      List<Row> host = collect(environment(phase), sql).rows();
      var table = environment(phase);
      var scan = NativePlanner.install(table);
      assertEquals(host, collect(table, sql).rows());
      assertEquals(0, scan.substitutions(), sql);
      assertTrue(!scan.fallbackReasons().isEmpty(), sql);
    }
  }

  @BeforeAll
  static void startCluster() throws Exception {
    reporter = InMemoryReporter.createWithRetainedMetrics();
    cluster =
        new MiniClusterResource(
            new MiniClusterResourceConfiguration.Builder()
                .setConfiguration(reporter.addToConfiguration(new Configuration()))
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(2)
                .build());
    cluster.before();
  }

  @AfterAll
  static void stopCluster() {
    if (cluster != null) cluster.after();
  }

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TUMBLE,false,false", "TWO_PHASE,TUMBLE,false,false",
    "ONE_PHASE,HOP,false,false", "TWO_PHASE,HOP,false,false",
    "ONE_PHASE,CUMULATE,false,false", "TWO_PHASE,CUMULATE,false,false",
    "ONE_PHASE,TUMBLE,true,false", "TWO_PHASE,TUMBLE,true,false",
    "ONE_PHASE,HOP,true,false", "TWO_PHASE,HOP,true,false",
    "ONE_PHASE,CUMULATE,true,false", "TWO_PHASE,CUMULATE,true,false",
    "ONE_PHASE,TUMBLE,false,true", "TWO_PHASE,TUMBLE,false,true",
    "ONE_PHASE,HOP,false,true", "TWO_PHASE,HOP,false,true",
    "ONE_PHASE,CUMULATE,false,true", "TWO_PHASE,CUMULATE,false,true",
    "ONE_PHASE,TUMBLE,true,true", "TWO_PHASE,TUMBLE,true,true",
    "ONE_PHASE,HOP,true,true", "TWO_PHASE,HOP,true,true",
    "ONE_PHASE,CUMULATE,true,true", "TWO_PHASE,CUMULATE,true,true"
  })
  void signedChangesAfterCheckpointPreserveEmptyNullAndNegativeGroups(
      String phase, String shape, boolean rocks, boolean groupingOnly) throws Exception {
    String window =
        switch (shape) {
          case "TUMBLE" -> "TUMBLE(TABLE changes, DESCRIPTOR(rt), INTERVAL '5' SECOND)";
          case "HOP" ->
              "HOP(TABLE changes, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '10' SECOND)";
          default ->
              "CUMULATE(TABLE changes, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '15' SECOND)";
        };
    String sql =
        "SELECT k, window_end"
            + (groupingOnly
                ? ""
                : ", SUM(v), COUNT(v), COUNT(*), AVG(v), AVG(CAST(v AS INT)), AVG(CAST(v AS"
                    + " SMALLINT)), AVG(CAST(v AS TINYINT)), AVG(CAST(v AS FLOAT)), AVG(CAST(v AS"
                    + " DOUBLE)), SUM(CAST(v AS FLOAT)), SUM(CAST(v AS DOUBLE)), SUM(CAST(v AS"
                    + " DECIMAL(38,2))), AVG(CAST(v AS DECIMAL(38,3)))")
            + " FROM TABLE("
            + window
            + ") GROUP BY k, window_start, window_end";
    List<Row> expected = new ArrayList<>();
    int end = shape.equals("TUMBLE") ? 5 : shape.equals("HOP") ? 10 : 15;
    for (int boundary = 5; boundary <= end; boundary += 5) {
      LocalDateTime timestamp = LocalDateTime.ofEpochSecond(boundary, 0, ZoneOffset.UTC);
      expected.add(Row.of(1, timestamp, 20L, 1L, 1L));
      expected.add(Row.of(2, timestamp, null, 0L, 1L));
      expected.add(Row.of(4, timestamp, 5L, 1L, 1L));
      expected.add(Row.of(5, timestamp, -3L, -1L, -1L));
      expected.add(Row.of(6, timestamp, Long.MIN_VALUE, -1L, -1L));
      expected.add(Row.of(7, timestamp, -2L, 2L, 2L));
      expected.add(Row.of(8, timestamp, 21L, 2L, 2L));
    }
    if (groupingOnly) {
      expected.replaceAll(row -> Row.project(row, new int[] {0, 1}));
    } else {
      expected.replaceAll(
          row -> {
            Long sum = (Long) row.getField(2);
            Long avg = sum == null ? null : sum / (Long) row.getField(3);
            Double floating =
                switch ((Integer) row.getField(0)) {
                  case 7 -> (double) Long.MAX_VALUE;
                  case 8 -> 10.5;
                  default -> avg == null ? null : avg.doubleValue();
                };
            BigDecimal decimalSum =
                switch ((Integer) row.getField(0)) {
                  case 6 -> new BigDecimal("9223372036854775808.00");
                  case 7 -> new BigDecimal("18446744073709551614.00");
                  default -> sum == null ? null : BigDecimal.valueOf(sum).setScale(2);
                };
            return Row.join(
                row,
                Row.of(
                    avg,
                    avg == null ? null : avg.intValue(),
                    avg == null ? null : avg.shortValue(),
                    avg == null ? null : avg.byteValue(),
                    floating == null ? null : floating.floatValue(),
                    floating,
                    floating == null ? null : (float) (floating * (Long) row.getField(3)),
                    floating == null ? null : floating * (Long) row.getField(3),
                    decimalSum,
                    decimalSum == null
                        ? null
                        : decimalSum.divide(
                            BigDecimal.valueOf((Long) row.getField(3)), 6, RoundingMode.HALF_UP)));
          });
    }
    expected.sort(Comparator.comparing(Row::toString));
    assertRecoveredWindowRows(phase, rocks, sql, expected);
  }

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TUMBLE,false,false", "TWO_PHASE,TUMBLE,false,false",
    "ONE_PHASE,HOP,false,false", "TWO_PHASE,HOP,false,false",
    "ONE_PHASE,CUMULATE,false,false", "TWO_PHASE,CUMULATE,false,false",
    "ONE_PHASE,TUMBLE,true,false", "TWO_PHASE,TUMBLE,true,false",
    "ONE_PHASE,HOP,true,false", "TWO_PHASE,HOP,true,false",
    "ONE_PHASE,CUMULATE,true,false", "TWO_PHASE,CUMULATE,true,false",
    "ONE_PHASE,HOP,false,true", "TWO_PHASE,HOP,false,true",
    "ONE_PHASE,HOP,true,true", "TWO_PHASE,HOP,true,true"
  })
  void filteredChangesPreserveIndependentAccumulatorsAndWindowLivenessAfterRecovery(
      String phase, String shape, boolean rocks, boolean visibleLiveCount) throws Exception {
    String window =
        switch (shape) {
          case "TUMBLE" -> "TUMBLE(TABLE changes, DESCRIPTOR(rt), INTERVAL '5' SECOND)";
          case "HOP" ->
              "HOP(TABLE changes, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '10' SECOND)";
          default ->
              "CUMULATE(TABLE changes, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '15' SECOND)";
        };
    String sql =
        "SELECT k, window_end, COUNT(*) FILTER (WHERE v > 10),"
            + " COUNT(v) FILTER (WHERE v >= 0), SUM(v) FILTER (WHERE v < 10),"
            + " AVG(CAST(v AS DECIMAL(38,3))) FILTER (WHERE v > 10),"
            + " COUNT(*) FILTER (WHERE v IS NULL)"
            + (visibleLiveCount ? ", COUNT(*)" : "")
            + " FROM TABLE("
            + window
            + ") GROUP BY k, window_start, window_end";
    List<Row> expected = new ArrayList<>();
    int end = shape.equals("TUMBLE") ? 5 : shape.equals("HOP") ? 10 : 15;
    for (int boundary = 5; boundary <= end; boundary += 5) {
      LocalDateTime timestamp = LocalDateTime.ofEpochSecond(boundary, 0, ZoneOffset.UTC);
      List<Row> groups =
          List.of(
              Row.of(1, timestamp, 1L, 1L, null, new BigDecimal("20.000000"), 0L, 1L),
              Row.of(2, timestamp, 0L, 0L, null, null, 1L, 1L),
              Row.of(4, timestamp, 0L, 1L, 5L, null, 0L, 1L),
              Row.of(5, timestamp, 0L, -1L, -3L, null, 0L, -1L),
              Row.of(6, timestamp, 0L, 0L, Long.MIN_VALUE, null, 0L, -1L),
              Row.of(
                  7, timestamp, 2L, 2L, null, new BigDecimal("9223372036854775807.000000"), 0L, 2L),
              Row.of(8, timestamp, 1L, 2L, null, new BigDecimal("11.000000"), 0L, 2L));
      for (Row row : groups) {
        expected.add(visibleLiveCount ? row : Row.project(row, new int[] {0, 1, 2, 3, 4, 5, 6}));
      }
    }
    expected.sort(Comparator.comparing(Row::toString));
    assertRecoveredWindowRows(phase, rocks, sql, expected);
  }

  private void assertRecoveredWindowRows(
      String phase, boolean rocks, String sql, List<Row> expected) throws Exception {
    for (boolean nativeEnabled : new boolean[] {false, true}) {
      String id = UUID.randomUUID().toString();
      RecoveryProof proof = new RecoveryProof();
      RECOVERY.put(id, proof);
      try {
        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        config.set(
            RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(10));
        if (rocks)
          config.setString(
              "state.backend.type", "tech.streamfusion.state.RocksDBNativeStateBackendFactory");
        var env = new TestStreamEnvironment(cluster.getMiniCluster(), 1);
        env.configure(config, getClass().getClassLoader());
        env.enableCheckpointing(50);
        var table = StreamTableEnvironment.create(env);
        table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
        table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
        var source =
            env.addSource(new RecoveringChanges(id))
                .returns(
                    Types.ROW_NAMED(
                        new String[] {"k", "millis", "v"}, Types.INT, Types.LONG, Types.LONG))
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                        .withTimestampAssigner((row, previous) -> (Long) row.getField(1)));
        table.createTemporaryView(
            "changes",
            table.fromChangelogStream(
                source,
                Schema.newBuilder()
                    .column("k", DataTypes.INT())
                    .column("millis", DataTypes.BIGINT())
                    .column("v", DataTypes.BIGINT())
                    .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
                    .watermark("rt", "SOURCE_WATERMARK()")
                    .build()));
        var scan = nativeEnabled ? NativePlanner.install(table) : null;
        Result result = collect(table, sql);
        assertEquals(expected, result.rows());
        assertTrue(proof.failed.get(), "failure must follow a completed checkpoint");
        assertEquals(3, proof.restored.get(), "source prefix must restore from checkpoint");
        if (nativeEnabled) {
          assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
          assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
          assertWindowRows(result.job(), phase);
        }
      } finally {
        RECOVERY.remove(id);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TUMBLE,false", "TWO_PHASE,TUMBLE,false",
    "ONE_PHASE,HOP,false", "TWO_PHASE,HOP,false",
    "ONE_PHASE,CUMULATE,false", "TWO_PHASE,CUMULATE,false",
    "ONE_PHASE,TUMBLE,true", "TWO_PHASE,TUMBLE,true",
    "ONE_PHASE,HOP,true", "TWO_PHASE,HOP,true",
    "ONE_PHASE,CUMULATE,true", "TWO_PHASE,CUMULATE,true"
  })
  void updatingTopOneRequiresWindowRetractionAndGroupLiveness(
      String phase, String shape, boolean distinct) throws Exception {
    String window =
        switch (shape) {
          case "TUMBLE" -> "TUMBLE(TABLE ranked, DESCRIPTOR(rt), INTERVAL '5' SECOND)";
          case "HOP" ->
              "HOP(TABLE ranked, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '10' SECOND)";
          default ->
              "CUMULATE(TABLE ranked, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '15' SECOND)";
        };
    String sql =
        "SELECT k, window_start, window_end, "
            + (distinct ? "COUNT(DISTINCT v)" : "COUNT(v), SUM(v)")
            + " FROM TABLE("
            + window
            + ") GROUP BY k, window_start, window_end";
    List<Row> host = collect(environment(phase), sql).rows();
    assertEquals(expected(shape, distinct), host, "unexpected released Flink result");

    TableEnvironment accelerated = environment(phase);
    var scan = NativePlanner.install(accelerated);
    var result = collect(accelerated, sql);
    assertEquals(host, result.rows(), "raw window changelog differs from Flink");
    if (!distinct) {
      assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
      assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
      assertNativeRows(result.job(), "NativeColumnarTopNExecNode");
      assertWindowRows(result.job(), phase);
      return;
    }
    assertEquals(0, scan.substitutions(), "retracting distinct window must stay on Flink");
    assertTrue(
        scan.fallbackReasons().stream()
            .anyMatch(
                reason ->
                    reason.contains(
                        "retracting input supports only aligned event-time TUMBLE/HOP/CUMULATE")),
        scan.fallbackReasons().toString());
  }

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TUMBLE,KEYED", "TWO_PHASE,TUMBLE,KEYED",
    "ONE_PHASE,HOP,KEYED", "TWO_PHASE,HOP,KEYED",
    "ONE_PHASE,CUMULATE,KEYED", "TWO_PHASE,CUMULATE,KEYED",
    "ONE_PHASE,TUMBLE,NULLABLE", "TWO_PHASE,TUMBLE,NULLABLE",
    "ONE_PHASE,HOP,NULLABLE", "TWO_PHASE,HOP,NULLABLE",
    "ONE_PHASE,CUMULATE,NULLABLE", "TWO_PHASE,CUMULATE,NULLABLE",
    "ONE_PHASE,TUMBLE,NONE", "TWO_PHASE,TUMBLE,NONE",
    "ONE_PHASE,HOP,NONE", "TWO_PHASE,HOP,NONE",
    "ONE_PHASE,CUMULATE,NONE", "TWO_PHASE,CUMULATE,NONE"
  })
  void groupingOnlyWindowsFollowTopOneMovingBetweenWindows(
      String phase, String shape, String keyMode) throws Exception {
    String window =
        switch (shape) {
          case "TUMBLE" -> "TUMBLE(TABLE ranked, DESCRIPTOR(rt), INTERVAL '5' SECOND)";
          case "HOP" ->
              "HOP(TABLE ranked, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '10' SECOND)";
          default ->
              "CUMULATE(TABLE ranked, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '15' SECOND)";
        };
    String key =
        switch (keyMode) {
          case "NULLABLE" -> "CASE WHEN k = 1 THEN CAST(NULL AS INT) ELSE k END";
          case "NONE" -> "";
          default -> "k";
        };
    String prefix = key.isEmpty() ? "" : key + ", ";
    String sql =
        "SELECT "
            + prefix
            + "window_start, window_end FROM TABLE("
            + window
            + ") GROUP BY "
            + prefix
            + "window_start, window_end";
    List<Row> expected =
        new ArrayList<>(
            expected(shape, false).stream()
                .map(
                    row -> {
                      Row projected =
                          Row.project(
                              row, keyMode.equals("NONE") ? new int[] {1, 2} : new int[] {0, 1, 2});
                      if (keyMode.equals("NULLABLE") && projected.getField(0).equals(1)) {
                        projected.setField(0, null);
                      }
                      return projected;
                    })
                .distinct()
                .toList());
    expected.sort(Comparator.comparing(Row::toString));
    assertEquals(expected, collect(environment(phase), sql).rows());
    var table = environment(phase);
    var scan = NativePlanner.install(table);
    var result = collect(table, sql);
    assertEquals(expected, result.rows());
    assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
    assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
    assertNativeRows(result.job(), "NativeColumnarTopNExecNode");
    assertWindowRows(result.job(), phase);
  }

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TUMBLE,TINYINT", "TWO_PHASE,TUMBLE,TINYINT",
    "ONE_PHASE,HOP,TINYINT", "TWO_PHASE,HOP,TINYINT",
    "ONE_PHASE,CUMULATE,TINYINT", "TWO_PHASE,CUMULATE,TINYINT",
    "ONE_PHASE,TUMBLE,SMALLINT", "TWO_PHASE,TUMBLE,SMALLINT",
    "ONE_PHASE,HOP,SMALLINT", "TWO_PHASE,HOP,SMALLINT",
    "ONE_PHASE,CUMULATE,SMALLINT", "TWO_PHASE,CUMULATE,SMALLINT",
    "ONE_PHASE,TUMBLE,INT", "TWO_PHASE,TUMBLE,INT",
    "ONE_PHASE,HOP,INT", "TWO_PHASE,HOP,INT",
    "ONE_PHASE,CUMULATE,INT", "TWO_PHASE,CUMULATE,INT",
    "ONE_PHASE,TUMBLE,BIGINT", "TWO_PHASE,TUMBLE,BIGINT",
    "ONE_PHASE,HOP,BIGINT", "TWO_PHASE,HOP,BIGINT",
    "ONE_PHASE,CUMULATE,BIGINT", "TWO_PHASE,CUMULATE,BIGINT",
    "ONE_PHASE,TUMBLE,FLOAT", "TWO_PHASE,TUMBLE,FLOAT",
    "ONE_PHASE,HOP,FLOAT", "TWO_PHASE,HOP,FLOAT",
    "ONE_PHASE,CUMULATE,FLOAT", "TWO_PHASE,CUMULATE,FLOAT",
    "ONE_PHASE,TUMBLE,DOUBLE", "TWO_PHASE,TUMBLE,DOUBLE",
    "ONE_PHASE,HOP,DOUBLE", "TWO_PHASE,HOP,DOUBLE",
    "ONE_PHASE,CUMULATE,DOUBLE", "TWO_PHASE,CUMULATE,DOUBLE"
  })
  void numericAggregatesRetractReplacedTopRows(String phase, String shape, String type)
      throws Exception {
    String window =
        switch (shape) {
          case "TUMBLE" -> "TUMBLE(TABLE ranked, DESCRIPTOR(rt), INTERVAL '5' SECOND)";
          case "HOP" ->
              "HOP(TABLE ranked, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '10' SECOND)";
          default ->
              "CUMULATE(TABLE ranked, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '15' SECOND)";
        };
    boolean floating = type.equals("FLOAT") || type.equals("DOUBLE");
    String sql =
        "SELECT k, window_start, window_end, COUNT(v), AVG(CAST(v AS "
            + type
            + "))"
            + (floating ? ", SUM(CAST(v AS " + type + "))" : "")
            + " FROM TABLE("
            + window
            + ") GROUP BY k, window_start, window_end";
    List<Row> expected = expected(shape, false);
    for (Row row : expected) {
      Long value = (Long) row.getField(4);
      if (value != null) {
        row.setField(
            4,
            switch (type) {
              case "TINYINT" -> value.byteValue();
              case "SMALLINT" -> value.shortValue();
              case "INT" -> value.intValue();
              case "FLOAT" -> value.floatValue();
              case "DOUBLE" -> value.doubleValue();
              default -> value;
            });
      }
    }
    if (floating) expected.replaceAll(row -> Row.join(row, Row.of(row.getField(4))));
    assertEquals(expected, collect(environment(phase), sql).rows());
    var table = environment(phase);
    var scan = NativePlanner.install(table);
    var result = collect(table, sql);
    assertEquals(expected, result.rows());
    assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
    assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
    assertNativeRows(result.job(), "NativeColumnarTopNExecNode");
    assertWindowRows(result.job(), phase);
  }

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TUMBLE",
    "TWO_PHASE,TUMBLE",
    "ONE_PHASE,HOP",
    "TWO_PHASE,HOP",
    "ONE_PHASE,CUMULATE",
    "TWO_PHASE,CUMULATE"
  })
  void floatingAggregatesPreserveNonfiniteValuesAndSignedZero(String phase, String shape)
      throws Exception {
    String window =
        switch (shape) {
          case "TUMBLE" -> "TUMBLE(TABLE changes, DESCRIPTOR(rt), INTERVAL '5' SECOND)";
          case "HOP" ->
              "HOP(TABLE changes, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '10' SECOND)";
          default ->
              "CUMULATE(TABLE changes, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '15' SECOND)";
        };
    String sql =
        "SELECT k, window_end, AVG(f), AVG(v), COUNT(v), COUNT(*), SUM(f), SUM(v) FROM TABLE("
            + window
            + ") GROUP BY k, window_start, window_end";
    List<Row> host = null;
    for (boolean nativeEnabled : new boolean[] {false, true}) {
      var env = new TestStreamEnvironment(cluster.getMiniCluster(), 1);
      var table = StreamTableEnvironment.create(env);
      table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
      table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
      var source =
          env.fromData(
                  Types.ROW_NAMED(
                      new String[] {"k", "millis", "f", "v"},
                      Types.INT,
                      Types.LONG,
                      Types.FLOAT,
                      Types.DOUBLE),
                  floatingRow(RowKind.INSERT, 1, 1e16),
                  floatingRow(RowKind.INSERT, 1, 1.0),
                  floatingRow(RowKind.DELETE, 1, 1e16),
                  floatingRow(RowKind.DELETE, 2, 0.0),
                  floatingRow(RowKind.INSERT, 3, Double.POSITIVE_INFINITY),
                  floatingRow(RowKind.DELETE, 3, Double.POSITIVE_INFINITY),
                  floatingRow(RowKind.INSERT, 3, 3.0),
                  floatingRow(RowKind.INSERT, 4, Double.NEGATIVE_INFINITY),
                  floatingRow(RowKind.DELETE, 4, Double.NEGATIVE_INFINITY),
                  floatingRow(RowKind.INSERT, 4, null),
                  floatingRow(RowKind.INSERT, 5, null),
                  floatingRow(RowKind.DELETE, 5, null),
                  floatingRow(RowKind.INSERT, 6, Double.NaN),
                  floatingRow(RowKind.INSERT, 6, 5.0),
                  floatingRow(RowKind.DELETE, 6, Double.NaN),
                  floatingRow(RowKind.INSERT, 7, -0.0),
                  floatingRow(RowKind.INSERT, 8, 16777216.0),
                  floatingRow(RowKind.INSERT, 8, 1.0),
                  floatingRow(RowKind.DELETE, 8, 16777216.0))
              .assignTimestampsAndWatermarks(
                  WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                      .withTimestampAssigner((row, previous) -> (Long) row.getField(1)));
      table.createTemporaryView(
          "changes",
          table.fromChangelogStream(
              source,
              Schema.newBuilder()
                  .column("k", DataTypes.INT())
                  .column("millis", DataTypes.BIGINT())
                  .column("f", DataTypes.FLOAT())
                  .column("v", DataTypes.DOUBLE())
                  .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
                  .watermark("rt", "SOURCE_WATERMARK()")
                  .build()));
      var scan = nativeEnabled ? NativePlanner.install(table) : null;
      Result result = collect(table, sql);
      List<Row> bits =
          result.rows().stream()
              .map(
                  row -> {
                    Row copy = Row.copy(row);
                    for (int column : new int[] {2, 6}) {
                      if (row.getField(column) != null)
                        copy.setField(
                            column, Float.floatToRawIntBits((Float) row.getField(column)));
                    }
                    for (int column : new int[] {3, 7}) {
                      if (row.getField(column) != null)
                        copy.setField(
                            column, Double.doubleToRawLongBits((Double) row.getField(column)));
                    }
                    return copy;
                  })
              .toList();
      if (!nativeEnabled) {
        host = bits;
        assertEquals(7 * (shape.equals("TUMBLE") ? 1 : shape.equals("HOP") ? 2 : 3), host.size());
      } else {
        assertEquals(host, bits);
        assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
        assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
        assertWindowRows(result.job(), phase);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TUMBLE", "TWO_PHASE,TUMBLE",
    "ONE_PHASE,HOP", "TWO_PHASE,HOP",
    "ONE_PHASE,CUMULATE", "TWO_PHASE,CUMULATE"
  })
  void decimalSumAndAverageRetractTopRowsAndPreserveScale(String phase, String shape)
      throws Exception {
    String window =
        switch (shape) {
          case "TUMBLE" -> "TUMBLE(TABLE ranked, DESCRIPTOR(rt), INTERVAL '5' SECOND)";
          case "HOP" ->
              "HOP(TABLE ranked, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '10' SECOND)";
          default ->
              "CUMULATE(TABLE ranked, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '15' SECOND)";
        };
    String sql =
        "SELECT k, window_start, window_end, SUM(CAST(v AS DECIMAL(12,2))),"
            + " SUM(CAST(v AS DECIMAL(38,18))), AVG(CAST(v AS DECIMAL(12,3))),"
            + " AVG(CAST(v AS DECIMAL(38,17))) FROM TABLE("
            + window
            + ") GROUP BY k, window_start, window_end";
    var host = collect(environment(phase), sql).rows();
    assertEquals(shape.equals("HOP") ? 6 : shape.equals("TUMBLE") ? 3 : 8, host.size());
    var table = environment(phase);
    var scan = NativePlanner.install(table);
    var actual = collect(table, sql);
    assertEquals(host, actual.rows());
    assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
    assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
    assertNativeRows(actual.job(), "NativeColumnarTopNExecNode");
    assertWindowRows(actual.job(), phase);
  }

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TUMBLE,SUM", "TWO_PHASE,TUMBLE,SUM",
    "ONE_PHASE,HOP,SUM", "TWO_PHASE,HOP,SUM",
    "ONE_PHASE,CUMULATE,SUM", "TWO_PHASE,CUMULATE,SUM",
    "ONE_PHASE,TUMBLE,AVG", "TWO_PHASE,TUMBLE,AVG",
    "ONE_PHASE,HOP,AVG", "TWO_PHASE,HOP,AVG",
    "ONE_PHASE,CUMULATE,AVG", "TWO_PHASE,CUMULATE,AVG"
  })
  void decimalSumAndAveragePreserveDifferentOverflowRules(
      String phase, String shape, String function) throws Exception {
    String window =
        switch (shape) {
          case "TUMBLE" -> "TUMBLE(TABLE changes, DESCRIPTOR(rt), INTERVAL '5' SECOND)";
          case "HOP" ->
              "HOP(TABLE changes, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '10' SECOND)";
          default ->
              "CUMULATE(TABLE changes, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '15' SECOND)";
        };
    String sql =
        "SELECT k, window_end, "
            + function
            + "(v), COUNT(v), COUNT(*) FROM TABLE("
            + window
            + ") GROUP BY k, window_start, window_end";
    String max = "9".repeat(36) + ".99";
    List<Row> host = null;
    for (boolean nativeEnabled : new boolean[] {false, true}) {
      var env = new TestStreamEnvironment(cluster.getMiniCluster(), 1);
      var table = StreamTableEnvironment.create(env);
      table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
      table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
      var source =
          env.fromData(
                  Types.ROW_NAMED(
                      new String[] {"k", "millis", "v"}, Types.INT, Types.LONG, Types.BIG_DEC),
                  decimalRow(RowKind.INSERT, 1, max),
                  decimalRow(RowKind.INSERT, 1, "0.01"),
                  decimalRow(RowKind.INSERT, 2, max),
                  decimalRow(RowKind.INSERT, 2, "0.01"),
                  decimalRow(RowKind.INSERT, 2, "3.25"),
                  decimalRow(RowKind.DELETE, 3, max),
                  decimalRow(RowKind.DELETE, 3, "0.01"),
                  decimalRow(RowKind.DELETE, 4, max),
                  decimalRow(RowKind.DELETE, 4, "0.01"),
                  decimalRow(RowKind.DELETE, 4, "0.25"),
                  decimalRow(RowKind.INSERT, 5, "1.50"),
                  decimalRow(RowKind.DELETE, 5, "1.00"),
                  decimalRow(RowKind.INSERT, 5, null),
                  decimalRow(RowKind.DELETE, 6, "3.00"),
                  decimalRow(RowKind.UPDATE_BEFORE, 7, "1.00"),
                  decimalRow(RowKind.UPDATE_AFTER, 7, "2.00"),
                  decimalRow(RowKind.INSERT, 7, null),
                  decimalRow(RowKind.INSERT, 8, max),
                  decimalRow(RowKind.INSERT, 8, max),
                  decimalRow(RowKind.DELETE, 8, "2.50"),
                  decimalRow(RowKind.INSERT, 9, null),
                  decimalRow(RowKind.INSERT, 10, max),
                  decimalRow(RowKind.INSERT, 10, "0.01"),
                  decimalRow(RowKind.DELETE, 10, "0.00"),
                  decimalRow(RowKind.DELETE, 10, "0.00"),
                  decimalRow(RowKind.INSERT, 10, "0.25"),
                  decimalRow(RowKind.INSERT, 11, "1.50"),
                  decimalRow(RowKind.DELETE, 11, "1.00"),
                  decimalRow(RowKind.INSERT, 11, "0.25"))
              .assignTimestampsAndWatermarks(
                  WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                      .withTimestampAssigner((row, previous) -> (Long) row.getField(1)));
      table.createTemporaryView(
          "changes",
          table.fromChangelogStream(
              source,
              Schema.newBuilder()
                  .column("k", DataTypes.INT())
                  .column("millis", DataTypes.BIGINT())
                  .column("v", DataTypes.DECIMAL(38, 2))
                  .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
                  .watermark("rt", "SOURCE_WATERMARK()")
                  .build()));
      var scan = nativeEnabled ? NativePlanner.install(table) : null;
      var result = collect(table, sql);
      if (!nativeEnabled) {
        host = result.rows();
        assertEquals(11 * (shape.equals("TUMBLE") ? 1 : shape.equals("HOP") ? 2 : 3), host.size());
        for (Row row : host) {
          BigDecimal expected =
              switch ((Integer) row.getField(0)) {
                case 2 -> new BigDecimal("3.25");
                case 4 -> new BigDecimal("-0.25");
                case 6 -> new BigDecimal("-3.00");
                case 8 -> new BigDecimal("-2.50");
                case 10 -> new BigDecimal("0.25");
                case 11 -> new BigDecimal("0.75");
                default -> null;
              };
          if (function.equals("AVG")) {
            expected =
                switch ((Integer) row.getField(0)) {
                  case 6 -> new BigDecimal("3.000000");
                  case 11 -> new BigDecimal("0.750000");
                  default -> null;
                };
          }
          assertEquals(expected, row.getField(2), row.toString());
        }
      } else {
        assertEquals(host, result.rows());
        assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
        assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
        assertWindowRows(result.job(), phase);
      }
    }
  }

  private static Row decimalRow(RowKind kind, int key, String value) {
    return Row.ofKind(kind, key, 1000L, value == null ? null : new BigDecimal(value));
  }

  private static Row floatingRow(RowKind kind, int key, Double value) {
    return Row.ofKind(kind, key, 1000L, value == null ? null : value.floatValue(), value);
  }

  @ParameterizedTest
  @CsvSource({
    "ONE_PHASE,TUMBLE,BIGINT", "TWO_PHASE,TUMBLE,BIGINT",
    "ONE_PHASE,HOP,BIGINT", "TWO_PHASE,HOP,BIGINT",
    "ONE_PHASE,CUMULATE,BIGINT", "TWO_PHASE,CUMULATE,BIGINT",
    "ONE_PHASE,TUMBLE,'DECIMAL(12,2)'", "TWO_PHASE,TUMBLE,'DECIMAL(12,2)'",
    "ONE_PHASE,HOP,'DECIMAL(12,2)'", "TWO_PHASE,HOP,'DECIMAL(12,2)'",
    "ONE_PHASE,CUMULATE,'DECIMAL(12,2)'", "TWO_PHASE,CUMULATE,'DECIMAL(12,2)'"
  })
  void reducedSumAndAverageKeepExplicitFallback(String phase, String shape, String type)
      throws Exception {
    String window =
        switch (shape) {
          case "TUMBLE" -> "TUMBLE(TABLE ranked, DESCRIPTOR(rt), INTERVAL '5' SECOND)";
          case "HOP" ->
              "HOP(TABLE ranked, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '10' SECOND)";
          default ->
              "CUMULATE(TABLE ranked, DESCRIPTOR(rt), INTERVAL '5' SECOND, INTERVAL '15' SECOND)";
        };
    String value = type.equals("BIGINT") ? "v" : "CAST(v AS " + type + ")";
    String sql =
        "SELECT k, window_start, window_end, COUNT("
            + value
            + "), AVG("
            + value
            + "), SUM("
            + value
            + ") FROM TABLE("
            + window
            + ") GROUP BY k, window_start, window_end";
    NativeParity.assertFallbackReasonContains(
        () -> environment(phase),
        sql,
        "window aggregate: retracting input supports only aligned event-time");
  }

  private static List<Row> expected(String shape, boolean distinct) {
    var rows = new ArrayList<Row>();
    if (shape.equals("HOP")) {
      rows.add(result(1, -5, 5, 20L, distinct));
      rows.add(result(3, -5, 5, null, distinct));
      rows.add(result(1, 0, 10, 20L, distinct));
      rows.add(result(2, 0, 10, 8L, distinct));
      rows.add(result(3, 0, 10, null, distinct));
      rows.add(result(2, 5, 15, 8L, distinct));
    } else {
      rows.add(result(1, 0, 5, 20L, distinct));
      rows.add(result(3, 0, 5, null, distinct));
      if (shape.equals("TUMBLE")) {
        rows.add(result(2, 5, 10, 8L, distinct));
      } else {
        for (int end : new int[] {10, 15}) {
          rows.add(result(1, 0, end, 20L, distinct));
          rows.add(result(2, 0, end, 8L, distinct));
          rows.add(result(3, 0, end, null, distinct));
        }
      }
    }
    rows.sort(Comparator.comparing(Row::toString));
    return rows;
  }

  private static Row result(int key, int start, int end, Long value, boolean distinct) {
    LocalDateTime left = LocalDateTime.ofEpochSecond(start, 0, ZoneOffset.UTC);
    LocalDateTime right = LocalDateTime.ofEpochSecond(end, 0, ZoneOffset.UTC);
    long count = value == null ? 0L : 1L;
    return distinct ? Row.of(key, left, right, count) : Row.of(key, left, right, count, value);
  }

  private record Result(List<Row> rows, JobID job) {}

  private static void assertWindowRows(JobID job, String phase) {
    if (phase.equals("TWO_PHASE")) {
      assertNativeRows(job, "NativeColumnarLocalWindowAggExecNode");
      assertNativeRows(job, "NativeColumnarGlobalWindowAggExecNode");
    } else {
      assertNativeRows(job, "NativeColumnarWindowAggExecNode");
    }
  }

  private static void assertNativeRows(JobID job, String operator) {
    var groups = reporter.findOperatorMetricGroups(job, "(?i)" + operator);
    assertTrue(
        !groups.isEmpty(),
        () ->
            "missing native operator "
                + operator
                + ": "
                + reporter.findOperatorMetricGroups(job, "Native").stream()
                    .map(g -> g.getAllVariables().get("<operator_name>"))
                    .toList());
    long inputs = 0, outputs = 0;
    for (var group : groups) {
      var metrics = reporter.getMetricsByGroup(group);
      inputs += ((Counter) metrics.get("numRecordsIn")).getCount();
      outputs += ((Counter) metrics.get("numRecordsOut")).getCount();
    }
    assertTrue(inputs > 0, operator + " consumed no rows");
    assertTrue(outputs > 0, operator + " emitted no rows");
  }

  private static Result collect(TableEnvironment table, String sql) throws Exception {
    var rows = new ArrayList<Row>();
    var result = table.executeSql(sql);
    try (var iterator = result.collect()) {
      iterator.forEachRemaining(rows::add);
    }
    rows.sort(Comparator.comparing(Row::toString));
    return new Result(rows, result.getJobClient().orElseThrow().getJobID());
  }

  private static TableEnvironment environment(String phase) {
    var env = new TestStreamEnvironment(cluster.getMiniCluster(), 1);
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    var source =
        env.fromData(
                Types.ROW_NAMED(
                    new String[] {"k", "millis", "v"}, Types.INT, Types.LONG, Types.LONG),
                Row.of(1, 1000L, 10L),
                Row.of(1, 2000L, 20L),
                Row.of(1, 3000L, 15L),
                Row.of(2, 4000L, 7L),
                Row.of(1, 2000L, 20L),
                Row.of(2, 6000L, 8L),
                Row.of(3, 2000L, null))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofDays(1))
                    .withTimestampAssigner((row, previous) -> (Long) row.getField(1)));
    table.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("millis", DataTypes.BIGINT())
            .column("v", DataTypes.BIGINT())
            .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")
            .watermark("rt", "SOURCE_WATERMARK()")
            .build());
    table.createTemporaryView(
        "ranked",
        table.sqlQuery(
            "SELECT k, rt, v FROM (SELECT k, rt, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v"
                + " DESC) AS rn FROM src) WHERE rn = 1"));
    return table;
  }

  private static final class RecoveryProof {
    final AtomicBoolean failed = new AtomicBoolean();
    final AtomicInteger restored = new AtomicInteger(-1);
  }

  private static final class ExplicitWatermarks implements WatermarkGenerator<Row> {
    @Override
    public void onEvent(Row row, long timestamp, WatermarkOutput output) {
      output.emitWatermark(new Watermark((Long) row.getField(3)));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {}
  }

  private static final class RecoveringChanges
      implements SourceFunction<Row>, CheckpointedFunction, CheckpointListener {
    private final String id;
    private int next;
    private boolean restored;
    private volatile boolean running = true;
    private volatile boolean completed;
    private transient ListState<Integer> state;
    private transient Map<Long, Integer> snapshots;

    RecoveringChanges(String id) {
      this.id = id;
    }

    @Override
    public void run(SourceContext<Row> context) throws Exception {
      if (!restored) {
        emit(context, 3);
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (running && !completed && System.nanoTime() < deadline) Thread.sleep(5);
        if (!running) return;
        if (!completed) throw new IllegalStateException("no completed window checkpoint");
        if (RECOVERY.get(id).failed.compareAndSet(false, true)) {
          throw new IllegalStateException("window source failure after completed checkpoint");
        }
      }
      emit(context, 15);
    }

    private void emit(SourceContext<Row> context, int end) {
      List<Row> changes =
          List.of(
              Row.ofKind(RowKind.INSERT, 1, 1000L, 10L),
              Row.ofKind(RowKind.INSERT, 2, 1000L, null),
              Row.ofKind(RowKind.INSERT, 3, 1000L, 7L),
              Row.ofKind(RowKind.UPDATE_BEFORE, 1, 1000L, 10L),
              Row.ofKind(RowKind.UPDATE_AFTER, 1, 2000L, 20L),
              Row.ofKind(RowKind.DELETE, 3, 1000L, 7L),
              Row.ofKind(RowKind.INSERT, 4, 1000L, 5L),
              Row.ofKind(RowKind.INSERT, 4, 1000L, 5L),
              Row.ofKind(RowKind.DELETE, 4, 1000L, 5L),
              Row.ofKind(RowKind.DELETE, 5, 1000L, 3L),
              Row.ofKind(RowKind.DELETE, 6, 1000L, Long.MIN_VALUE),
              Row.ofKind(RowKind.INSERT, 7, 1000L, Long.MAX_VALUE),
              Row.ofKind(RowKind.INSERT, 7, 1000L, Long.MAX_VALUE),
              Row.ofKind(RowKind.INSERT, 8, 1000L, 10L),
              Row.ofKind(RowKind.INSERT, 8, 1000L, 11L));
      synchronized (context.getCheckpointLock()) {
        while (running && next < end) context.collect(changes.get(next++));
      }
    }

    @Override
    public void cancel() {
      running = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      state.update(List.of(next));
      snapshots.put(context.getCheckpointId(), next);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      state =
          context
              .getOperatorStateStore()
              .getListState(new ListStateDescriptor<>("window-source-offset", Integer.class));
      snapshots = new ConcurrentHashMap<>();
      restored = context.isRestored();
      if (restored) {
        for (int offset : state.get()) next = offset;
        RECOVERY.get(id).restored.set(next);
      }
    }

    @Override
    public void notifyCheckpointComplete(long checkpoint) {
      if (snapshots.getOrDefault(checkpoint, 0) == 3) completed = true;
      snapshots.keySet().removeIf(id -> id <= checkpoint);
    }
  }
}
