package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class FlinkCollectionLegacyConsumerSqlHarnessTest {
  private final RecoveryExecutionMetrics metrics = new RecoveryExecutionMetrics();

  @AfterEach
  void closeCluster() {
    metrics.close();
  }

  private StreamTableEnvironment environment(int parallelism, String type) {
    var env = metrics.environment(new Configuration(), parallelism, 0);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    var rows =
        new ArrayList<>(
            List.of(
                Row.of("", -128L),
                Row.of("", -128L),
                Row.of("", null),
                Row.of(null, 32767L),
                Row.of("é🙂", null),
                Row.of("?", Long.MIN_VALUE),
                Row.of("\ud800", Long.MAX_VALUE),
                Row.ofKind(RowKind.UPDATE_BEFORE, "", -128L),
                Row.ofKind(RowKind.UPDATE_AFTER, "", 127L),
                Row.ofKind(RowKind.DELETE, "", -128L),
                Row.ofKind(RowKind.DELETE, "é🙂", null),
                Row.of("é🙂", 7L)));
    for (Row row : rows) {
      Long value = (Long) row.getField(1);
      if (value == null) continue;
      if (type.equals("DATE"))
        row.setField(1, java.time.LocalDate.ofEpochDay(Math.floorMod(value, 730000) - 365000));
      if (type.equals("TIME"))
        row.setField(
            1, java.time.LocalTime.ofNanoOfDay(Math.floorMod(value, 86400000) * 1_000_000L));
    }
    boolean timestamp = type.startsWith("TIMESTAMP");
    boolean yearMonth =
        type.startsWith("INTERVAL") && (type.contains("YEAR") || type.contains("MONTH"));
    boolean interval = type.startsWith("INTERVAL");
    if (timestamp) table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
    for (Row row : rows) {
      if (row.getField(1) == null) continue;
      if (timestamp) {
        long value = (Long) row.getField(1);
        var date =
            java.time.LocalDateTime.of(value < 0 ? 1 : 9999, 1, 1, 0, 0, 0, 123456789)
                .plusSeconds(Math.floorMod(value, 86400));
        row.setField(
            1, type.startsWith("TIMESTAMP_LTZ") ? date.toInstant(java.time.ZoneOffset.UTC) : date);
      } else if (yearMonth)
        row.setField(1, Integer.valueOf((int) ((Long) row.getField(1)).longValue()));
    }
    var logicalValueType = DataTypes.of(type.equals("TIME") ? "TIME(3)" : type);
    if (interval)
      logicalValueType = logicalValueType.bridgedTo(yearMonth ? Integer.class : Long.class);
    var input =
        tech.streamfusion.compat.FlinkTestSources.fromData(
            env,
            rows,
            Types.ROW_NAMED(
                new String[] {"k", "v"},
                Types.STRING,
                type.startsWith("TIMESTAMP_LTZ")
                    ? Types.INSTANT
                    : timestamp
                        ? Types.LOCAL_DATE_TIME
                        : yearMonth
                            ? Types.INT
                            : type.equals("DATE")
                                ? Types.LOCAL_DATE
                                : type.equals("TIME") ? Types.LOCAL_TIME : Types.LONG));
    table.createTemporaryView(
        "src",
        table.fromChangelogStream(
            input,
            Schema.newBuilder()
                .column("k", DataTypes.STRING())
                .column("v", logicalValueType)
                .build()));
    return table;
  }

  @ParameterizedTest
  @CsvSource({
    "COLLECT,TINYINT",
    "COLLECT,SMALLINT",
    "COLLECT,INT",
    "COLLECT,BIGINT",
    "COLLECT,BOOLEAN",
    "ARRAY_AGG,TINYINT",
    "ARRAY_AGG,SMALLINT",
    "ARRAY_AGG,INT",
    "ARRAY_AGG,BIGINT",
    "ARRAY_AGG,BOOLEAN",
    "COLLECT,FLOAT",
    "COLLECT,DOUBLE",
    "COLLECT,'DECIMAL(8,2)'",
    "COLLECT,'DECIMAL(20,4)'",
    "COLLECT,'DECIMAL(38,18)'",
    "ARRAY_AGG,FLOAT",
    "ARRAY_AGG,DOUBLE",
    "ARRAY_AGG,'DECIMAL(8,2)'",
    "ARRAY_AGG,'DECIMAL(20,4)'",
    "ARRAY_AGG,'DECIMAL(38,18)'"
  })
  void legacyExternalRowsKeepOrderedChangesAndValues(String function, String type)
      throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    for (int parallelism : new int[] {1, 3}) {
      String value = type.equals("BOOLEAN") ? "(v>0)" : "CAST(v AS " + type + ")";
      String sql =
          "SELECT k,"
              + function
              + "("
              + value
              + "),"
              + function
              + "(DISTINCT "
              + value
              + "),"
              + function
              + "(COALESCE("
              + value
              + ",CAST("
              + (type.equals("BOOLEAN") ? "FALSE" : "0")
              + " AS "
              + type
              + "))),COUNT(*) FROM src GROUP BY k";
      assertEquals(
          perKey(collect(sql, parallelism, false, false)),
          perKey(collect(sql, parallelism, true, true)));
    }
  }

  @ParameterizedTest
  @CsvSource({"COLLECT,DATE", "COLLECT,TIME", "ARRAY_AGG,DATE", "ARRAY_AGG,TIME"})
  void temporalCollectionsRetainLegacyRowsAcrossExchanges(String function, String type)
      throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    String sql =
        "SELECT k," + function + "(v)," + function + "(DISTINCT v),COUNT(*) FROM src GROUP BY k";
    for (int parallelism : new int[] {1, 3})
      assertEquals(
          perKey(collect(type, sql, parallelism, false, false)),
          perKey(collect(type, sql, parallelism, true, true)));
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("temporalCollectionProfiles")
  void timestampAndIntervalCollectionsKeepLegacyRows(String function, String type)
      throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    String sql =
        "SELECT k," + function + "(v)," + function + "(DISTINCT v),COUNT(*) FROM src GROUP BY k";
    for (int parallelism : new int[] {1, 3})
      assertEquals(
          perKey(collect(type, sql, parallelism, false, false)),
          perKey(collect(type, sql, parallelism, true, true)));
  }

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments>
      temporalCollectionProfiles() {
    return FlinkPrimitiveCollectionSqlHarnessTest.collectionSignatures()
        .filter(
            args -> {
              String type = (String) args.get()[1];
              return type.startsWith("TIMESTAMP") || type.startsWith("INTERVAL");
            });
  }

  @ParameterizedTest
  @ValueSource(strings = {"COLLECT", "ARRAY_AGG"})
  void emptyGlobalLegacyResultsKeepTheirReleasedSemantics(String function) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    String sql = "SELECT " + function + "(v) FROM src WHERE v=42";
    assertEquals(collect(sql, 1, false, false), collect(sql, 1, true, true, "NativeCalc"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"INT", "BIGINT"})
  void ignoreNullsKeepsNullArrayContainersInExternalRows(String type) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_AGG");
    String sql =
        "SELECT k,ARRAY_AGG(CAST(v AS " + type + ")) IGNORE NULLS,COUNT(*) FROM src GROUP BY k";
    assertEquals(perKey(collect(sql, 1, false, false)), perKey(collect(sql, 1, true, true)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"COLLECT", "ARRAY_AGG"})
  void disabledRuntimeRetainsLegacyFallback(String function) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction(function);
    String property = "streamfusion.experimental.jvmGroupAggregates";
    String previous = System.getProperty(property);
    try {
      System.setProperty(property, "false");
      String sql = "SELECT k," + function + "(v) FROM src GROUP BY k";
      assertEquals(perKey(collect(sql, 1, false, false)), perKey(collect(sql, 1, true, false)));
    } finally {
      if (previous == null) System.clearProperty(property);
      else System.setProperty(property, previous);
    }
  }

  private List<List<Object>> collect(
      String sql, int parallelism, boolean nativeEnabled, boolean expectNative, String... consumed)
      throws Exception {
    return collect("BIGINT", sql, parallelism, nativeEnabled, expectNative, consumed);
  }

  private List<List<Object>> collect(
      String type,
      String sql,
      int parallelism,
      boolean nativeEnabled,
      boolean expectNative,
      String... consumed)
      throws Exception {
    var previous =
        metrics.nativeOperators().stream()
            .map(row -> row.get("jobId"))
            .collect(java.util.stream.Collectors.toSet());
    var table = environment(parallelism, type);
    var scan = nativeEnabled ? NativePlanner.install(table) : null;
    var result = new ArrayList<List<Object>>();
    var query = table.sqlQuery(sql);
    try (var rows = table.toRetractStream(query, Row.class).executeAndCollect()) {
      while (rows.hasNext()) {
        var next = rows.next();
        var fields = new ArrayList<Object>();
        fields.add(next.f0);
        for (int i = 0; i < next.f1.getArity(); i++)
          fields.add(NativeParity.comparableValue(next.f1.getField(i)));
        result.add(fields);
      }
    }
    var work =
        metrics.nativeOperators().stream()
            .filter(row -> !previous.contains(row.get("jobId")))
            .collect(java.util.stream.Collectors.toList());
    if (expectNative) {
      assertTrue(scan.substitutions() > 0, scan.fallbackReasons().toString());
      if (consumed.length == 0)
        RecoveryExecutionMetrics.assertConsumed(work, "NativeColumnarGroupAggregate");
      else for (String operator : consumed) RecoveryExecutionMetrics.assertConsumed(work, operator);
    } else assertTrue(work.isEmpty(), work.toString());
    SharedFlinkCluster.assertNativeMemoryReleased("collection-legacy-consumer");
    return result;
  }

  private static java.util.Map<Object, List<List<Object>>> perKey(List<List<Object>> rows) {
    var result = new java.util.HashMap<Object, List<List<Object>>>();
    for (var row : rows) result.computeIfAbsent(row.get(1), ignored -> new ArrayList<>()).add(row);
    return result;
  }
}
