package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

class FlinkGenericDistinctPartialsSqlHarnessTest {
  private static final org.apache.flink.api.common.typeinfo.TypeInformation<Row> INPUT_TYPE =
      Types.ROW_NAMED(
          new String[] {"k", "n", "selected", "raw_value"},
          Types.INT,
          Types.INT,
          Types.BOOLEAN,
          Types.STRING);
  private static final Schema SCHEMA =
      Schema.newBuilder()
          .column("k", DataTypes.INT())
          .column("n", DataTypes.INT())
          .column("selected", DataTypes.BOOLEAN())
          .column("raw_value", DataTypes.STRING())
          .build();

  static Stream<String> types() {
    return Stream.of(
            "ARRAY",
            "NESTED_ARRAY",
            "ARRAY_FLOAT",
            "ARRAY_DOUBLE",
            "ARRAY_STRING",
            "ARRAY_DECIMAL",
            "ARRAY_ARRAY",
            "INTERVAL_MONTH",
            "INTERVAL_DAY")
        .filter(
            type ->
                Arrays.asList(System.getProperty("generic.distinct.types", type).split(","))
                    .contains(type));
  }

  static Stream<Arguments> profiles() {
    return types()
        .flatMap(
            type ->
                Stream.of("ONE_PHASE", "TWO_PHASE")
                    .flatMap(
                        phase ->
                            Stream.of(false, true)
                                .flatMap(
                                    retract ->
                                        Stream.of(false, true)
                                            .map(
                                                retained ->
                                                    Arguments.of(
                                                        type, phase, retract, retained)))));
  }

  @ParameterizedTest
  @MethodSource("profiles")
  void typedMembershipRetainsReleasedPhaseAndChangelogContracts(
      String type, String phase, boolean retract, boolean retained) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var env = metrics.environment(new Configuration(), 1, 0);
                var table = StreamTableEnvironment.create(env);
                var source = fromData(env, INPUT_TYPE, rows(retract).toArray(Row[]::new));
                table.createTemporaryView(
                    "src",
                    retract
                        ? table.fromChangelogStream(source, SCHEMA, ChangelogMode.all())
                        : table.fromDataStream(source, SCHEMA));
                return configure(table, phase, retained);
              },
              query(type, "src"));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeColumnarGroupAggregate");
      if (phase.equals("TWO_PHASE")) {
        assertTrue(runs.nativeRun().plan().contains("NativeColumnarLocalGroupAggregate"));
        RecoveryExecutionMetrics.assertConsumed(
            metrics.nativeOperators(), "NativeColumnarLocalGroupAggregate");
      }
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"ONE_PHASE", "TWO_PHASE"})
  void literalArraysAcrossUnionKeepTheirRepresentation(String phase) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table =
                    StreamTableEnvironment.create(metrics.environment(new Configuration(), 1, 0));
                return configure(table, phase, false);
              },
              "SELECT a,COUNT(DISTINCT b) FROM (VALUES (2,ARRAY[1,2]),(2,ARRAY[2,3]),"
                  + "(2,ARRAY[1,2]),(5,ARRAY[3,4])) t(a,b) GROUP BY a");
      assertNull(runs.host().failure(), runs::toString);
      assertNull(runs.nativeRun().failure(), runs::toString);
      assertEquals(NativeFailureParity.Route.NATIVE, runs.nativeRun().route());
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      // Independent VALUES branches can reach a key in different mini-batch bundles.
      assertEquals(
          SqlAuditHarness.materialized(runs.host().rows()),
          SqlAuditHarness.materialized(runs.nativeRun().rows()),
          runs::toString);
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeColumnarGroupAggregate");
      if (phase.equals("TWO_PHASE"))
        RecoveryExecutionMetrics.assertConsumed(
            metrics.nativeOperators(), "NativeColumnarLocalGroupAggregate");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "ARRAY_STRING,ONE_PHASE",
    "ARRAY_STRING,TWO_PHASE",
    "INTERVAL_MONTH,ONE_PHASE",
    "INTERVAL_MONTH,TWO_PHASE"
  })
  void representedValuesKeepNullableStringGroupingKeys(String type, String phase) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var env = metrics.environment(new Configuration(), 1, 0);
                var table = StreamTableEnvironment.create(env);
                table.createTemporaryView(
                    "src",
                    table.fromChangelogStream(
                        fromData(env, INPUT_TYPE, rows(true).toArray(Row[]::new)),
                        SCHEMA,
                        ChangelogMode.all()));
                return configure(table, phase, true);
              },
              withStringGroups(query(type, "src")));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeColumnarGroupAggregate");
      if (phase.equals("TWO_PHASE"))
        RecoveryExecutionMetrics.assertConsumed(
            metrics.nativeOperators(), "NativeColumnarLocalGroupAggregate");
    }
  }

  static Stream<Arguments> recoveries() {
    return Stream.of(
            "ARRAY",
            "NESTED_ARRAY",
            "ARRAY_FLOAT",
            "INTERVAL_MONTH",
            "INTERVAL_DAY",
            "STRING_KEY_ARRAY_STRING",
            "STRING_KEY_INTERVAL_MONTH")
        .filter(
            type ->
                Arrays.asList(System.getProperty("generic.distinct.types", type).split(","))
                    .contains(type))
        .flatMap(
            type ->
                Stream.of("hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")
                    .flatMap(
                        backend ->
                            Stream.of(false, true)
                                .map(retained -> Arguments.of(type, backend, retained))));
  }

  @ParameterizedTest
  @MethodSource("recoveries")
  void genericViewsAndSharedClocksSurviveTwoRestores(String type, String backend, boolean retained)
      throws Exception {
    var inputs = new ArrayList<Row>();
    for (int i = 0; i < 12; i++) inputs.addAll(rows(true));
    var expected = new HashMap<List<Object>, Long>();
    for (Integer key : Arrays.asList(null, 1)) {
      inputs.add(Row.of(key, 6, true, "6"));
      inputs.add(Row.of(key, 6, false, "6"));
      inputs.add(Row.of(key, 7, false, "7"));
      inputs.add(Row.of(key, null, null, null));
      if (type.startsWith("STRING_KEY_")) {
        expected.put(
            Arrays.asList(key == null ? null : key.toString(), 2L, 1L, 2L, 2L, "7", 4L), 1L);
      } else {
        expected.put(Arrays.asList(key, 2L, 1L, 2L, 4L), 1L);
      }
    }
    try (var recovery =
        new PortableSqlRecovery(backend, inputs, INPUT_TYPE, SCHEMA, true, 8, 24)
            .withExecutionMetrics()) {
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure(recovery.uninterrupted(), "TWO_PHASE", retained),
              () -> configure(recovery.get(), "TWO_PHASE", retained),
              query(type, "recovery_input"));
      assertNull(runs.host().failure(), runs::toString);
      assertNull(runs.nativeRun().failure(), runs::toString);
      assertEquals(NativeFailureParity.Route.NATIVE, runs.nativeRun().route());
      // Checkpoint drains can change intermediate mini-batch updates, but not the final table.
      assertEquals(expected, SqlAuditHarness.materialized(runs.host().rows()), runs::toString);
      assertEquals(expected, SqlAuditHarness.materialized(runs.nativeRun().rows()), runs::toString);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarLocalGroupAggregate");
    }
  }

  static Stream<Arguments> rejectedKeyTypes() {
    return Stream.of("BINARY", "VARBINARY", "MAP", "ROW", "MULTISET")
        .flatMap(
            type -> Stream.of("ONE_PHASE", "TWO_PHASE").map(phase -> Arguments.of(type, phase)));
  }

  @ParameterizedTest
  @MethodSource("rejectedKeyTypes")
  void releasedDistinctKeyFactoryRejectsTheSameRemainingRoots(String type, String phase) {
    var runs =
        NativeFailureParity.run(
            () -> {
              org.apache.flink.table.types.DataType valueType;
              Object value;
              switch (type) {
                case "BINARY":
                  valueType = DataTypes.BINARY(4);
                  value = new byte[] {0, 1, 0, 0};
                  break;
                case "VARBINARY":
                  valueType = DataTypes.BYTES();
                  value = new byte[] {0, 1};
                  break;
                case "MAP":
                  valueType = DataTypes.MAP(DataTypes.INT(), DataTypes.INT());
                  value = Map.of(1, 2);
                  break;
                case "ROW":
                  valueType =
                      DataTypes.ROW(
                          DataTypes.FIELD("n", DataTypes.INT()),
                          DataTypes.FIELD("s", DataTypes.STRING()));
                  value = Row.of(1, "v");
                  break;
                case "MULTISET":
                  valueType = DataTypes.MULTISET(DataTypes.INT());
                  value = Map.of(1, 2);
                  break;
                default:
                  throw new IllegalArgumentException(type);
              }
              var env =
                  org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
                      .getExecutionEnvironment();
              var table = StreamTableEnvironment.create(env);
              var rowType =
                  DataTypes.ROW(
                      DataTypes.FIELD("k", DataTypes.INT()), DataTypes.FIELD("v", valueType));
              var source =
                  fromData(
                      env,
                      org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(rowType),
                      Row.of(1, value));
              table.createTemporaryView(
                  "src",
                  table.fromDataStream(
                      source,
                      Schema.newBuilder()
                          .column("k", DataTypes.INT())
                          .column("v", valueType)
                          .build()));
              return configure(table, phase, false);
            },
            "SELECT k,COUNT(DISTINCT v) FROM src GROUP BY k");
    runs.assertFailure(
        org.apache.flink.table.api.TableException.class,
        "Distinct aggregate function does not support type: " + type,
        NativeFailureParity.Phase.PLANNING,
        NativeFailureParity.Route.UNPLANNED);
  }

  private static TableEnvironment configure(
      TableEnvironment table, String phase, boolean retained) {
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.size", "2");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("table.exec.state.ttl", retained ? "1 h" : "0 ms");
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    return table;
  }

  private static String withStringGroups(String sql) {
    return sql.replace(
            "SELECT k, selected,",
            "SELECT CAST(k AS STRING) AS k, selected, CAST(n AS STRING) AS s,")
        .replace("COUNT(*) FROM", "COUNT(DISTINCT s),MAX(s),COUNT(*) FROM");
  }

  private static String query(String type, String source) {
    if (type.startsWith("STRING_KEY_"))
      return withStringGroups(query(type.substring("STRING_KEY_".length()), source));
    String value, declared;
    switch (type) {
      case "ARRAY":
        value = "ARRAY[n, CAST(NULL AS INT), -n]";
        declared = "ARRAY<INT>";
        break;
      case "NESTED_ARRAY":
        value = "ARRAY[ROW(n, CAST(NULL AS STRING)), ROW(-n, CAST(n AS STRING))]";
        declared = "ARRAY<ROW<f0 INT, f1 STRING>>";
        break;
      case "ARRAY_ARRAY":
        value = "ARRAY[ARRAY[n, CAST(NULL AS INT)], ARRAY[-n]]";
        declared = "ARRAY<ARRAY<INT>>";
        break;
      case "ARRAY_FLOAT":
      case "ARRAY_DOUBLE":
        String number = type.equals("ARRAY_FLOAT") ? "FLOAT" : "DOUBLE";
        value = "ARRAY[CAST(raw_value AS " + number + "), CAST(NULL AS " + number + ")]";
        declared = "ARRAY<" + number + ">";
        break;
      case "ARRAY_STRING":
        value = "ARRAY[CAST(n AS STRING), CAST(NULL AS STRING)]";
        declared = "ARRAY<STRING>";
        break;
      case "ARRAY_DECIMAL":
        value = "ARRAY[CAST(n AS DECIMAL(12,2)), CAST(NULL AS DECIMAL(12,2))]";
        declared = "ARRAY<DECIMAL(12,2)>";
        break;
      case "INTERVAL_MONTH":
        value = "INTERVAL '1' MONTH * n";
        declared = "INTERVAL MONTH";
        break;
      case "INTERVAL_DAY":
        value = "INTERVAL '1' DAY * n";
        declared = "INTERVAL DAY";
        break;
      default:
        throw new IllegalArgumentException(type);
    }
    return "SELECT k, COUNT(DISTINCT v), COUNT(DISTINCT v) FILTER (WHERE selected),"
        + " COUNT(DISTINCT v) FILTER (WHERE NOT selected), COUNT(*) FROM (SELECT k, selected,"
        + " CASE WHEN n IS NULL THEN CAST(NULL AS "
        + declared
        + ") ELSE "
        + value
        + " END AS v FROM "
        + source
        + ") typed GROUP BY k";
  }

  private static List<Row> rows(boolean retract) {
    var rows = new ArrayList<Row>();
    for (Integer key : Arrays.asList(null, 1)) {
      var block =
          List.of(
              Row.ofKind(RowKind.INSERT, key, 0, true), Row.ofKind(RowKind.INSERT, key, 1, true),
              Row.ofKind(RowKind.INSERT, key, 2, false), Row.ofKind(RowKind.INSERT, key, 3, true),
              Row.ofKind(RowKind.INSERT, key, 3, false),
                  Row.ofKind(RowKind.INSERT, key, null, null),
              Row.ofKind(RowKind.DELETE, key, 3, true),
                  Row.ofKind(RowKind.UPDATE_BEFORE, key, 1, true),
              Row.ofKind(RowKind.UPDATE_AFTER, key, 4, false),
                  Row.ofKind(RowKind.DELETE, key, 2, false),
              Row.ofKind(RowKind.DELETE, key, 3, false),
                  Row.ofKind(RowKind.DELETE, key, null, null),
              Row.ofKind(RowKind.DELETE, key, 4, false), Row.ofKind(RowKind.DELETE, key, 0, true),
              Row.ofKind(RowKind.INSERT, key, 5, true), Row.ofKind(RowKind.INSERT, key, 1, true),
              Row.ofKind(RowKind.DELETE, key, 5, true), Row.ofKind(RowKind.DELETE, key, 1, true));
      for (var row : block) {
        if (!retract && row.getKind() != RowKind.INSERT) continue;
        Integer n = (Integer) row.getField(1);
        String raw =
            n == null
                ? null
                : n % 5 == 0 ? "NaN" : n % 5 == 1 ? "-0.0" : n % 5 == 2 ? "0.0" : n.toString();
        rows.add(Row.ofKind(row.getKind(), key, n, row.getField(2), raw));
      }
    }
    return rows;
  }
}
