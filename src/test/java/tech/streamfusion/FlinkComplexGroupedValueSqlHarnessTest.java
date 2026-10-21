package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.compat.FlinkCompat;

class FlinkComplexGroupedValueSqlHarnessTest {
  private static final org.apache.flink.api.common.typeinfo.TypeInformation<Row> INPUT_TYPE =
      Types.ROW_NAMED(new String[] {"k", "n", "selected"}, Types.INT, Types.INT, Types.BOOLEAN);
  private static final Schema SCHEMA =
      Schema.newBuilder()
          .column("k", DataTypes.INT())
          .column("n", DataTypes.INT())
          .column("selected", DataTypes.BOOLEAN())
          .build();

  static Stream<Arguments> fixtures() {
    return Stream.of(
            "BINARY",
            "BYTES",
            "ARRAY",
            "MAP",
            "ROW",
            "NESTED_ARRAY",
            "INTERVAL_MONTH",
            "INTERVAL_DAY",
            "MULTISET")
        .filter(
            type ->
                java.util.Arrays.asList(System.getProperty("ordered.complex.type", type).split(","))
                    .contains(type))
        .flatMap(
            type ->
                Stream.of("ONE_PHASE", "TWO_PHASE")
                    .filter(
                        phase -> phase.equals(System.getProperty("ordered.complex.phase", phase)))
                    .flatMap(
                        phase ->
                            Stream.of(false, true)
                                .flatMap(
                                    distinct ->
                                        Stream.of(false, true)
                                            .map(
                                                retract ->
                                                    Arguments.of(
                                                        type, phase, distinct, retract)))));
  }

  @ParameterizedTest
  @MethodSource("fixtures")
  void firstAndLastKeepComplexValuesAndReleasedPhaseContracts(
      String type, String phase, boolean distinct, boolean retract) {
    verifyValues(type, phase, distinct, retract, 0);
  }

  static Stream<Arguments> retainedFixtures() {
    return fixtures().filter(args -> !(boolean) args.get()[2] && (boolean) args.get()[3]);
  }

  @ParameterizedTest
  @MethodSource("retainedFixtures")
  void retainedComplexRetractionsPreserveSelections(
      String type, String phase, boolean distinct, boolean retract) {
    verifyValues(type, phase, distinct, retract, 3_600_000);
  }

  private static void verifyValues(
      String type, String phase, boolean distinct, boolean retract, long retention) {
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var env = metrics.environment(new Configuration(), 1, 0);
                var table = StreamTableEnvironment.create(env);
                boolean bag = type.equals("MULTISET");
                var sourceType =
                    bag
                        ? org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(
                            DataTypes.ROW(
                                DataTypes.FIELD("k", DataTypes.INT()),
                                DataTypes.FIELD("n", DataTypes.MULTISET(DataTypes.INT())),
                                DataTypes.FIELD("selected", DataTypes.BOOLEAN())))
                        : INPUT_TYPE;
                Schema sourceSchema =
                    bag
                        ? Schema.newBuilder()
                            .column("k", DataTypes.INT())
                            .column("n", DataTypes.MULTISET(DataTypes.INT()))
                            .column("selected", DataTypes.BOOLEAN())
                            .build()
                        : SCHEMA;
                var inputs = rows(retract);
                if (bag) {
                  var bags = new java.util.ArrayList<Row>();
                  for (Row row : inputs) {
                    java.util.Map<Integer, Integer> value = null;
                    if (row.getField(1) != null) {
                      value = new java.util.LinkedHashMap<>();
                      value.put((Integer) row.getField(1), 2);
                      value.put(null, 1);
                    }
                    bags.add(Row.ofKind(row.getKind(), row.getField(0), value, row.getField(2)));
                  }
                  inputs = bags;
                }
                var input = fromData(env, sourceType, inputs.toArray(Row[]::new));
                table.createTemporaryView(
                    "src",
                    retract
                        ? table.fromChangelogStream(input, sourceSchema, ChangelogMode.all())
                        : table.fromDataStream(input, sourceSchema));
                return configure(table, phase, retention);
              },
              query(type, distinct, "src"));
      if (!FlinkCompat.GENERIC_ORDERED_VALUES
          || distinct
              && (type.equals("BINARY")
                  || type.equals("BYTES")
                  || type.equals("MAP")
                  || type.equals("ROW")
                  || type.equals("MULTISET"))) {
        runs.assertFailure(
            org.apache.flink.table.api.TableException.class,
            NativeFailureParity.Phase.PLANNING,
            NativeFailureParity.Route.UNPLANNED);
        return;
      }
      boolean expectNative =
          Boolean.parseBoolean(System.getProperty("ordered.complex.expectNative", "true"));
      runs.assertSuccess(
          expectNative ? NativeFailureParity.Route.NATIVE : NativeFailureParity.Route.FALLBACK);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      if (expectNative)
        RecoveryExecutionMetrics.assertConsumed(
            metrics.nativeOperators(), "NativeColumnarGroupAggregate");
    }
  }

  static Stream<Arguments> recoveries() {
    return Stream.of("ARRAY", "NESTED_ARRAY", "ROW", "BINARY")
        .flatMap(
            type ->
                (type.equals("ROW") || type.equals("BINARY")
                        ? Stream.of(false)
                        : Stream.of(false, true))
                    .flatMap(
                        distinct ->
                            Stream.of(
                                    "hashmap",
                                    "tech.streamfusion.state.RocksDBNativeStateBackendFactory")
                                .map(backend -> Arguments.of(type, distinct, backend))));
  }

  @ParameterizedTest
  @MethodSource("recoveries")
  void selectedValuesAndDistinctMembershipSurviveTwoRestores(
      String type, boolean distinct, String backend) throws Exception {
    verifyRecovery(type, distinct, backend, 0);
  }

  static Stream<Arguments> retainedRecoveries() {
    return recoveries().filter(args -> !(boolean) args.get()[1]);
  }

  @ParameterizedTest
  @MethodSource("retainedRecoveries")
  void retainedComplexViewsSurviveTwoRestores(String type, boolean distinct, String backend)
      throws Exception {
    verifyRecovery(type, distinct, backend, 3_600_000);
  }

  private static void verifyRecovery(String type, boolean distinct, String backend, long retention)
      throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        FlinkCompat.GENERIC_ORDERED_VALUES,
        "Released Flink 1.18 FIRST/LAST factories reject collection and ROW inputs");
    try (var recovery =
        new PortableSqlRecovery(backend, rows(true), INPUT_TYPE, SCHEMA, true, 4, 8)
            .withExecutionMetrics()) {
      var runs =
          NativeFailureParity.runRecovery(
              () -> configure(recovery.uninterrupted(), "TWO_PHASE", retention),
              () -> configure(recovery.get(), "TWO_PHASE", retention),
              query(type, distinct, "recovery_input"));
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
    }
  }

  private static TableEnvironment configure(TableEnvironment table, String phase, long retention) {
    table.getConfig().setIdleStateRetention(java.time.Duration.ofMillis(retention));
    table.getConfig().set("table.optimizer.agg-phase-strategy", phase);
    table
        .getConfig()
        .set(
            "table.exec.mini-batch.enabled",
            System.getProperty("ordered.complex.miniBatch", "true"));
    table.getConfig().set("table.exec.mini-batch.size", "2");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    return table;
  }

  private static String query(String type, boolean distinct, String source) {
    String value;
    switch (type) {
      case "MULTISET":
        value = "n";
        break;
      case "INTERVAL_MONTH":
        value = "INTERVAL '1' MONTH * n";
        break;
      case "INTERVAL_DAY":
        value = "INTERVAL '1' DAY * n";
        break;
      case "BINARY":
        value =
            "CAST(CASE MOD(n,3) WHEN 0 THEN X'0001' WHEN 1 THEN X'0002' ELSE X'0003' END AS"
                + " BINARY(4))";
        break;
      case "BYTES":
        value = "CAST(CONCAT('v', CAST(n AS STRING)) AS BYTES)";
        break;
      case "ARRAY":
        value = "ARRAY[n, CAST(NULL AS INT), -n]";
        break;
      case "MAP":
        value = "MAP[CAST(n AS STRING), n, 'x', CAST(NULL AS INT)]";
        break;
      case "ROW":
        value = "ROW(n, CONCAT('v', CAST(n AS STRING)))";
        break;
      case "NESTED_ARRAY":
        value = "ARRAY[ROW(n, CAST(NULL AS STRING)), ROW(-n, CAST(n AS STRING))]";
        break;
      default:
        throw new IllegalArgumentException(type);
    }
    String arg = distinct ? "DISTINCT v" : "v";
    return "SELECT k, FIRST_VALUE("
        + arg
        + "), LAST_VALUE("
        + arg
        + "), FIRST_VALUE("
        + arg
        + ") FILTER (WHERE selected), LAST_VALUE("
        + arg
        + ") FILTER (WHERE selected), COUNT(*)"
        + " FROM (SELECT k, selected, CASE WHEN n IS NULL THEN CAST(NULL AS "
        + declaredType(type)
        + ") ELSE "
        + value
        + " END AS v FROM "
        + source
        + ") typed GROUP BY k";
  }

  private static String declaredType(String type) {
    switch (type) {
      case "MULTISET":
        return "MULTISET<INT>";
      case "INTERVAL_MONTH":
        return "INTERVAL MONTH";
      case "INTERVAL_DAY":
        return "INTERVAL DAY";
      case "BINARY":
        return "BINARY(4)";
      case "BYTES":
        return "BYTES";
      case "ARRAY":
        return "ARRAY<INT>";
      case "MAP":
        return "MAP<STRING, INT>";
      case "ROW":
        return "ROW<f0 INT, f1 STRING>";
      case "NESTED_ARRAY":
        return "ARRAY<ROW<f0 INT, f1 STRING>>";
      default:
        throw new IllegalArgumentException(type);
    }
  }

  private static List<Row> rows(boolean retract) {
    if (!retract)
      return List.of(
          Row.of(null, 3, true),
          Row.of(null, 1, false),
          Row.of(null, 3, false),
          Row.of(null, null, null),
          Row.of(null, 2, true),
          Row.of(null, 4, false));
    return List.of(
        Row.ofKind(RowKind.INSERT, null, 3, true),
        Row.ofKind(RowKind.INSERT, null, 1, false),
        Row.ofKind(RowKind.INSERT, null, 3, false),
        Row.ofKind(RowKind.INSERT, null, null, null),
        Row.ofKind(RowKind.INSERT, null, 2, true),
        Row.ofKind(RowKind.DELETE, null, 3, true),
        Row.ofKind(RowKind.UPDATE_BEFORE, null, 1, false),
        Row.ofKind(RowKind.UPDATE_AFTER, null, 4, false),
        Row.ofKind(RowKind.DELETE, null, 2, true),
        Row.ofKind(RowKind.DELETE, null, null, null),
        Row.ofKind(RowKind.DELETE, null, 3, false),
        Row.ofKind(RowKind.DELETE, null, 4, false),
        Row.ofKind(RowKind.INSERT, null, 5, true),
        Row.ofKind(RowKind.INSERT, null, 1, true),
        Row.ofKind(RowKind.DELETE, null, 5, true),
        Row.ofKind(RowKind.DELETE, null, 1, true));
  }
}
