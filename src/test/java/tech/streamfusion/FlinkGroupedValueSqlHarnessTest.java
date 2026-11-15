package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.NativeFailureParity.Phase.ROW_EVALUATION;
import static tech.streamfusion.NativeFailureParity.Route.NATIVE;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkGroupedValueSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "TINYINT",
        "SMALLINT",
        "INT",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "DECIMAL(20,3)",
        "STRING",
        "DATE",
        "TIME(0)",
        "TIME(3)",
        "BOOLEAN",
        "TIMESTAMP(9)",
        "TIMESTAMP_LTZ(3)"
      })
  void firstAndLastPreserveArrivalOrderAndIgnoreNulls(String type) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireFirstLastType(type);
    compare(
        () -> environment(type, false),
        "SELECT k, FIRST_VALUE(v), LAST_VALUE(v), COUNT(*) FROM src GROUP BY k");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "INT",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "DECIMAL(20,3)",
        "STRING",
        "DATE",
        "TIME(0)",
        "TIME(3)",
        "BOOLEAN",
        "TIMESTAMP(9)",
        "TIMESTAMP_LTZ(3)"
      })
  void retractionsRemoveTheOldestMatchingOccurrence(String type) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireFirstLastType(type);
    compare(
        () -> environment(type, true),
        "SELECT k, FIRST_VALUE(v), LAST_VALUE(v), COUNT(*) FROM src GROUP BY k");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "TINYINT",
        "SMALLINT",
        "INT",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "DECIMAL(20,3)",
        "STRING",
        "DATE",
        "TIME(3)",
        "BOOLEAN",
        "TIMESTAMP(9)",
        "TIMESTAMP_LTZ(3)"
      })
  void retainedRetractionsExecuteNativelyAcrossReleasedPhaseChoices(String type) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireFirstLastType(type);
    for (boolean mini : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> {
                  var table = environment(type, true, metrics);
                  table.getConfig().set("table.exec.state.ttl", "1 h");
                  table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(mini));
                  table.getConfig().set("table.exec.mini-batch.size", "3");
                  table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
                  table
                      .getConfig()
                      .set("table.optimizer.agg-phase-strategy", mini ? "TWO_PHASE" : "ONE_PHASE");
                  return table;
                },
                "SELECT k, FIRST_VALUE(v), LAST_VALUE(v), FIRST_VALUE(v) FILTER (WHERE k = 1),"
                    + " COUNT(*) FROM src GROUP BY k");
        runs.assertSuccess(NATIVE);
        RecoveryExecutionMetrics.assertConsumed(
            metrics.nativeOperators(), "NativeColumnarGroupAggregate");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void filtersKeepTheirOwnValueState(boolean retract) throws Exception {
    compare(
        () -> environment("BIGINT", retract),
        "SELECT k, FIRST_VALUE(v) FILTER (WHERE v >= 20), "
            + "LAST_VALUE(v) FILTER (WHERE v < 30), COUNT(*) FROM src GROUP BY k");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "INT",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "DECIMAL(20,3)",
        "STRING",
        "DATE",
        "TIME(0)",
        "TIME(3)",
        "BOOLEAN",
        "TIMESTAMP(9)",
        "TIMESTAMP_LTZ(3)"
      })
  void singleValuePreservesOneElementIncludingNull(String type) throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        !type.startsWith("TIME(")
            || tech.streamfusion.compat.FlinkTestCapabilities.CAST_TIME_SINGLE_VALUE,
        "Flink 1.18 casts TIME to TIME(0), but SINGLE_VALUE infers TIME(3)");
    compare(() -> environment(type, false), "SELECT id, SINGLE_VALUE(v) FROM src GROUP BY id");
    compare(() -> environment(type, true), "SELECT id, SINGLE_VALUE(v) FROM src GROUP BY id");
    compare(() -> environment(type, false), "SELECT SINGLE_VALUE(v) FROM src WHERE id < 0");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void singleValueRejectsTwoElementsEvenWhenTheyAreNull(boolean nulls) {
    String sql = "SELECT SINGLE_VALUE(" + (nulls ? "CAST(NULL AS BIGINT)" : "v") + ") FROM src";
    var comparison = NativeFailureParity.run(() -> environment("BIGINT", false), sql);
    comparison.assertFailure(
        RuntimeException.class,
        "SingleValueAggFunction received more than one element.",
        ROW_EVALUATION,
        NATIVE);
    assertTrue(
        java.util.Arrays.stream(comparison.nativeRun().rootCause().getStackTrace())
            .anyMatch(frame -> frame.getMethodName().equals("updateGroupAggregator")));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void retainedFirstLastAndStateTtlHintsExecuteNatively(boolean hint) throws Exception {
    if (hint) tech.streamfusion.compat.FlinkTestCapabilities.requireStateTtlHint();
    try (var metrics = new RecoveryExecutionMetrics()) {
      Supplier<TableEnvironment> source =
          () -> {
            var table = environment("BIGINT", true, metrics);
            if (!hint) table.getConfig().setIdleStateRetention(java.time.Duration.ofHours(1));
            return table;
          };
      compare(
          source,
          "SELECT "
              + (hint ? "/*+ STATE_TTL('src' = '1h') */ " : "")
              + "k, FIRST_VALUE(v), LAST_VALUE(v) FROM src GROUP BY k");
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeColumnarGroupAggregate");
    }
  }

  @org.junit.jupiter.api.Test
  void zeroRetentionHintAdmitsRetractionsDespiteGlobalTtl() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireStateTtlHint();
    compare(
        () -> {
          var table = environment("BIGINT", true);
          table.getConfig().setIdleStateRetention(java.time.Duration.ofHours(1));
          return table;
        },
        "SELECT /*+ STATE_TTL('src' = '0s') */ k, FIRST_VALUE(v), LAST_VALUE(v) FROM src GROUP BY"
            + " k");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void distinctFirstLastPreserveMembershipAndArrivalOrder(boolean retract) throws Exception {
    compare(
        () -> environment("STRING", retract),
        "SELECT k, FIRST_VALUE(DISTINCT v), LAST_VALUE(DISTINCT v), COUNT(*) FROM src GROUP BY k");
    compare(
        () -> environment("STRING", retract),
        "SELECT k, FIRST_VALUE(DISTINCT v) FILTER (WHERE id >= 3), "
            + "LAST_VALUE(DISTINCT v) FILTER (WHERE id <> 6), COUNT(*) FROM src GROUP BY k");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL(20,3)",
        "CHAR(8)", "DATE", "TIME(0)", "TIME(3)", "BOOLEAN", "TIMESTAMP(9)", "TIMESTAMP_LTZ(3)"
      })
  void distinctScalarFirstLastKeepTypedMembershipAndIndependentFilters(String type)
      throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireFirstLastType(type);
    for (boolean retract : new boolean[] {false, true}) {
      compare(
          () -> environment(type, retract),
          "SELECT k, FIRST_VALUE(DISTINCT v), LAST_VALUE(DISTINCT v), COUNT(*) FROM src GROUP BY"
              + " k");
      compare(
          () -> environment(type, retract),
          "SELECT k, FIRST_VALUE(DISTINCT v) FILTER (WHERE id >= 3), "
              + "LAST_VALUE(DISTINCT v) FILTER (WHERE id <> 6), COUNT(*) FROM src GROUP BY k");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("distinctRecoveryCases")
  void distinctScalarMembershipRestoresBeforeRemovalAndReinsertion(String backend, String type)
      throws Exception {
    distinctRecovery(backend, type, 0);
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("distinctRecoveryCases")
  void retainedDistinctMembershipAndSelectedValuesSurviveTwoRestores(String backend, String type)
      throws Exception {
    distinctRecovery(backend, type, 3_600_000);
  }

  private static void distinctRecovery(String backend, String type, long retention)
      throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireFirstLastType(type);
    List<Row> input = new ArrayList<>();
    for (int id = 0; id < 64; id++) {
      int slot = (id / 2) % 4;
      String raw = id % 11 == 0 ? null : value(type, slot == 1 ? 20 : slot == 3 ? 30 : 10);
      input.add(Row.of(id, id % 2, raw));
    }
    for (int id = 0; id < 64; id++) {
      if ((id / 2) % 2 == 0) {
        Row original = input.get(id);
        input.add(
            Row.ofKind(
                RowKind.DELETE, original.getField(0), original.getField(1), original.getField(2)));
      }
    }
    for (int id = 1000; id < 1004; id++) input.add(Row.of(id, id % 2, value(type, 10)));
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                input,
                Types.ROW_NAMED(
                    new String[] {"id", "k", "raw_value"}, Types.INT, Types.INT, Types.STRING),
                org.apache.flink.table.api.Schema.newBuilder()
                    .column("id", org.apache.flink.table.api.DataTypes.INT())
                    .column("k", org.apache.flink.table.api.DataTypes.INT())
                    .column("raw_value", org.apache.flink.table.api.DataTypes.STRING())
                    .build(),
                true,
                24,
                80)
            .withExecutionMetrics()) {
      java.util.function.UnaryOperator<TableEnvironment> configure =
          table -> {
            table.getConfig().setIdleStateRetention(java.time.Duration.ofMillis(retention));
            table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
            table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
            table.getConfig().set("table.exec.mini-batch.enabled", "false");
            table.getConfig().set("streamfusion.transpose.batchRows", "7");
            String expression =
                type.equals("TIMESTAMP_LTZ(3)")
                    ? "TO_TIMESTAMP_LTZ(CAST(raw_value AS BIGINT), 3)"
                    : "CAST(raw_value AS " + type + ")";
            table.createTemporaryView(
                "typed_values",
                table.sqlQuery("SELECT id, k, " + expression + " AS v FROM recovery_input"));
            return table;
          };
      var result =
          NativeFailureParity.runRecovery(
              () -> configure.apply(recovery.uninterrupted()),
              () -> configure.apply(recovery.get()),
              "SELECT k, FIRST_VALUE(DISTINCT v), LAST_VALUE(DISTINCT v), "
                  + "FIRST_VALUE(DISTINCT v) FILTER (WHERE MOD(id, 3) <> 0), "
                  + "LAST_VALUE(DISTINCT v) FILTER (WHERE MOD(id, 5) <> 0) "
                  + "FROM typed_values GROUP BY k");
      result.assertSuccess(NATIVE);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("distinct-typed-first-last-recovery");
    }
  }

  private static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments>
      distinctRecoveryCases() {
    return List.of("hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory").stream()
        .flatMap(
            backend ->
                List.of(
                        "TINYINT",
                        "SMALLINT",
                        "INT",
                        "BIGINT",
                        "FLOAT",
                        "DOUBLE",
                        "DECIMAL(20,3)",
                        "STRING",
                        "CHAR(8)",
                        "DATE",
                        "TIME(0)",
                        "TIME(3)",
                        "BOOLEAN",
                        "TIMESTAMP(9)",
                        "TIMESTAMP_LTZ(3)")
                    .stream()
                    .map(type -> org.junit.jupiter.params.provider.Arguments.of(backend, type)));
  }

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments>
      retainedDistinctCases() {
    return java.util.stream.Stream.of(
            "TINYINT",
            "SMALLINT",
            "INT",
            "BIGINT",
            "FLOAT",
            "DOUBLE",
            "DECIMAL(20,3)",
            "STRING",
            "CHAR(8)",
            "DATE",
            "TIME(0)",
            "TIME(3)",
            "BOOLEAN",
            "TIMESTAMP(9)",
            "TIMESTAMP_LTZ(3)")
        .flatMap(
            type ->
                java.util.stream.Stream.of(false, true)
                    .flatMap(
                        retract ->
                            java.util.stream.Stream.of(false, true)
                                .map(
                                    mini ->
                                        org.junit.jupiter.params.provider.Arguments.of(
                                            type, retract, mini))));
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("retainedDistinctCases")
  void retainedDistinctSelectionsShareMembershipAcrossReleasedPhaseChoices(
      String type, boolean retract, boolean mini) {
    tech.streamfusion.compat.FlinkTestCapabilities.requireFirstLastType(type);
    try (var metrics = new RecoveryExecutionMetrics()) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment(type, retract, metrics);
                table.getConfig().set("table.exec.state.ttl", "1 h");
                table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(mini));
                table.getConfig().set("table.exec.mini-batch.size", "3");
                table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
                table
                    .getConfig()
                    .set("table.optimizer.agg-phase-strategy", mini ? "TWO_PHASE" : "ONE_PHASE");
                return table;
              },
              "SELECT k,FIRST_VALUE(DISTINCT v),LAST_VALUE(DISTINCT v),FIRST_VALUE(DISTINCT v)"
                  + " FILTER(WHERE k=1),COUNT(DISTINCT v),COUNT(*) FROM src GROUP BY k");
      runs.assertSuccess(NATIVE);
      RecoveryExecutionMetrics.assertConsumed(
          metrics.nativeOperators(), "NativeColumnarGroupAggregate");
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void distinctFirstLastBoundedVarcharAndEmptyResults(boolean retract) throws Exception {
    compare(
        () -> environment("VARCHAR(8)", retract),
        "SELECT k, FIRST_VALUE(DISTINCT v), LAST_VALUE(DISTINCT v) FROM src GROUP BY k");
    compare(
        () -> environment("STRING", retract),
        "SELECT FIRST_VALUE(DISTINCT v), LAST_VALUE(DISTINCT v) FROM src WHERE id < 0");
    compare(
        () -> {
          var table = environment("STRING", retract);
          table.getConfig().setIdleStateRetention(java.time.Duration.ofHours(1));
          return table;
        },
        "SELECT k, FIRST_VALUE(DISTINCT v) FROM src GROUP BY k");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, AVG(k), SINGLE_VALUE(v), COUNT(DISTINCT k) FROM src GROUP BY id",
        "SELECT SINGLE_VALUE(v) FROM src WHERE id = 1",
        "SELECT SINGLE_VALUE(v) FROM src WHERE id < 0"
      })
  void twoPhaseSingleValuePreservesNullCardinalityAndPartialOffsets(String sql) throws Exception {
    Supplier<TableEnvironment> source = () -> twoPhaseSingleEnvironment(5);
    String plan = NativePlanner.explain(source.get(), sql);
    assertTrue(plan.contains("NativeColumnarLocalGroupAggregate"), plan);
    assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
    NativeParity.assertChangelogParity(source, sql);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 5})
  void twoPhaseSingleValueRejectsSecondElementLocallyOrGlobally(int bundle) {
    for (String argument : List.of("v", "CAST(NULL AS STRING)")) {
      String sql = "SELECT k, SINGLE_VALUE(" + argument + ") FROM src GROUP BY k";
      var source = (Supplier<TableEnvironment>) () -> twoPhaseSingleEnvironment(bundle);
      String plan = NativePlanner.explain(source.get(), sql);
      assertTrue(plan.contains("NativeColumnarLocalGroupAggregate"), plan);
      assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
      NativeFailureParity.run(source, sql)
          .assertFailure(
              RuntimeException.class,
              "SingleValueAggFunction received more than one element.",
              ROW_EVALUATION,
              NATIVE);
    }
  }

  @org.junit.jupiter.api.Test
  void twoPhaseSingleValueFilterRetainsHostPlanningFailure() {
    NativeFailureParity.run(
            () -> twoPhaseSingleEnvironment(5),
            "SELECT k, SINGLE_VALUE(v) FILTER (WHERE id = 2) FROM src GROUP BY k")
        .assertFailure(
            IllegalArgumentException.class,
            "",
            NativeFailureParity.Phase.PLANNING,
            NativeFailureParity.Route.UNPLANNED);
  }

  private static TableEnvironment twoPhaseSingleEnvironment(int bundle) {
    var table = environment("STRING", false);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.size", Integer.toString(bundle));
    table.getConfig().set("table.exec.mini-batch.allow-latency", "100 ms");
    return table;
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL(20,3)", "CHAR(8)",
        "DATE", "TIME(0)", "TIME(3)", "BOOLEAN", "TIMESTAMP(9)", "TIMESTAMP_LTZ(3)"
      })
  void splitSingleValueKeepsTypedPartialsNullsAndCardinalityErrors(String type) throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        !type.startsWith("TIME(")
            || tech.streamfusion.compat.FlinkTestCapabilities.CAST_TIME_SINGLE_VALUE);
    for (int bundle : new int[] {1, 5}) {
      Supplier<TableEnvironment> setup =
          () -> {
            var table = environment(type, false);
            table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
            table.getConfig().set("table.exec.mini-batch.enabled", "true");
            table.getConfig().set("table.exec.mini-batch.size", Integer.toString(bundle));
            table.getConfig().set("table.exec.mini-batch.allow-latency", "100 ms");
            return table;
          };
      String sql = "SELECT id, AVG(k), SINGLE_VALUE(v), COUNT(DISTINCT k) FROM src GROUP BY id";
      String plan = NativePlanner.explain(setup.get(), sql);
      assertTrue(plan.contains("NativeColumnarLocalGroupAggregate"), plan);
      assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
      NativeParity.assertChangelogParity(setup, sql);
      NativeFailureParity.run(setup, "SELECT k, SINGLE_VALUE(v) FROM src GROUP BY k")
          .assertFailure(
              RuntimeException.class,
              "SingleValueAggFunction received more than one element.",
              ROW_EVALUATION,
              NATIVE);
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "hashmap,BIGINT",
    "hashmap,'DECIMAL(20,3)'",
    "hashmap,DATE",
    "hashmap,TIME(3)",
    "hashmap,BOOLEAN",
    "hashmap,TIMESTAMP(9)",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,BIGINT",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,'DECIMAL(20,3)'",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,DATE",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,TIME(3)",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,BOOLEAN",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,TIMESTAMP(9)"
  })
  void splitSingleValueRestoresNullCardinalityBeforeRejectingADuplicate(
      String backend, String type) {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        !type.startsWith("TIME(")
            || tech.streamfusion.compat.FlinkTestCapabilities.CAST_TIME_SINGLE_VALUE);
    assertSplitSingleValueRecovery(backend, type, false);
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"})
  void splitSingleValueRejectsDuplicatesDuringCoalescedCheckpointFlush(String backend) {
    assertSplitSingleValueRecovery(backend, "BOOLEAN", true);
  }

  private static void assertSplitSingleValueRecovery(
      String backend, String type, boolean holdAfterInput) {
    List<Row> input = new ArrayList<>();
    for (int id = 0; id < 96; id++)
      input.add(Row.of(id, id % 7 == 0 ? null : value(type, 10 + 10 * (id % 3))));
    input.add(Row.of(0, value(type, 20)));
    try (var recovery =
        new PortableSqlRecovery(
                backend,
                input,
                Types.ROW_NAMED(new String[] {"id", "raw_value"}, Types.INT, Types.STRING),
                org.apache.flink.table.api.Schema.newBuilder()
                    .column("id", org.apache.flink.table.api.DataTypes.INT())
                    .column("raw_value", org.apache.flink.table.api.DataTypes.STRING())
                    .build(),
                false,
                24,
                63)
            .withExecutionMetrics()) {
      if (holdAfterInput) recovery.holdAfterInput();
      java.util.function.UnaryOperator<TableEnvironment> configure =
          table -> {
            table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
            table.getConfig().set("table.exec.mini-batch.enabled", "true");
            table.getConfig().set("table.exec.mini-batch.size", "13");
            table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
            table.getConfig().set("streamfusion.transpose.batchRows", "7");
            return table;
          };
      var result =
          NativeFailureParity.runRecovery(
              () -> configure.apply(recovery.uninterrupted()),
              () -> configure.apply(recovery.get()),
              "SELECT id, SINGLE_VALUE(CAST(raw_value AS "
                  + type
                  + ")) FROM recovery_input GROUP BY id");
      result.assertFailure(
          RuntimeException.class,
          "SingleValueAggFunction received more than one element.",
          ROW_EVALUATION,
          NATIVE);
      if (holdAfterInput) {
        assertTrue(
            java.util.Arrays.stream(result.nativeRun().rootCause().getStackTrace())
                .anyMatch(
                    frame ->
                        frame.getClassName().equals("tech.streamfusion.operator.BatchCoalescer")
                            && frame.getMethodName().equals("flush")),
            result.toString());
      }
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarLocalGroupAggregate");
    }
  }

  private static void compare(Supplier<TableEnvironment> environment, String sql) throws Exception {
    String plan = NativePlanner.explain(environment.get(), sql);
    assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
    NativeParity.assertOrderedKindedParity(environment, sql);
  }

  private static TableEnvironment environment(String type, boolean retract) {
    return environment(type, retract, null);
  }

  private static TableEnvironment environment(
      String type, boolean retract, RecoveryExecutionMetrics metrics) {
    var env =
        metrics == null
            ? StreamExecutionEnvironment.getExecutionEnvironment()
            : metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    String a = value(type, 10), b = value(type, 20), c = value(type, 30);
    List<Row> rows =
        new ArrayList<>(
            List.of(
                Row.of(1, 1, null),
                Row.of(2, 1, a),
                Row.of(3, 1, b),
                Row.of(4, 1, a),
                Row.of(5, 2, null),
                Row.of(6, 1, c),
                Row.of(7, 2, b),
                Row.of(8, 1, null)));
    if (retract) {
      rows.addAll(
          List.of(
              Row.ofKind(RowKind.DELETE, 2, 1, a),
              Row.ofKind(RowKind.UPDATE_BEFORE, 6, 1, c),
              Row.ofKind(RowKind.UPDATE_AFTER, 6, 1, b),
              Row.ofKind(RowKind.DELETE, 4, 1, a),
              Row.ofKind(RowKind.DELETE, 3, 1, b),
              Row.ofKind(RowKind.DELETE, 6, 1, b),
              Row.ofKind(RowKind.DELETE, 1, 1, null),
              Row.ofKind(RowKind.DELETE, 8, 1, null),
              Row.of(9, 1, c)));
    }
    var source =
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "k", "raw_value"}, Types.INT, Types.INT, Types.STRING),
            rows.toArray(Row[]::new));
    if (retract) table.createTemporaryView("input_rows", table.fromChangelogStream(source));
    else table.createTemporaryView("input_rows", source);
    String expression =
        type.equals("TIMESTAMP_LTZ(3)")
            ? "TO_TIMESTAMP_LTZ(CAST(raw_value AS BIGINT), 3)"
            : "CAST(raw_value AS " + type + ")";
    table.createTemporaryView(
        "src", table.sqlQuery("SELECT id, k, " + expression + " AS v FROM input_rows"));
    return table;
  }

  private static String value(String type, int value) {
    switch (type) {
      case "DATE":
        return "2020-01-" + value;
      case "TIME(0)":
      case "TIME(3)":
        return "12:34:" + value + ".123";
      case "BOOLEAN":
        return value == 20 ? "false" : "true";
      case "TIMESTAMP(9)":
        return "2020-01-01 00:00:" + value + ".123456789";
      default:
        return Integer.toString(value);
    }
  }
}
