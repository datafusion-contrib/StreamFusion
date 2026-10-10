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
  @ValueSource(strings = {"FLOAT", "DOUBLE"})
  void unverifiedValueTypesRetainExplicitFallback(String type) throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> environment(type, false),
        "SELECT k, FIRST_VALUE(v), LAST_VALUE(v) FROM src GROUP BY k",
        "over an unsupported value type");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void retractingFirstLastWithIndependentMapTtlFallBack(boolean hint) throws Exception {
    if (hint) tech.streamfusion.compat.FlinkTestCapabilities.requireStateTtlHint();
    Supplier<TableEnvironment> source =
        () -> {
          var table = environment("BIGINT", true);
          if (!hint) table.getConfig().setIdleStateRetention(java.time.Duration.ofHours(1));
          return table;
        };
    NativeParity.assertFallbackReasonContains(
        source,
        "SELECT "
            + (hint ? "/*+ STATE_TTL('src' = '1h') */ " : "")
            + "k, FIRST_VALUE(v), LAST_VALUE(v) FROM src GROUP BY k",
        "independently expiring value/order maps");
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
  @ValueSource(booleans = {false, true})
  void distinctFirstLastBoundedVarcharAndEmptyResults(boolean retract) throws Exception {
    compare(
        () -> environment("VARCHAR(8)", retract),
        "SELECT k, FIRST_VALUE(DISTINCT v), LAST_VALUE(DISTINCT v) FROM src GROUP BY k");
    compare(
        () -> environment("STRING", retract),
        "SELECT FIRST_VALUE(DISTINCT v), LAST_VALUE(DISTINCT v) FROM src WHERE id < 0");
    NativeParity.assertFallbackReasonContains(
        () -> {
          var table = environment("STRING", retract);
          table.getConfig().setIdleStateRetention(java.time.Duration.ofHours(1));
          return table;
        },
        "SELECT k, FIRST_VALUE(DISTINCT v) FROM src GROUP BY k",
        "independently expiring value/order maps");
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

  private static void compare(Supplier<TableEnvironment> environment, String sql) throws Exception {
    String plan = NativePlanner.explain(environment.get(), sql);
    assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
    NativeParity.assertOrderedKindedParity(environment, sql);
  }

  private static TableEnvironment environment(String type, boolean retract) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
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
