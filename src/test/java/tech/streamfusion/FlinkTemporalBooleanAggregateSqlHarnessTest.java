package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkTemporalBooleanAggregateSqlHarnessTest {
  @org.junit.jupiter.api.Test
  void twoPhaseExtremaRetainTypedPartialsAndFilteredNulls() throws Exception {
    var source =
        (java.util.function.Supplier<TableEnvironment>)
            () -> {
              var table = environment(false);
              table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
              table.getConfig().set("table.exec.mini-batch.enabled", "true");
              table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
              table.getConfig().set("table.exec.mini-batch.size", "3");
              return table;
            };
    String sql =
        "SELECT k, MIN(d), MAX(d), MIN(t), MAX(t), MIN(b), MAX(b), MAX(t) FILTER (WHERE keep),"
            + " MIN(b) FILTER (WHERE keep), COUNT(*) FROM src GROUP BY k";
    String plan = NativePlanner.explain(source.get(), sql);
    assertTrue(plan.contains("NativeColumnarLocalGroupAggregate"), plan);
    assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
    NativeParity.assertChangelogParity(source, sql);
  }

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> valueModes() {
    return java.util.stream.Stream.of("t", "b")
        .flatMap(
            column ->
                java.util.stream.Stream.of(false, true)
                    .map(
                        retract ->
                            org.junit.jupiter.params.provider.Arguments.of(column, retract)));
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("valueModes")
  void orderedValueFiltersKeepSeparateState(String column, boolean retract) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireFirstLastType(
        column.equals("t") ? "TIME(3)" : "BOOLEAN");
    compare(
        retract,
        "SELECT k, FIRST_VALUE("
            + column
            + ") FILTER (WHERE keep), "
            + "LAST_VALUE("
            + column
            + ") FILTER (WHERE NOT keep), COUNT(*) FROM src GROUP BY k");
  }

  @ParameterizedTest
  @ValueSource(strings = {"t", "b"})
  void singleValueRetainsTypedValuesAndNulls(String column) throws Exception {
    compare(false, "SELECT SINGLE_VALUE(" + column + ") FROM src WHERE k = 3");
    compare(false, "SELECT SINGLE_VALUE(" + column + ") FROM src WHERE k = 2");
    compare(false, "SELECT SINGLE_VALUE(" + column + ") FROM src WHERE k < 0");
  }

  @ParameterizedTest
  @ValueSource(strings = {"t", "b"})
  void singleValueRetainsCardinalityErrors(String column) {
    NativeFailureParity.run(
            () -> environment(false), "SELECT SINGLE_VALUE(" + column + ") FROM src")
        .assertFailure(
            RuntimeException.class,
            "SingleValueAggFunction received more than one element.",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @ValueSource(strings = {"t", "b"})
  void retractingValueMapTtlStillFallsBack(String column) throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireFirstLastType(
        column.equals("t") ? "TIME(3)" : "BOOLEAN");
    NativeParity.assertFallbackReasonContains(
        () -> {
          var table = environment(true);
          table.getConfig().setIdleStateRetention(java.time.Duration.ofHours(1));
          return table;
        },
        "SELECT k, FIRST_VALUE(" + column + "), LAST_VALUE(" + column + ") FROM src GROUP BY k",
        "independently expiring value/order maps");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void extremaRetainValuesNullsFiltersAndRetractions(boolean retract) throws Exception {
    String sql =
        "SELECT k, MIN(d), MAX(d), MIN(t), MAX(t), MIN(b), MAX(b), "
            + "MIN(d) FILTER (WHERE keep), MAX(t) FILTER (WHERE keep), "
            + "MIN(b) FILTER (WHERE keep), COUNT(*) FROM src GROUP BY k";
    compare(retract, sql);
    compare(
        retract,
        "SELECT k, MIN(DISTINCT d), MAX(DISTINCT t), MIN(DISTINCT b), COUNT(*) FROM src GROUP BY"
            + " k");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void globalAndEmptyExtremaKeepTypedNulls(boolean retract) throws Exception {
    compare(retract, "SELECT MIN(d), MAX(t), MIN(b), MAX(b) FROM src");
    compare(retract, "SELECT MIN(d), MAX(t), MIN(b), MAX(b) FROM src WHERE k < 0");
  }

  private static void compare(boolean retract, String sql) throws Exception {
    var source = (java.util.function.Supplier<TableEnvironment>) () -> environment(retract);
    assertTrue(NativePlanner.explain(source.get(), sql).contains("NativeColumnarGroupAggregate"));
    NativeParity.assertOrderedKindedParity(source, sql);
  }

  private static TableEnvironment environment(boolean retract) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    LocalDate low = LocalDate.of(1960, 2, 29), high = LocalDate.of(2024, 2, 29);
    LocalTime early = LocalTime.of(0, 0, 0, 1_000_000),
        late = LocalTime.of(23, 59, 59, 999_000_000);
    List<Row> rows =
        new ArrayList<>(
            List.of(
                Row.of(1, high, late, true, true), Row.of(1, low, early, false, false),
                Row.of(1, low, early, false, true), Row.of(2, null, null, null, null),
                Row.of(1, null, null, null, true), Row.of(3, high, early, true, false)));
    if (retract)
      rows.addAll(
          List.of(
              Row.ofKind(RowKind.DELETE, 1, low, early, false, false),
              Row.ofKind(RowKind.DELETE, 1, low, early, false, true),
              Row.ofKind(RowKind.UPDATE_BEFORE, 1, high, late, true, true),
              Row.ofKind(RowKind.UPDATE_AFTER, 1, low, early, false, true),
              Row.ofKind(RowKind.DELETE, 1, low, early, false, true),
              Row.ofKind(RowKind.DELETE, 1, null, null, null, true),
              Row.ofKind(RowKind.DELETE, 2, null, null, null, null),
              Row.of(1, high, late, true, true)));
    var stream =
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"k", "d", "t", "b", "keep"},
                Types.INT,
                Types.LOCAL_DATE,
                Types.LOCAL_TIME,
                Types.BOOLEAN,
                Types.BOOLEAN),
            rows.toArray(Row[]::new));
    var schema =
        Schema.newBuilder()
            .column("k", INT())
            .column("d", DATE())
            .column("t", TIME(3))
            .column("b", BOOLEAN())
            .column("keep", BOOLEAN())
            .build();
    table.createTemporaryView(
        "src",
        retract ? table.fromChangelogStream(stream, schema) : table.fromDataStream(stream, schema));
    return table;
  }
}
