package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkUnnestScalarConstructorSqlHarnessTest {
  @Test
  void constructorRunsAsANativeExpressionWithinUnnest() {
    var table = floatingSource();
    var scan = NativePlanner.install(table);
    String plan =
        table.explainSql("SELECT id, e FROM src CROSS JOIN UNNEST(ARRAY[id,id+1]) AS u(e)");
    assertTrue(
        scan.functionExecutions().stream()
            .anyMatch(
                execution ->
                    execution.contains("NativeUnnestOperand")
                        && execution.contains("native=[ARRAY]")),
        plan + scan.functionExecutions());
  }

  @ParameterizedTest
  @ValueSource(strings = {"f", "d"})
  void floatingConstructorPreservesPrimitiveGroupKeyIdentity(String column) {
    for (String select : new String[] {"e", "COUNT(*)"}) {
      String sql =
          "SELECT "
              + select
              + " FROM src CROSS JOIN UNNEST(ARRAY["
              + column
              + ","
              + column
              + "]) AS u(e)"
              + (select.equals("COUNT(*)") ? " GROUP BY e" : "");
      var comparison =
          NativeFailureParity.run(FlinkUnnestScalarConstructorSqlHarnessTest::floatingSource, sql);
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().rows(), comparison.nativeRun().rows());
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "0,false", "3,false", "6,false", "9,false", "0,true", "3,true", "6,true", "9,true"
  })
  void timestampConstructorsFollowReleasedCompactStorage(int precision, boolean localZoned) {
    String type = localZoned ? "TIMESTAMP_LTZ(9)" : "TIMESTAMP(9)";
    var raw =
        NativeFailureParity.run(
            () -> timestampSource(precision, localZoned),
            "SELECT k, CAST(v AS " + type + ") FROM src");
    assertEquals(null, raw.host().failure());
    assertEquals(10_987_654, nanos(raw.host().rows().get(0).get(2)));
    var comparison =
        NativeFailureParity.run(
            () -> timestampSource(precision, localZoned),
            "SELECT k, CAST(e AS " + type + ") FROM src CROSS JOIN UNNEST(ARRAY[v,v]) AS u(e)");
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(comparison.host().rows(), comparison.nativeRun().rows());
    assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
    assertEquals(
        precision <= 3 ? 10_000_000 : 10_987_654,
        nanos(comparison.nativeRun().rows().get(0).get(2)));
    assertEquals(
        precision <= 3 ? 999_000_000 : 999_999_999,
        nanos(comparison.nativeRun().rows().get(5).get(2)));
  }

  private static int nanos(Object value) {
    return value instanceof java.time.Instant
        ? ((java.time.Instant) value).getNano()
        : ((java.time.LocalDateTime) value).getNano();
  }

  private static TableEnvironment timestampSource(int precision, boolean localZoned) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    org.apache.flink.table.types.logical.LogicalType valueType =
        localZoned
            ? new org.apache.flink.table.types.logical.LocalZonedTimestampType(precision)
            : new org.apache.flink.table.types.logical.TimestampType(precision);
    var type =
        org.apache.flink.table.types.logical.RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
              new org.apache.flink.table.types.logical.BigIntType(), valueType
            },
            new String[] {"k", "v"});
    var input =
        env.fromSequence(0, 2)
            .map(
                i ->
                    (org.apache.flink.table.data.RowData)
                        org.apache.flink.table.data.GenericRowData.of(
                            i,
                            i == 1
                                ? null
                                : org.apache.flink.table.data.TimestampData.fromEpochMillis(
                                    i == 0 ? 10 : -1, i == 0 ? 987654 : 999999)))
            .returns(org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(type));
    table.createTemporaryView("src", input);
    return table;
  }

  private static TableEnvironment floatingSource() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    var source =
        env.fromSequence(0, 7)
            .map(
                i -> {
                  long[] doubles = {
                    0x7ff8000000000001L,
                    0xfff8000000000002L,
                    0L,
                    Long.MIN_VALUE,
                    0x7ff0000000000000L,
                    0xfff0000000000000L,
                    0x3ff8000000000000L,
                    0L
                  };
                  int[] floats = {
                    0x7fc00001,
                    0xffc00002,
                    0,
                    Integer.MIN_VALUE,
                    0x7f800000,
                    0xff800000,
                    0x3fc00000,
                    0
                  };
                  return Row.of(
                      i.intValue(),
                      i == 7 ? null : Float.intBitsToFloat(floats[i.intValue()]),
                      i == 7 ? null : Double.longBitsToDouble(doubles[i.intValue()]));
                })
            .returns(
                Types.ROW_NAMED(
                    new String[] {"id", "f", "d"}, Types.INT, Types.FLOAT, Types.DOUBLE));
    table.createTemporaryView("src", source);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "false");
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    return table;
  }
}
