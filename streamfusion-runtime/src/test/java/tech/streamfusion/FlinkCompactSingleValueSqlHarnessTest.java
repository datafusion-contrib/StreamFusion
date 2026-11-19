package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkCompactSingleValueSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void directTimePartialsPreserveSubsecondValuesInPureAndMixedGroups(boolean mixed)
      throws Exception {
    for (int width : new int[] {1, 7, 128}) {
      java.util.function.Supplier<TableEnvironment> setup = () -> timeEnvironment(width);
      String sql =
          "SELECT id, SINGLE_VALUE(v)"
              + (mixed ? ", AVG(id), COUNT(DISTINCT id)" : "")
              + " FROM src GROUP BY id";
      String plan = NativePlanner.explain(setup.get(), sql);
      assertTrue(plan.contains("NativeColumnarLocalGroupAggregate"), plan);
      assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
      NativeParity.assertChangelogParity(setup, sql);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"BIGINT", "STRING", "DECIMAL(20,3)", "DATE", "BOOLEAN", "TIMESTAMP(9)"})
  void nullableCompositeKeysAndPureSingleValuesSurvivePhysicalSplits(String keyType)
      throws Exception {
    for (int width : new int[] {1, 7, 128}) {
      java.util.function.Supplier<TableEnvironment> setup = () -> environment(keyType, width);
      String sql = "SELECT id, k, SINGLE_VALUE(v) FROM src GROUP BY id, k";
      String plan = NativePlanner.explain(setup.get(), sql);
      assertTrue(plan.contains("NativeColumnarLocalGroupAggregate"), plan);
      assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
      NativeParity.assertChangelogParity(setup, sql);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"BIGINT", "STRING", "DECIMAL(20,3)", "DATE", "BOOLEAN", "TIMESTAMP(9)"})
  void nullAndDuplicateCompositeKeysRetainCardinalityFailure(String keyType) {
    for (int width : new int[] {1, 128}) {
      NativeFailureParity.run(
              () -> environment(keyType, width), "SELECT k, SINGLE_VALUE(v) FROM src GROUP BY k")
          .assertFailure(
              RuntimeException.class,
              "SingleValueAggFunction received more than one element.",
              NativeFailureParity.Phase.ROW_EVALUATION,
              NativeFailureParity.Route.NATIVE);
    }
  }

  private static TableEnvironment environment(String keyType, int width) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(java.time.ZoneId.of("America/New_York"));
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.size", "13");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("streamfusion.transpose.batchRows", Integer.toString(width));
    var rows = new ArrayList<Row>();
    for (int id = 0; id < 41; id++) {
      String rawKey;
      switch (keyType) {
        case "DATE":
          rawKey = "1969-12-31";
          break;
        case "TIMESTAMP(9)":
          rawKey = "1969-12-31 23:59:59.999999999";
          break;
        case "BOOLEAN":
          rawKey = "true";
          break;
        default:
          rawKey = "7";
      }
      rows.add(
          Row.of(
              id,
              id % 3 == 0 ? null : rawKey,
              id % 5 == 0 ? null : "1969-12-31 23:59:59.999999999"));
    }
    table.createTemporaryView(
        "input_rows",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "raw_key", "raw_value"}, Types.INT, Types.STRING, Types.STRING),
            rows.toArray(Row[]::new)));
    table.createTemporaryView(
        "src",
        table.sqlQuery(
            "SELECT id, CAST(raw_key AS "
                + keyType
                + ") AS k, CAST(raw_value AS TIMESTAMP(9)) AS v FROM input_rows"));
    return table;
  }

  private static TableEnvironment timeEnvironment(int width) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.size", "13");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    table.getConfig().set("streamfusion.transpose.batchRows", Integer.toString(width));
    var rows = new ArrayList<Row>();
    for (int id = 0; id < 41; id++) {
      rows.add(
          Row.of(
              id,
              id % 5 == 0
                  ? null
                  : java.time.LocalTime.ofNanoOfDay(
                      (id == 39 ? 86_399_999L : id * 997L) * 1_000_000)));
    }
    table.createTemporaryView(
        "src",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"id", "v"}, Types.INT, Types.LOCAL_TIME),
            rows.toArray(Row[]::new)),
        Schema.newBuilder().column("id", DataTypes.INT()).column("v", DataTypes.TIME(3)).build());
    return table;
  }
}
