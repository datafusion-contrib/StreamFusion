package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkSingleValueDistinctSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"BIGINT,false", "BIGINT,true", "STRING,false", "STRING,true"})
  void releasedPlannerDiscardsDistinctAndStillCountsEveryRow(String type, boolean nulls) {
    String argument = nulls ? "CAST(NULL AS " + type + ")" : "CAST(v AS " + type + ")";
    var comparison =
        NativeFailureParity.run(
            () -> environment(1, false), "SELECT SINGLE_VALUE(DISTINCT " + argument + ") FROM src");
    comparison.assertFailure(
        RuntimeException.class,
        "SingleValueAggFunction received more than one element.",
        NativeFailureParity.Phase.ROW_EVALUATION,
        NativeFailureParity.Route.NATIVE);
    assertTrue(comparison.nativeRun().plan().contains("NativeColumnarGroupAggregate"));
  }

  @ParameterizedTest
  @CsvSource({"BIGINT,false", "BIGINT,true", "STRING,false", "STRING,true"})
  void oneRowAndEmptyInputRetainOrdinarySingleValueSemantics(String type, boolean nulls) {
    String argument = nulls ? "CAST(NULL AS " + type + ")" : "CAST(v AS " + type + ")";
    for (String predicate : new String[] {"id = 0", "id < 0"}) {
      var comparison =
          NativeFailureParity.run(
              () -> environment(1, false),
              "SELECT SINGLE_VALUE(DISTINCT " + argument + ") FROM src WHERE " + predicate);
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().rows(), comparison.nativeRun().rows());
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
    }
  }

  @ParameterizedTest
  @CsvSource({"1,false", "1,true", "5,false", "5,true"})
  void splitStringPlanRetainsDuplicateAndNullCardinality(int bundle, boolean nulls) {
    String argument = nulls ? "CAST(NULL AS STRING)" : "CAST(v AS STRING)";
    var comparison =
        NativeFailureParity.run(
            () -> environment(bundle, true),
            "SELECT k, SINGLE_VALUE(DISTINCT " + argument + ") FROM src GROUP BY k");
    comparison.assertFailure(
        RuntimeException.class,
        "SingleValueAggFunction received more than one element.",
        NativeFailureParity.Phase.ROW_EVALUATION,
        NativeFailureParity.Route.NATIVE);
    assertTrue(comparison.nativeRun().plan().contains("NativeColumnarLocalGroupAggregate"));
  }

  private static TableEnvironment environment(int bundle, boolean split) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    var source =
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"id", "k", "v"}, Types.INT, Types.INT, Types.LONG),
            Row.of(0, 0, 7L),
            Row.of(1, 0, 7L));
    table.createTemporaryView("src", source);
    table.getConfig().set("table.optimizer.agg-phase-strategy", split ? "TWO_PHASE" : "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(split));
    if (split) {
      table.getConfig().set("table.exec.mini-batch.size", Integer.toString(bundle));
      table.getConfig().set("table.exec.mini-batch.allow-latency", "1 ms");
    }
    return table;
  }
}
