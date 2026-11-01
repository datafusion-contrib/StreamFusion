package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

/** Integer expressions retaining the rowwise source/sink, JNI and both transposes. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class IntegralExpressionPipelineBenchmark {
  @Test
  void boundedRuntimeExpressionWithBothPerimeterTransposes() throws Exception {
    int count = Integer.getInteger("integral.expression.rows", 1_000_000);
    int warmups = Integer.getInteger("integral.expression.warmups", 2);
    int runs = Integer.getInteger("integral.expression.runs", 5);
    boolean previous = Boolean.getBoolean("sf.integral.previous");
    String expression = System.getProperty("sf.integral.expression", "ABS(v+CAST(1 AS $TYPE))")
        .replace("$TYPE", "INT");
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        boolean nativeRun = ((trial + turn) & 1) == 1;
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
          var table = StreamTableEnvironment.create(env);
          var source = env.fromSequence(0, count - 1).map(i -> Row.of(i % 7 == 0 ? null
              : i % 17 == 0 ? Integer.MIN_VALUE : (int) ((i % 2 == 0 ? -1 : 1) * i * 7919)))
              .returns(Types.ROW_NAMED(new String[] {"v"}, Types.INT));
          table.createTemporaryView("inputs", source,
              Schema.newBuilder().column("v", DataTypes.INT()).build());
          table.executeSql("CREATE TABLE output_rows(v INT) WITH ('connector'='blackhole')");
          String sql = "INSERT INTO output_rows SELECT " + expression + " FROM inputs";
          var scan = nativeRun ? NativePlanner.install(table) : null;
          String plan = table.explainSql(sql);
          if (nativeRun) {
            assertTrue(plan.contains("NativeCalc"), plan);
            assertTrue(plan.contains("RowDataToArrow"), plan);
            assertTrue(plan.contains("ArrowToRowData"), plan);
            assertFalse(scan.functionExecutions().isEmpty());
            assertEquals(!previous,
                scan.functionExecutions().stream().allMatch(route -> route.contains("jvm=[]")),
                scan.functionExecutions().toString());
            assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
          }
          long start = System.nanoTime();
          table.executeSql(sql).await();
          double elapsed = (System.nanoTime() - start) / 1e9;
          if (nativeRun) RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
          System.out.printf(Locale.ROOT,
              "[integral-expression-pipeline] engine=%s trial=%d warmup=%s rows=%d seconds=%.9f%n",
              nativeRun ? previous ? "previous" : "native" : "stock", trial, trial < warmups, count, elapsed);
        }
      }
    }
  }
}
