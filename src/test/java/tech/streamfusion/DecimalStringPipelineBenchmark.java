package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.DecimalStringBenchmarkFunctions;
import tech.streamfusion.planner.NativePlanner;

/** Bounded row-to-row jobs include planning, JNI and both perimeter transposes. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class DecimalStringPipelineBenchmark {
  @Test
  void decimalFormattingFromRowsToBlackhole() throws Exception {
    int count = Integer.getInteger("decimal.string.pipeline.rows", 1_000_000);
    int warmups = Integer.getInteger("decimal.string.pipeline.warmups", 2);
    int runs = Integer.getInteger("decimal.string.pipeline.runs", 5);
    boolean previous = Boolean.getBoolean("sf.decimal.string.previous");
    boolean tryCast = Boolean.getBoolean("sf.decimal.string.tryCast");
    boolean fallback = previous && tryCast;
    assertEquals(fallback ? -1 : previous ? 17 : 11,
        DecimalStringBenchmarkFunctions.register(18, 3)[0]);
    DecimalStringBenchmarkFunctions.close();
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        boolean nativeRun = ((trial + turn) & 1) == 1;
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
          var table = StreamTableEnvironment.create(env);
          var source = env.fromSequence(0, count - 1)
              .map(i -> Row.of(i % 7 == 0 ? null
                  : BigDecimal.valueOf((i % 2 == 0 ? -1 : 1) * (i * 7919 + 1), 3)))
              .returns(Types.ROW_NAMED(new String[] {"d"}, Types.BIG_DEC));
          table.createTemporaryView("inputs", source,
              Schema.newBuilder().column("d", DataTypes.DECIMAL(18, 3)).build());
          table.executeSql("CREATE TABLE output_rows(v STRING) WITH ('connector'='blackhole')");
          String sql = "INSERT INTO output_rows SELECT " + (tryCast ? "TRY_CAST" : "CAST")
              + "(d AS STRING) FROM inputs";
          var scan = nativeRun ? NativePlanner.install(table) : null;
          String plan = table.explainSql(sql);
          if (nativeRun && !fallback) {
            assertTrue(plan.contains("NativeCalc"), plan);
            assertTrue(plan.contains("RowDataToArrow"), plan);
            assertTrue(plan.contains("ArrowToRowData"), plan);
            assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
            assertFalse(scan.functionExecutions().isEmpty());
            assertTrue(scan.functionExecutions().stream()
                .allMatch(route -> previous != route.contains("jvm=[]")),
                scan.functionExecutions().toString());
          } else if (nativeRun) {
            assertEquals(0, scan.substitutions());
            assertTrue(scan.fallbackReasons().stream()
                .anyMatch(reason -> reason.contains("unsupported TRY_CAST")),
                scan.fallbackReasons().toString());
          }
          long start = System.nanoTime();
          table.executeSql(sql).await();
          double seconds = (System.nanoTime() - start) / 1e9;
          if (nativeRun && !fallback)
            RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
          if (nativeRun && fallback) assertTrue(metrics.nativeOperators().isEmpty());
          System.out.printf(Locale.ROOT,
              "[decimal-string-pipeline] engine=%s trial=%d warmup=%s rows=%d seconds=%.9f%n",
              nativeRun ? previous ? "previous" : "native" : "stock", trial, trial < warmups, count, seconds);
        }
      }
    }
  }
}
