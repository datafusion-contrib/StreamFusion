package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

/** Standalone exact decimal unary work with the row source, both transposes and a rowwise sink. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class DecimalUnaryPipelineBenchmark {
  @Test
  void boundedRowSourceToRowSink() throws Exception {
    int count = Integer.getInteger("decimal.unary.pipeline.rows", 1_000_000);
    int warmups = Integer.getInteger("decimal.unary.pipeline.warmups", 2);
    int runs = Integer.getInteger("decimal.unary.pipeline.runs", 5);
    String engine = System.getProperty("decimal.unary.pipeline.engine", "native");
    boolean nativeRun = !engine.equals("flink");
    String function = System.getProperty("decimal.unary.pipeline.function", "ABS");
    assertTrue(java.util.List.of("ABS", "NEGATE").contains(function));
    String routeName = function.equals("NEGATE") ? "-" : function;
    int precision = Integer.getInteger("decimal.unary.pipeline.precision", 18);
    int scale = Integer.getInteger("decimal.unary.pipeline.scale", 3);
    String type = "DECIMAL(" + precision + "," + scale + ")";
    for (int trial = 0; trial < warmups + runs; trial++) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        var max = java.math.BigInteger.TEN.pow(precision).subtract(java.math.BigInteger.ONE);
        var source =
            env.fromSequence(0, count - 1)
                .map(
                    i -> {
                      java.math.BigInteger value =
                          i % 17 == 0
                              ? max
                              : java.math.BigInteger.valueOf(
                                  (precision > 18 ? 9007199254740993L : 0) + i * 7919L + 1);
                      if (i % 2 == 0) value = value.negate();
                      return Row.of(i % 7 == 0 ? null : new java.math.BigDecimal(value, scale));
                    })
                .returns(Types.ROW_NAMED(new String[] {"v"}, Types.BIG_DEC));
        table.createTemporaryView(
            "inputs",
            source,
            Schema.newBuilder().column("v", DataTypes.DECIMAL(precision, scale)).build());
        table.executeSql("CREATE TABLE output_rows(v " + type + ") WITH ('connector'='blackhole')");
        String expression = function.equals("NEGATE") ? "-v" : "ABS(v)";
        String sql = "INSERT INTO output_rows SELECT " + expression + " FROM inputs";
        if (nativeRun) {
          var scan = NativePlanner.install(table);
          table.explainSql(sql);
          assertTrue(
              scan.functionExecutions().stream()
                  .anyMatch(
                      route ->
                          route.contains(routeName)
                              && (engine.equals("previous")
                                  ? route.contains("jvm=[" + routeName)
                                  : route.contains("jvm=[]"))),
              scan.functionExecutions().toString());
        }
        String plan = table.explainSql(sql);
        if (nativeRun) {
          assertTrue(plan.contains("NativeCalc"), plan);
          assertTrue(plan.contains("RowDataToArrow"), plan);
          assertTrue(plan.contains("ArrowToRowData"), plan);
        }
        long start = System.nanoTime();
        table.executeSql(sql).await();
        double elapsed = (System.nanoTime() - start) / 1e9;
        if (nativeRun)
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        System.out.printf(
            Locale.ROOT,
            "[decimal-unary-pipeline] function=%s type=%s engine=%s trial=%d warmup=%s rows=%d"
                + " seconds=%.9f%n",
            function,
            type,
            engine,
            trial,
            trial < warmups,
            count,
            elapsed);
      }
    }
  }
}
