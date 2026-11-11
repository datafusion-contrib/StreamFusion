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

/** Whole-job CONCAT diagnostic, including the row source, both transposes and a row sink. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class ByteStringConcatPipelineBenchmark {
  @Test
  void rowSourceToRowSink() throws Exception {
    int count = Integer.getInteger("byte.concat.rows", 2_000_000);
    int width = Integer.getInteger("byte.concat.width", 16);
    boolean unicode = Boolean.getBoolean("byte.concat.unicode");
    int warmups = Integer.getInteger("byte.concat.warmups", 3);
    int runs = Integer.getInteger("byte.concat.runs", 5);
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int order = 0; order < 2; order++) {
        boolean nativeRun = (trial + order) % 2 != 0;
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
          var table = StreamTableEnvironment.create(env);
          String prefix = (unicode ? "\u4e2d" : "a").repeat(width);
          var input =
              env.fromSequence(0, count - 1)
                  .map(i -> Row.of(prefix + i % 256))
                  .returns(Types.ROW_NAMED(new String[] {"s"}, Types.STRING));
          table.createTemporaryView(
              "src", input, Schema.newBuilder().column("s", DataTypes.STRING()).build());
          table.executeSql("CREATE TABLE output_rows(v STRING) WITH ('connector'='blackhole')");
          String sql = "INSERT INTO output_rows SELECT CONCAT(s, '!') FROM src";
          if (nativeRun) {
            var scan = NativePlanner.install(table);
            String plan = table.explainSql(sql);
            assertTrue(plan.contains("NativeCalc"), plan);
            assertTrue(plan.contains("RowDataToArrow"), plan);
            assertTrue(plan.contains("ArrowToRowData"), plan);
            assertTrue(plan.contains("byteStrings="), plan);
            assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
          }
          long start = System.nanoTime();
          table.executeSql(sql).await();
          double seconds = (System.nanoTime() - start) / 1e9;
          if (nativeRun)
            RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
          System.out.printf(
              Locale.ROOT,
              "byte_concat_pipeline,%s,%d,%d,%s,%d,%s,%.9f%n",
              nativeRun ? "native" : "flink",
              count,
              width,
              unicode,
              trial,
              trial < warmups,
              seconds);
        }
      }
    }
  }
}
