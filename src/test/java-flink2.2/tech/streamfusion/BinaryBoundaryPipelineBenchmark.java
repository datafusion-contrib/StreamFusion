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
import tech.streamfusion.planner.BinaryBoundaryBenchmarkFunctions;
import tech.streamfusion.planner.NativePlanner;

/** Bounded diagnostics retain the rowwise source, sink, JNI and both transposes. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class BinaryBoundaryPipelineBenchmark {
  @Test
  void binaryBoundaryComparisonFromRowsToBlackhole() throws Exception {
    int count = Integer.getInteger("binary.boundary.pipeline.rows", 1_000_000);
    int warmups = Integer.getInteger("binary.boundary.pipeline.warmups", 2);
    int runs = Integer.getInteger("binary.boundary.pipeline.runs", 5);
    boolean previous = Boolean.getBoolean("sf.binary.boundary.previous");
    boolean suffix = BinaryBoundaryBenchmarkFunctions.suffix();
    assertEquals(previous ? 17 : 6, BinaryBoundaryBenchmarkFunctions.register(64, false)[0]);
    BinaryBoundaryBenchmarkFunctions.close();
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        boolean nativeRun = ((trial + turn) & 1) == 1;
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
          var table = StreamTableEnvironment.create(env);
          var source =
              env.fromSequence(0, count - 1)
                  .map(i -> {
                    byte[] b = new byte[64];
                    for (int j = 0; j < b.length; j++) b[j] = (byte) (i * 131 + j * 127);
                    byte[] p = suffix
                        ? java.util.Arrays.copyOfRange(b, b.length - 4, b.length)
                        : java.util.Arrays.copyOf(b, 4);
                    if (i % 3 == 0) p[0] ^= 1;
                    return Row.of(i % 7 == 0 ? null : b, i % 11 == 0 ? null : p);
                  })
                  .returns(Types.ROW_NAMED(new String[] {"b", "p"},
                      Types.PRIMITIVE_ARRAY(Types.BYTE), Types.PRIMITIVE_ARRAY(Types.BYTE)));
          table.createTemporaryView("inputs", source,
              Schema.newBuilder().column("b", DataTypes.BYTES()).column("p", DataTypes.BYTES()).build());
          table.executeSql("CREATE TABLE output_rows(v BOOLEAN) WITH ('connector'='blackhole')");
          String sql = "INSERT INTO output_rows SELECT " + BinaryBoundaryBenchmarkFunctions.functionName() + "(b,p) FROM inputs";
          var scan = nativeRun ? NativePlanner.install(table) : null;
          String plan = table.explainSql(sql);
          if (nativeRun) {
            assertTrue(plan.contains("NativeCalc"), plan);
            assertTrue(plan.contains("RowDataToArrow"), plan);
            assertTrue(plan.contains("ArrowToRowData"), plan);
            assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
            assertFalse(scan.functionExecutions().isEmpty());
            assertTrue(scan.functionExecutions().stream()
                .allMatch(route -> previous != route.contains("jvm=[]")),
                scan.functionExecutions().toString());
          }
          long start = System.nanoTime();
          table.executeSql(sql).await();
          double seconds = (System.nanoTime() - start) / 1e9;
          if (nativeRun) RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
          System.out.printf(Locale.ROOT,
              "[binary-boundary-pipeline] function=%s engine=%s trial=%d warmup=%s rows=%d seconds=%.9f%n",
              BinaryBoundaryBenchmarkFunctions.functionName(), nativeRun ? previous ? "previous" : "native" : "stock", trial, trial < warmups,
              count, seconds);
        }
      }
    }
  }
}
