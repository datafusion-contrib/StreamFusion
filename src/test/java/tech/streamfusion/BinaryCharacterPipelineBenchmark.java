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
import tech.streamfusion.planner.BinaryCharacterBenchmarkFunctions;
import tech.streamfusion.planner.NativePlanner;

/** Complete diagnostics retain the rowwise source, sink, JNI and both perimeter transposes. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class BinaryCharacterPipelineBenchmark {
  @Test
  void bytesFromRowsToCharacterBlackhole() throws Exception {
    int count = Integer.getInteger("binary.character.pipeline.rows", 1_000_000);
    int warmups = Integer.getInteger("binary.character.pipeline.warmups", 2);
    int runs = Integer.getInteger("binary.character.pipeline.runs", 5);
    boolean previous = Boolean.getBoolean("sf.binary.character.previous");
    BinaryCharacterBenchmarkFunctions.register(64, false);
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        boolean nativeRun = ((trial + turn) & 1) == 1;
        try (var metrics = new RecoveryExecutionMetrics()) {
          var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
          var table = StreamTableEnvironment.create(env);
          var source = env.fromSequence(0, count - 1)
              .map(i -> Row.of(i % 11 == 0 ? null : BinaryCharacterBenchmarkFunctions.bytes(64, 3, i.intValue())))
              .returns(Types.ROW_NAMED(new String[] {"b"}, Types.PRIMITIVE_ARRAY(Types.BYTE)));
          table.createTemporaryView("inputs", source,
              Schema.newBuilder().column("b", DataTypes.BYTES()).build());
          table.executeSql("CREATE TABLE output_rows(s STRING) WITH ('connector'='blackhole')");
          String sql = "INSERT INTO output_rows SELECT CAST(b AS STRING) FROM inputs";
          var scan = nativeRun ? NativePlanner.install(table) : null;
          String plan = table.explainSql(sql);
          if (nativeRun && !previous) {
            assertTrue(plan.contains("NativeCalc"), plan);
            assertTrue(plan.contains("RowDataToArrow"), plan);
            assertTrue(plan.contains("ArrowToRowData"), plan);
            assertTrue(scan.fallbackReasons().isEmpty(), scan.fallbackReasons().toString());
            assertFalse(scan.functionExecutions().isEmpty());
            assertTrue(scan.functionExecutions().stream().allMatch(route -> route.contains("jvm=[]")),
                scan.functionExecutions().toString());
          } else if (nativeRun) {
            assertEquals(0, scan.substitutions());
            assertTrue(scan.fallbackReasons().stream().anyMatch(r -> r.contains("unsupported CAST")),
                scan.fallbackReasons().toString());
          }
          long start = System.nanoTime();
          table.executeSql(sql).await();
          double seconds = (System.nanoTime() - start) / 1e9;
          if (nativeRun && !previous)
            RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
          if (nativeRun && previous) assertTrue(metrics.nativeOperators().isEmpty());
          System.out.printf(Locale.ROOT,
              "[binary-character-pipeline] engine=%s trial=%d warmup=%s rows=%d seconds=%.9f%n",
              nativeRun ? previous ? "previous" : "native" : "stock", trial, trial < warmups, count, seconds);
        }
      }
    }
  }
}
