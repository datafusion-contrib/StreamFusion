package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;

/** Final STRING output through the row source, both transposes and a blackhole row sink. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class IntegerSplitPipelineBenchmark {
  @Test
  void boundedRowSourceToRowSink() throws Exception {
    int count = Integer.getInteger("split.pipeline.rows", 2_000_000);
    int warmups = Integer.getInteger("split.pipeline.warmups", 2);
    int runs = Integer.getInteger("split.pipeline.runs", 5);
    boolean nativeRun = System.getProperty("split.pipeline.engine", "native").equals("native");
    String base = "ab".repeat(132);
    for (int trial = 0; trial < warmups + runs; trial++) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        StreamExecutionEnvironment env =
            metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
        var tables = StreamTableEnvironment.create(env);
        var source =
            env.fromSequence(0, count - 1)
                .map(
                    i -> {
                      char delimiter = new char[] {',', '|', '\u00e9'}[(int) (i % 3)];
                      return Row.of(
                          i % 7 == 0
                              ? null
                              : "key" + i % 16 + delimiter + base + delimiter + "tail",
                          (int) delimiter,
                          1);
                    })
                .returns(
                    Types.ROW_NAMED(
                        new String[] {"s", "delimiter", "idx"},
                        Types.STRING,
                        Types.INT,
                        Types.INT));
        tables.createTemporaryView(
            "inputs",
            source,
            Schema.newBuilder()
                .column("s", DataTypes.STRING())
                .column("delimiter", DataTypes.INT())
                .column("idx", DataTypes.INT())
                .build());
        tables.executeSql("CREATE TABLE output_rows(v STRING) WITH ('connector'='blackhole')");
        String statement =
            "INSERT INTO output_rows SELECT SPLIT_INDEX(s, delimiter, idx) FROM inputs";
        String plan =
            nativeRun ? NativePlanner.explain(tables, statement) : tables.explainSql(statement);
        if (nativeRun) {
          assertTrue(plan.contains("NativeCalc"), plan);
          assertTrue(plan.contains("RowDataToArrow"), plan);
          assertTrue(plan.contains("ArrowToRowData"), plan);
        }
        long start = System.nanoTime();
        tables.executeSql(statement).await();
        double elapsed = (System.nanoTime() - start) / 1e9;
        if (nativeRun)
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        System.out.printf(
            Locale.ROOT,
            "[integer-split-pipeline] engine=%s trial=%d warmup=%s rows=%d seconds=%.9f%n",
            nativeRun ? "native" : "flink",
            trial,
            trial < warmups,
            count,
            elapsed);
      }
    }
  }
}
