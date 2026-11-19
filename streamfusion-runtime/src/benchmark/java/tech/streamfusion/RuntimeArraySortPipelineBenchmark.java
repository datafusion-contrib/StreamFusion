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

/** Conversion-inclusive diagnostic retaining the row source, both transposes and row sink. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class RuntimeArraySortPipelineBenchmark {
  @Test
  void boundedRowSourceToRowSink() throws Exception {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ARRAY_SORT");
    int count = Integer.getInteger("array-sort.pipeline.rows", 500_000);
    int warmups = Integer.getInteger("array-sort.pipeline.warmups", 1);
    int runs = Integer.getInteger("array-sort.pipeline.runs", 3);
    String engine = System.getProperty("array-sort.pipeline.engine", "native");
    boolean nativeRun = !engine.equals("flink");
    for (int trial = 0; trial < warmups + runs; trial++) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new org.apache.flink.configuration.Configuration(), 1, 0);
        var tables = StreamTableEnvironment.create(env);
        var source =
            env.fromSequence(0, count - 1)
                .map(
                    row -> {
                      Integer[] values = null;
                      if (row % 7 != 0) {
                        values = new Integer[8];
                        for (int i = 0; i < values.length; i++)
                          if (i % 5 != 0) values[i] = (int) ((row + i * 7) % 17 - 8);
                      }
                      return Row.of(
                          values,
                          row % 11 == 0 ? null : row % 2 == 0,
                          row % 13 == 0 ? null : row % 4 < 2);
                    })
                .returns(
                    Types.ROW_NAMED(
                        new String[] {"a", "up", "nf"},
                        Types.OBJECT_ARRAY(Types.INT),
                        Types.BOOLEAN,
                        Types.BOOLEAN));
        tables.createTemporaryView(
            "inputs",
            source,
            Schema.newBuilder()
                .column("a", DataTypes.ARRAY(DataTypes.INT()))
                .column("up", DataTypes.BOOLEAN())
                .column("nf", DataTypes.BOOLEAN())
                .build());
        tables.executeSql("CREATE TABLE output_rows(v ARRAY<INT>) WITH ('connector'='blackhole')");
        String sql =
            "INSERT INTO output_rows SELECT ARRAY_SORT(ARRAY_SLICE(a,1,31),up,nf) FROM inputs";
        if (nativeRun) {
          var scan = NativePlanner.install(tables);
          String plan = tables.explainSql(sql);
          assertTrue(plan.contains("NativeCalc"), plan);
          assertTrue(plan.contains("RowDataToArrow"), plan);
          assertTrue(plan.contains("ArrowToRowData"), plan);
          assertTrue(
              scan.functionExecutions().stream()
                  .anyMatch(
                      route ->
                          route.contains("ARRAY_SORT")
                              && (engine.equals("native") == route.contains("jvm=[]"))),
              scan.functionExecutions().toString());
        }
        long start = System.nanoTime();
        tables.executeSql(sql).await();
        double seconds = (System.nanoTime() - start) / 1e9;
        if (nativeRun)
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        System.out.printf(
            Locale.ROOT,
            "[runtime-array-sort-pipeline] engine=%s trial=%d warmup=%s rows=%d seconds=%.9f%n",
            engine,
            trial,
            trial < warmups,
            count,
            seconds);
      }
    }
  }
}
