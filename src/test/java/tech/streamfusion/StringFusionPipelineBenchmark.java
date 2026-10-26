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

/** Full bounded jobs, including the row source, both transposes and the blackhole row sink. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class StringFusionPipelineBenchmark {
  @Test
  void boundedRowSourceToRowSink() throws Exception {
    int count = Integer.getInteger("fusion.pipeline.rows", 2_000_000);
    int warmups = Integer.getInteger("fusion.pipeline.warmups", 2);
    int runs = Integer.getInteger("fusion.pipeline.runs", 5);
    int minLength = Integer.getInteger("fusion.pipeline.minLength", 1);
    String engines = System.getProperty("fusion.pipeline.engines", "both");
    String expression = System.getProperty("fusion.pipeline.expression", "LPAD(s,n,'X') = 'a?'");
    String base = "a?" + "ab?c".repeat(64);
    for (int trial = 0; trial < warmups + runs; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        boolean nativeRun = ((trial + turn) & 1) == 1;
        if (engines.equals("stock") && nativeRun || engines.equals("native") && !nativeRun)
          continue;
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        var tables = StreamTableEnvironment.create(env);
        tables
            .getConfig()
            .getConfiguration()
            .setString("pipeline.operator-chaining.enabled", "true");
        var source =
            env.fromSequence(0, count - 1)
                .map(
                    i ->
                        Row.of(
                            i % 7 == 0 ? null : base + "|" + (i % 16), minLength + (int) (i % 3)))
                .returns(Types.ROW_NAMED(new String[] {"s", "n"}, Types.STRING, Types.INT));
        tables.createTemporaryView(
            "inputs",
            source,
            Schema.newBuilder()
                .column("s", DataTypes.STRING())
                .column("n", DataTypes.INT())
                .build());
        tables.executeSql("CREATE TABLE output_rows(v BOOLEAN) WITH ('connector'='blackhole')");
        String query = "SELECT " + expression + " FROM inputs";
        String statement = "INSERT INTO output_rows " + query;
        String plan =
            nativeRun ? NativePlanner.explain(tables, statement) : tables.explainSql(statement);
        if (nativeRun) {
          assertTrue(plan.contains("NativeCalc"), plan);
          assertTrue(plan.contains("RowDataToArrow"), plan);
          assertTrue(plan.contains("ArrowToRowData"), plan);
        }
        long start = System.nanoTime();
        tables.executeSql(statement).await();
        System.out.printf(
            Locale.ROOT,
            "[string-fusion-pipeline] engine=%s trial=%d warmup=%s rows=%d seconds=%.9f%n",
            nativeRun ? "native" : "flink",
            trial,
            trial < warmups,
            count,
            (System.nanoTime() - start) / 1e9);
      }
    }
  }
}
