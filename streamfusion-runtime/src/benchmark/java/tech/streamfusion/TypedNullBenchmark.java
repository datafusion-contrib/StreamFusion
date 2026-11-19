package tech.streamfusion;

import java.util.Arrays;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

/** Folded NULL projections alongside runtime arithmetic, including both row/Arrow transposes. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class TypedNullBenchmark {
  private static final long ROWS = Long.getLong("typedNull.rows", 2_000_000L);
  private static final int WARMUP = Integer.getInteger("typedNull.warmup", 2);
  private static final int RUNS = Integer.getInteger("typedNull.runs", 5);

  @Test
  void compareFoldedProjections() throws Exception {
    for (String type : new String[] {"DECIMAL(38,18)", "MAP<STRING, ARRAY<DECIMAL(38,18)>>"}) {
      TableEnvironment tables = inputs();
      String plan = NativePlanner.explain(tables, prepare(tables, type));
      if (!plan.contains("NativeCalc")
          || !plan.contains("RowDataToArrow")
          || !plan.contains("ArrowToRowData")) {
        throw new IllegalStateException("Missing native Calc or transpose: " + plan);
      }
      double[][] times = new double[2][RUNS];
      for (int trial = 0; trial < WARMUP + RUNS; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          int engine = (trial + turn) % 2;
          TableEnvironment run = inputs();
          String sql = prepare(run, type);
          PhysicalPlanScan scan = engine == 1 ? NativePlanner.install(run) : null;
          long start = System.nanoTime();
          run.executeSql(sql).await();
          double elapsed = (System.nanoTime() - start) / 1e9;
          if (scan != null && scan.substitutions() == 0)
            throw new IllegalStateException(scan.fallbackReasons().toString());
          if (trial >= WARMUP) times[engine][trial - WARMUP] = elapsed;
        }
      }
      System.out.printf(
          Locale.ROOT,
          "[typed-null] type=%s rows=%d Flink=%.6fs Native=%.6fs flink_trials=%s"
              + " native_trials=%s%n",
          type,
          ROWS,
          median(times[0]),
          median(times[1]),
          Arrays.toString(times[0]),
          Arrays.toString(times[1]));
    }
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  private static String prepare(TableEnvironment tables, String type) {
    tables.executeSql(
        "CREATE TABLE sink (id BIGINT, v " + type + ") WITH ('connector' = 'blackhole')");
    return "INSERT INTO sink SELECT id + 1, CAST(NULL AS " + type + ") FROM inputs";
  }

  private static TableEnvironment inputs() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    tables.createTemporaryView(
        "inputs",
        env.fromSequence(0, ROWS - 1)
            .map(id -> Row.of(id))
            .returns(Types.ROW_NAMED(new String[] {"id"}, Types.LONG)),
        Schema.newBuilder().column("id", DataTypes.BIGINT()).build());
    return tables;
  }
}
