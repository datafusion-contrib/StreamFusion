package tech.streamfusion;

import java.math.BigDecimal;
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

/** Interleaved single-expression measurements, including both row/Arrow boundaries. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class DecimalExpressionBenchmark {
  private static final long ROWS = Long.getLong("decimal.rows", 2_000_000L);
  private static final int WARMUP = Integer.getInteger("decimal.warmup", 2);
  private static final int RUNS = Integer.getInteger("decimal.runs", 5);

  @Test
  void compareExpressions() throws Exception {
    for (boolean wide : new boolean[] {false, true}) {
      for (String name : new String[] {"CAST", "ADD", "SUBTRACT", "MULTIPLY"}) {
        String expression;
        switch (name) {
          case "CAST":
            expression = wide ? "CAST(a AS DECIMAL(28, 6))" : "CAST(a AS DECIMAL(12, 2))";
            break;
          case "ADD":
            expression = "a + b";
            break;
          case "SUBTRACT":
            expression = "a - b";
            break;
          case "MULTIPLY":
            expression = "a * b";
            break;
          default:
            throw new IllegalArgumentException(name);
        }
        TableEnvironment check = environment(wide);
        String sql = prepare(check, expression);
        String plan = NativePlanner.explain(check, sql);
        if (!plan.contains("NativeCalc")
            || !plan.contains("RowDataToArrow")
            || !plan.contains("ArrowToRowData")) {
          throw new IllegalStateException("Both transposes must be measured: " + plan);
        }
        double[][] seconds = new double[2][RUNS];
        for (int trial = 0; trial < WARMUP + RUNS; trial++) {
          for (int turn = 0; turn < 2; turn++) {
            int engine = (trial + turn) % 2;
            double elapsed = run(wide, expression, engine == 1);
            if (trial >= WARMUP) {
              seconds[engine][trial - WARMUP] = elapsed;
            }
          }
        }
        System.out.printf(
            Locale.ROOT,
            "[decimal] %s input=%s rows=%d Flink=%.6fs Native=%.6fs flink_trials=%s"
                + " native_trials=%s%n",
            name,
            wide ? "DECIMAL(38,20)" : "DECIMAL(18,3)",
            ROWS,
            median(seconds[0]),
            median(seconds[1]),
            Arrays.toString(seconds[0]),
            Arrays.toString(seconds[1]));
      }
    }
  }

  private static String prepare(TableEnvironment table, String expression) {
    String select = "SELECT " + expression + " AS v, TRUE AS anchor FROM inputs";
    String type =
        table
            .sqlQuery(select)
            .getResolvedSchema()
            .getColumnDataTypes()
            .get(0)
            .getLogicalType()
            .asSerializableString();
    table.executeSql(
        "CREATE TABLE sink (v " + type + ", anchor BOOLEAN) WITH ('connector' = 'blackhole')");
    return "INSERT INTO sink " + select;
  }

  private static double run(boolean wide, String expression, boolean nativeRun) throws Exception {
    TableEnvironment table = environment(wide);
    String sql = prepare(table, expression);
    PhysicalPlanScan scan = nativeRun ? NativePlanner.install(table) : null;
    long start = System.nanoTime();
    table.executeSql(sql).await();
    double seconds = (System.nanoTime() - start) / 1e9;
    if (nativeRun && scan.substitutions() == 0) {
      throw new IllegalStateException("Unexpected fallback: " + scan.fallbackReasons());
    }
    return seconds;
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    int middle = sorted.length / 2;
    return sorted.length % 2 == 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
  }

  private static TableEnvironment environment(boolean wide) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    String[] samples =
        wide
            ? new String[] {
              "123456789012345678.12345678901234567890",
              "-987654321098765432.98765432109876543210",
              "0.12345678901234567895",
              "0"
            }
            : new String[] {"12345.678", "-98765.435", "0.005", "0"};
    BigDecimal[] values = Arrays.stream(samples).map(BigDecimal::new).toArray(BigDecimal[]::new);
    table.createTemporaryView(
        "inputs",
        env.fromSequence(0, ROWS - 1)
            .map(i -> Row.of(i % 16 == 0 ? null : values[(int) (i % 4)], values[(int) (i / 4 % 4)]))
            .returns(Types.ROW_NAMED(new String[] {"a", "b"}, Types.BIG_DEC, Types.BIG_DEC)),
        Schema.newBuilder()
            .column("a", DataTypes.DECIMAL(wide ? 38 : 18, wide ? 20 : 3))
            .column("b", DataTypes.DECIMAL(wide ? 38 : 18, wide ? 20 : 3))
            .build());
    return table;
  }
}
