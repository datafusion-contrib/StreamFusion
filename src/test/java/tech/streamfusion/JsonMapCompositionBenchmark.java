package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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

@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class JsonMapCompositionBenchmark {
  private static final long ROWS = Long.getLong("jsonmap.rows", 2_000_000L);
  private static final int WARMUP = Integer.getInteger("jsonmap.warmup", 2);
  private static final int RUNS = Integer.getInteger("jsonmap.runs", 5);
  private static final String SQL =
      "INSERT INTO sink SELECT k, COUNT(*) FROM inputs "
          + "WHERE CHAR_LENGTH(JSON_STRING(m)) > 264 GROUP BY k";

  @Test
  void jsonFilterAndGroupedCount() throws Exception {
    String plan = NativePlanner.explain(environment(), SQL);
    for (String required :
        new String[] {
          "jsonEvaluation=[JVM]", "NativeColumnarGroupAggregate", "RowDataToArrow", "ArrowToRowData"
        }) {
      if (!plan.contains(required)) throw new IllegalStateException(plan);
    }
    double[][] times = new double[2][RUNS];
    List<String> csv = new ArrayList<>(List.of("engine,trial,rows,seconds"));
    for (int trial = 0; trial < WARMUP + RUNS; trial++) {
      for (int turn = 0; turn < 2; turn++) {
        int engine = (trial + turn) % 2;
        var table = environment();
        var scan = engine == 1 ? NativePlanner.install(table) : null;
        long start = System.nanoTime();
        table.executeSql(SQL).await();
        double seconds = (System.nanoTime() - start) / 1e9;
        if (scan != null && scan.substitutions() == 0)
          throw new IllegalStateException(scan.explainSummary());
        if (trial >= WARMUP) {
          times[engine][trial - WARMUP] = seconds;
          csv.add(
              String.format(
                  Locale.ROOT,
                  "%s,%d,%d,%.9f",
                  engine == 1 ? "native" : "flink",
                  trial - WARMUP,
                  ROWS,
                  seconds));
        }
      }
    }
    double host = median(times[0]);
    double nativeTime = median(times[1]);
    Files.write(
        Path.of(System.getProperty("jsonmap.output", "target/json-map-composition.csv")), csv);
    System.out.printf(
        Locale.ROOT,
        "[json-map-composition] rows=%d Flink=%.6fs Native=%.6fs ratio=%.3fx host_trials=%s"
            + " native_trials=%s%n",
        ROWS,
        host,
        nativeTime,
        host / nativeTime,
        Arrays.toString(times[0]),
        Arrays.toString(times[1]));
  }

  private static double median(double[] values) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  private static TableEnvironment environment() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    String[] payloads = {"a".repeat(264), "b".repeat(264)};
    var source =
        env.fromSequence(0, ROWS - 1)
            .map(
                i -> {
                  if (i % 7 == 0) return Row.of(i % 4096, null);
                  Map<String, String> map = new LinkedHashMap<>();
                  map.put("first", payloads[(int) (i % 2)]);
                  map.put("absent", null);
                  map.put("last", "tail");
                  return Row.of(i % 4096, map);
                })
            .returns(
                Types.ROW_NAMED(
                    new String[] {"k", "m"}, Types.LONG, Types.MAP(Types.STRING, Types.STRING)));
    table.createTemporaryView(
        "inputs",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("m", DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.STRING()))
            .build());
    table.executeSql(
        "CREATE TEMPORARY TABLE sink (k BIGINT, n BIGINT) WITH ('connector'='blackhole')");
    return table;
  }
}
