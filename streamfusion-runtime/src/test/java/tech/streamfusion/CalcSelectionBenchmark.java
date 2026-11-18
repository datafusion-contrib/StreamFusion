package tech.streamfusion;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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

/** Selective Calc diagnostics with rowwise source/sink and a matching full-payload control. */
@EnabledIfEnvironmentVariable(named = "SF_BENCHMARK", matches = "true")
class CalcSelectionBenchmark {
  private static final long ROWS = Long.getLong("selection.rows", 2_000_000L);
  private static final int WARMUP = Integer.getInteger("selection.warmup", 2);
  private static final int RUNS = Integer.getInteger("selection.runs", 5);

  @Test
  void selectivePayloads() throws Exception {
    if (ROWS < 1 || WARMUP < 0 || RUNS < 1)
      throw new IllegalArgumentException("Invalid trial sizes");
    String engine = System.getProperty("selection.engine", "both");
    if (!List.of("both", "flink", "native").contains(engine))
      throw new IllegalArgumentException("Invalid engine");
    List<String> csv =
        new ArrayList<>(List.of("bytes,selectivity,projection,rows,engine,trial,seconds"));
    Path output = Path.of(System.getProperty("selection.output", "target/calc-selection.csv"));
    if (output.getParent() != null) Files.createDirectories(output.getParent());
    for (String size : System.getProperty("selection.bytes", "32,256,4096").split(",")) {
      int bytes = Integer.parseInt(size);
      if (bytes < 0) throw new IllegalArgumentException("Negative payload size");
      for (String select : System.getProperty("selection.percent", "0,1,10,50,100").split(",")) {
        int percent = Integer.parseInt(select);
        if (percent < 0 || percent > 100) throw new IllegalArgumentException("Invalid selectivity");
        for (String projection : List.of("identity", "substring")) {
          String sql = sql(percent, projection);
          String plan = NativePlanner.explain(environment(bytes, percent), sql);
          for (String required : List.of("NativeCalc", "RowDataToArrow", "ArrowToRowData")) {
            if (!plan.contains(required))
              throw new IllegalStateException(required + " missing: " + plan);
          }
          for (int trial = 0; trial < WARMUP + RUNS; trial++) {
            for (int turn = 0; turn < 2; turn++) {
              boolean nativeRun = (trial + turn) % 2 == 1;
              String name = nativeRun ? "native" : "flink";
              if (!engine.equals("both") && !engine.equals(name)) continue;
              TableEnvironment tables = environment(bytes, percent);
              var scan = nativeRun ? NativePlanner.install(tables) : null;
              long start = System.nanoTime();
              tables.executeSql(sql).await();
              double seconds = (System.nanoTime() - start) / 1e9;
              if (scan != null && scan.substitutions() == 0)
                throw new IllegalStateException(scan.fallbackReasons().toString());
              if (trial >= WARMUP) {
                String row =
                    String.format(
                        Locale.ROOT,
                        "%d,%d,%s,%d,%s,%d,%.6f",
                        bytes,
                        percent,
                        projection,
                        ROWS,
                        name,
                        trial - WARMUP,
                        seconds);
                csv.add(row);
                System.out.println("CALC_SELECTION," + row);
                Files.write(output, csv);
              }
            }
          }
        }
      }
    }
  }

  private static String sql(int percent, String projection) {
    return "INSERT INTO sink SELECT id, "
        + (projection.equals("identity") ? "payload" : "SUBSTRING(payload, 1, 32)")
        + ", amount + CAST(1 AS DECIMAL(20,2)) FROM src WHERE MOD(id, 100) < "
        + percent;
  }

  private static TableEnvironment environment(int bytes, int percent) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var tables = StreamTableEnvironment.create(env);
    String payload = "x".repeat(bytes);
    tables.createTemporaryView(
        "src",
        env.fromSequence(0, ROWS - 1)
            .map(
                i ->
                    Row.of(
                        percent != 100 && i % 13 == 0 ? null : (int) (i % Integer.MAX_VALUE),
                        i % 7 == 0 ? null : payload,
                        i % 11 == 0 ? null : BigDecimal.valueOf(i % 100000, 2)))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"id", "payload", "amount"},
                    Types.INT,
                    Types.STRING,
                    Types.BIG_DEC)),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("payload", DataTypes.STRING())
            .column("amount", DataTypes.DECIMAL(20, 2))
            .build());
    tables.executeSql(
        "CREATE TABLE sink (id INT, payload STRING, amount DECIMAL(21,2)) WITH ('connector' ="
            + " 'blackhole')");
    return tables;
  }
}
