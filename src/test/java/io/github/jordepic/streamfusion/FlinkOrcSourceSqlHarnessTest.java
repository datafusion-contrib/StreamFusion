package io.github.jordepic.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

/**
 * The native ORC source reads the same data the host's filesystem+orc source does. ORC files are
 * self-describing, so the reader derives the schema from each file; this covers the unambiguous scalar
 * types (decimal/timestamp parity is tracked separately).
 */
class FlinkOrcSourceSqlHarnessTest {

  @Test
  void nativeOrcSourceMatchesHost() throws Exception {
    // An Orc input directory both sources read (written once by the host).
    Path input = Files.createTempDirectory("orc-source-in");
    writeInput(input);

    assertEquals(sorted(readBack(input, false)), sorted(readBack(input, true)));
  }

  /**
   * Writes the input directory via the host filesystem+orc sink. Runs in batch mode so the sink
   * commits its files at job completion — the streaming filesystem sink commits only on checkpoint,
   * which the ORC writer does not drive to completion in this harness.
   */
  private static void writeInput(Path directory) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"k", "n", "name", "ratio", "flag"},
                Types.LONG,
                Types.INT,
                Types.STRING,
                Types.DOUBLE,
                Types.BOOLEAN),
            Row.of(1L, 10, "a", 1.5, true),
            Row.of(2L, 20, "b", 2.5, false),
            Row.of(3L, 30, "c", 3.5, true),
            Row.of(4L, 40, "d", 4.5, false));
    tEnv.createTemporaryView(
        "s",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("n", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .column("ratio", DataTypes.DOUBLE())
            .column("flag", DataTypes.BOOLEAN())
            .build());
    tEnv.executeSql(orcTable("in_write", directory));
    tEnv.executeSql("INSERT INTO in_write SELECT * FROM s").await();
  }

  private static List<List<Object>> readBack(Path directory, boolean useNative) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(orcTable("r", directory));
    PhysicalPlanScan scan = useNative ? NativePlanner.install(tEnv) : null;
    List<List<Object>> rows = new ArrayList<>();
    try (CloseableIterator<Row> iterator = tEnv.executeSql("SELECT * FROM r").collect()) {
      while (iterator.hasNext()) {
        Row row = iterator.next();
        List<Object> fields = new ArrayList<>(row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
          fields.add(row.getField(i));
        }
        rows.add(fields);
      }
    }
    if (useNative) {
      assertTrue(scan.substitutions() >= 1, "source did not route to native");
    }
    return rows;
  }

  private static String orcTable(String name, Path directory) {
    return "CREATE TABLE "
        + name
        + " (k BIGINT, n INT, name STRING, ratio DOUBLE, flag BOOLEAN)"
        + " WITH ('connector' = 'filesystem', 'path' = '"
        + directory.toUri()
        + "', 'format' = 'orc')";
  }

  private static List<List<Object>> sorted(List<List<Object>> rows) {
    rows.sort(Comparator.comparing(Object::toString));
    return rows;
  }
}
