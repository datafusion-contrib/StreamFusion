package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

/** The native Parquet source reads the same data the host's filesystem+parquet source does. */
class FlinkParquetSourceSqlHarnessTest {

  @Test
  void nativeParquetSourceMatchesHost() throws Exception {
    // A Parquet input directory both sources read (written once by the host).
    Path input = Files.createTempDirectory("source-in");
    writeInput(input);

    Path hostOut = Files.createTempDirectory("source-host-out");
    Path nativeOut = Files.createTempDirectory("source-native-out");
    copy(input, hostOut, false);
    copy(input, nativeOut, true);

    assertEquals(sorted(readBack(hostOut)), sorted(readBack(nativeOut)));
  }

  /** Writes the input directory via the host filesystem+parquet sink. */
  private static void writeInput(Path directory) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.INT),
            Row.of(1L, 10),
            Row.of(2L, 20),
            Row.of(3L, 30),
            Row.of(4L, 40));
    tEnv.createTemporaryView(
        "s",
        source,
        Schema.newBuilder().column("k", DataTypes.BIGINT()).column("v", DataTypes.INT()).build());
    tEnv.executeSql(parquetTable("in_write", directory));
    tEnv.executeSql("INSERT INTO in_write SELECT * FROM s").await();
  }

  /** Runs {@code INSERT INTO out SELECT * FROM in} over filesystem Parquet tables. */
  private static void copy(Path input, Path output, boolean useNative) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(parquetTable("in_read", input));
    tEnv.executeSql(parquetTable("out_write", output));
    PhysicalPlanScan scan = useNative ? NativePlanner.install(tEnv) : null;
    tEnv.executeSql("INSERT INTO out_write SELECT * FROM in_read").await();
    if (useNative) {
      // Both the source and the sink route to native.
      assertTrue(scan.substitutions() >= 2, "source and sink did not route to native");
    }
  }

  private static List<List<Object>> readBack(Path directory) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(parquetTable("r", directory));
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
    return rows;
  }

  private static String parquetTable(String name, Path directory) {
    return "CREATE TABLE "
        + name
        + " (k BIGINT, v INT) WITH ('connector' = 'filesystem', 'path' = '"
        + directory.toUri()
        + "', 'format' = 'parquet')";
  }

  private static List<List<Object>> sorted(List<List<Object>> rows) {
    rows.sort(Comparator.comparing(Object::toString));
    return rows;
  }
}
