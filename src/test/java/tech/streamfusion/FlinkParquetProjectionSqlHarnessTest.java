package tech.streamfusion;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

/**
 * A projected read over the native Parquet source. Flink pushes the projection into the scan, so the
 * scan's schema is a subset (and possibly a reorder) of the file's columns; the native source must
 * emit exactly those columns in that order. Without honoring the projection it emits every file
 * column and the downstream column indices misalign — silently wrong when the misread column shares
 * a type. This pins that the source honors projection pushdown.
 */
class FlinkParquetProjectionSqlHarnessTest {

  @Test
  void projectionDroppingLeadingColumnMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("proj-in");
    writeInput(input);
    // SELECT v drops the leading key column k; the scan projects to [v].
    NativeParity.assertParity(() -> readEnvironment(input), "SELECT v FROM t");
  }

  @Test
  void projectionReorderingColumnsMatchesHost() throws Exception {
    Path input = Files.createTempDirectory("proj-reorder-in");
    writeInput(input);
    // Reordered projection [v, k] (file order is [k, v]).
    NativeParity.assertParity(() -> readEnvironment(input), "SELECT v, k FROM t");
  }

  private static void writeInput(Path directory) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(table(directory));
    tEnv.executeSql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)").await();
  }

  private static TableEnvironment readEnvironment(Path directory) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(table(directory));
    return tEnv;
  }

  private static String table(Path directory) {
    return "CREATE TABLE t (k BIGINT, v BIGINT) WITH ('connector' = 'filesystem', 'path' = '"
        + directory.toUri()
        + "', 'format' = 'parquet')";
  }
}
