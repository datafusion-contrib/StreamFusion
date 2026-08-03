package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

/**
 * Confirms the host's filesystem+parquet sink resolves and writes on the test classpath — the
 * baseline the native sink is measured against. Establishes the dependency footprint (the connector,
 * the Parquet format, and the Hadoop libraries the format pulls in) before the comparison is built.
 */
class FlinkParquetSinkSmokeTest {

  @Test
  void hostWritesParquetFiles() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100); // the filesystem sink commits files on checkpoint
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.INT),
            Row.of(1L, 10),
            Row.of(2L, 20),
            Row.of(3L, 30));
    tEnv.createTemporaryView(
        "s",
        source,
        Schema.newBuilder().column("k", DataTypes.BIGINT()).column("v", DataTypes.INT()).build());

    Path directory = Files.createTempDirectory("flink-parquet");
    tEnv.executeSql(
        "CREATE TABLE pq (k BIGINT, v INT) WITH ('connector' = 'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");
    tEnv.executeSql("INSERT INTO pq SELECT * FROM s").await();

    // Flink's filesystem sink commits files named `part-<uuid>-<n>` (no extension), possibly nested.
    try (Stream<Path> tree = Files.walk(directory)) {
      long committed =
          tree.filter(Files::isRegularFile)
              .filter(path -> path.getFileName().toString().startsWith("part-"))
              .count();
      assertTrue(committed > 0, "host should have written Parquet part files");
    }
  }
}
