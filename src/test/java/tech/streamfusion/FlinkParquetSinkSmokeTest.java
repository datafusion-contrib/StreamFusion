package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

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

  @Test
  void nativeWritesNestedStructListAndMap() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    Path directory = Files.createTempDirectory("native-nested-parquet");
    String columns =
        "id BIGINT, details ROW<name STRING, scores ARRAY<INT>>, tags MAP<STRING, BIGINT>";
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"id", "details", "tags"},
                Types.LONG,
                Types.ROW_NAMED(
                    new String[] {"name", "scores"},
                    Types.STRING,
                    Types.OBJECT_ARRAY(Types.INT)),
                Types.MAP(Types.STRING, Types.LONG)),
            Row.of(1L, Row.of("first", new Integer[] {1, 2}), Map.of("x", 10L)),
            Row.of(
                2L,
                Row.of(null, new Integer[] {3, null}),
                Collections.singletonMap("y", null)));
    tEnv.createTemporaryView(
        "nested_source",
        source,
        Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column(
                "details",
                DataTypes.ROW(
                    DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("scores", DataTypes.ARRAY(DataTypes.INT()))))
            .column("tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()))
            .build());
    tEnv.executeSql(
        "CREATE TABLE pq ("
            + columns
            + ") WITH ('connector'='filesystem', 'path'='"
            + directory.toUri()
            + "', 'format'='parquet')");
    PhysicalPlanScan scan = NativePlanner.install(tEnv);

    tEnv.executeSql("INSERT INTO pq SELECT * FROM nested_source").await();

    assertTrue(
        scan.substitutions() > 0,
        () -> "nested Parquet sink did not accelerate: " + scan.fallbackReasons());
    tEnv.executeSql(
        "CREATE TABLE read_back ("
            + columns
            + ") WITH ('connector'='filesystem', 'path'='"
            + directory.toUri()
            + "', 'format'='parquet')");
    List<Row> rows = new ArrayList<>();
    try (CloseableIterator<Row> results =
        tEnv.executeSql("SELECT * FROM read_back").collect()) {
      results.forEachRemaining(rows::add);
    }
    assertEquals(2, rows.size());
    assertEquals(
        java.util.Set.of(1L, 2L),
        rows.stream().map(row -> (Long) row.getField(0)).collect(java.util.stream.Collectors.toSet()));
  }
}
