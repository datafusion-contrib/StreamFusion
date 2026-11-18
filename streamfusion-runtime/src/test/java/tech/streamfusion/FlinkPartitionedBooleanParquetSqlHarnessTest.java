package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkPartitionedBooleanParquetSqlHarnessTest {
  private String previousLatency;

  @BeforeEach
  void useSizeDrivenBatches() {
    previousLatency = System.getProperty("streamfusion.transpose.flushLatencyMs");
    System.setProperty("streamfusion.transpose.flushLatencyMs", "0");
  }

  @AfterEach
  void restoreLatency() {
    if (previousLatency == null) System.clearProperty("streamfusion.transpose.flushLatencyMs");
    else System.setProperty("streamfusion.transpose.flushLatencyMs", previousLatency);
  }

  static Stream<Arguments> slices() {
    return Stream.of(1, 7, 8, 9, 63, 64, 65)
        .flatMap(
            offset ->
                Stream.of(false, true)
                    .flatMap(
                        booleanPartition ->
                            Stream.of(false, true)
                                .map(reverse -> Arguments.of(offset, booleanPartition, reverse))));
  }

  @ParameterizedTest
  @MethodSource("slices")
  void typedReadbackAndDirectoriesPreserveSlicedBooleans(
      int firstLength, boolean booleanPartition, boolean reverse) throws Exception {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < firstLength + 17; i++) {
      boolean first = (i < firstLength) ^ reverse;
      rows.add(
          Row.of(
              i,
              booleanPartition ? first : first ? "a" : "b",
              i % 11 == 3 ? null : i % 2 == 0,
              i % 13 == 9 ? null : "row-" + i));
    }
    verify(rows, booleanPartition);
  }

  @Test
  void twoRowsPreserveBothBooleanPartitions() throws Exception {
    verify(List.of(Row.of(0, true, true, "first"), Row.of(1, false, false, "second")), true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void alternatingBooleanPartitionsRemainCorrectAcrossManyBatches(boolean reverse)
      throws Exception {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 40_960; i++)
      rows.add(Row.of(i, (i % 2 == 0) ^ reverse, i % 11 == 3 ? null : i % 3 == 0, "row-" + i));
    verify(rows, true);
  }

  private static void verify(List<Row> input, boolean booleanPartition) throws Exception {
    Path host = Files.createTempDirectory("boolean-parquet-host");
    Path nativeOutput = Files.createTempDirectory("boolean-parquet-native");
    try (var metrics = new RecoveryExecutionMetrics()) {
      write(metrics, host, input, booleanPartition, false);
      write(metrics, nativeOutput, input, booleanPartition, true);
      List<List<Object>> expected = new ArrayList<>();
      for (Row row : input) expected.add(fields(row));
      expected.sort(Comparator.comparingInt(row -> (Integer) row.get(0)));
      assertEquals(expected, read(metrics, host, booleanPartition));
      assertEquals(expected, read(metrics, nativeOutput, booleanPartition));
      Set<String> expectedDirectories =
          booleanPartition ? Set.of("p=true", "p=false") : Set.of("p=a", "p=b");
      assertEquals(expectedDirectories, directories(host));
      assertEquals(expectedDirectories, directories(nativeOutput));
    }
  }

  private static String columns(boolean booleanPartition) {
    return "id INT, p " + (booleanPartition ? "BOOLEAN" : "STRING") + ", flag BOOLEAN, note STRING";
  }

  private static void write(
      RecoveryExecutionMetrics metrics,
      Path directory,
      List<Row> input,
      boolean booleanPartition,
      boolean nativeEnabled)
      throws Exception {
    var env = metrics.environment(new Configuration(), 1, 0);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.native.enabled", Boolean.toString(nativeEnabled));
    table.getConfig().set("streamfusion.transpose.batchRows", "1024");
    table.createTemporaryView(
        "src",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "p", "flag", "note"},
                Types.INT,
                booleanPartition ? Types.BOOLEAN : Types.STRING,
                Types.BOOLEAN,
                Types.STRING),
            input.toArray(new Row[0])));
    table.executeSql(
        "CREATE TABLE result_rows ("
            + columns(booleanPartition)
            + ") PARTITIONED BY (p) WITH ('connector'='filesystem','format'='parquet','path'='"
            + directory.toUri()
            + "')");
    if (nativeEnabled) NativePlanner.install(table);
    String sql = "INSERT INTO result_rows SELECT * FROM src";
    if (nativeEnabled) {
      String plan = table.explainSql(sql);
      assertTrue(plan.contains("NativeParquetSink"), plan);
    }
    var result = table.executeSql(sql);
    result.await();
    if (nativeEnabled) {
      String job = result.getJobClient().orElseThrow().getJobID().toString();
      assertTrue(
          metrics.recordsIn(job, "native-parquet-partition-split") > 0,
          "native partition splitter must consume input");
    }
  }

  private static List<List<Object>> read(
      RecoveryExecutionMetrics metrics, Path directory, boolean booleanPartition) throws Exception {
    var table = StreamTableEnvironment.create(metrics.environment(new Configuration(), 1, 0));
    table.getConfig().set("streamfusion.native.enabled", "false");
    table.executeSql(
        "CREATE TABLE result_rows ("
            + columns(booleanPartition)
            + ") PARTITIONED BY (p) WITH ('connector'='filesystem','format'='parquet','path'='"
            + directory.toUri()
            + "')");
    List<List<Object>> rows = new ArrayList<>();
    try (var iterator = table.executeSql("SELECT * FROM result_rows").collect()) {
      while (iterator.hasNext()) rows.add(fields(iterator.next()));
    }
    rows.sort(Comparator.comparingInt(row -> (Integer) row.get(0)));
    return rows;
  }

  private static List<Object> fields(Row row) {
    List<Object> values = new ArrayList<>();
    for (int i = 0; i < row.getArity(); i++) values.add(row.getField(i));
    return values;
  }

  private static Set<String> directories(Path directory) throws Exception {
    try (var paths = Files.list(directory)) {
      return paths
          .filter(Files::isDirectory)
          .map(path -> path.getFileName().toString())
          .collect(java.util.stream.Collectors.toCollection(TreeSet::new));
    }
  }
}
