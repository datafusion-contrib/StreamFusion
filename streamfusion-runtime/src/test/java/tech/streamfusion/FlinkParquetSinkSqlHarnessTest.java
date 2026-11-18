package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

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
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;

/** The native Parquet sink writes the same data the host's filesystem+parquet sink does. */
class FlinkParquetSinkSqlHarnessTest {
  @Test
  void nativeParquetSinkMatchesHost() throws Exception {
    Path hostDirectory = Files.createTempDirectory("sink-host");
    Path nativeDirectory = Files.createTempDirectory("sink-native");

    writeInsert(hostDirectory, false);
    writeInsert(nativeDirectory, true);

    assertEquals(sorted(readBack(hostDirectory)), sorted(readBack(nativeDirectory)));
  }

  @Test
  void nestedRowKindsStayInsideExecutionAndOutOfTheFileSchema() throws Exception {
    var directories = new ArrayList<Path>();
    for (boolean nativeSink : new boolean[] {false, true}) {
      Path path = Files.createTempDirectory("nested-file-fields");
      directories.add(path);
      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      env.enableCheckpointing(100);
      var table = StreamTableEnvironment.create(env);
      var nested = Types.ROW_NAMED(new String[] {"n", "s"}, Types.LONG, Types.STRING);
      table.createTemporaryView(
          "nested_source",
          fromData(
              env,
              Types.ROW_NAMED(new String[] {"k", "r"}, Types.INT, nested),
              Row.of(1, Row.ofKind(org.apache.flink.types.RowKind.DELETE, 42L, "é🙂")),
              Row.of(2, Row.ofKind(org.apache.flink.types.RowKind.UPDATE_BEFORE, null, "")),
              Row.of(3, null)));
      table.executeSql(
          "CREATE TABLE nested_file(k INT, r ROW<n BIGINT, s STRING>) WITH "
              + "('connector'='filesystem', 'format'='parquet', 'path'='"
              + path.toUri()
              + "')");
      PhysicalPlanScan scan = nativeSink ? NativePlanner.install(table) : null;
      table.executeSql("INSERT INTO nested_file SELECT * FROM nested_source").await();
      if (scan != null) assertTrue(scan.substitutions() > 0, "Native file sink must execute");
      try (var files = Files.walk(path)) {
        for (Path file :
            files
                .filter(Files::isRegularFile)
                .filter(file -> file.getFileName().toString().startsWith("part-"))
                .collect(java.util.stream.Collectors.toList())) {
          try (var reader =
              org.apache.parquet.hadoop.ParquetFileReader.open(
                  org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                      new org.apache.hadoop.fs.Path(file.toUri()),
                      new org.apache.hadoop.conf.Configuration()))) {
            assertEquals(
                2,
                reader
                    .getFooter()
                    .getFileMetaData()
                    .getSchema()
                    .getType("r")
                    .asGroupType()
                    .getFieldCount());
          }
        }
      }
    }
    var results = new ArrayList<List<String>>();
    for (Path path : directories) {
      var table =
          StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
      table.getConfig().set("table.exec.resource.default-parallelism", "1");
      table.executeSql(
          "CREATE TABLE nested_file(k INT, r ROW<n BIGINT, s STRING>) WITH "
              + "('connector'='filesystem', 'format'='parquet', 'path'='"
              + path.toUri()
              + "')");
      var rows = new ArrayList<String>();
      try (var iterator = table.executeSql("SELECT * FROM nested_file").collect()) {
        while (iterator.hasNext()) rows.add(iterator.next().toString());
      }
      rows.sort(String::compareTo);
      assertEquals(3, rows.size());
      results.add(rows);
    }
    assertEquals(results.get(0), results.get(1));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void int96SqlSinkMatchesHost(boolean utc) throws Exception {
    Path stock = Files.createTempDirectory("int96-host");
    Path nativePath = Files.createTempDirectory("int96-native");
    writeTimestampInsert(stock, false, utc);
    writeTimestampInsert(nativePath, true, utc);
    assertEquals(timestampFileRows(stock), timestampFileRows(nativePath));
  }

  private static void writeTimestampInsert(Path directory, boolean useNative, boolean utc)
      throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    var table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "timestamps",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"id", "ts"}, Types.INT, Types.LOCAL_DATE_TIME),
            Row.of(1, java.time.LocalDateTime.parse("1969-12-31T23:59:59.123456789")),
            Row.of(2, java.time.LocalDateTime.parse("0001-01-01T00:00:00.999999999")),
            Row.of(3, (Object) null)),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("ts", DataTypes.TIMESTAMP(9))
            .build());
    table.executeSql(
        "CREATE TABLE pq (id INT, ts TIMESTAMP(9)) PARTITIONED BY (id) WITH ("
            + "'connector'='filesystem', 'format'='parquet', 'path'='"
            + directory.toUri()
            + "', 'parquet.timestamp.time.unit'='unused-for-int96', 'parquet.utc-timezone'='"
            + utc
            + "')");
    var scan = useNative ? NativePlanner.install(table) : null;
    table.executeSql("INSERT INTO pq SELECT * FROM timestamps").await();
    if (useNative)
      assertTrue(scan.substitutions() > 0, "INT96 sink fell back: " + scan.fallbackReasons());
  }

  private static List<String> timestampFileRows(Path directory) throws Exception {
    var rows = new ArrayList<String>();
    try (var files = Files.walk(directory)) {
      for (Path file :
          files
              .filter(Files::isRegularFile)
              .filter(f -> f.getFileName().toString().startsWith("part-"))
              .collect(ListCollectors.toList())) {
        try (var reader =
            org.apache.parquet.hadoop.ParquetReader.builder(
                    new org.apache.parquet.hadoop.example.GroupReadSupport(),
                    new org.apache.hadoop.fs.Path(file.toUri()))
                .build()) {
          org.apache.parquet.example.data.Group row;
          while ((row = reader.read()) != null)
            rows.add(file.getParent().getFileName() + ":" + row);
        }
      }
    }
    assertEquals(3, rows.size());
    rows.sort(String::compareTo);
    return rows;
  }

  private static void writeInsert(Path directory, boolean useNative) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100); // both sinks commit files on checkpoint
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    DataStream<Row> source =
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.INT),
            Row.of(1L, 10),
            Row.of(2L, 20),
            Row.of(3L, 30),
            Row.of(4L, 40));
    tEnv.createTemporaryView(
        "s",
        source,
        Schema.newBuilder().column("k", DataTypes.BIGINT()).column("v", DataTypes.INT()).build());
    tEnv.executeSql(
        "CREATE TABLE pq (k BIGINT, v INT) WITH ('connector' = 'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");

    PhysicalPlanScan scan = useNative ? NativePlanner.install(tEnv) : null;
    tEnv.executeSql("INSERT INTO pq SELECT * FROM s").await();
    if (useNative) {
      assertTrue(scan.substitutions() > 0, "sink did not route to native");
    }
  }

  private static List<List<Object>> readBack(Path directory) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE r (k BIGINT, v INT) WITH ('connector' = 'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet')");

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

  private static List<List<Object>> sorted(List<List<Object>> rows) {
    rows.sort(Comparator.comparing(Object::toString));
    return rows;
  }
}
