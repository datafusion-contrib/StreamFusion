package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;
import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;

/**
 * A PARTITIONED BY table through the native sink matches the host end to end: the same rows read
 * back, the same partition directories, the same _SUCCESS markers from the success-file commit
 * policy, and the same Parquet schema in the footers (decimals as minimal fixed-width binaries,
 * INT64 timestamps) — across a type matrix including a pre-1970 sub-unit timestamp, where floor
 * semantics rather than truncation decide the written value.
 */
class FlinkPartitionedParquetSinkSqlHarnessTest {

  private static final String COLUMNS =
      "dt STRING, v INT, price DECIMAL(10, 2), big DECIMAL(38, 10), dy DATE, ts TIMESTAMP(6)";

  @Test
  void partitionedNativeParquetSinkMatchesHost() throws Exception {
    Path hostDirectory = Files.createTempDirectory("psink-host");
    Path nativeDirectory = Files.createTempDirectory("psink-native");

    writeInsert(hostDirectory, false);
    writeInsert(nativeDirectory, true);

    assertEquals(sorted(readBack(hostDirectory)), sorted(readBack(nativeDirectory)));
    assertEquals(partitionDirectories(hostDirectory), partitionDirectories(nativeDirectory));
    assertEquals(successFiles(hostDirectory), successFiles(nativeDirectory));
    assertTrue(successFiles(nativeDirectory).contains("dt=a"), "success marker missing");
    assertEquals(footerSchema(hostDirectory), footerSchema(nativeDirectory));
  }

  private static void writeInsert(Path directory, boolean useNative) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    tEnv.executeSql(
        "CREATE TABLE pq ("
            + COLUMNS
            + ") PARTITIONED BY (dt) WITH ("
            + "'connector' = 'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet',"
            + "'sink.partition-commit.policy.kind' = 'success-file',"
            + "'parquet.write.int64.timestamp' = 'true',"
            + "'parquet.utc-timezone' = 'true',"
            + "'parquet.timestamp.time.unit' = 'millis')");

    // Rows arrive from a DataStream view rather than SQL VALUES so the insert plans as a bare
    // source→sink (a VALUES Calc with DATE literals would keep the plan on the host and moot the
    // test). The 1969-12-31T23:59:59.998500 timestamp lands between milliseconds: written at
    // millis precision it must floor to .998, not truncate toward zero to .999.
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"dt", "v", "price", "big", "dy", "ts"},
                Types.STRING,
                Types.INT,
                Types.BIG_DEC,
                Types.BIG_DEC,
                Types.LOCAL_DATE,
                Types.LOCAL_DATE_TIME),
            Row.of(
                "a",
                1,
                new BigDecimal("12.34"),
                new BigDecimal("1234567890.0123456789"),
                LocalDate.of(2024, 5, 1),
                LocalDateTime.of(2024, 5, 1, 12, 0, 0, 123_456_000)),
            Row.of(
                "a",
                2,
                new BigDecimal("-99999999.99"),
                new BigDecimal("-1.0000000000"),
                LocalDate.of(1969, 1, 1),
                LocalDateTime.of(1969, 12, 31, 23, 59, 59, 998_500_000)),
            // No null partition value here: Flink's own partitioned SOURCE cannot read a
            // __DEFAULT_PARTITION__ directory back (its path re-generation throws on the restored
            // null), in either engine. Null routing parity is covered at the operator level, where
            // both engines name the directory with the same Flink code.
            Row.of(
                "b",
                3,
                new BigDecimal("0.01"),
                new BigDecimal("0.0000000000"),
                LocalDate.of(2024, 5, 2),
                LocalDateTime.of(2024, 5, 2, 0, 0)));
    tEnv.createTemporaryView(
        "s",
        source,
        Schema.newBuilder()
            .column("dt", DataTypes.STRING())
            .column("v", DataTypes.INT())
            .column("price", DataTypes.DECIMAL(10, 2))
            .column("big", DataTypes.DECIMAL(38, 10))
            .column("dy", DataTypes.DATE())
            .column("ts", DataTypes.TIMESTAMP(6))
            .build());

    PhysicalPlanScan scan = useNative ? NativePlanner.install(tEnv) : null;
    tEnv.executeSql("INSERT INTO pq SELECT * FROM s").await();
    if (useNative) {
      assertTrue(
          scan.substitutions() > 0,
          () -> "sink did not route to native; reasons=" + scan.fallbackReasons());
    }
  }

  private static List<List<Object>> readBack(Path directory) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql(
        "CREATE TABLE r ("
            + COLUMNS
            + ") PARTITIONED BY (dt) WITH ('connector' = 'filesystem', 'path' = '"
            + directory.toUri()
            + "', 'format' = 'parquet',"
            + "'parquet.write.int64.timestamp' = 'true',"
            + "'parquet.utc-timezone' = 'true',"
            + "'parquet.timestamp.time.unit' = 'millis')");

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

  private static TreeSet<String> partitionDirectories(Path directory) {
    TreeSet<String> partitions = new TreeSet<>();
    File[] children = directory.toFile().listFiles(File::isDirectory);
    if (children != null) {
      for (File child : children) {
        partitions.add(child.getName());
      }
    }
    return partitions;
  }

  private static TreeSet<String> successFiles(Path directory) {
    TreeSet<String> marked = new TreeSet<>();
    File[] children = directory.toFile().listFiles(File::isDirectory);
    if (children != null) {
      for (File child : children) {
        if (new File(child, "_SUCCESS").exists()) {
          marked.add(child.getName());
        }
      }
    }
    return marked;
  }

  /** The Parquet message type of one committed part file — identical schemas prove type parity. */
  private static String footerSchema(Path directory) throws Exception {
    File partition = new File(directory.toFile(), "dt=a");
    File[] parts = partition.listFiles((dir, name) -> !name.startsWith(".") && !name.startsWith("_"));
    assertTrue(parts != null && parts.length > 0, "no committed part file under dt=a");
    try (ParquetFileReader reader =
        ParquetFileReader.open(
            HadoopInputFile.fromPath(
                new org.apache.hadoop.fs.Path(parts[0].toURI()),
                new org.apache.hadoop.conf.Configuration()))) {
      return reader.getFileMetaData().getSchema().toString();
    }
  }

  private static List<List<Object>> sorted(List<List<Object>> rows) {
    rows.sort(Comparator.comparing(Object::toString));
    return rows;
  }
}
