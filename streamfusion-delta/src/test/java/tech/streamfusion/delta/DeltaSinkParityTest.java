package tech.streamfusion.delta;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.flink.sink.Conversions;
import io.delta.flink.sink.DeltaSinkConf;
import io.delta.flink.sink.DeltaWriterResult;
import io.delta.flink.table.HadoopTable;
import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.FileStatus;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Existing Delta SQL partition and merge-on-read scenarios through the native data-file writer. */
class DeltaSinkParityTest {

  private static final StructType DELTA_SCHEMA =
      new StructType(
          List.of(
              new StructField("id", LongType.LONG, false),
              new StructField("v", IntegerType.INTEGER, true),
              new StructField("dt", StringType.STRING, true)));

  @org.junit.jupiter.api.Test
  void nativeFilesPublishTypedDeltaStatistics() throws Exception {
    Path nativePath = Files.createTempDirectory("delta-native-stats");
    runAppend(nativePath, true);

    StringBuilder log = new StringBuilder();
    try (java.util.stream.Stream<Path> files = Files.list(nativePath.resolve("_delta_log"))) {
      for (Path file : files.filter(path -> path.toString().endsWith(".json")).sorted().toList()) {
        log.append(Files.readString(file));
      }
    }
    String commits = log.toString();
    assertTrue(commits.contains("\\\"minValues\\\":{\\\"id\\\":"), commits);
    assertTrue(commits.contains("\\\"maxValues\\\":{\\\"id\\\":"), commits);
    assertTrue(commits.contains("\\\"nullCount\\\":{\\\"id\\\":0"), commits);
  }

  @org.junit.jupiter.api.Test
  void partitionedMergeOnReadUsesPublishedStrategyAndKeepsJavaDeletionVectors() throws Exception {
    Path nativePath = Files.createTempDirectory("delta-native");
    createDeletionVectorTable(nativePath);

    runAppend(nativePath, true);
    runNativeArrowUpsert(nativePath);

    assertEquals(
        List.of(List.of(1L, 100, "a"), List.of(3L, 30, "b"), List.of(4L, 40, "b")),
        sorted(readLogicalRows(nativePath)));
    assertTrue(hasDeletionVector(nativePath), "the published Java merge path did not publish a DV");
  }

  @org.junit.jupiter.api.Test
  void nestedStructListAndMapMatchTheConnector() throws Exception {
    Path host = Files.createTempDirectory("delta-nested-host");
    Path nativePath = Files.createTempDirectory("delta-nested-native");

    runNestedAppend(host, false);
    runNestedAppend(nativePath, true);

    assertEquals(readNestedRows(host), readNestedRows(nativePath));
  }

  @org.junit.jupiter.api.Disabled("published Delta API does not expose an engine hook for catalogs")
  @org.junit.jupiter.api.Test
  void unityCatalogManagedTableRoutesThroughTheNativeWriter() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    tableEnv.executeSql("CREATE TEMPORARY TABLE src (id BIGINT) WITH ('connector'='datagen')");
    tableEnv.executeSql(
        "CREATE TEMPORARY TABLE sink (id BIGINT) WITH ('connector'='delta', "
            + "'endpoint'='http://catalog.invalid', 'token'='test-token')");
    PhysicalPlanScan scan = NativePlanner.install(tableEnv);

    tableEnv.explainSql("INSERT INTO sink SELECT * FROM src");

    assertAccelerated(scan);
  }

  @org.junit.jupiter.api.Disabled("published Delta API does not expose an engine hook for catalogs")
  @org.junit.jupiter.api.Test
  void unityCatalogPathTableRoutesThroughTheNativeWriter() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    tableEnv.executeSql("CREATE TEMPORARY TABLE src (id BIGINT) WITH ('connector'='datagen')");
    tableEnv.executeSql(
        "CREATE TEMPORARY TABLE sink (id BIGINT) WITH ('connector'='delta', "
            + "'type'='ucpath', 'unitycatalog.table_name'='main.default.sink', "
            + "'unitycatalog.endpoint'='http://catalog.invalid', "
            + "'unitycatalog.token'='test-token')");
    PhysicalPlanScan scan = NativePlanner.install(tableEnv);

    tableEnv.explainSql("INSERT INTO sink SELECT * FROM src");

    assertAccelerated(scan);
  }

  private static void runNestedAppend(Path path, boolean nativeWriter) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    DataStream<org.apache.flink.types.Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"id", "details", "tags", "dt"},
                Types.LONG,
                Types.ROW_NAMED(
                    new String[] {"name", "scores"},
                    Types.STRING,
                    Types.OBJECT_ARRAY(Types.INT)),
                Types.MAP(Types.STRING, Types.LONG),
                Types.STRING),
            org.apache.flink.types.Row.of(
                1L,
                org.apache.flink.types.Row.of("first", new Integer[] {1, 2}),
                Map.of("x", 10L),
                "a"),
            org.apache.flink.types.Row.of(
                2L,
                org.apache.flink.types.Row.of(null, new Integer[] {3, null}),
                Collections.singletonMap("y", null),
                "b"));
    tableEnv.createTemporaryView(
        "nested_changes",
        source,
        Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column(
                "details",
                DataTypes.ROW(
                    DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("scores", DataTypes.ARRAY(DataTypes.INT()))))
            .column("tags", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()))
            .column("dt", DataTypes.STRING())
            .build());
    tableEnv.executeSql(
        "CREATE TEMPORARY TABLE sink (id BIGINT NOT NULL, "
            + "details ROW<name STRING, scores ARRAY<INT>>, tags MAP<STRING, BIGINT>, dt STRING) "
            + "WITH ('connector'='"
            + "delta"
            + "', 'table_path'='"
            + path.toUri()
            + "', 'partitions'='dt', 'file_rolling.strategy'='count', "
            + "'file_rolling.count'='1000')");
    PhysicalPlanScan scan = nativeWriter ? NativePlanner.install(tableEnv) : null;
    tableEnv.executeSql("INSERT INTO sink SELECT * FROM nested_changes").await();
    assertAccelerated(scan);
  }

  private static List<List<Object>> readNestedRows(Path path) throws Exception {
    StructType details =
        new StructType()
            .add("name", StringType.STRING)
            .add("scores", new ArrayType(IntegerType.INTEGER, true));
    StructType schema =
        new StructType()
            .add("id", LongType.LONG, false)
            .add("details", details)
            .add("tags", new MapType(StringType.STRING, LongType.LONG, true))
            .add("dt", StringType.STRING);
    Engine engine = DefaultEngine.create(new org.apache.hadoop.conf.Configuration());
    Snapshot snapshot = TableManager.loadSnapshot(path.toString()).build(engine);
    Scan scan = snapshot.getScanBuilder().withReadSchema(schema).build();
    Row scanState = scan.getScanState(engine);
    StructType physicalSchema = ScanStateRow.getPhysicalDataReadSchema(scanState);
    List<List<Object>> result = new ArrayList<>();
    try (CloseableIterator<FilteredColumnarBatch> files = scan.getScanFiles(engine)) {
      while (files.hasNext()) {
        try (CloseableIterator<Row> fileRows = files.next().getRows()) {
          while (fileRows.hasNext()) {
            Row file = fileRows.next();
            FileStatus status = InternalScanFileUtils.getAddFileStatus(file);
            try (CloseableIterator<FilteredColumnarBatch> data =
                Scan.transformPhysicalData(
                    engine,
                    scanState,
                    file,
                    engine
                        .getParquetHandler()
                        .readParquetFiles(
                            singletonCloseableIterator(status), physicalSchema, Optional.empty())
                        .map(read -> read.getData()))) {
              while (data.hasNext()) {
                try (CloseableIterator<Row> rows = data.next().getRows()) {
                  while (rows.hasNext()) {
                    Row row = rows.next();
                    Row detail = row.getStruct(1);
                    result.add(
                        List.of(
                            row.getLong(0),
                            detail.isNullAt(0) ? "null" : detail.getString(0),
                            renderInts(detail.getArray(1).getElements()),
                            row.getMap(2).getKeys().getString(0),
                            row.getMap(2).getValues().isNullAt(0)
                                ? "null"
                                : Long.toString(row.getMap(2).getValues().getLong(0)),
                            row.getString(3)));
                  }
                }
              }
            }
          }
        }
      }
    }
    return sorted(result);
  }

  private static String renderInts(ColumnVector values) {
    List<String> result = new ArrayList<>();
    for (int i = 0; i < values.getSize(); i++) {
      result.add(values.isNullAt(i) ? "null" : Integer.toString(values.getInt(i)));
    }
    return result.toString();
  }

  private static void createDeletionVectorTable(Path path) {
    HadoopTable table =
        new HadoopTable(
            path.toUri(),
            Map.of("delta.enableDeletionVectors", "true"),
            DELTA_SCHEMA,
            List.of("dt"));
    table.open();
  }

  private static void runAppend(Path path, boolean nativeWriter) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    DataStream<org.apache.flink.types.Row> source =
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"id", "v", "dt"}, Types.LONG, Types.INT, Types.STRING),
            org.apache.flink.types.Row.of(1L, 10, "a"),
            org.apache.flink.types.Row.of(2L, 20, "a"),
            org.apache.flink.types.Row.of(3L, 30, "b"));
    tableEnv.createTemporaryView("changes", source, sourceSchema());
    createSink(tableEnv, path, "append", nativeWriter);
    PhysicalPlanScan scan = nativeWriter ? NativePlanner.install(tableEnv) : null;
    tableEnv.executeSql("INSERT INTO sink SELECT * FROM changes").await();
    assertAccelerated(scan);
  }

  private static void runNativeArrowUpsert(Path path) throws Exception {
    RowType rowType =
        RowType.of(
            new LogicalType[] {
              new BigIntType(false), new IntType(), new VarCharType(VarCharType.MAX_LENGTH)
            },
            new String[] {"id", "v", "dt"});
    Map<String, String> options = Map.of("write.mode", "upsert", "primary_key", "0");
    DeltaSinkConf conf = new DeltaSinkConf(rowType, options);
    NativeDeltaHadoopTable table =
        new NativeDeltaHadoopTable(path.toUri(), Map.of(), DELTA_SCHEMA, List.of("dt"));
    table.open();
    NativeDeltaSinkWriter writer =
        new NativeDeltaSinkWriter("native-mor-test", 0, 0, table, conf);
    List<RowData> changes =
        List.of(
            changedRow(RowKind.UPDATE_AFTER, 1L, 100, "a"),
            changedRow(RowKind.DELETE, 2L, 20, "a"),
            changedRow(RowKind.INSERT, 4L, 40, "b"));
    ArrowKernelRows batch =
        new ArrowKernelRows(
            RowDataArrowConverter.write(changes, rowType, NativeAllocator.SHARED, true),
            rowType,
            DELTA_SCHEMA);
    writer.write(
        batch,
        new SinkWriter.Context() {
          @Override
          public long currentWatermark() {
            return Long.MIN_VALUE;
          }

          @Override
          public Long timestamp() {
            return null;
          }
        });
    List<Row> actions = new ArrayList<>();
    for (DeltaWriterResult result : writer.prepareCommit()) {
      actions.addAll(result.getDeltaActions());
    }
    table.commit(
        CloseableIterable.inMemoryIterable(
            io.delta.kernel.internal.util.Utils.toCloseableIterator(actions.iterator())),
        "native-mor-test",
        1,
        Map.of());
    writer.close();
  }

  private static GenericRowData changedRow(RowKind kind, long id, int value, String partition) {
    GenericRowData row = GenericRowData.of(id, value, StringData.fromString(partition));
    row.setRowKind(kind);
    return row;
  }

  private static Schema sourceSchema() {
    return Schema.newBuilder()
        .column("id", DataTypes.BIGINT().notNull())
        .column("v", DataTypes.INT())
        .column("dt", DataTypes.STRING())
        .primaryKey("id")
        .build();
  }

  private static void createSink(
      StreamTableEnvironment tableEnv, Path path, String mode, boolean nativeWriter) {
    tableEnv.executeSql(
        "CREATE TEMPORARY TABLE sink ("
            + "id BIGINT NOT NULL, v INT, dt STRING, PRIMARY KEY (id) NOT ENFORCED) WITH ("
            + "'connector'='"
            + "delta"
            + "', 'table_path'='"
            + path.toUri()
            + "', 'partitions'='dt'"
            + ", 'write.mode'='"
            + mode
            + "'"
            + ", 'file_rolling.strategy'='count', 'file_rolling.count'='1000')");
  }

  private static void assertAccelerated(PhysicalPlanScan scan) {
    if (scan != null) {
      assertTrue(
          scan.substitutions() > 0,
          () -> "Delta sink did not accelerate: " + scan.fallbackReasons());
    }
  }

  private static List<List<Object>> readLogicalRows(Path tablePath) throws Exception {
    Engine engine = DefaultEngine.create(new org.apache.hadoop.conf.Configuration());
    Snapshot snapshot = TableManager.loadSnapshot(tablePath.toString()).build(engine);
    Scan scan = snapshot.getScanBuilder().withReadSchema(DELTA_SCHEMA).build();
    Row scanState = scan.getScanState(engine);
    StructType physicalSchema = ScanStateRow.getPhysicalDataReadSchema(scanState);
    List<List<Object>> rows = new ArrayList<>();
    try (CloseableIterator<FilteredColumnarBatch> files = scan.getScanFiles(engine)) {
      while (files.hasNext()) {
        try (CloseableIterator<Row> fileRows = files.next().getRows()) {
          while (fileRows.hasNext()) {
            Row file = fileRows.next();
            FileStatus status = InternalScanFileUtils.getAddFileStatus(file);
            try (CloseableIterator<FilteredColumnarBatch> data =
                Scan.transformPhysicalData(
                    engine,
                    scanState,
                    file,
                    engine
                        .getParquetHandler()
                        .readParquetFiles(
                            singletonCloseableIterator(status), physicalSchema, Optional.empty())
                        .map(result -> result.getData()))) {
              while (data.hasNext()) {
                try (CloseableIterator<Row> logicalRows = data.next().getRows()) {
                  while (logicalRows.hasNext()) {
                    Row row = logicalRows.next();
                    rows.add(
                        Arrays.asList(
                            row.getLong(0),
                            row.isNullAt(1) ? null : row.getInt(1),
                            row.isNullAt(2) ? null : row.getString(2)));
                  }
                }
              }
            }
          }
        }
      }
    }
    return rows;
  }

  private static boolean hasDeletionVector(Path tablePath) throws Exception {
    Engine engine = DefaultEngine.create(new org.apache.hadoop.conf.Configuration());
    Snapshot snapshot = TableManager.loadSnapshot(tablePath.toString()).build(engine);
    try (CloseableIterator<FilteredColumnarBatch> files =
        snapshot.getScanBuilder().build().getScanFiles(engine)) {
      while (files.hasNext()) {
        try (CloseableIterator<Row> rows = files.next().getRows()) {
          while (rows.hasNext()) {
            if (InternalScanFileUtils.getDeletionVectorDescriptorFromRow(rows.next()) != null) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  private static List<List<Object>> sorted(List<List<Object>> rows) {
    List<List<Object>> copy = new ArrayList<>(rows);
    copy.sort(Comparator.comparing(Object::toString));
    return copy;
  }
}
