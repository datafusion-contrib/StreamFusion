package tech.streamfusion.hudi;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieTableSink;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.Native;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchTypeInformation;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.operator.TaskOffHeapMemory;
import tech.streamfusion.parquet.NativeParquet;

class HudiMorSinkTest {
  @TempDir Path temporary;
  private static final ResolvedSchema SCHEMA =
      ResolvedSchema.of(
          Column.physical("id", DataTypes.BIGINT().notNull()),
          Column.physical("name", DataTypes.STRING()),
          Column.physical("ts", DataTypes.BIGINT().notNull()));
  private static final RowType TYPE =
      (RowType) SCHEMA.toPhysicalRowDataType().notNull().getLogicalType();

  @AfterEach
  void releasesBuffersWritersAndReservations() {
    assertTrue(Native.liveNativeHandles().isBlank(), Native.liveNativeHandles());
    assertTrue(NativeParquet.liveNativeHandles().isBlank(), NativeParquet.liveNativeHandles());
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
  }

  @Test
  void originalIndexAndLogHandleWriteArrowUpdatesAndOrderedDeletes() throws Exception {
    var changes =
        List.of(
            Row.ofKind(RowKind.INSERT, 1L, "old", 10L),
            Row.ofKind(RowKind.INSERT, 2L, "untouched", 20L),
            Row.ofKind(RowKind.INSERT, 3L, "deleted", 30L),
            Row.ofKind(RowKind.INSERT, 4L, "surviving", 30L),
            Row.ofKind(RowKind.UPDATE_AFTER, 1L, "updated", 30L),
            Row.ofKind(RowKind.UPDATE_AFTER, 1L, "stale", 1L),
            Row.ofKind(RowKind.DELETE, 3L, null, 40L),
            Row.ofKind(RowKind.DELETE, 4L, null, 1L),
            Row.ofKind(RowKind.INSERT, 5L, "inserted", 5L));
    var conf = configuration();
    write(conf, changes, 3);
    var fileGroups = verifyNativeLogBlocks(conf);
    assertEquals(
        List.of(
            "+I[1, updated, 30]",
            "+I[2, untouched, 20]",
            "+I[4, surviving, 30]",
            "+I[5, inserted, 5]"),
        read(conf));
    write(
        conf,
        List.of(
            Row.ofKind(RowKind.UPDATE_AFTER, 2L, "second-commit", 40L),
            Row.ofKind(RowKind.DELETE, 5L, null, 10L)),
        1);
    assertEquals(fileGroups, verifyNativeLogBlocks(conf), "Java's index must reuse file groups");
    assertEquals(
        List.of("+I[1, updated, 30]", "+I[2, second-commit, 40]", "+I[4, surviving, 30]"),
        read(conf));
  }

  private Configuration configuration() {
    var conf =
        Configuration.fromMap(
            Map.of(
                "path", temporary.resolve("target").toUri().toString(),
                "table.type", "MERGE_ON_READ",
                "write.operation", "upsert",
                "metadata.enabled", "false",
                "compaction.async.enabled", "false",
                "compaction.schedule.enabled", "false",
                "index.type", "BUCKET",
                "hoodie.bucket.index.num.buckets", "4",
                "write.precombine", "false"));
    conf.set(FlinkOptions.TABLE_NAME, "target");
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "id");
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "");
    conf.setString("precombine.field", "ts");
    conf.setString("hoodie.logfile.data.block.format", "parquet");
    conf.set(FlinkOptions.WRITE_TASKS, 1);
    conf.set(FlinkOptions.WRITE_BATCH_SIZE, 0.00005);
    conf.set(FlinkOptions.WRITE_TASK_MAX_SIZE, 256D);
    conf.set(
        FlinkOptions.SOURCE_AVRO_SCHEMA, HoodieSchemaConverter.convertToSchema(TYPE).toString());
    return conf;
  }

  @Test
  void sqlPostImagesUseTheColumnarSinkAndUnverifiedPoliciesStayStock() throws Exception {
    var memory = new Configuration();
    memory.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(1024));
    var env = StreamExecutionEnvironment.createLocalEnvironment(1, memory);
    var sql = StreamTableEnvironment.create(env);
    var conf = configuration();
    sql.executeSql(ddl(conf));
    var changes =
        List.of(
            Row.ofKind(RowKind.INSERT, 1L, "old", 10L),
            Row.ofKind(RowKind.INSERT, 2L, "deleted", 20L),
            Row.ofKind(RowKind.UPDATE_AFTER, 1L, "updated", 30L),
            Row.ofKind(RowKind.DELETE, 2L, null, 40L));
    var input =
        env.fromCollection(
            changes,
            org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(
                new String[] {"id", "name", "ts"},
                org.apache.flink.api.common.typeinfo.Types.LONG,
                org.apache.flink.api.common.typeinfo.Types.STRING,
                org.apache.flink.api.common.typeinfo.Types.LONG));
    sql.createTemporaryView(
        "mutations",
        sql.fromChangelogStream(
            input,
            org.apache.flink.table.api.Schema.newBuilder()
                .column("id", DataTypes.BIGINT().notNull())
                .column("name", DataTypes.STRING())
                .column("ts", DataTypes.BIGINT())
                .primaryKey("id")
                .build(),
            org.apache.flink.table.connector.ChangelogMode.upsert()));
    tech.streamfusion.planner.NativePlanner.install(sql);
    String query = "INSERT INTO target SELECT id,name,ts FROM mutations";
    String plan = sql.explainSql(query);
    assertTrue(plan.contains("NativeHudiSink"), plan);
    assertTrue(plan.contains("writeMode=[MOR upsert]"), plan);
    assertFalse(plan.contains("ArrowToRow"), plan);
    sql.executeSql(query).await();
    assertEquals(List.of("+I[1, updated, 30]"), read(conf));
    verifyNativeLogBlocks(conf);
    var nullable = new Configuration(conf);
    nullable.set(FlinkOptions.PATH, temporary.resolve("nullable-target").toUri().toString());
    sql.executeSql(
        ddl(nullable)
            .replace("CREATE TABLE target", "CREATE TABLE nullable_ordering")
            .replace("ts BIGINT NOT NULL", "ts BIGINT"));
    String nullablePlan =
        sql.explainSql("INSERT INTO nullable_ordering SELECT id,name,ts FROM mutations");
    assertFalse(nullablePlan.contains("NativeHudiSink"), nullablePlan);
    for (var option :
        Map.of(
                "write.precombine", "true",
                "write.tasks", "2",
                "hoodie.logfile.data.block.format", "avro",
                "index.type", "FLINK_STATE",
                "metadata.enabled", "true",
                "hoodie.metadata.enable", "true",
                "hoodie.allow.operation.metadata.field", "true")
            .entrySet()) {
      String fallback =
          sql.explainSql(
              "INSERT INTO target /*+ OPTIONS('"
                  + option.getKey()
                  + "'='"
                  + option.getValue()
                  + "') */ SELECT id,name,ts FROM mutations");
      assertFalse(fallback.contains("NativeHudiSink"), fallback);
    }
  }

  private static String ddl(Configuration conf) {
    String options =
        conf.toMap().entrySet().stream()
            .filter(entry -> !entry.getKey().equals(FlinkOptions.SOURCE_AVRO_SCHEMA.key()))
            .map(
                entry ->
                    "'"
                        + entry.getKey().replace("'", "''")
                        + "'='"
                        + entry.getValue().replace("'", "''")
                        + "'")
            .collect(java.util.stream.Collectors.joining(","));
    return "CREATE TABLE target (id BIGINT,name STRING,ts BIGINT NOT NULL,PRIMARY KEY(id) NOT"
        + " ENFORCED) WITH ('connector'='hudi',"
        + options
        + ")";
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"EVENT_TIME_ORDERING", "COMMIT_TIME_ORDERING"})
  void deleteThenReinsertMatchesTheStockWriterAtFlushBoundaries(String mode) throws Exception {
    var conf = configuration();
    conf.set(FlinkOptions.RECORD_MERGE_MODE, mode);
    var changes =
        List.of(
            Row.ofKind(RowKind.INSERT, 1L, "old", 10L),
            Row.ofKind(RowKind.DELETE, 1L, null, 20L),
            Row.ofKind(RowKind.INSERT, 1L, "new", 20L));
    write(conf, changes, 3);
    assertEquals(writeAndReadStock(conf, changes), read(conf));
  }

  private List<String> writeAndReadStock(Configuration conf, List<Row> changes) throws Exception {
    var stock = new Configuration(conf);
    stock.set(FlinkOptions.PATH, temporary.resolve("stock").toUri().toString());
    var memory = new Configuration();
    memory.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(1024));
    var env = StreamExecutionEnvironment.createLocalEnvironment(1, memory);
    var sql = StreamTableEnvironment.create(env);
    sql.executeSql(ddl(stock));
    var input =
        env.fromCollection(
            changes,
            org.apache.flink.api.common.typeinfo.Types.ROW_NAMED(
                new String[] {"id", "name", "ts"},
                org.apache.flink.api.common.typeinfo.Types.LONG,
                org.apache.flink.api.common.typeinfo.Types.STRING,
                org.apache.flink.api.common.typeinfo.Types.LONG));
    sql.createTemporaryView(
        "mutations",
        sql.fromChangelogStream(
            input,
            org.apache.flink.table.api.Schema.newBuilder()
                .column("id", DataTypes.BIGINT().notNull())
                .column("name", DataTypes.STRING())
                .column("ts", DataTypes.BIGINT())
                .primaryKey("id")
                .build(),
            org.apache.flink.table.connector.ChangelogMode.upsert()));
    sql.executeSql("INSERT INTO target SELECT * FROM mutations").await();
    return read(stock);
  }

  @Test
  void writerBudgetFlushesExistingArrowBuffersThroughTheJavaCoordinator() throws Exception {
    var conf = configuration();
    conf.set(FlinkOptions.WRITE_BATCH_SIZE, 256D);
    conf.set(FlinkOptions.WRITE_TASK_MAX_SIZE, 201D);
    String payload = "x".repeat(1 << 18);
    List<Row> changes = new ArrayList<>();
    for (long id = 0; id < 8; id++) {
      changes.add(Row.ofKind(RowKind.INSERT, id, payload, 1L));
    }
    for (long id = 7; id >= 0; id--) {
      changes.add(Row.ofKind(RowKind.DELETE, id, null, 1L));
      changes.add(Row.ofKind(RowKind.INSERT, id, payload, 1L));
    }
    write(conf, changes, changes.size());
    var actual = read(conf);
    assertTrue(
        writeAndReadStock(conf, changes).equals(actual),
        "Native results must match stock under cross-bucket memory preemption");
    verifyNativeLogBlocks(conf, false, 5);
  }

  @Test
  void parallelArrowInputsRouteToTheOriginalSingleBucketWriter() throws Exception {
    var conf = configuration();
    conf.set(FlinkOptions.WRITE_BATCH_SIZE, 1D);
    List<Row> changes = new ArrayList<>();
    List<String> expected = new ArrayList<>();
    for (long id = 0; id < 32; id++) changes.add(Row.ofKind(RowKind.INSERT, id, "old", 10L));
    for (long id = 0; id < 32; id++) {
      changes.add(Row.ofKind(RowKind.UPDATE_AFTER, id, "new", 20L));
      if (id % 5 == 0) changes.add(Row.ofKind(RowKind.DELETE, id, null, 30L));
      else expected.add("+I[" + id + ", new, 20]");
    }
    write(conf, changes, 8, false, 2);
    Collections.sort(expected);
    assertEquals(expected, read(conf));
    verifyNativeLogBlocks(conf);
  }

  @Test
  void checkpointRecoveryPreservesTheOriginalBucketIndexAndTransactions() throws Exception {
    var conf = configuration();
    conf.set(FlinkOptions.WRITE_BATCH_SIZE, 1D);
    List<Row> changes = new ArrayList<>();
    List<String> expected = new ArrayList<>();
    for (long id = 0; id < 512; id++) changes.add(Row.ofKind(RowKind.INSERT, id, "old", 10L));
    for (long id = 0; id < 512; id++) changes.add(Row.ofKind(RowKind.UPDATE_AFTER, id, "new", 20L));
    for (long id = 0; id < 512; id++) {
      if (id % 7 == 0) changes.add(Row.ofKind(RowKind.DELETE, id, null, 30L));
      else expected.add("+I[" + id + ", new, 20]");
    }
    write(conf, changes, 8, true);
    assertTrue(java.nio.file.Files.exists(temporary.resolve("sink-failed-once")));
    Collections.sort(expected);
    assertEquals(expected, read(conf));
    verifyNativeLogBlocks(conf);
  }

  private void write(Configuration conf, List<Row> changes, int batchRows) throws Exception {
    write(conf, changes, batchRows, false);
  }

  private void write(Configuration conf, List<Row> changes, int batchRows, boolean recover)
      throws Exception {
    write(conf, changes, batchRows, recover, 1);
  }

  private void write(
      Configuration conf, List<Row> changes, int batchRows, boolean recover, int parallelism)
      throws Exception {
    var memory = new Configuration();
    memory.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(1024));
    var env = StreamExecutionEnvironment.createLocalEnvironment(parallelism, memory);
    if (recover) {
      env.enableCheckpointing(100);
      env.setRestartStrategy(
          org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(1, 0));
    }
    var input =
        env.fromSequence(0, (changes.size() - 1) / batchRows)
            .map(
                (MapFunction<Long, ArrowBatch>)
                    batch -> {
                      List<RowData> values = new ArrayList<>();
                      int start = Math.toIntExact(batch) * batchRows;
                      for (int i = start; i < Math.min(changes.size(), start + batchRows); i++) {
                        Row row = changes.get(i);
                        values.add(
                            GenericRowData.ofKind(
                                row.getKind(),
                                row.getField(0),
                                row.getField(1) == null
                                    ? null
                                    : StringData.fromString((String) row.getField(1)),
                                row.getField(2)));
                      }
                      return new ArrowBatch(
                          RowDataArrowConverter.write(values, TYPE, NativeAllocator.SHARED, true));
                    })
            .returns(ArrowBatchTypeInformation.INSTANCE);
    org.apache.flink.streaming.api.datastream.DataStream<ArrowBatch> batches = input;
    assertEquals(parallelism, batches.getParallelism());
    if (recover)
      batches =
          input.transform(
              "fail-after-committed-checkpoint",
              ArrowBatchTypeInformation.INSTANCE,
              new HudiCheckpointFailure(temporary.resolve("sink-failed-once").toString()));
    NativeHudiMorSink.append(
        batches,
        new HoodieTableSink(conf, SCHEMA),
        TYPE,
        new org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext(true));
    env.execute("MOR Arrow write");
  }

  private java.util.Set<String> verifyNativeLogBlocks(Configuration conf) throws Exception {
    return verifyNativeLogBlocks(conf, true, 1);
  }

  private java.util.Set<String> verifyNativeLogBlocks(
      Configuration conf, boolean requireDeletes, int minimumParquetBlocks) throws Exception {
    var source =
        new org.apache.hudi.table.HoodieTableSource(
            org.apache.hudi.util.SerializableSchema.create(SCHEMA),
            new org.apache.hudi.storage.StoragePath(conf.get(FlinkOptions.PATH)),
            List.of(),
            "__HIVE_DEFAULT_PARTITION__",
            conf);
    var meta = source.getMetaClient();
    var storage = meta.getStorage();
    var schema = new org.apache.hudi.common.table.TableSchemaResolver(meta).getTableSchema();
    var paths = new java.util.HashSet<String>();
    var fileGroups = new java.util.HashSet<String>();
    int parquet = 0;
    int deletes = 0;
    for (var split : source.buildHoodieSplits(meta, conf)) {
      for (String path : split.getLogPaths().orElse(List.of())) {
        if (!paths.add(path)) continue;
        var file =
            new org.apache.hudi.common.model.HoodieLogFile(
                new org.apache.hudi.storage.StoragePath(path));
        fileGroups.add(file.getFileId());
        try (var reader =
            org.apache.hudi.common.table.log.HoodieLogFormat.newReader(storage, file, schema)) {
          while (reader.hasNext()) {
            var block = reader.next();
            switch (block.getBlockType()) {
              case PARQUET_DATA_BLOCK:
                Path payload = temporary.resolve("verify-block.parquet");
                try {
                  var location = block.getBlockContentLocation().get();
                  byte[] bytes = new byte[Math.toIntExact(location.getBlockSize())];
                  try (var content = storage.openSeekable(file.getPath(), 64 << 10, true)) {
                    content.seek(location.getContentPositionInLogFile());
                    content.readFully(bytes);
                  }
                  java.nio.file.Files.write(payload, bytes);
                  var footer =
                      org.apache.hudi.common.util.ParquetUtils.readMetadata(
                          storage, new org.apache.hudi.storage.StoragePath(payload.toUri()));
                  assertTrue(footer.getFileMetaData().getCreatedBy().contains("parquet-rs"));
                  assertTrue(footer.getBlocks().stream().mapToLong(b -> b.getRowCount()).sum() > 0);
                } finally {
                  java.nio.file.Files.deleteIfExists(payload);
                }
                parquet++;
                break;
              case DELETE_BLOCK:
                assertFalse(
                    block
                        .getLogBlockHeader()
                        .containsKey(
                            org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType
                                .BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS));
                deletes++;
                break;
              default:
                fail("Unexpected written Hudi block: " + block.getBlockType());
            }
          }
        }
      }
    }
    assertTrue(
        parquet >= minimumParquetBlocks,
        "Expected native log blocks, including any required early memory flushes");
    if (requireDeletes)
      assertTrue(deletes > 0, "The original Java handle must produce key-delete blocks");
    return fileGroups;
  }

  private List<String> read(Configuration conf) throws Exception {
    var memory = new Configuration();
    memory.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(1024));
    var sql =
        StreamTableEnvironment.create(StreamExecutionEnvironment.createLocalEnvironment(1, memory));
    sql.executeSql(
        "CREATE TABLE target (id BIGINT,name STRING,ts BIGINT,PRIMARY KEY(id) NOT ENFORCED)"
            + " WITH ('connector'='hudi','path'='"
            + conf.get(FlinkOptions.PATH)
            + "','table.type'='MERGE_ON_READ','precombine.field'='ts','metadata.enabled'='false',"
            + "'read.source-v2.enabled'='true')");
    List<String> rows = new ArrayList<>();
    try (var result = sql.executeSql("SELECT id,name,ts FROM target").collect()) {
      while (result.hasNext()) rows.add(result.next().toString());
    }
    Collections.sort(rows);
    return rows;
  }
}
