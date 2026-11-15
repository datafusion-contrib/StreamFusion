package tech.streamfusion.hudi;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTableSource;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.SerializableSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.TaskOffHeapMemory;

class NativeHudiMorSourceTest {
  @TempDir Path temporary;
  private static final ResolvedSchema SCHEMA =
      ResolvedSchema.of(
          Column.physical("id", DataTypes.BIGINT()),
          Column.physical("name", DataTypes.STRING()),
          Column.physical("ts", DataTypes.BIGINT()));
  private static final RowType OUTPUT = (RowType) SCHEMA.toPhysicalRowDataType().getLogicalType();
  private StreamTableEnvironment sql;
  private StreamExecutionEnvironment environment;

  @AfterEach
  void closesReadersMemoryAndFileCaches() throws Exception {
    try {
      assertTrue(tech.streamfusion.parquet.NativeParquet.liveNativeHandles().isBlank());
      assertEquals(0, TaskOffHeapMemory.reservedBytes());
      try (var files = java.nio.file.Files.list(temporary)) {
        assertFalse(
            files.anyMatch(path -> path.getFileName().toString().startsWith("streamfusion-hudi-")));
      }
    } finally {
      org.apache.flink.util.FileUtils.deleteDirectory(temporary.toFile());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet"})
  void javaSelectedAndFetchedLogOnlyGroupsMatchStockUpdatesAndDeletes(String format)
      throws Exception {
    var table = fixture(format);
    var expected = new ArrayList<String>();
    try (var rows = sql.executeSql("SELECT id,name,ts FROM h").collect()) {
      while (rows.hasNext()) expected.add(rows.next().toString());
    }
    Collections.sort(expected);
    assertEquals(
        List.of(
            "+I[1, updated, 30]",
            "+I[2, untouched, 20]",
            "+I[4, surviving, 30]",
            "+I[5, inserted, 5]"),
        expected);
    var source = NativeHudiSource.from(table, OUTPUT, 2, 1);
    var splits = splits(source);
    assertFalse(splits.isEmpty());
    assertTrue(splits.stream().anyMatch(split -> split.getBasePath().isEmpty()));
    var actual = new ArrayList<String>();
    var context = context();
    try (var reader = source.createReader(context)) {
      reader.addSplits(splits);
      reader.notifyNoMoreSplits();
      reader.start();
      InputStatus status;
      do {
        reader.isAvailable().get(20, TimeUnit.SECONDS);
        status = reader.pollNext(output(actual));
      } while (status != InputStatus.END_OF_INPUT);
    }
    Collections.sort(actual);
    assertEquals(expected, actual);
    assertEquals(4, context.metricGroup().getIOMetricGroup().getNumRecordsInCounter().getCount());
  }

  @Test
  void avroLogsRetainTheStockReader() throws Exception {
    var table = fixture("avro");
    assertThrows(IllegalArgumentException.class, () -> NativeHudiSource.from(table, OUTPUT, 2, 1));
  }

  @Test
  void storedIntermediateOperationsRetainTheStockReader() throws Exception {
    var table = fixture("parquet", false, false, true);
    table.getConf().set(FlinkOptions.READ_AS_STREAMING, true);
    assertTrue(
        new org.apache.hudi.common.table.TableSchemaResolver(table.getMetaClient())
            .getTableSchema()
            .getField(org.apache.hudi.common.model.HoodieRecord.OPERATION_METADATA_FIELD)
            .isPresent());
    assertThrows(IllegalArgumentException.class, () -> NativeHudiSource.from(table, OUTPUT, 2, 1));
  }

  @Test
  void hudiSpillsOnlyLocationsAndPreservesTheArrowPayload() throws Exception {
    var table = fixture("parquet");
    var meta = table.getMetaClient();
    TaskOffHeapMemory.initialize(memory());
    String text = "é".repeat(32768);
    var root =
        tech.streamfusion.operator.RowDataArrowConverter.write(
            List.of(
                org.apache.flink.table.data.GenericRowData.of(
                    1L, org.apache.flink.table.data.StringData.fromString(text), 2L)),
            OUTPUT,
            tech.streamfusion.operator.NativeAllocator.SHARED);
    try (var context =
        new HudiArrowReadContext(
            meta, org.apache.hudi.common.util.Option.empty(), 2, 8L << 20, null, null)) {
      var input = context.retain(root, OUTPUT);
      var cursor = new HudiAppendIOFactory.Cursor(input.batch, input.batch.vectors);
      cursor.setRowId(0);
      cursor.setRowKind(RowKind.UPDATE_AFTER);
      assertSame(
          cursor,
          context
              .getRecordContext()
              .toBinaryRow(
                  HoodieSchemaConverter.convertToSchema((RowType) OUTPUT.copy(false)), cursor));
      var projectedType =
          RowType.of(
              new org.apache.flink.table.types.logical.LogicalType[] {
                new org.apache.flink.table.types.logical.VarCharType(Integer.MAX_VALUE),
                new org.apache.flink.table.types.logical.BigIntType()
              },
              new String[] {"name", "id"});
      var projected =
          context
              .getRecordContext()
              .projectRecord(
                  HoodieSchemaConverter.convertToSchema((RowType) OUTPUT.copy(false)),
                  HoodieSchemaConverter.convertToSchema((RowType) projectedType.copy(false)),
                  Map.of())
              .apply(cursor);
      assertSame(input.batch, ((HudiAppendIOFactory.Cursor) projected).batch);
      var record =
          new org.apache.hudi.common.table.read.BufferedRecord<org.apache.flink.table.data.RowData>(
              "1",
              2L,
              projected,
              context
                  .getRecordContext()
                  .encodeSchema(
                      HoodieSchemaConverter.convertToSchema((RowType) projectedType.copy(false))),
              org.apache.hudi.common.model.HoodieOperation.UPDATE_AFTER);
      var codec = context.getRecordSerializer();
      assertTrue(codec.serialize(record).length < 2048, "spill bytes contain only control fields");
      try (var spill =
          new org.apache.hudi.common.util.collection.ExternalSpillableMap<
              String,
              org.apache.hudi.common.table.read.BufferedRecord<
                  org.apache.flink.table.data.RowData>>(
              1,
              temporary.toString(),
              new org.apache.hudi.common.util.DefaultSizeEstimator<>(),
              context.getRecordSizeEstimator(),
              org.apache.hudi.common.util.collection.ExternalSpillableMap.DiskMapType.BITCASK,
              codec,
              false,
              "Arrow row-location regression")) {
        spill.put("1", record);
        assertTrue(spill.getDiskBasedMapNumEntries() > 0);
        var restored = spill.get("1");
        var view = (HudiAppendIOFactory.Cursor) restored.getRecord();
        assertSame(input.batch, view.batch);
        assertEquals(text, view.getString(0).toString());
        assertEquals(1L, view.getLong(1));
        assertEquals(RowKind.UPDATE_AFTER, view.getRowKind());
        assertEquals(record.getHoodieOperation(), restored.getHoodieOperation());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("unchecked")
  void continuousMergeEmitsTheSamePostImagesAndKeyDeletesAsStock(boolean base) throws Exception {
    var table = fixture("parquet", base);
    table.getConf().set(FlinkOptions.READ_AS_STREAMING, true);
    table.getConf().set(FlinkOptions.CHANGELOG_ENABLED, true);
    var expected =
        stockRows(table, table.buildHoodieSplits(table.getMetaClient(), table.getConf()));
    assertTrue(expected.contains("-D[3, null, null]"), expected.toString());
    var actual = new ArrayList<String>();
    var nativeSource = NativeHudiSource.from(table, OUTPUT, 2, 1);
    readNative(
        nativeSource, table.buildHoodieSplits(table.getMetaClient(), table.getConf()), actual);
    Collections.sort(expected);
    Collections.sort(actual);
    assertEquals(expected, actual);
  }

  @Test
  void continuousDiscoveryAndCheckpointRecoveryPreserveNewCommitDeletes() throws Exception {
    var table = fixture("parquet", false, true);
    table.getConf().set(FlinkOptions.READ_AS_STREAMING, true);
    table.getConf().set(FlinkOptions.CHANGELOG_ENABLED, true);
    table.getConf().set(FlinkOptions.READ_START_COMMIT, "earliest");
    var source = NativeHudiSource.from(table, OUTPUT, 11, 1);
    org.apache.hudi.source.enumerator.HoodieSplitEnumeratorState state;
    List<HoodieSourceSplit> initial;
    try (var discovery = new NativeHudiSourceTest.ContinuousDiscovery(source, null)) {
      initial = discovery.discover();
      assertFalse(initial.isEmpty());
      state = discovery.enumerator.snapshotState(1);
    }
    var initialRows = new ArrayList<String>();
    readNative(source, initial, initialRows);
    assertEquals(1000, initialRows.size());
    var changes =
        environment.fromCollection(
            List.of(
                Row.ofKind(RowKind.UPDATE_AFTER, 1L, "new", 3L),
                Row.ofKind(RowKind.DELETE, 2L, null, 3L),
                Row.ofKind(RowKind.DELETE, 3L, null, 0L),
                Row.ofKind(RowKind.INSERT, 2000L, "added", 1L)),
            Types.ROW_NAMED(
                new String[] {"id", "name", "ts"}, Types.LONG, Types.STRING, Types.LONG));
    sql.createTemporaryView(
        "new_changes",
        sql.fromChangelogStream(
            changes,
            Schema.newBuilder()
                .column("id", DataTypes.BIGINT().notNull())
                .column("name", DataTypes.STRING())
                .column("ts", DataTypes.BIGINT())
                .primaryKey("id")
                .build()));
    sql.executeSql("INSERT INTO h SELECT * FROM new_changes").await();
    var stateCodec = source.getEnumeratorCheckpointSerializer();
    state = stateCodec.deserialize(stateCodec.getVersion(), stateCodec.serialize(state));
    List<HoodieSourceSplit> updates;
    try (var discovery = new NativeHudiSourceTest.ContinuousDiscovery(source, state)) {
      updates = discovery.discover();
      assertTrue(updates.size() > 1);
      assertTrue(updates.stream().allMatch(split -> split.getInstantRange().isPresent()));
    }
    var splitCodec = source.getSplitSerializer();
    var stockSplits = new ArrayList<HoodieSourceSplit>();
    for (var split : updates)
      stockSplits.add(splitCodec.deserialize(splitCodec.getVersion(), splitCodec.serialize(split)));
    var expected = stockRows(table, stockSplits);
    assertTrue(expected.contains("-D[2, null, null]"), expected.toString());
    var actual = new ArrayList<String>();
    List<HoodieSourceSplit> pending;
    try (var reader = source.createReader(context())) {
      reader.addSplits(updates);
      reader.start();
      while (actual.isEmpty()) {
        reader.isAvailable().get(20, TimeUnit.SECONDS);
        reader.pollNext(output(actual));
      }
      pending = reader.snapshotState(2);
    }
    var restored = new ArrayList<HoodieSourceSplit>();
    for (var split : pending)
      restored.add(splitCodec.deserialize(splitCodec.getVersion(), splitCodec.serialize(split)));
    readNative(NativeHudiSource.from(table, OUTPUT, 3, 1), restored, actual);
    Collections.sort(expected);
    Collections.sort(actual);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void continuousSqlPreservesDeleteKindsThroughANativeCalc(boolean base) throws Exception {
    fixture("parquet", base);
    sql.executeSql(
        "ALTER TABLE h SET ('read.streaming.enabled'='true',"
            + "'read.start-commit'='earliest','read.streaming.check-interval'='1',"
            + "'changelog.enabled'='true')");
    tech.streamfusion.planner.NativePlanner.install(sql);
    String query = "SELECT name,id+1,ts FROM h";
    List<String> expected = null;
    for (boolean nativeRead : List.of(false, true)) {
      sql.getConfig()
          .getConfiguration()
          .setString("streamfusion.native.enabled", Boolean.toString(nativeRead));
      String plan = tech.streamfusion.planner.NativePlanner.explain(sql, query);
      assertEquals(nativeRead, plan.contains("Java Hudi"), plan);
      if (nativeRead) assertFalse(plan.contains("RowDataToArrow"), plan);
      var actual = new ArrayList<String>();
      var running = sql.executeSql(query);
      try (var result = running.collect()) {
        var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
          executor
              .submit(
                  () -> {
                    for (int i = 0; i < 5; i++) actual.add(result.next().toString());
                  })
              .get(30, TimeUnit.SECONDS);
        } finally {
          executor.shutdownNow();
        }
      } finally {
        NativeHudiSourceTest.awaitStopped(running);
      }
      Collections.sort(actual);
      if (expected == null) expected = actual;
      else assertEquals(expected, actual);
      assertTrue(actual.contains("-D[null, 4, null]"), actual.toString());
    }
  }

  @Test
  void continuousTombstonesPreserveNullsForDeclaredNotNullNonKeyFields() throws Exception {
    var table = fixture("parquet");
    table.getConf().set(FlinkOptions.READ_AS_STREAMING, true);
    table.getConf().set(FlinkOptions.CHANGELOG_ENABLED, true);
    var strict =
        new RowType(
            false,
            List.of(
                new RowType.RowField(
                    "id", new org.apache.flink.table.types.logical.BigIntType(false)),
                new RowType.RowField(
                    "name",
                    new org.apache.flink.table.types.logical.VarCharType(Integer.MAX_VALUE)),
                new RowType.RowField(
                    "ts", new org.apache.flink.table.types.logical.BigIntType(false))));
    var expected =
        stockRows(table, table.buildHoodieSplits(table.getMetaClient(), table.getConf()));
    Collections.sort(expected);
    for (int rows : new int[] {1, 2, 4096}) {
      var actual = new ArrayList<String>();
      readNative(
          NativeHudiSource.from(table, strict, rows, 1),
          table.buildHoodieSplits(table.getMetaClient(), table.getConf()),
          actual);
      Collections.sort(actual);
      assertEquals(expected, actual);
    }
  }

  @Test
  void fullyDeletedBaseBatchesAreReleasedWithinTheMergeBudget() throws Exception {
    var table = fixture("parquet", true);
    String values =
        java.util.stream.IntStream.range(10, 74)
            .mapToObj(id -> "(CAST(" + id + " AS BIGINT))")
            .collect(java.util.stream.Collectors.joining(","));
    sql.executeSql(
            "INSERT INTO h /*+ OPTIONS('write.operation'='insert') */ "
                + "SELECT id,REPEAT('x',49152),CAST(1 AS BIGINT) FROM (VALUES "
                + values
                + ") AS ids(id)")
        .await();
    var changes = new ArrayList<Row>();
    for (int id = 10; id < 74; id++) changes.add(Row.ofKind(RowKind.DELETE, (long) id, null, 2L));
    sql.createTemporaryView(
        "wide_deletes",
        sql.fromChangelogStream(
            environment.fromCollection(
                changes,
                Types.ROW_NAMED(
                    new String[] {"id", "name", "ts"}, Types.LONG, Types.STRING, Types.LONG)),
            Schema.newBuilder()
                .column("id", DataTypes.BIGINT().notNull())
                .column("name", DataTypes.STRING())
                .column("ts", DataTypes.BIGINT())
                .primaryKey("id")
                .build()));
    sql.executeSql("INSERT INTO h SELECT * FROM wide_deletes").await();
    table.getMetaClient().reloadActiveTimeline();
    table.getConf().set(FlinkOptions.COMPACTION_MAX_MEMORY, 1);
    var expected =
        stockRows(table, table.buildHoodieSplits(table.getMetaClient(), table.getConf()));
    assertEquals(4, expected.size());
    var actual = new ArrayList<String>();
    readNative(
        NativeHudiSource.from(table, OUTPUT, 3, 1),
        table.buildHoodieSplits(table.getMetaClient(), table.getConf()),
        actual);
    Collections.sort(expected);
    Collections.sort(actual);
    assertEquals(expected, actual);
  }

  @SuppressWarnings("unchecked")
  private List<String> stockRows(HoodieTableSource table, List<HoodieSourceSplit> selected)
      throws Exception {
    var factory = HoodieTableSource.class.getDeclaredMethod("createHoodieSource");
    factory.setAccessible(true);
    var stock =
        (org.apache.hudi.source.HoodieSource<org.apache.flink.table.data.RowData>)
            factory.invoke(table);
    var expected = new ArrayList<String>();
    ReaderOutput<org.apache.flink.table.data.RowData> stockOutput =
        (ReaderOutput<org.apache.flink.table.data.RowData>)
            Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {ReaderOutput.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("collect")) {
                    var row = (org.apache.flink.table.data.RowData) args[0];
                    expected.add(
                        Row.ofKind(
                                row.getRowKind(),
                                row.isNullAt(0) ? null : row.getLong(0),
                                row.isNullAt(1) ? null : row.getString(1).toString(),
                                row.isNullAt(2) ? null : row.getLong(2))
                            .toString());
                  }
                  if (method.getName().equals("createOutputForSplit")) return proxy;
                  return null;
                });
    try (var reader = stock.createReader(context())) {
      reader.addSplits(selected);
      reader.notifyNoMoreSplits();
      reader.start();
      InputStatus status;
      do {
        reader.isAvailable().get(20, TimeUnit.SECONDS);
        status = reader.pollNext(stockOutput);
      } while (status != InputStatus.END_OF_INPUT);
    }
    return expected;
  }

  private void readNative(
      NativeHudiSource source, List<HoodieSourceSplit> selected, List<String> actual)
      throws Exception {
    try (var reader = source.createReader(context())) {
      reader.addSplits(selected);
      reader.notifyNoMoreSplits();
      reader.start();
      InputStatus status;
      do {
        reader.isAvailable().get(20, TimeUnit.SECONDS);
        status = reader.pollNext(output(actual));
      } while (status != InputStatus.END_OF_INPUT);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void checkpointResumesMergedLogicalRowsWithDifferentBatchSize(boolean spill) throws Exception {
    var table = fixture("parquet");
    if (spill) {
      table
          .getConf()
          .setString(
              org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_COMPACTION.key(),
              "1");
      var props =
          org.apache.hudi.util.FlinkClientUtil.getReadProps(
              table.getMetaClient().getTableConfig(),
              org.apache.hudi.util.FlinkWriteClients.getHoodieClientConfig(table.getConf()));
      assertEquals(
          "1",
          props.getProperty(
              org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key()));
    }
    var source = NativeHudiSource.from(table, OUTPUT, 1, 1);
    var actual = new ArrayList<String>();
    List<HoodieSourceSplit> checkpoint;
    try (var reader = source.createReader(context())) {
      reader.addSplits(splits(source));
      reader.start();
      while (actual.isEmpty()) {
        reader.isAvailable().get(20, TimeUnit.SECONDS);
        reader.pollNext(output(actual));
      }
      assertEquals(1, actual.size(), "The checkpoint follows the emitted Arrow batch");
      checkpoint = reader.snapshotState(1);
      assertTrue(checkpoint.stream().anyMatch(split -> split.getConsumed() == 1));
      var codec = source.getSplitSerializer();
      var saved = new ArrayList<HoodieSourceSplit>();
      for (var split : checkpoint)
        saved.add(codec.deserialize(codec.getVersion(), codec.serialize(split)));
      checkpoint = saved;
    }
    var restored = NativeHudiSource.from(table, OUTPUT, 3, 1);
    try (var reader = restored.createReader(context())) {
      reader.addSplits(checkpoint);
      reader.notifyNoMoreSplits();
      reader.start();
      InputStatus status;
      do {
        reader.isAvailable().get(20, TimeUnit.SECONDS);
        status = reader.pollNext(output(actual));
      } while (status != InputStatus.END_OF_INPUT);
    }
    Collections.sort(actual);
    assertEquals(
        List.of(
            "+I[1, updated, 30]",
            "+I[2, untouched, 20]",
            "+I[4, surviving, 30]",
            "+I[5, inserted, 5]"),
        actual);
  }

  @Test
  void baseAndParquetLogsMatchStockOrderedUpdatesAndDeletes() throws Exception {
    var table = fixture("parquet", true);
    var source = NativeHudiSource.from(table, OUTPUT, 2, 1);
    var files = splits(source);
    assertTrue(
        files.stream()
            .anyMatch(
                split ->
                    split.getBasePath().isPresent()
                        && !split.getLogPaths().orElse(List.of()).isEmpty()));
    var actual = new ArrayList<String>();
    try (var reader = source.createReader(context())) {
      reader.addSplits(files);
      reader.notifyNoMoreSplits();
      reader.start();
      InputStatus status;
      do {
        reader.isAvailable().get(20, TimeUnit.SECONDS);
        status = reader.pollNext(output(actual));
      } while (status != InputStatus.END_OF_INPUT);
    }
    var expected = new ArrayList<String>();
    try (var rows = sql.executeSql("SELECT id,name,ts FROM h").collect()) {
      while (rows.hasNext()) expected.add(rows.next().toString());
    }
    Collections.sort(actual);
    Collections.sort(expected);
    assertEquals(
        List.of(
            "+I[1, updated, 30]",
            "+I[2, untouched, 20]",
            "+I[4, surviving, 30]",
            "+I[5, inserted, 5]"),
        expected);
    assertEquals(expected, actual);
  }

  @Test
  @SuppressWarnings("unchecked")
  void exportedArrowBatchesRemainValidAfterMergerAndCacheClose() throws Exception {
    var source = NativeHudiSource.from(fixture("parquet"), OUTPUT, 1, 1);
    var held = new ArrayList<ArrowBatch>();
    ReaderOutput<ArrowBatch> output =
        (ReaderOutput<ArrowBatch>)
            Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {ReaderOutput.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("collect")) held.add((ArrowBatch) args[0]);
                  if (method.getName().equals("createOutputForSplit")) return proxy;
                  return null;
                });
    try {
      try (var reader = source.createReader(context())) {
        reader.addSplits(splits(source));
        reader.notifyNoMoreSplits();
        reader.start();
        InputStatus status;
        do {
          reader.isAvailable().get(20, TimeUnit.SECONDS);
          status = reader.pollNext(output);
        } while (status != InputStatus.END_OF_INPUT);
      }
      assertTrue(tech.streamfusion.parquet.NativeParquet.liveNativeHandles().isBlank());
      var actual = new ArrayList<String>();
      var consumer = output(actual);
      for (var batch : held) consumer.collect(batch);
      held.clear();
      assertEquals(4, actual.size());
    } finally {
      for (var batch : held) batch.root().close();
    }
  }

  @Test
  void sqlProjectionKeepsKeysAndOrderingInsideTheUpstreamMerger() throws Exception {
    var table = fixture("parquet");
    String query = "SELECT name,id+1 FROM h";
    var expected = new ArrayList<String>();
    try (var rows = sql.executeSql(query).collect()) {
      while (rows.hasNext()) expected.add(rows.next().toString());
    }
    tech.streamfusion.planner.NativePlanner.install(sql);
    String plan = tech.streamfusion.planner.NativePlanner.explain(sql, query);
    assertTrue(plan.contains("native Parquet"), plan);
    assertTrue(plan.contains("Java Hudi"), plan);
    assertFalse(plan.contains("RowDataToArrow"), plan);
    var actual = new ArrayList<String>();
    try (var rows = sql.executeSql(query).collect()) {
      while (rows.hasNext()) actual.add(rows.next().toString());
    }
    Collections.sort(actual);
    Collections.sort(expected);
    assertEquals(expected, actual);
    table.getConf().set(FlinkOptions.READ_AS_STREAMING, true);
    assertDoesNotThrow(() -> NativeHudiSource.validate(table, OUTPUT));
    table.getConf().set(FlinkOptions.READ_AS_STREAMING, false);
    table.getConf().set(FlinkOptions.MERGE_TYPE, "skip_merge");
    assertThrows(IllegalArgumentException.class, () -> NativeHudiSource.validate(table, OUTPUT));
  }

  @Test
  void nullablePrimitivePayloadsStayColumnarThroughJavaMerging() throws Exception {
    sql =
        StreamTableEnvironment.create(
            StreamExecutionEnvironment.createLocalEnvironment(1, memory()));
    String path = temporary.resolve("typed").toUri().toString();
    sql.executeSql(
        "CREATE TABLE typed (id BIGINT,name STRING,ts BIGINT,n INT,active BOOLEAN,"
            + "PRIMARY KEY(id) NOT ENFORCED) WITH ('connector'='hudi','path'='"
            + path
            + "','table.type'='MERGE_ON_READ','write.operation'='upsert','write.tasks'='1',"
            + "'metadata.enabled'='false','compaction.async.enabled'='false',"
            + "'compaction.schedule.enabled'='false','read.source-v2.enabled'='true',"
            + "'precombine.field'='n','index.type'='BUCKET','hoodie.bucket.index.num.buckets'='2',"
            + "'hoodie.logfile.data.block.format'='parquet')");
    sql.executeSql(
            "INSERT INTO typed VALUES (1,CAST(NULL AS STRING),1,1,TRUE),"
                + "(2,'two',1,CAST(NULL AS INT),CAST(NULL AS BOOLEAN)),(3,'three',1,-3,FALSE)")
        .await();
    sql.executeSql("INSERT INTO typed VALUES (1,'updated',2,7,FALSE)").await();
    String query = "SELECT active,n,name,id+1 FROM typed";
    var expected = new ArrayList<String>();
    try (var rows = sql.executeSql(query).collect()) {
      while (rows.hasNext()) expected.add(rows.next().toString());
    }
    tech.streamfusion.planner.NativePlanner.install(sql);
    String plan = tech.streamfusion.planner.NativePlanner.explain(sql, query);
    assertTrue(plan.contains("Java Hudi"), plan);
    assertFalse(plan.contains("RowDataToArrow"), plan);
    var actual = new ArrayList<String>();
    try (var rows = sql.executeSql(query).collect()) {
      while (rows.hasNext()) actual.add(rows.next().toString());
    }
    Collections.sort(actual);
    Collections.sort(expected);
    assertEquals(
        List.of("+I[false, -3, three, 4]", "+I[false, 7, updated, 2]", "+I[null, null, two, 3]"),
        expected);
    assertEquals(expected, actual);
  }

  @Test
  void equalEventVersionsMatchTheJavaWinner() throws Exception {
    fixture("parquet");
    sql.executeSql("INSERT INTO h VALUES (1,'tie',30)").await();
    assertSqlMatchesStock("SELECT name,id+1 FROM h");
  }

  @Test
  void commitTimeUpdatesAndUnorderedKeyDeletesMatchStock() throws Exception {
    var env = StreamExecutionEnvironment.createLocalEnvironment(1, memory());
    sql = StreamTableEnvironment.create(env);
    String path = temporary.resolve("commit-ordered").toUri().toString();
    sql.executeSql(
        "CREATE TABLE h (id BIGINT,name STRING,PRIMARY KEY(id) NOT ENFORCED)"
            + " WITH ('connector'='hudi','path'='"
            + path
            + "','table.type'='MERGE_ON_READ',"
            + "'write.tasks'='1','write.operation'='upsert','metadata.enabled'='false',"
            + "'compaction.async.enabled'='false','compaction.schedule.enabled'='false',"
            + "'read.source-v2.enabled'='true','index.type'='BUCKET','hoodie.bucket.index.num.buckets'='2',"
            + "'hoodie.logfile.data.block.format'='parquet')");
    sql.executeSql("INSERT INTO h VALUES (1,'old'),(2,CAST(NULL AS STRING))").await();
    sql.executeSql("INSERT INTO h VALUES (1,'later')").await();
    assertSqlMatchesStock("SELECT name,id+1 FROM h");
    sql.getConfig().getConfiguration().setString("streamfusion.native.enabled", "false");
    var deletes =
        env.fromCollection(
            List.of(Row.ofKind(RowKind.DELETE, 1L, null), Row.ofKind(RowKind.DELETE, 2L, null)),
            Types.ROW_NAMED(new String[] {"id", "name"}, Types.LONG, Types.STRING));
    sql.createTemporaryView(
        "deletes",
        sql.fromChangelogStream(
            deletes,
            Schema.newBuilder()
                .column("id", DataTypes.BIGINT().notNull())
                .column("name", DataTypes.STRING())
                .primaryKey("id")
                .build()));
    sql.executeSql("INSERT INTO h SELECT * FROM deletes").await();
    assertSqlMatchesStock("SELECT name,id+1 FROM h");
  }

  private void assertSqlMatchesStock(String query) throws Exception {
    var config = sql.getConfig().getConfiguration();
    tech.streamfusion.planner.NativePlanner.install(sql);
    config.setString("streamfusion.native.enabled", "false");
    var expected = new ArrayList<String>();
    try (var rows = sql.executeSql(query).collect()) {
      while (rows.hasNext()) expected.add(rows.next().toString());
    }
    config.setString("streamfusion.native.enabled", "true");
    String plan = tech.streamfusion.planner.NativePlanner.explain(sql, query);
    assertTrue(plan.contains("Java Hudi"), plan);
    assertFalse(plan.contains("RowDataToArrow"), plan);
    var actual = new ArrayList<String>();
    try (var rows = sql.executeSql(query).collect()) {
      while (rows.hasNext()) actual.add(rows.next().toString());
    }
    Collections.sort(actual);
    Collections.sort(expected);
    assertEquals(expected, actual);
  }

  @Test
  void checkpointedPipelineRecoversAfterFailureInsideAMergedGroup() throws Exception {
    var table = fixture("parquet", false, true);
    var source = NativeHudiSource.from(table, OUTPUT, 8, 1);
    assertTrue(splits(source).size() > 1);
    var env = StreamExecutionEnvironment.createLocalEnvironment(1, memory());
    env.enableCheckpointing(100);
    env.setRestartStrategy(
        org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(1, 0));
    String marker = temporary.resolve("failed-after-checkpoint").toString();
    var input =
        env.fromSource(
                source,
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                "MOR input",
                tech.streamfusion.operator.ArrowBatchTypeInformation.INSTANCE)
            .transform(
                "fail inside group",
                tech.streamfusion.operator.ArrowBatchTypeInformation.INSTANCE,
                new HudiCheckpointFailure(marker));
    String path = temporary.resolve("recovered").toUri().toString();
    var conf =
        Configuration.fromMap(
            Map.of(
                "path",
                path,
                "table.type",
                "COPY_ON_WRITE",
                "write.operation",
                "insert",
                "metadata.enabled",
                "false",
                "hoodie.populate.meta.fields",
                "false",
                "partitionpath.field",
                "",
                "read.source-v2.enabled",
                "true"));
    conf.set(FlinkOptions.TABLE_NAME, "recovered");
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "id");
    conf.set(
        FlinkOptions.SOURCE_AVRO_SCHEMA, HoodieSchemaConverter.convertToSchema(OUTPUT).toString());
    NativeHudiAppendSink.append(
        input,
        new org.apache.hudi.table.HoodieTableSink(conf, SCHEMA),
        OUTPUT,
        new org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext(false));
    env.execute("MOR file-group recovery");
    assertTrue(java.nio.file.Files.exists(Path.of(marker)));
    var read =
        StreamTableEnvironment.create(
            StreamExecutionEnvironment.createLocalEnvironment(1, memory()));
    read.executeSql(
        "CREATE TABLE recovered (id BIGINT,name STRING,ts BIGINT,PRIMARY KEY(id) NOT ENFORCED)"
            + " WITH ('connector'='hudi','path'='"
            + path
            + "','table.type'='COPY_ON_WRITE',"
            + "'metadata.enabled'='false','hoodie.populate.meta.fields'='false','read.source-v2.enabled'='true')");
    try (var rows =
        read.executeSql("SELECT COUNT(*),COUNT(DISTINCT id),SUM(id) FROM recovered").collect()) {
      Row row = null;
      while (rows.hasNext()) {
        var next = rows.next();
        if (next.getKind() == RowKind.INSERT || next.getKind() == RowKind.UPDATE_AFTER) row = next;
      }
      assertNotNull(row);
      assertEquals(1000L, row.getField(0));
      assertEquals(1000L, row.getField(1));
      assertEquals(999L * 1000 / 2, row.getField(2));
    }
  }

  @Test
  @EnabledIfSystemProperty(named = "sf.hudi.benchmark", matches = "true")
  void benchmarkCompleteMorAggregatePipeline() throws Exception {
    assertTrue(System.getProperty("java.library.path").contains("/release"), "Use -Pbench");
    int count = Integer.getInteger("sf.hudi.benchmark.rows", 200000);
    int updates = count / 4;
    benchmarkFixture(count, updates);
    tech.streamfusion.planner.NativePlanner.install(sql);
    var config = sql.getConfig().getConfiguration();
    config.setString("table.exec.mini-batch.enabled", "true");
    config.setString("table.exec.mini-batch.allow-latency", "50 ms");
    config.setString("table.exec.mini-batch.size", "4096");
    String query = "SELECT SUM(id),SUM(ts),COUNT(name),COUNT(*) FROM h";
    var times = List.of(new ArrayList<Double>(), new ArrayList<Double>(), new ArrayList<Double>());
    for (int iteration = -2; iteration < 5; iteration++) {
      for (int side = 0; side < 3; side++) {
        int engine = Math.floorMod(iteration + side, 3);
        config.setString("streamfusion.native.enabled", Boolean.toString(engine > 0));
        config.setString("streamfusion.operator.hudiSource.enabled", Boolean.toString(engine == 2));
        String plan = tech.streamfusion.planner.NativePlanner.explain(sql, query);
        assertEquals(engine == 2, plan.contains("Java Hudi"), plan);
        if (engine > 0) assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
        long start = System.nanoTime();
        Row last = null;
        try (var rows = sql.executeSql(query).collect()) {
          while (rows.hasNext()) {
            var row = rows.next();
            if (row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER)
              last = row;
          }
        }
        double millis = (System.nanoTime() - start) / 1_000_000.0;
        assertBenchmarkResult(last, count, updates);
        if (iteration >= 0) times.get(engine).add(millis);
      }
    }
    System.out.println(
        "HUDI_MOR_AGGREGATE_BENCH rows="
            + count
            + " updates="
            + updates
            + " stock_ms="
            + times.get(0)
            + " previous_sf_ms="
            + times.get(1)
            + " native_ms="
            + times.get(2));
  }

  @Test
  @EnabledIfSystemProperty(named = "sf.hudi.benchmark", matches = "true")
  void benchmarkCompleteMorAppendPipeline() throws Exception {
    assertTrue(System.getProperty("java.library.path").contains("/release"), "Use -Pbench");
    int count = Integer.getInteger("sf.hudi.benchmark.rows", 200000);
    int updates = count / 4;
    benchmarkFixture(count, updates);
    tech.streamfusion.planner.NativePlanner.install(sql);
    var config = sql.getConfig().getConfiguration();
    var times = List.of(new ArrayList<Double>(), new ArrayList<Double>(), new ArrayList<Double>());
    for (int iteration = -2; iteration < 5; iteration++) {
      for (int side = 0; side < 3; side++) {
        int engine = Math.floorMod(iteration + side, 3);
        config.setString("streamfusion.native.enabled", Boolean.toString(engine > 0));
        config.setString("streamfusion.operator.hudiSource.enabled", Boolean.toString(engine == 2));
        Path target = temporary.resolve("output-" + iteration + "-" + engine);
        sql.executeSql(
            "CREATE TABLE output (id BIGINT,name STRING,ts BIGINT,PRIMARY KEY(id) NOT ENFORCED)"
                + " WITH ('connector'='hudi','path'='"
                + target.toUri()
                + "','table.type'='COPY_ON_WRITE','write.operation'='insert','write.tasks'='1',"
                + "'write.buffer.type'='NONE','hoodie.populate.meta.fields'='false',"
                + "'metadata.enabled'='false')");
        String query = "INSERT INTO output SELECT id,name,ts FROM h";
        String plan = tech.streamfusion.planner.NativePlanner.explain(sql, query);
        assertEquals(engine == 2, plan.contains("Java Hudi"), plan);
        assertEquals(engine > 0, plan.contains("NativeHudiSink"), plan);
        if (engine == 2) assertFalse(plan.contains("RowToArrow"), plan);
        System.out.println("HUDI_MOR_TRIAL start iteration=" + iteration + " engine=" + engine);
        long start = System.nanoTime();
        sql.executeSql(query).await();
        double millis = (System.nanoTime() - start) / 1_000_000.0;
        System.out.println("HUDI_MOR_TRIAL committed ms=" + millis);
        config.setString("streamfusion.native.enabled", "false");
        Row last = null;
        try (var rows =
            sql.executeSql("SELECT SUM(id),SUM(ts),COUNT(name),COUNT(*) FROM output").collect()) {
          while (rows.hasNext()) {
            var row = rows.next();
            if (row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER)
              last = row;
          }
        }
        assertBenchmarkResult(last, count, updates);
        System.out.println("HUDI_MOR_TRIAL verified iteration=" + iteration + " engine=" + engine);
        sql.executeSql("DROP TABLE output");
        org.apache.flink.util.FileUtils.deleteDirectory(target.toFile());
        if (iteration >= 0) times.get(engine).add(millis);
      }
    }
    System.out.println(
        "HUDI_MOR_APPEND_BENCH rows="
            + count
            + " updates="
            + updates
            + " stock_ms="
            + times.get(0)
            + " previous_sf_ms="
            + times.get(1)
            + " native_ms="
            + times.get(2));
  }

  @Test
  @EnabledIfSystemProperty(named = "sf.hudi.benchmark", matches = "true")
  void benchmarkContinuousMorCalcPipeline() throws Exception {
    assertTrue(System.getProperty("java.library.path").contains("/release"), "Use -Pbench");
    int count = Integer.getInteger("sf.hudi.benchmark.rows", 200000);
    int updates = count / 4;
    int deletes = count / 100;
    benchmarkFixture(count, updates);
    var changes =
        environment
            .fromSequence(0, deletes - 1L)
            .map(id -> Row.ofKind(RowKind.DELETE, count - 1L - id * 4, null, 3L))
            .returns(
                Types.ROW_NAMED(
                    new String[] {"id", "name", "ts"}, Types.LONG, Types.STRING, Types.LONG));
    sql.createTemporaryView(
        "deleted_keys",
        sql.fromChangelogStream(
            changes,
            Schema.newBuilder()
                .column("id", DataTypes.BIGINT().notNull())
                .column("name", DataTypes.STRING())
                .column("ts", DataTypes.BIGINT())
                .primaryKey("id")
                .build()));
    sql.executeSql("INSERT INTO h SELECT * FROM deleted_keys").await();
    sql.executeSql(
        "ALTER TABLE h SET ('read.streaming.enabled'='true','read.start-commit'='earliest',"
            + "'read.streaming.check-interval'='1','changelog.enabled'='true')");
    tech.streamfusion.planner.NativePlanner.install(sql);
    var config = sql.getConfig().getConfiguration();
    String query = "SELECT id+1,name,ts FROM h";
    long expectedNames = 0;
    long expectedHash = 0;
    for (int id = 0; id < count; id++) {
      if (id % 4 == 3 && id >= count - deletes * 4) continue;
      String name = id % 4 == 0 ? "updated-" + id / 4 : id % 7 == 0 ? null : "value-" + id;
      if (name != null) {
        expectedNames++;
        expectedHash += name.hashCode();
      }
    }
    long[] expected = {
      (long) count * (count + 1) / 2,
      (long) count + updates - deletes,
      expectedNames,
      expectedHash,
      deletes,
      0
    };
    var times = List.of(new ArrayList<Double>(), new ArrayList<Double>(), new ArrayList<Double>());
    for (int iteration = -2; iteration < 5; iteration++) {
      for (int side = 0; side < 3; side++) {
        int engine = Math.floorMod(iteration + side, 3);
        config.setString("streamfusion.native.enabled", Boolean.toString(engine > 0));
        config.setString("streamfusion.operator.hudiSource.enabled", Boolean.toString(engine == 2));
        String plan = tech.streamfusion.planner.NativePlanner.explain(sql, query);
        assertEquals(engine == 2, plan.contains("Java Hudi"), plan);
        if (engine > 0) assertTrue(plan.contains("NativeCalc"), plan);
        if (engine == 2) assertFalse(plan.contains("RowDataToArrow"), plan);
        System.out.println("HUDI_MOR_TRIAL start iteration=" + iteration + " engine=" + engine);
        long start = System.nanoTime();
        var running = sql.executeSql(query);
        double millis;
        try (var rows = running.collect()) {
          var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
          try {
            long[] actual =
                executor
                    .submit(
                        () -> {
                          long[] totals = new long[6];
                          for (int i = 0; i < count; i++) {
                            Row row = rows.next();
                            totals[0] += (long) row.getField(0);
                            if (row.getField(2) != null) totals[1] += (long) row.getField(2);
                            if (row.getField(1) != null) {
                              totals[2]++;
                              totals[3] += row.getField(1).hashCode();
                            }
                            if (row.getKind() == RowKind.DELETE) totals[4]++;
                            else if (row.getKind() != RowKind.INSERT) totals[5]++;
                          }
                          return totals;
                        })
                    .get(60, TimeUnit.SECONDS);
            millis = (System.nanoTime() - start) / 1_000_000.0;
            assertArrayEquals(expected, actual);
          } finally {
            executor.shutdownNow();
          }
        } finally {
          NativeHudiSourceTest.awaitStopped(running);
        }
        System.out.println("HUDI_MOR_TRIAL verified ms=" + millis);
        if (iteration >= 0) times.get(engine).add(millis);
      }
    }
    System.out.println(
        "HUDI_MOR_CONTINUOUS_BENCH rows="
            + count
            + " updates="
            + updates
            + " deletes="
            + deletes
            + " stock_ms="
            + times.get(0)
            + " previous_sf_ms="
            + times.get(1)
            + " native_ms="
            + times.get(2));
  }

  private static void assertBenchmarkResult(Row last, int count, int updates) {
    assertNotNull(last);
    assertEquals((long) count * (count - 1) / 2, last.getField(0));
    assertEquals((long) count + updates, last.getField(1));
    assertEquals((long) count - (count + 6) / 7 + (updates + 6) / 7, last.getField(2));
    assertEquals((long) count, last.getField(3));
  }

  private void benchmarkFixture(int count, int updates) throws Exception {
    environment = StreamExecutionEnvironment.createLocalEnvironment(1, memory());
    var env = environment;
    sql = StreamTableEnvironment.create(env);
    String path = temporary.resolve("table").toUri().toString();
    sql.executeSql(
        "CREATE TABLE h (id BIGINT,name STRING,ts BIGINT,PRIMARY KEY(id) NOT ENFORCED)"
            + " WITH ('connector'='hudi','path'='"
            + path
            + "','table.type'='MERGE_ON_READ',"
            + "'write.tasks'='1','write.operation'='upsert','metadata.enabled'='false',"
            + "'compaction.async.enabled'='false','compaction.schedule.enabled'='false',"
            + "'read.source-v2.enabled'='true','precombine.field'='ts','write.precombine'='false',"
            + "'index.bootstrap.enabled'='true','hoodie.logfile.data.block.format'='parquet',"
            + "'compaction.max_memory'='256')");
    var rowType =
        Types.ROW_NAMED(new String[] {"id", "name", "ts"}, Types.LONG, Types.STRING, Types.LONG);
    sql.createTemporaryView(
        "seed",
        env.fromSequence(0, count - 1L)
            .map(id -> Row.of(id, id % 7 == 0 ? null : "value-" + id, 1L))
            .returns(rowType));
    sql.executeSql("INSERT INTO h /*+ OPTIONS('write.operation'='insert') */ SELECT * FROM seed")
        .await();
    sql.createTemporaryView(
        "updates",
        env.fromSequence(0, updates - 1L)
            .map(id -> Row.of(id * 4, "updated-" + id, 2L))
            .returns(rowType));
    sql.executeSql("INSERT INTO h SELECT * FROM updates").await();
  }

  private HoodieTableSource fixture(String format) throws Exception {
    return fixture(format, false);
  }

  private HoodieTableSource fixture(String format, boolean base) throws Exception {
    return fixture(format, base, false);
  }

  private HoodieTableSource fixture(String format, boolean base, boolean buckets) throws Exception {
    return fixture(format, base, buckets, false);
  }

  private HoodieTableSource fixture(
      String format, boolean base, boolean buckets, boolean storedOperations) throws Exception {
    environment = StreamExecutionEnvironment.createLocalEnvironment(1, memory());
    var env = environment;
    sql = StreamTableEnvironment.create(env);
    String path = temporary.resolve("table").toUri().toString();
    var options = new java.util.HashMap<String, String>();
    options.put("connector", "hudi");
    options.put("path", path);
    options.put("table.type", "MERGE_ON_READ");
    options.put("write.tasks", "1");
    options.put("write.operation", "upsert");
    options.put("metadata.enabled", "false");
    options.put("compaction.async.enabled", "false");
    options.put("compaction.schedule.enabled", "false");
    options.put("read.source-v2.enabled", "true");
    options.put("precombine.field", "ts");
    options.put("write.precombine", "false");
    options.put("index.bootstrap.enabled", "true");
    options.put("hoodie.logfile.data.block.format", format);
    if (storedOperations) options.put("changelog.enabled", "true");
    if (buckets) {
      options.put("index.type", "BUCKET");
      options.put("hoodie.bucket.index.num.buckets", "8");
    }
    String ddl =
        options.entrySet().stream()
            .map(entry -> "'" + entry.getKey() + "'='" + entry.getValue() + "'")
            .collect(java.util.stream.Collectors.joining(","));
    sql.executeSql(
        "CREATE TABLE h (id BIGINT,name STRING,ts BIGINT,PRIMARY KEY(id) NOT ENFORCED) WITH ("
            + ddl
            + ")");
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
    if (base) {
      sql.executeSql(
              "INSERT INTO h /*+ OPTIONS('write.operation'='insert') */ VALUES "
                  + "(1,'old',10),(2,'untouched',20),(3,'deleted',30),(4,'surviving',30)")
          .await();
      changes = changes.subList(4, changes.size());
    }
    if (buckets) {
      changes = new ArrayList<>();
      for (int i = 0; i < 1000; i++)
        changes.add(Row.ofKind(RowKind.INSERT, (long) i, "value-" + i, 1L));
    }
    var input =
        env.fromCollection(
            changes,
            Types.ROW_NAMED(
                new String[] {"id", "name", "ts"}, Types.LONG, Types.STRING, Types.LONG));
    sql.createTemporaryView(
        "mutations",
        sql.fromChangelogStream(
            input,
            Schema.newBuilder()
                .column("id", DataTypes.BIGINT().notNull())
                .column("name", DataTypes.STRING())
                .column("ts", DataTypes.BIGINT())
                .primaryKey("id")
                .build()));
    sql.executeSql("INSERT INTO h SELECT * FROM mutations").await();
    var conf = Configuration.fromMap(options);
    conf.set(FlinkOptions.TABLE_NAME, "h");
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "id");
    conf.set(
        FlinkOptions.SOURCE_AVRO_SCHEMA, HoodieSchemaConverter.convertToSchema(OUTPUT).toString());
    return new HoodieTableSource(
        SerializableSchema.create(SCHEMA),
        new StoragePath(path),
        List.of(),
        "__HIVE_DEFAULT_PARTITION__",
        conf);
  }

  private Configuration memory() {
    var conf = new Configuration();
    conf.set(
        org.apache.flink.configuration.TaskManagerOptions.TASK_OFF_HEAP_MEMORY,
        org.apache.flink.configuration.MemorySize.ofMebiBytes(1024));
    conf.set(org.apache.flink.configuration.CoreOptions.TMP_DIRS, temporary.toString());
    return conf;
  }

  @SuppressWarnings("unchecked")
  private SourceReaderContext context() {
    var metrics =
        org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup.wrap(
            org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups
                .createUnregisteredOperatorMetricGroup());
    return (SourceReaderContext)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {SourceReaderContext.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "getConfiguration":
                  return memory();
                case "metricGroup":
                  return metrics;
                case "getIndexOfSubtask":
                  return 0;
                case "currentParallelism":
                  return 1;
                case "getUserCodeClassLoader":
                  return SimpleUserCodeClassLoader.create(getClass().getClassLoader());
                default:
                  return null;
              }
            });
  }

  @SuppressWarnings("unchecked")
  private List<HoodieSourceSplit> splits(NativeHudiSource source) throws Exception {
    var assigned = new ArrayList<HoodieSourceSplit>();
    var context =
        (SplitEnumeratorContext<HoodieSourceSplit>)
            Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {SplitEnumeratorContext.class},
                (proxy, method, args) -> {
                  switch (method.getName()) {
                    case "currentParallelism":
                      return 1;
                    case "registeredReaders":
                      return Map.of(0, new ReaderInfo(0, "localhost"));
                    case "assignSplit":
                      assigned.add((HoodieSourceSplit) args[0]);
                      return null;
                    case "metricGroup":
                      return UnregisteredMetricsGroup.createSplitEnumeratorMetricGroup();
                    default:
                      return null;
                  }
                });
    try (var enumerator = source.createEnumerator(context)) {
      enumerator.start();
      int before;
      do {
        before = assigned.size();
        enumerator.handleSplitRequest(0, "localhost");
      } while (assigned.size() > before);
    }
    return assigned;
  }

  private static ReaderOutput<ArrowBatch> output(List<String> rows) {

    return (ReaderOutput<ArrowBatch>)
        Proxy.newProxyInstance(
            NativeHudiMorSourceTest.class.getClassLoader(),
            new Class<?>[] {ReaderOutput.class},
            (proxy, method, args) -> {
              if (method.getName().equals("collect")) {
                try (var root = ((ArrowBatch) args[0]).root()) {
                  for (int row = 0; row < root.getRowCount(); row++) {
                    var kinds =
                        (org.apache.arrow.vector.TinyIntVector)
                            root.getVector(
                                tech.streamfusion.operator.RowDataArrowConverter.ROW_KIND_COLUMN);
                    var kind =
                        kinds == null ? RowKind.INSERT : RowKind.fromByteValue(kinds.get(row));
                    rows.add(
                        kind.shortString()
                            + "["
                            + root.getVector("id").getObject(row)
                            + ", "
                            + root.getVector("name").getObject(row)
                            + ", "
                            + root.getVector("ts").getObject(row)
                            + "]");
                  }
                }
              }
              if (method.getName().equals("createOutputForSplit")) return proxy;
              return null;
            });
  }
}
