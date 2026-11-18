package tech.streamfusion.hudi;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTableSource;
import org.apache.hudi.util.SerializableSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.TaskOffHeapMemory;
import tech.streamfusion.parquet.NativeParquet;

class NativeHudiSourceTest {
  @TempDir Path temporary;
  private StreamTableEnvironment sql;
  private static final RowType OUTPUT =
      RowType.of(
          new org.apache.flink.table.types.logical.LogicalType[] {
            new VarCharType(VarCharType.MAX_LENGTH), new BigIntType()
          },
          new String[] {"name", "id"});

  @BeforeAll
  static void initializeMemory() {
    TaskOffHeapMemory.initialize(memoryConfiguration());
  }

  @Test
  void tablesWithoutACompletedSnapshotRetainTheStockSource() throws Exception {
    String path = temporary.resolve("missing-table").toUri().toString();
    var conf = Configuration.fromMap(Map.of("path", path, "read.source-v2.enabled", "true"));
    var schema =
        ResolvedSchema.of(
            Column.physical("name", DataTypes.STRING()), Column.physical("id", DataTypes.BIGINT()));
    conf.set(FlinkOptions.TABLE_NAME, "empty");
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "id");
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "");
    conf.set(FlinkOptions.METADATA_ENABLED, false);
    conf.set(
        FlinkOptions.SOURCE_AVRO_SCHEMA,
        org.apache.hudi.util.HoodieSchemaConverter.convertToSchema(OUTPUT).toString());
    org.apache.hudi.util.StreamerUtil.initTableIfNotExists(conf);
    var table =
        new HoodieTableSource(
            SerializableSchema.create(schema),
            new StoragePath(path),
            List.of(),
            "__HIVE_DEFAULT_PARTITION__",
            conf);
    assertThrows(IllegalArgumentException.class, () -> NativeHudiSource.validate(table, OUTPUT));
  }

  @AfterEach
  void releasesNativeReadersAndReservations() {
    assertTrue(NativeParquet.liveNativeHandles().isBlank());
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
  }

  private static Configuration memoryConfiguration() {
    var conf = new Configuration();
    conf.set(
        org.apache.flink.configuration.TaskManagerOptions.TASK_OFF_HEAP_MEMORY,
        org.apache.flink.configuration.MemorySize.ofMebiBytes(1024));
    return conf;
  }

  @Test
  void stockWritesNativeProjectionAndChangedBatchSizeRecovery() throws Exception {
    HoodieTableSource table = fixture(103);
    NativeHudiSource source = NativeHudiSource.from(table, OUTPUT, 11, 1);
    List<HoodieSourceSplit> splits = splits(source);
    assertFalse(splits.isEmpty());
    List<String> actual = new ArrayList<>();
    List<HoodieSourceSplit> checkpoint;
    var context = context();
    try (var reader = source.createReader(context)) {
      reader.addSplits(splits);
      reader.start();
      assertTrue(reader.snapshotState(0).stream().allMatch(s -> s.getConsumed() == 0));
      while (actual.isEmpty()) {
        reader.isAvailable().get(10, TimeUnit.SECONDS);
        reader.pollNext(output(actual));
      }
      checkpoint = reader.snapshotState(1);
      assertTrue(actual.size() < 103);
      assertEquals(
          actual.size(),
          context.metricGroup().getIOMetricGroup().getNumRecordsInCounter().getCount());
      assertEquals(
          actual.size(), checkpoint.stream().mapToLong(HoodieSourceSplit::getConsumed).sum());
      var serializer = source.getSplitSerializer();
      var restored = new ArrayList<HoodieSourceSplit>();
      for (var split : checkpoint)
        restored.add(serializer.deserialize(serializer.getVersion(), serializer.serialize(split)));
      checkpoint = restored;
    }
    var restored = NativeHudiSource.from(table, OUTPUT, 3, 1);
    try (var reader = restored.createReader(context())) {
      reader.addSplits(checkpoint);
      reader.notifyNoMoreSplits();
      reader.start();
      InputStatus status;
      do {
        reader.isAvailable().get(10, TimeUnit.SECONDS);
        status = reader.pollNext(output(actual));
      } while (status != InputStatus.END_OF_INPUT);
    }
    var expected = new ArrayList<String>();
    for (int i = 0; i < 103; i++) expected.add((i % 7 == 0 ? "null" : "value-" + i) + ":" + i);
    java.util.Collections.sort(expected);
    java.util.Collections.sort(actual);
    assertEquals(expected, actual);
    assertTrue(NativeParquet.liveNativeHandles().isBlank());
  }

  @Test
  void declinesUnverifiedSourceContracts() throws Exception {
    var table = fixture(2);
    table.getConf().set(FlinkOptions.READ_START_COMMIT, "earliest");
    assertThrows(IllegalArgumentException.class, () -> NativeHudiSource.validate(table, OUTPUT));
    table.getConf().removeConfig(FlinkOptions.READ_START_COMMIT);
    table.getConf().set(FlinkOptions.READ_AS_STREAMING, true);
    assertDoesNotThrow(() -> NativeHudiSource.validate(table, OUTPUT));
    table.getConf().set(FlinkOptions.READ_END_COMMIT, "99999999999999999");
    assertThrows(IllegalArgumentException.class, () -> NativeHudiSource.validate(table, OUTPUT));
    table.getConf().removeConfig(FlinkOptions.READ_END_COMMIT);
    table.getConf().set(FlinkOptions.READ_AS_STREAMING, false);
    table.applyLimit(1);
    assertThrows(IllegalArgumentException.class, () -> NativeHudiSource.validate(table, OUTPUT));
    table.applyLimit(-1);
    table.getConf().set(FlinkOptions.CDC_ENABLED, true);
    assertThrows(IllegalArgumentException.class, () -> NativeHudiSource.validate(table, OUTPUT));
  }

  @Test
  void continuousCowRetainsJavaDiscoveryAndResumesFilteredRowsAfterCheckpoint() throws Exception {
    var table = fixture(37);
    table.getConf().set(FlinkOptions.READ_AS_STREAMING, true);
    table.getConf().set(FlinkOptions.READ_START_COMMIT, "earliest");
    var source = NativeHudiSource.from(table, OUTPUT, 7, 1);
    assertEquals(
        org.apache.flink.api.connector.source.Boundedness.CONTINUOUS_UNBOUNDED,
        source.getBoundedness());
    org.apache.hudi.source.enumerator.HoodieSplitEnumeratorState state;
    List<HoodieSourceSplit> initial;
    try (var discovery = new ContinuousDiscovery(source, null)) {
      initial = discovery.discover();
      assertFalse(initial.isEmpty());
      state = discovery.enumerator.snapshotState(1);
    }
    var actual = new ArrayList<String>();
    readSplits(source, initial, actual);
    assertEquals(37, actual.size());
    sql.executeSql(
            "INSERT INTO h /*+ OPTIONS('write.operation'='upsert') */ VALUES"
                + " (1,'updated'),(100,'new')")
        .await();
    var stateCodec = source.getEnumeratorCheckpointSerializer();
    state = stateCodec.deserialize(stateCodec.getVersion(), stateCodec.serialize(state));
    List<HoodieSourceSplit> updates;
    try (var restoredDiscovery = new ContinuousDiscovery(source, state)) {
      updates = restoredDiscovery.discover();
      assertFalse(updates.isEmpty());
      assertTrue(updates.stream().allMatch(split -> split.getInstantRange().isPresent()));
    }
    var changed = new ArrayList<String>();
    List<HoodieSourceSplit> pending;
    try (var reader = source.createReader(context())) {
      reader.addSplits(updates);
      reader.start();
      while (changed.isEmpty()) {
        reader.isAvailable().get(10, TimeUnit.SECONDS);
        reader.pollNext(output(changed));
      }
      pending = reader.snapshotState(2);
    }
    var splitCodec = source.getSplitSerializer();
    var saved = new ArrayList<HoodieSourceSplit>();
    for (var split : pending)
      saved.add(splitCodec.deserialize(splitCodec.getVersion(), splitCodec.serialize(split)));
    readSplits(NativeHudiSource.from(table, OUTPUT, 1, 1), saved, changed);
    java.util.Collections.sort(changed);
    assertEquals(List.of("new:100", "updated:1"), changed);
  }

  private static void readSplits(
      NativeHudiSource source, List<HoodieSourceSplit> splits, List<String> output)
      throws Exception {
    try (var reader = source.createReader(context())) {
      reader.addSplits(splits);
      reader.notifyNoMoreSplits();
      reader.start();
      InputStatus status;
      do {
        reader.isAvailable().get(10, TimeUnit.SECONDS);
        status = reader.pollNext(output(output));
      } while (status != InputStatus.END_OF_INPUT);
    }
  }

  @Test
  void continuousSqlMatchesStockAcrossANewCommit() throws Exception {
    fixture(37);
    sql.executeSql(
        "ALTER TABLE h SET ('read.streaming.enabled'='true',"
            + "'read.start-commit'='earliest','read.streaming.check-interval'='1')");
    tech.streamfusion.planner.NativePlanner.install(sql);
    var config = sql.getConfig().getConfiguration();
    String query = "SELECT name,id+1 FROM h";
    for (boolean nativeRead : List.of(false, true)) {
      config.setString("streamfusion.native.enabled", Boolean.toString(nativeRead));
      assertEquals(
          nativeRead,
          sql.explainSql(query).contains("native Parquet"),
          tech.streamfusion.planner.NativePlanner.explain(sql, query));
      var actual = new ArrayList<String>();
      var running = sql.executeSql(query);
      try (var rows = running.collect()) {
        var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
          var initial =
              executor.submit(
                  () -> {
                    for (int i = 0; i < 37; i++) actual.add(rows.next().toString());
                  });
          initial.get(30, TimeUnit.SECONDS);
          String changed = nativeRead ? "native-update" : "stock-update";
          sql.executeSql(
                  "INSERT INTO h /*+ OPTIONS('write.operation'='upsert') */ VALUES (1,'"
                      + changed
                      + "')")
              .await();
          assertEquals(
              "+I[" + changed + ", 2]",
              executor.submit(() -> rows.next().toString()).get(30, TimeUnit.SECONDS));
        } finally {
          executor.shutdownNow();
        }
      }
      awaitStopped(running);
      assertEquals(37, actual.size());
    }
  }

  @Test
  @EnabledIfSystemProperty(named = "sf.hudi.benchmark", matches = "true")
  void benchmarkContinuousAggregatePipeline() throws Exception {
    assertTrue(System.getProperty("java.library.path").contains("/release"), "Use -Pbench");
    int count = Integer.getInteger("sf.hudi.benchmark.rows", 200000);
    fixture(count);
    sql.executeSql(
        "ALTER TABLE h SET ('read.streaming.enabled'='true',"
            + "'read.streaming.check-interval'='1')");
    tech.streamfusion.planner.NativePlanner.install(sql);
    var config = sql.getConfig().getConfiguration();
    config.setString("table.exec.mini-batch.enabled", "true");
    config.setString("table.exec.mini-batch.allow-latency", "50 ms");
    config.setString("table.exec.mini-batch.size", "4096");
    String query = "SELECT SUM(id), COUNT(name), COUNT(*) FROM h";
    var times = List.of(new ArrayList<Double>(), new ArrayList<Double>(), new ArrayList<Double>());
    for (int iteration = -2; iteration < 5; iteration++) {
      for (int side = 0; side < 3; side++) {
        int engine = Math.floorMod(iteration + side, 3);
        config.setString("streamfusion.native.enabled", Boolean.toString(engine > 0));
        config.setString("streamfusion.operator.hudiSource.enabled", Boolean.toString(engine == 2));
        String plan = sql.explainSql(query);
        assertEquals(engine == 2, plan.contains("native Parquet"), plan);
        if (engine > 0) assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
        long start = System.nanoTime();
        double millis;
        var running = sql.executeSql(query);
        try (var rows = running.collect()) {
          var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
          try {
            var finalRow =
                executor
                    .submit(
                        () -> {
                          while (rows.hasNext()) {
                            var row = rows.next();
                            if ((row.getKind() == org.apache.flink.types.RowKind.INSERT
                                    || row.getKind() == org.apache.flink.types.RowKind.UPDATE_AFTER)
                                && Long.valueOf(count).equals(row.getField(2))) return row;
                          }
                          throw new AssertionError(
                              "Continuous Hudi did not reach the expected count");
                        })
                    .get(45, TimeUnit.SECONDS);
            millis = (System.nanoTime() - start) / 1_000_000.0;
            assertEquals((long) count * (count - 1) / 2, finalRow.getField(0));
            assertEquals((long) count - (count + 6) / 7, finalRow.getField(1));
          } finally {
            executor.shutdownNow();
          }
        }
        awaitStopped(running);
        if (iteration >= 0) times.get(engine).add(millis);
      }
    }
    System.out.println(
        "HUDI_CONTINUOUS_AGGREGATE_BENCH rows="
            + count
            + " stock_ms="
            + times.get(0)
            + " previous_sf_ms="
            + times.get(1)
            + " native_ms="
            + times.get(2));
  }

  static void awaitStopped(org.apache.flink.table.api.TableResult running) throws Exception {
    running
        .getJobClient()
        .orElseThrow()
        .getJobExecutionResult()
        .handle((result, failure) -> null)
        .get(30, TimeUnit.SECONDS);
  }

  static final class ContinuousDiscovery implements AutoCloseable {
    final org.apache.flink.api.connector.source.SplitEnumerator<
            HoodieSourceSplit, org.apache.hudi.source.enumerator.HoodieSplitEnumeratorState>
        enumerator;
    final List<HoodieSourceSplit> assigned = new ArrayList<>();
    Runnable discovery;

    @SuppressWarnings({"unchecked", "rawtypes"})
    ContinuousDiscovery(
        NativeHudiSource source, org.apache.hudi.source.enumerator.HoodieSplitEnumeratorState state)
        throws Exception {
      var context =
          (SplitEnumeratorContext<HoodieSourceSplit>)
              Proxy.newProxyInstance(
                  NativeHudiSourceTest.class.getClassLoader(),
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
                      case "callAsync":
                        discovery =
                            () -> {
                              Object result = null;
                              Throwable failure = null;
                              try {
                                result = ((java.util.concurrent.Callable) args[0]).call();
                              } catch (Throwable caught) {
                                failure = caught;
                              }
                              ((java.util.function.BiConsumer) args[1]).accept(result, failure);
                            };
                        return null;
                      case "metricGroup":
                        return UnregisteredMetricsGroup.createSplitEnumeratorMetricGroup();
                      default:
                        return null;
                    }
                  });
      enumerator =
          state == null
              ? source.createEnumerator(context)
              : source.restoreEnumerator(context, state);
      enumerator.start();
    }

    List<HoodieSourceSplit> discover() {
      discovery.run();
      int previous;
      do {
        previous = assigned.size();
        enumerator.handleSplitRequest(0, "localhost");
      } while (assigned.size() > previous);
      return new ArrayList<>(assigned);
    }

    @Override
    public void close() throws Exception {
      enumerator.close();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"SNAPPY", "GZIP", "ZSTD", "UNCOMPRESSED"})
  void stockReadsArrowAppendWithJavaCommitAndFileRolling(String compression) throws Exception {
    var table = fixture(103, true);
    var output =
        RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
              new VarCharType(VarCharType.MAX_LENGTH),
              new BigIntType(),
              new org.apache.flink.table.types.logical.IntType(),
              new org.apache.flink.table.types.logical.BooleanType()
            },
            new String[] {"name", "id", "n", "active"});
    var source = NativeHudiSource.from(table, output, 7, 1);
    var env = StreamExecutionEnvironment.createLocalEnvironment(1, memoryConfiguration());
    var input =
        env.fromSource(
            source,
            org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
            "input",
            tech.streamfusion.operator.ArrowBatchTypeInformation.INSTANCE);
    var schema =
        ResolvedSchema.of(
            Column.physical("name", DataTypes.STRING()),
            Column.physical("id", DataTypes.BIGINT()),
            Column.physical("n", DataTypes.INT()),
            Column.physical("active", DataTypes.BOOLEAN()));
    String path = temporary.resolve("native-output").toUri().toString();
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
                "true",
                "hoodie.parquet.max.file.size",
                "100"));
    conf.setString("hoodie.parquet.compression.codec", compression);
    conf.set(FlinkOptions.TABLE_NAME, "native_output");
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "id");
    conf.set(
        FlinkOptions.SOURCE_AVRO_SCHEMA,
        org.apache.hudi.util.HoodieSchemaConverter.convertToSchema(output).toString());
    NativeHudiAppendSink.append(
        input,
        new org.apache.hudi.table.HoodieTableSink(conf, schema),
        output,
        new org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext(true));
    env.execute("Arrow append");
    var target =
        new HoodieTableSource(
            SerializableSchema.create(schema),
            new StoragePath(path),
            List.of(),
            "__HIVE_DEFAULT_PARTITION__",
            conf);
    var meta = target.getMetaClient();
    var files = target.getBaseFileOnlyFileSlices(meta);
    assertTrue(files.size() > 1, "small target size must roll between Arrow batches");
    long written = 0;
    for (var file : files) {
      var footer =
          org.apache.hudi.common.util.ParquetUtils.readMetadata(
              meta.getStorage(), file.getBaseFile().get().getStoragePath());
      assertTrue(footer.getFileMetaData().getCreatedBy().contains("parquet-rs"));
      for (var block : footer.getBlocks()) {
        written += block.getRowCount();
        for (var column : block.getColumns()) assertEquals(compression, column.getCodec().name());
      }
    }
    assertEquals(103, written);
    sql.executeSql(
        "CREATE TABLE native_output (name STRING, id BIGINT, n INT, active BOOLEAN,"
            + " PRIMARY KEY(id) NOT ENFORCED) WITH ('connector'='hudi','path'='"
            + path
            + "','table.type'='COPY_ON_WRITE','metadata.enabled'='false',"
            + "'hoodie.populate.meta.fields'='false','read.source-v2.enabled'='true')");
    var expected = new ArrayList<String>();
    var actual = new ArrayList<String>();
    try (var rows = sql.executeSql("SELECT name, id, n, active FROM h").collect()) {
      while (rows.hasNext()) expected.add(rows.next().toString());
    }
    try (var rows = sql.executeSql("SELECT name, id, n, active FROM native_output").collect()) {
      while (rows.hasNext()) actual.add(rows.next().toString());
    }
    java.util.Collections.sort(expected);
    java.util.Collections.sort(actual);
    assertEquals(expected, actual);
  }

  @Test
  void validatedSnapshotRemainsFixedAcrossLaterCommits() throws Exception {
    var table = fixture(5);
    var plan = NativeHudiSource.plan(table, OUTPUT);
    sql.executeSql("INSERT INTO h VALUES (99, 'later')").await();
    var source = plan.create(3, 1);
    long[] count = {0};
    consume(
        source,
        splits(source),
        batch -> {
          try (var root = batch.root()) {
            count[0] += root.getRowCount();
          }
        });
    assertEquals(5, count[0], "admission and enumeration must use the same visible snapshot");
  }

  @Test
  void sqlAppendsStayColumnarAndRetainStockFallback() throws Exception {
    fixture(103, true);
    String path = temporary.resolve("sql-output").toUri().toString();
    sql.executeSql(
        "CREATE TABLE native_output (name STRING, id BIGINT, n INT, active BOOLEAN,"
            + " PRIMARY KEY(id) NOT ENFORCED) WITH ('connector'='hudi','path'='"
            + path
            + "','table.type'='COPY_ON_WRITE','write.operation'='insert','write.tasks'='1',"
            + "'metadata.enabled'='false','hoodie.populate.meta.fields'='false',"
            + "'read.source-v2.enabled'='true')");
    tech.streamfusion.planner.NativePlanner.install(sql);
    String query = "INSERT INTO native_output SELECT name, id, n, active FROM h";
    String plan = sql.explainSql(query);
    assertTrue(plan.contains("NativeHudiSink"), plan);
    assertTrue(plan.contains("native Parquet"), plan);
    assertFalse(plan.contains("RowToArrow"), plan);
    assertFalse(plan.contains("ArrowToRow"), plan);
    sql.executeSql(query).await();
    sql.getConfig().getConfiguration().setString("streamfusion.native.enabled", "false");
    var actual = new ArrayList<String>();
    var expected = new ArrayList<String>();
    try (var rows = sql.executeSql("SELECT name,id,n,active FROM native_output").collect()) {
      while (rows.hasNext()) actual.add(rows.next().toString());
    }
    try (var rows = sql.executeSql("SELECT name,id,n,active FROM h").collect()) {
      while (rows.hasNext()) expected.add(rows.next().toString());
    }
    java.util.Collections.sort(actual);
    java.util.Collections.sort(expected);
    assertEquals(expected, actual);
    sql.getConfig().getConfiguration().setString("streamfusion.native.enabled", "true");
    assertFalse(
        sql.explainSql("INSERT INTO h SELECT id,name,n,active FROM native_output")
            .contains("NativeHudiSink"),
        "metadata-bearing sink must stay stock");
  }

  @Test
  void appendRestoresTheOriginalCoordinatorAfterCommittedFailure() throws Exception {
    var env = StreamExecutionEnvironment.createLocalEnvironment(1, memoryConfiguration());
    env.enableCheckpointing(100);
    env.setRestartStrategy(
        org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(1, 0));
    String failed = temporary.resolve("failed-once").toString();
    var input =
        env.addSource(
            new RecoveringArrowSource(failed),
            "recoverable-input",
            tech.streamfusion.operator.ArrowBatchTypeInformation.INSTANCE);
    var schema =
        ResolvedSchema.of(
            Column.physical("name", DataTypes.STRING()), Column.physical("id", DataTypes.BIGINT()));
    String path = temporary.resolve("recovered-output").toUri().toString();
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
        FlinkOptions.SOURCE_AVRO_SCHEMA,
        org.apache.hudi.util.HoodieSchemaConverter.convertToSchema(OUTPUT).toString());
    NativeHudiAppendSink.append(
        input,
        new org.apache.hudi.table.HoodieTableSink(conf, schema),
        OUTPUT,
        new org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext(false));
    var execution = env.execute("recover native append");
    assertEquals(1, execution.<Integer>getAccumulatorResult("recoveredAttempt"));
    assertTrue(java.nio.file.Files.exists(Path.of(failed)));
    var readEnv = StreamExecutionEnvironment.createLocalEnvironment(1, memoryConfiguration());
    var read = StreamTableEnvironment.create(readEnv);
    read.executeSql(
        "CREATE TABLE recovered (name STRING,id BIGINT, PRIMARY KEY(id) NOT ENFORCED)"
            + " WITH ('connector'='hudi','path'='"
            + path
            + "','table.type'='COPY_ON_WRITE',"
            + "'metadata.enabled'='false','hoodie.populate.meta.fields'='false','read.source-v2.enabled'='true')");
    var ids = new java.util.TreeSet<Long>();
    int count = 0;
    try (var rows = read.executeSql("SELECT id FROM recovered").collect()) {
      while (rows.hasNext()) {
        count++;
        ids.add((Long) rows.next().getField(0));
      }
    }
    assertEquals(1000, count);
    assertEquals(1000, ids.size());
    assertEquals(0L, ids.first());
    assertEquals(999L, ids.last());
    var target =
        new HoodieTableSource(
            SerializableSchema.create(schema),
            new StoragePath(path),
            List.of(),
            "__HIVE_DEFAULT_PARTITION__",
            conf);
    var meta = target.getMetaClient();
    var files = target.getBaseFileOnlyFileSlices(meta);
    assertTrue(files.size() > 1, "Recovery must retain checkpoint-rolled data files");
    for (var file : files) {
      var storagePath = file.getBaseFile().get().getStoragePath();
      var footer =
          org.apache.hudi.common.util.ParquetUtils.readMetadata(meta.getStorage(), storagePath);
      assertTrue(
          footer.getFileMetaData().getCreatedBy().contains("parquet-rs"),
          storagePath + ": " + footer.getFileMetaData().getCreatedBy());
    }
  }

  private static final class RecoveringArrowSource
      extends org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction<ArrowBatch>
      implements org.apache.flink.streaming.api.checkpoint.CheckpointedFunction,
          org.apache.flink.api.common.state.CheckpointListener {
    private final String marker;
    private volatile boolean running = true;
    private volatile boolean checkpointComplete;
    private int next;
    private transient org.apache.flink.api.common.state.ListState<Integer> state;

    RecoveringArrowSource(String marker) {
      this.marker = marker;
    }

    @Override
    public void initializeState(
        org.apache.flink.runtime.state.FunctionInitializationContext context) throws Exception {
      state =
          context
              .getOperatorStateStore()
              .getListState(
                  new org.apache.flink.api.common.state.ListStateDescriptor<>(
                      "offset", Integer.class));
      for (int restored : state.get()) next = restored;
    }

    @Override
    public void snapshotState(org.apache.flink.runtime.state.FunctionSnapshotContext context)
        throws Exception {
      state.update(List.of(next));
    }

    @Override
    public void notifyCheckpointComplete(long checkpoint) {
      checkpointComplete = true;
    }

    @Override
    public void cancel() {
      running = false;
    }

    @Override
    public void run(SourceContext<ArrowBatch> context) throws Exception {
      getRuntimeContext()
          .getIntCounter("recoveredAttempt")
          .add(getRuntimeContext().getAttemptNumber());
      while (running && next < 1000) {
        synchronized (context.getCheckpointLock()) {
          if (checkpointComplete && !java.nio.file.Files.exists(Path.of(marker))) {
            java.nio.file.Files.createFile(Path.of(marker));
            throw new RuntimeException("failure after a completed Hudi checkpoint");
          }
          var rows = new ArrayList<org.apache.flink.table.data.RowData>();
          int end = Math.min(next + 17, 1000);
          for (int i = next; i < end; i++)
            rows.add(
                org.apache.flink.table.data.GenericRowData.of(
                    org.apache.flink.table.data.StringData.fromString("value-" + i), (long) i));
          var root =
              tech.streamfusion.operator.RowDataArrowConverter.write(
                  rows, OUTPUT, tech.streamfusion.operator.NativeAllocator.SHARED);
          var batch = new ArrowBatch(root);
          try {
            context.collect(batch);
          } catch (RuntimeException | Error failure) {
            batch.root().close();
            throw failure;
          }
          next = end;
        }
        Thread.sleep(20);
      }
    }
  }

  @Test
  void appendAdmissionRejectsUnverifiedFileAndFailurePolicies() throws Exception {
    var conf =
        Configuration.fromMap(
            Map.of(
                "path",
                temporary.resolve("target").toUri().toString(),
                "table.type",
                "COPY_ON_WRITE",
                "write.operation",
                "insert",
                "metadata.enabled",
                "false",
                "hoodie.populate.meta.fields",
                "false",
                "partitionpath.field",
                ""));
    conf.set(FlinkOptions.TABLE_NAME, "target");
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "id");
    conf.set(
        FlinkOptions.SOURCE_AVRO_SCHEMA,
        org.apache.hudi.util.HoodieSchemaConverter.convertToSchema(OUTPUT).toString());
    var schema =
        ResolvedSchema.of(
            Column.physical("name", DataTypes.STRING()), Column.physical("id", DataTypes.BIGINT()));
    var table = new org.apache.hudi.table.HoodieTableSink(conf, schema);
    assertDoesNotThrow(() -> NativeHudiAppendSink.validate(table, OUTPUT));
    for (var setting :
        List.of(
            Map.entry("hoodie.populate.meta.fields", "true"),
            Map.entry("hoodie.metadata.enable", "true"),
            Map.entry("write.ignore.failed", "true"),
            Map.entry("hoodie.table.base.file.format", "ORC"),
            Map.entry("hoodie.parquet.compression.codec", "LZO"),
            Map.entry("parquet.encryption.footer.key", "key"),
            Map.entry("hoodie.parquet.page.row.count.limit", "1"))) {
      var changed = new Configuration(conf);
      changed.setString(setting.getKey(), setting.getValue());
      var rejected = new org.apache.hudi.table.HoodieTableSink(changed, schema);
      assertThrows(
          IllegalArgumentException.class,
          () -> NativeHudiAppendSink.validate(rejected, OUTPUT),
          setting.toString());
    }
    table.applyOverwrite(true);
    assertThrows(
        IllegalArgumentException.class, () -> NativeHudiAppendSink.validate(table, OUTPUT));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void sqlAppendRetainsNotNullErrors(boolean nativeMode) throws Exception {
    var env = StreamExecutionEnvironment.createLocalEnvironment(1, memoryConfiguration());
    sql = StreamTableEnvironment.create(env);
    String path = temporary.resolve("not-null-output").toUri().toString();
    sql.executeSql(
        "CREATE TABLE target (name STRING,id BIGINT,PRIMARY KEY(id) NOT ENFORCED) WITH"
            + " ('connector'='hudi','path'='"
            + path
            + "','table.type'='COPY_ON_WRITE',"
            + "'write.operation'='insert','write.tasks'='1','metadata.enabled'='false',"
            + "'hoodie.populate.meta.fields'='false')");
    if (nativeMode) tech.streamfusion.planner.NativePlanner.install(sql);
    String query = "INSERT INTO target SELECT CAST('value' AS STRING),CAST(NULL AS BIGINT)";
    assertEquals(nativeMode, sql.explainSql(query).contains("NativeHudiSink"));
    var failure = assertThrows(Exception.class, () -> sql.executeSql(query).await());
    StringBuilder messages = new StringBuilder();
    for (Throwable cause = failure; cause != null; cause = cause.getCause())
      messages.append(cause.getMessage()).append('\n');
    assertTrue(messages.toString().contains("Column 'id' is NOT NULL"), messages.toString());
  }

  @Test
  void batchesRemainValidAfterReaderClose() throws Exception {
    var table = fixture(5);
    var source = NativeHudiSource.from(table, OUTPUT, 3, 1);
    var split = splits(source).get(0);
    org.apache.arrow.vector.VectorSchemaRoot root;
    try (var pending = new HudiReadBatches();
        var reader = new HudiSplitReader(table.getConf(), OUTPUT, 3, pending)) {
      reader.open(split);
      var records = reader.readBatch(split, 1, () -> false);
      assertEquals(split.splitId(), records.nextSplit());
      var batch = records.nextRecordFromSplit().record();
      pending.claim(batch);
      root = batch.root();
    }
    try (var owned = root) {
      assertEquals(3, owned.getRowCount());
      assertNotNull(owned.getVector("id").getObject(0));
    }
  }

  @Test
  void cancellationReleasesUnclaimedFetchedBatches() throws Exception {
    var table = fixture(13);
    var source = NativeHudiSource.from(table, OUTPUT, 3, 1);
    var split = splits(source).get(0);
    try (var pending = new HudiReadBatches();
        var reader = new HudiSplitReader(table.getConf(), OUTPUT, 3, pending)) {
      reader.open(split);
      assertNotNull(reader.readBatch(split, 1, () -> false));
      assertEquals(0, split.getConsumed(), "prefetch must not advance emitted state");
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void restoresBetweenStockAndNativeReaders(boolean nativeFirst) throws Exception {
    var table = fixture(37);
    var nativeSource = NativeHudiSource.from(table, OUTPUT, 4, 1);
    var stock = stockSource(table);
    var files = splits(nativeSource);
    var actual = new ArrayList<String>();
    List<HoodieSourceSplit> checkpoint;
    if (nativeFirst) {
      try (var reader = nativeSource.createReader(context())) {
        reader.addSplits(files);
        reader.start();
        while (actual.isEmpty()) {
          reader.isAvailable().get(10, TimeUnit.SECONDS);
          reader.pollNext(output(actual));
        }
        checkpoint = reader.snapshotState(1);
      }
      consume(
          stock,
          checkpoint,
          row ->
              actual.add(
                  (row.isNullAt(0) ? "null" : row.getString(0).toString()) + ":" + row.getLong(1)));
    } else {
      try (var reader = stock.createReader(context())) {
        reader.addSplits(files);
        reader.start();
        var target =
            new CountingOutput<org.apache.flink.table.data.RowData>(
                row ->
                    actual.add(
                        (row.isNullAt(0) ? "null" : row.getString(0).toString())
                            + ":"
                            + row.getLong(1)));
        while (actual.isEmpty()) {
          reader.isAvailable().get(10, TimeUnit.SECONDS);
          reader.pollNext(target);
        }
        checkpoint = reader.snapshotState(1);
      }
      consume(
          nativeSource,
          checkpoint,
          batch -> {
            try (var root = batch.root()) {
              for (int i = 0; i < root.getRowCount(); i++)
                actual.add(
                    root.getVector("name").getObject(i) + ":" + root.getVector("id").getObject(i));
            }
          });
    }
    var expected = new ArrayList<String>();
    for (int i = 0; i < 37; i++) expected.add((i % 7 == 0 ? "null" : "value-" + i) + ":" + i);
    java.util.Collections.sort(expected);
    java.util.Collections.sort(actual);
    assertEquals(expected, actual);
  }

  @SuppressWarnings("unchecked")
  private static org.apache.hudi.source.HoodieSource<org.apache.flink.table.data.RowData>
      stockSource(HoodieTableSource original) {
    var table = (HoodieTableSource) original.copy();
    table.applyProjection(new int[][] {{1}, {0}}, null);
    var provider =
        (org.apache.hudi.adapter.DataStreamScanProviderAdapter)
            table.getScanRuntimeProvider(
                org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext
                    .INSTANCE);
    var stream = provider.produceDataStream(StreamExecutionEnvironment.createLocalEnvironment(1));
    return (org.apache.hudi.source.HoodieSource<org.apache.flink.table.data.RowData>)
        ((org.apache.flink.streaming.api.transformations.SourceTransformation<?, ?, ?>)
                stream.getTransformation())
            .getSource();
  }

  @Test
  void sqlSourceIsColumnarAndMatchesStockHudi() throws Exception {
    fixture(103, true);
    String query = "SELECT active, name, n, id + 1 FROM h";
    var expected = new ArrayList<String>();
    try (var rows = sql.executeSql(query).collect()) {
      while (rows.hasNext()) expected.add(rows.next().toString());
    }
    String filtered = "SELECT name, id FROM h WHERE id > 1";
    var expectedFiltered = new ArrayList<String>();
    try (var rows = sql.executeSql(filtered).collect()) {
      while (rows.hasNext()) expectedFiltered.add(rows.next().toString());
    }
    tech.streamfusion.planner.NativePlanner.install(sql);
    assertTrue(sql.explainSql(query).contains("native Parquet"));
    var actual = new ArrayList<String>();
    try (var rows = sql.executeSql(query).collect()) {
      while (rows.hasNext()) actual.add(rows.next().toString());
    }
    java.util.Collections.sort(expected);
    java.util.Collections.sort(actual);
    assertEquals(expected, actual);
    assertFalse(sql.explainSql("SELECT name, id FROM h").contains("native Parquet"));
    assertFalse(sql.explainSql("SELECT name, id FROM h LIMIT 1").contains("native Parquet"));
    assertTrue(sql.explainSql(filtered).contains("native Parquet"));
    var actualFiltered = new ArrayList<String>();
    try (var rows = sql.executeSql(filtered).collect()) {
      while (rows.hasNext()) actualFiltered.add(rows.next().toString());
    }
    java.util.Collections.sort(expectedFiltered);
    java.util.Collections.sort(actualFiltered);
    assertEquals(expectedFiltered, actualFiltered);
  }

  @Test
  @EnabledIfSystemProperty(named = "sf.hudi.benchmark", matches = "true")
  @SuppressWarnings("unchecked")
  void benchmarkSourceReaderWithJavaIoAndArrowImport() throws Exception {
    assertTrue(System.getProperty("java.library.path").contains("/release"), "Use -Pbench");
    int count = Integer.getInteger("sf.hudi.benchmark.rows", 200000);
    var table = fixture(count);
    var stock = stockSource(table);
    var nativeSource = NativeHudiSource.from(table, OUTPUT, 4096, 1);
    var files = splits(nativeSource);
    var stockTimes = new ArrayList<Double>();
    var nativeTimes = new ArrayList<Double>();
    for (int iteration = -2; iteration < 5; iteration++) {
      for (int side = 0; side < 2; side++) {
        boolean nativeRead = ((iteration + side) & 1) == 0;
        long[] checksum = {0, 0};
        long start = System.nanoTime();
        if (nativeRead) {
          consume(
              nativeSource,
              files,
              batch -> {
                try (var root = batch.root()) {
                  var ids = (org.apache.arrow.vector.BigIntVector) root.getVector("id");
                  var names = (org.apache.arrow.vector.VarCharVector) root.getVector("name");
                  for (int i = 0; i < root.getRowCount(); i++) {
                    checksum[0]++;
                    checksum[1] += ids.get(i) + (names.isNull(i) ? 0 : names.getValueLength(i));
                  }
                }
              });
        } else {
          consume(
              stock,
              files,
              row -> {
                checksum[0]++;
                checksum[1] +=
                    row.getLong(1)
                        + (row.isNullAt(0)
                            ? 0
                            : ((org.apache.flink.table.data.binary.BinaryStringData)
                                    row.getString(0))
                                .getSizeInBytes());
              });
        }
        double millis = (System.nanoTime() - start) / 1_000_000.0;
        assertEquals(count, checksum[0]);
        long expected = (long) count * (count - 1) / 2;
        for (int i = 0; i < count; i++) if (i % 7 != 0) expected += ("value-" + i).length();
        assertEquals(expected, checksum[1]);
        if (iteration >= 0) (nativeRead ? nativeTimes : stockTimes).add(millis);
      }
    }
    System.out.println(
        "HUDI_READER_BENCH rows="
            + count
            + " stock_ms="
            + stockTimes
            + " native_ms="
            + nativeTimes);
  }

  @Test
  @EnabledIfSystemProperty(named = "sf.hudi.benchmark", matches = "true")
  void benchmarkCompleteSqlReadIncludingRowResultBoundary() throws Exception {
    assertTrue(System.getProperty("java.library.path").contains("/release"), "Use -Pbench");
    int count = Integer.getInteger("sf.hudi.benchmark.rows", 200000);
    fixture(count);
    tech.streamfusion.planner.NativePlanner.install(sql);
    var stockTimes = new ArrayList<Double>();
    var nativeTimes = new ArrayList<Double>();
    for (int iteration = -2; iteration < 5; iteration++) {
      for (int side = 0; side < 2; side++) {
        boolean nativeRead = ((iteration + side) & 1) == 0;
        sql.getConfig()
            .getConfiguration()
            .setString("streamfusion.native.enabled", Boolean.toString(nativeRead));
        assertEquals(
            nativeRead, sql.explainSql("SELECT name, id + 1 FROM h").contains("native Parquet"));
        long start = System.nanoTime();
        long rows = 0;
        long checksum = 0;
        try (var result = sql.executeSql("SELECT name, id + 1 FROM h").collect()) {
          while (result.hasNext()) {
            var row = result.next();
            rows++;
            checksum += (Long) row.getField(1);
          }
        }
        double millis = (System.nanoTime() - start) / 1_000_000.0;
        assertEquals(count, rows);
        assertEquals((long) count * (count + 1) / 2, checksum);
        if (iteration >= 0) (nativeRead ? nativeTimes : stockTimes).add(millis);
      }
    }
    System.out.println(
        "HUDI_SQL_BENCH rows=" + count + " stock_ms=" + stockTimes + " native_ms=" + nativeTimes);
  }

  @Test
  @EnabledIfSystemProperty(named = "sf.hudi.benchmark", matches = "true")
  void benchmarkCompleteAggregatePipeline() throws Exception {
    assertTrue(System.getProperty("java.library.path").contains("/release"), "Use -Pbench");
    int count = Integer.getInteger("sf.hudi.benchmark.rows", 200000);
    fixture(count);
    tech.streamfusion.planner.NativePlanner.install(sql);
    var config = sql.getConfig().getConfiguration();
    config.setString("table.exec.mini-batch.enabled", "true");
    config.setString("table.exec.mini-batch.allow-latency", "50 ms");
    config.setString("table.exec.mini-batch.size", "4096");
    String query = "SELECT SUM(id), COUNT(name) FROM h";
    var times = List.of(new ArrayList<Double>(), new ArrayList<Double>(), new ArrayList<Double>());
    for (int iteration = -2; iteration < 5; iteration++) {
      for (int side = 0; side < 3; side++) {
        int engine = Math.floorMod(iteration + side, 3);
        config.setString("streamfusion.native.enabled", Boolean.toString(engine > 0));
        config.setString("streamfusion.operator.hudiSource.enabled", Boolean.toString(engine == 2));
        String plan = sql.explainSql(query);
        assertEquals(engine == 2, plan.contains("native Parquet"), plan);
        if (engine > 0) assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
        long start = System.nanoTime();
        org.apache.flink.types.Row last = null;
        try (var result = sql.executeSql(query).collect()) {
          while (result.hasNext()) {
            var row = result.next();
            if (row.getKind() == org.apache.flink.types.RowKind.INSERT
                || row.getKind() == org.apache.flink.types.RowKind.UPDATE_AFTER) last = row;
          }
        }
        double millis = (System.nanoTime() - start) / 1_000_000.0;
        assertNotNull(last);
        assertEquals((long) count * (count - 1) / 2, last.getField(0));
        assertEquals((long) count - (count + 6) / 7, last.getField(1));
        if (iteration >= 0) times.get(engine).add(millis);
      }
    }
    System.out.println(
        "HUDI_AGGREGATE_BENCH rows="
            + count
            + " stock_ms="
            + times.get(0)
            + " previous_sf_ms="
            + times.get(1)
            + " native_ms="
            + times.get(2));
  }

  @Test
  @EnabledIfSystemProperty(named = "sf.hudi.benchmark", matches = "true")
  void benchmarkCompleteArrowReadWritePipeline() throws Exception {
    assertTrue(System.getProperty("java.library.path").contains("/release"), "Use -Pbench");
    int count = Integer.getInteger("sf.hudi.benchmark.rows", 200000);
    fixture(count);
    tech.streamfusion.planner.NativePlanner.install(sql);
    var config = sql.getConfig().getConfiguration();
    config.setString("table.exec.mini-batch.enabled", "true");
    config.setString("table.exec.mini-batch.allow-latency", "50 ms");
    config.setString("table.exec.mini-batch.size", "4096");
    var times = List.of(new ArrayList<Double>(), new ArrayList<Double>(), new ArrayList<Double>());
    for (int iteration = -2; iteration < 5; iteration++) {
      for (int side = 0; side < 3; side++) {
        int engine = Math.floorMod(iteration + side, 3);
        String target = "target_" + (iteration + 2) + "_" + engine;
        String path = temporary.resolve(target).toUri().toString();
        sql.executeSql(
            "CREATE TABLE "
                + target
                + " (name STRING,id BIGINT, PRIMARY KEY(id) NOT ENFORCED)"
                + " WITH ('connector'='hudi','path'='"
                + path
                + "','table.type'='COPY_ON_WRITE',"
                + "'write.operation'='insert','write.tasks'='1','metadata.enabled'='false',"
                + "'hoodie.populate.meta.fields'='false','read.source-v2.enabled'='true')");
        config.setString("streamfusion.native.enabled", Boolean.toString(engine > 0));
        config.setString("streamfusion.operator.hudiSink.enabled", Boolean.toString(engine == 2));
        String query = "INSERT INTO " + target + " SELECT name,id+1 FROM h";
        String plan = sql.explainSql(query);
        assertEquals(engine == 2, plan.contains("NativeHudiSink"), plan);
        assertEquals(engine > 0, plan.contains("native Parquet"), plan);
        if (engine == 2) assertFalse(plan.contains("ArrowToRow"), plan);
        long start = System.nanoTime();
        sql.executeSql(query).await();
        double millis = (System.nanoTime() - start) / 1_000_000.0;
        config.setString("streamfusion.native.enabled", "false");
        org.apache.flink.types.Row last = null;
        try (var result =
            sql.executeSql("SELECT COUNT(*),SUM(id),COUNT(name) FROM " + target).collect()) {
          while (result.hasNext()) {
            var row = result.next();
            if (row.getKind() == org.apache.flink.types.RowKind.INSERT
                || row.getKind() == org.apache.flink.types.RowKind.UPDATE_AFTER) last = row;
          }
        }
        assertNotNull(last);
        assertEquals((long) count, last.getField(0));
        assertEquals((long) count * (count + 1) / 2, last.getField(1));
        assertEquals((long) count - (count + 6) / 7, last.getField(2));
        if (iteration >= 0) times.get(engine).add(millis);
      }
    }
    System.out.println(
        "HUDI_READ_WRITE_BENCH rows="
            + count
            + " stock_ms="
            + times.get(0)
            + " previous_sf_ms="
            + times.get(1)
            + " native_ms="
            + times.get(2));
  }

  private static <T> void consume(
      org.apache.flink.api.connector.source.Source<
              T, HoodieSourceSplit, org.apache.hudi.source.enumerator.HoodieSplitEnumeratorState>
          source,
      List<HoodieSourceSplit> files,
      java.util.function.Consumer<T> consumer)
      throws Exception {
    var splits = new ArrayList<HoodieSourceSplit>();
    var serializer = source.getSplitSerializer();
    for (var split : files) {
      var copy = serializer.deserialize(serializer.getVersion(), serializer.serialize(split));
      splits.add(copy);
    }
    try (var reader = source.createReader(context())) {
      reader.addSplits(splits);
      reader.notifyNoMoreSplits();
      reader.start();
      var output = new CountingOutput<T>(consumer);
      InputStatus status;
      do {
        reader.isAvailable().get(30, TimeUnit.SECONDS);
        status = reader.pollNext(output);
      } while (status != InputStatus.END_OF_INPUT);
    }
  }

  private static final class CountingOutput<T> implements ReaderOutput<T> {
    private final java.util.function.Consumer<T> consume;

    CountingOutput(java.util.function.Consumer<T> consume) {
      this.consume = consume;
    }

    @Override
    public void collect(T record) {
      consume.accept(record);
    }

    @Override
    public void collect(T record, long timestamp) {
      consume.accept(record);
    }

    @Override
    public void emitWatermark(org.apache.flink.api.common.eventtime.Watermark watermark) {}

    @Override
    public void markIdle() {}

    @Override
    public void markActive() {}

    @Override
    public org.apache.flink.api.connector.source.SourceOutput<T> createOutputForSplit(String id) {
      return this;
    }

    @Override
    public void releaseOutputForSplit(String id) {}
  }

  private HoodieTableSource fixture(int count) throws Exception {
    return fixture(count, false);
  }

  private HoodieTableSource fixture(int count, boolean allTypes) throws Exception {
    var env = StreamExecutionEnvironment.createLocalEnvironment(1, memoryConfiguration());
    sql = StreamTableEnvironment.create(env);
    String path = temporary.resolve("table").toUri().toString();
    sql.executeSql(
        "CREATE TABLE h (id BIGINT, name STRING, "
            + (allTypes ? "n INT, active BOOLEAN, " : "")
            + "PRIMARY KEY (id) NOT ENFORCED) WITH ("
            + "'connector'='hudi','path'='"
            + path
            + "','table.type'='COPY_ON_WRITE',"
            + "'write.operation'='insert','write.tasks'='1','metadata.enabled'='false',"
            + "'read.source-v2.enabled'='true')");
    var values = new ArrayList<org.apache.flink.types.Row>();
    for (int i = 0; i < count; i++)
      values.add(
          allTypes
              ? org.apache.flink.types.Row.of(
                  (long) i,
                  i % 7 == 0 ? null : "value-" + i,
                  i % 3 == 0 ? null : -i,
                  i % 5 == 0 ? null : i % 2 == 0)
              : org.apache.flink.types.Row.of((long) i, i % 7 == 0 ? null : "value-" + i));
    sql.createTemporaryView(
        "input_rows",
        env.fromCollection(
            values,
            new org.apache.flink.api.java.typeutils.RowTypeInfo(
                allTypes
                    ? new org.apache.flink.api.common.typeinfo.TypeInformation<?>[] {
                      org.apache.flink.api.common.typeinfo.Types.LONG,
                      org.apache.flink.api.common.typeinfo.Types.STRING,
                      org.apache.flink.api.common.typeinfo.Types.INT,
                      org.apache.flink.api.common.typeinfo.Types.BOOLEAN
                    }
                    : new org.apache.flink.api.common.typeinfo.TypeInformation<?>[] {
                      org.apache.flink.api.common.typeinfo.Types.LONG,
                      org.apache.flink.api.common.typeinfo.Types.STRING
                    },
                allTypes
                    ? new String[] {"id", "name", "n", "active"}
                    : new String[] {"id", "name"})));
    sql.executeSql("INSERT INTO h SELECT * FROM input_rows").await();
    Configuration conf =
        Configuration.fromMap(
            Map.of(
                "path",
                path,
                "table.type",
                "COPY_ON_WRITE",
                "read.source-v2.enabled",
                "true",
                "metadata.enabled",
                "false",
                "read.tasks",
                "1"));
    conf.set(FlinkOptions.TABLE_NAME, "h");
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "id");
    var schema =
        allTypes
            ? ResolvedSchema.of(
                Column.physical("id", DataTypes.BIGINT()),
                Column.physical("name", DataTypes.STRING()),
                Column.physical("n", DataTypes.INT()),
                Column.physical("active", DataTypes.BOOLEAN()))
            : ResolvedSchema.of(
                Column.physical("id", DataTypes.BIGINT()),
                Column.physical("name", DataTypes.STRING()));
    conf.set(
        FlinkOptions.SOURCE_AVRO_SCHEMA,
        org.apache.hudi.util.HoodieSchemaConverter.convertToSchema(
                (RowType) schema.toPhysicalRowDataType().getLogicalType())
            .toString());
    return new HoodieTableSource(
        SerializableSchema.create(schema),
        new StoragePath(path),
        List.of(),
        "__HIVE_DEFAULT_PARTITION__",
        conf);
  }

  @SuppressWarnings("unchecked")
  private static List<HoodieSourceSplit> splits(NativeHudiSource source) throws Exception {
    var result = new ArrayList<HoodieSourceSplit>();
    var context =
        (SplitEnumeratorContext<HoodieSourceSplit>)
            Proxy.newProxyInstance(
                NativeHudiSourceTest.class.getClassLoader(),
                new Class<?>[] {SplitEnumeratorContext.class},
                (proxy, method, args) -> {
                  switch (method.getName()) {
                    case "currentParallelism":
                      return 1;
                    case "registeredReaders":
                      return Map.of(0, new ReaderInfo(0, "localhost"));
                    case "assignSplit":
                      result.add((HoodieSourceSplit) args[0]);
                      return null;
                    case "metricGroup":
                      return UnregisteredMetricsGroup.createSplitEnumeratorMetricGroup();
                    default:
                      return null;
                  }
                });
    try (var enumerator = source.createEnumerator(context)) {
      enumerator.start();
      int previous;
      do {
        previous = result.size();
        enumerator.handleSplitRequest(0, "localhost");
      } while (result.size() > previous);
    }
    return result;
  }

  private static SourceReaderContext context() {
    var metrics =
        org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup.wrap(
            org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups
                .createUnregisteredOperatorMetricGroup());
    return (SourceReaderContext)
        Proxy.newProxyInstance(
            NativeHudiSourceTest.class.getClassLoader(),
            new Class<?>[] {SourceReaderContext.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "metricGroup":
                  return metrics;
                case "getConfiguration":
                  return memoryConfiguration();
                case "getLocalHostName":
                  return "localhost";
                case "getIndexOfSubtask":
                  return 0;
                case "currentParallelism":
                  return 1;
                case "getUserCodeClassLoader":
                  return SimpleUserCodeClassLoader.create(
                      NativeHudiSourceTest.class.getClassLoader());
                default:
                  return null;
              }
            });
  }

  @SuppressWarnings("unchecked")
  private static ReaderOutput<ArrowBatch> output(List<String> result) {
    return (ReaderOutput<ArrowBatch>)
        Proxy.newProxyInstance(
            NativeHudiSourceTest.class.getClassLoader(),
            new Class<?>[] {ReaderOutput.class},
            (proxy, method, args) -> {
              if (method.getName().equals("collect")) {
                try (var root = ((ArrowBatch) args[0]).root()) {
                  for (int i = 0; i < root.getRowCount(); i++)
                    result.add(
                        root.getVector("name").getObject(i)
                            + ":"
                            + root.getVector("id").getObject(i));
                }
              }
              return method.getName().equals("createOutputForSplit") ? proxy : null;
            });
  }
}
