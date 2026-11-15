package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.actions.BinPackRewriteFilePlanner;
import org.apache.iceberg.actions.RewriteDataFilesCommitManager;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.expressions.Expressions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class NativeIcebergRewriteTest {
  @TempDir Path temp;

  @org.junit.jupiter.api.BeforeEach
  void initializeMemory() {
    tech.streamfusion.operator.TaskOffHeapMemory.initialize(
        org.apache.flink.configuration.Configuration.fromMap(
            Map.of("taskmanager.memory.task.off-heap.size", "128mb")));
  }

  @org.junit.jupiter.api.AfterEach
  void resourcesReleased() {
    assertEquals("", NativeIceberg.liveNativeHandles());
    assertEquals(0L, tech.streamfusion.operator.NativeAllocator.SHARED.getAllocatedMemory());
    assertEquals(0L, tech.streamfusion.operator.TaskOffHeapMemory.reservedBytes());
  }

  @org.junit.jupiter.api.Test
  void flinkJobUsesTheExistingPlannerAndCommitter() throws Exception {
    var table = NativeIcebergReaderTest.fixture(temp, true);
    var expected = read(table);
    var configuration =
        org.apache.flink.configuration.Configuration.fromMap(
            Map.of(
                "restart-strategy.type", "none", "taskmanager.memory.task.off-heap.size", "128mb"));
    var env =
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
            .getExecutionEnvironment(configuration);
    env.setParallelism(1);
    var groups =
        NativeIcebergMaintenance.rewriteDataFiles(env, table, Map.of("rewrite-all", "true"), false);
    assertEquals(1, groups.size());
    assertFalse(groups.get(0).addedFiles().isEmpty());
    table.refresh();
    assertEquals(expected, read(table));
  }

  @org.junit.jupiter.api.Test
  @org.junit.jupiter.api.Timeout(120)
  void continuousMaintenanceKeepsTheStockGraphAndCommitsNativeFiles() throws Exception {
    var table = NativeIcebergReaderTest.fixture(temp, true);
    var expected = read(table);
    long original = table.currentSnapshot().snapshotId();
    var configuration =
        org.apache.flink.configuration.Configuration.fromMap(
            Map.of(
                "restart-strategy.type", "none", "taskmanager.memory.task.off-heap.size", "128mb"));
    var env =
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
            .getExecutionEnvironment(configuration);
    env.setParallelism(1);
    env.enableCheckpointing(100);
    var maintenance =
        org.apache.iceberg.flink.maintenance.api.TableMaintenance.forTable(
                env, org.apache.iceberg.flink.TableLoader.fromHadoopTable(table.location()))
            .uidSuffix("native-rewrite")
            .rateLimit(java.time.Duration.ofMillis(100))
            .lockCheckDelay(java.time.Duration.ofMillis(100))
            .add(
                org.apache.iceberg.flink.maintenance.api.RewriteDataFiles.builder()
                    .scheduleOnCommitCount(1)
                    .rewriteAll(true));
    NativeIcebergMaintenance.append(env, maintenance);
    var job = env.executeAsync();
    try {
      long deadline = System.nanoTime() + java.time.Duration.ofSeconds(60).toNanos();
      while (table.currentSnapshot().snapshotId() == original && System.nanoTime() < deadline) {
        Thread.sleep(100);
        table.refresh();
        if (job.getJobStatus().get().isGloballyTerminalState()) job.getJobExecutionResult().get();
      }
      assertNotEquals(original, table.currentSnapshot().snapshotId());
      assertEquals("replace", table.currentSnapshot().operation());
      assertEquals(expected, read(table));
      try (var tasks = table.newScan().planFiles()) {
        for (var task : tasks) assertNativeFile(table, task.file());
      }
    } finally {
      job.cancel().get();
      try {
        job.getJobExecutionResult().get();
      } catch (java.util.concurrent.ExecutionException cancelled) {
        /* cancellation completes cleanup */
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "true,false", "false,true", "true,true"})
  void existingPlannerAndCommitterRewriteNativeBatches(boolean deletes, boolean sort)
      throws Exception {
    var table = NativeIcebergReaderTest.fixture(temp, deletes);
    var factory =
        new GenericFileWriterFactory.Builder(table).dataFileFormat(FileFormat.PARQUET).build();
    var writer =
        factory.newDataWriter(
            table
                .encryption()
                .encrypt(table.io().newOutputFile(table.location() + "/second.parquet")),
            table.spec(),
            null);
    try (writer) {
      var row = GenericRecord.create(table.schema());
      row.setField("id", 17);
      row.setField("payload", "new-after-delete");
      writer.write(row);
    }
    table.newAppend().appendFile(writer.toDataFile()).commit();
    if (sort) table.replaceSortOrder().desc("id").commit();
    var before = read(table);
    long snapshot = table.currentSnapshot().snapshotId();
    var planner = new BinPackRewriteFilePlanner(table, Expressions.alwaysTrue(), snapshot, true);
    planner.init(Map.of("rewrite-all", "true"));
    var groups = new LinkedHashSet<RewriteFileGroup>();
    try (var planned = planner.plan().groups()) {
      for (var group : planned) {
        group.setOutputFiles(
            IcebergRewriteFiles.rewrite(table, group, 0, 0, sort, 2L << 20, temp.toString()));
        groups.add(group);
      }
    }
    assertEquals(1, groups.size());
    assertFalse(groups.iterator().next().addedFiles().isEmpty());
    for (var file : groups.iterator().next().addedFiles()) {
      if (sort) {
        assertEquals(table.sortOrder().orderId(), file.sortOrderId());
        int previous = Integer.MAX_VALUE;
        try (var input =
            org.apache.iceberg.parquet.Parquet.read(table.io().newInputFile(file.location()))
                .project(table.schema())
                .createReaderFunc(
                    physical ->
                        org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(
                            table.schema(), physical))
                .build()) {
          for (var value : input) {
            int id = (Integer) ((org.apache.iceberg.data.Record) value).getField("id");
            assertTrue(id <= previous);
            previous = id;
          }
        }
      }
      assertNativeFile(table, file);
    }
    new RewriteDataFilesCommitManager(table, snapshot).commitOrClean(groups);
    table.refresh();
    assertEquals(before, read(table));
    assertEquals(deletes ? 100 : 102, before.size());
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @org.junit.jupiter.api.Test
  void closingSortBeforeDrainingReleasesBorrowedArrowBuffers() throws Exception {
    var schema =
        new org.apache.iceberg.Schema(
            org.apache.iceberg.types.Types.NestedField.optional(
                1, "id", org.apache.iceberg.types.Types.LongType.get()));
    var order = org.apache.iceberg.SortOrder.builderFor(schema).asc("id").build();
    var arrow =
        tech.streamfusion.arrow.ArrowConversion.toArrowSchema(
            org.apache.iceberg.flink.FlinkSchemaUtil.convert(schema));
    try (var sorter = new IcebergRewriteSort(arrow, schema, order, 2L << 20, temp.toString())) {
      var root =
          org.apache.arrow.vector.VectorSchemaRoot.create(
              arrow, tech.streamfusion.operator.NativeAllocator.SHARED);
      root.allocateNew();
      var vector = (org.apache.arrow.vector.BigIntVector) root.getVector("id");
      for (int row = 0; row < 4096; row++) vector.setSafe(row, 4096L - row);
      root.setRowCount(4096);
      sorter.push(root);
      sorter.finish();
    }
  }

  @org.junit.jupiter.api.Test
  void rejectedSortCreationReleasesTheExportedSchemaAndReservation() {
    var schema =
        new org.apache.iceberg.Schema(
            org.apache.iceberg.types.Types.NestedField.optional(
                1, "id", org.apache.iceberg.types.Types.LongType.get()));
    var order = org.apache.iceberg.SortOrder.builderFor(schema).asc("id").build();
    var arrow =
        tech.streamfusion.arrow.ArrowConversion.toArrowSchema(
            org.apache.iceberg.flink.FlinkSchemaUtil.convert(schema));
    assertThrows(
        RuntimeException.class,
        () -> new IcebergRewriteSort(arrow, schema, order, 1, temp.toString()));
  }

  @org.junit.jupiter.api.Test
  void singleStringAndCompositeEqualityDeletesKeepNullAndSequenceSemantics() throws Exception {
    var schema =
        new org.apache.iceberg.Schema(
            org.apache.iceberg.types.Types.NestedField.required(
                1, "id", org.apache.iceberg.types.Types.IntegerType.get()),
            org.apache.iceberg.types.Types.NestedField.optional(
                2, "payload", org.apache.iceberg.types.Types.StringType.get()));
    var table =
        new org.apache.iceberg.hadoop.HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                org.apache.iceberg.PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"),
                temp.resolve("strings").toString());
    var factory =
        new GenericFileWriterFactory.Builder(table)
            .dataFileFormat(FileFormat.PARQUET)
            .deleteFileFormat(FileFormat.PARQUET)
            .equalityFieldIds(new int[] {2})
            .equalityDeleteRowSchema(new org.apache.iceberg.Schema(schema.findField("payload")))
            .build();
    var input =
        factory.newDataWriter(
            table
                .encryption()
                .encrypt(table.io().newOutputFile(table.location() + "/input.parquet")),
            table.spec(),
            null);
    try (input) {
      String[] values = {"", null, "é🐍", "A", "z", "é🐍", null};
      for (int id = 0; id < values.length; id++) {
        var row = GenericRecord.create(schema);
        row.setField("id", id);
        row.setField("payload", values[id]);
        input.write(row);
      }
    }
    table.newAppend().appendFile(input.toDataFile()).commit();
    var single =
        factory.newEqualityDeleteWriter(
            table
                .encryption()
                .encrypt(table.io().newOutputFile(table.location() + "/single.parquet")),
            table.spec(),
            null);
    try (single) {
      for (String value : java.util.Arrays.asList("é🐍", null)) {
        var key = GenericRecord.create(new org.apache.iceberg.Schema(schema.findField("payload")));
        key.setField("payload", value);
        single.write(key);
      }
    }
    var compositeFactory =
        new GenericFileWriterFactory.Builder(table)
            .deleteFileFormat(FileFormat.PARQUET)
            .equalityFieldIds(new int[] {1, 2})
            .equalityDeleteRowSchema(schema)
            .build();
    var composite =
        compositeFactory.newEqualityDeleteWriter(
            table
                .encryption()
                .encrypt(table.io().newOutputFile(table.location() + "/composite.parquet")),
            table.spec(),
            null);
    try (composite) {
      var key = GenericRecord.create(schema);
      key.setField("id", 3);
      key.setField("payload", "A");
      composite.write(key);
    }
    table
        .newRowDelta()
        .addDeletes(single.toDeleteFile())
        .addDeletes(composite.toDeleteFile())
        .commit();
    var later =
        factory.newDataWriter(
            table
                .encryption()
                .encrypt(table.io().newOutputFile(table.location() + "/later.parquet")),
            table.spec(),
            null);
    try (later) {
      var row = GenericRecord.create(schema);
      row.setField("id", 8);
      row.setField("payload", "é🐍");
      later.write(row);
    }
    table.newAppend().appendFile(later.toDataFile()).commit();
    var expected = read(table);
    assertEquals(List.of("0:", "4:z", "8:é🐍"), expected);
    long snapshot = table.currentSnapshot().snapshotId();
    var planner = new BinPackRewriteFilePlanner(table, Expressions.alwaysTrue(), snapshot, true);
    planner.init(Map.of("rewrite-all", "true"));
    var rewritten = new LinkedHashSet<RewriteFileGroup>();
    try (var groups = planner.plan().groups()) {
      for (var group : groups) {
        group.setOutputFiles(IcebergRewriteFiles.rewrite(table, group, 0, 0));
        rewritten.add(group);
      }
    }
    new RewriteDataFilesCommitManager(table, snapshot).commitOrClean(rewritten);
    table.refresh();
    assertEquals(expected, read(table));
  }

  @org.junit.jupiter.api.Test
  void rewriteUsesThePlannersOutputPartitionSpecAndTableCodec() throws Exception {
    var table = NativeIcebergReaderTest.fixture(temp, false);
    var expected = read(table);
    table
        .updateSpec()
        .addField(org.apache.iceberg.expressions.Expressions.bucket("id", 4))
        .commit();
    table.updateProperties().set("write.parquet.compression-codec", "gzip").commit();
    long snapshot = table.currentSnapshot().snapshotId();
    var planner = new BinPackRewriteFilePlanner(table, Expressions.alwaysTrue(), snapshot, true);
    planner.init(Map.of("rewrite-all", "true"));
    var rewritten = new LinkedHashSet<RewriteFileGroup>();
    var partitions = new java.util.HashSet<Integer>();
    try (var groups = planner.plan().groups()) {
      for (var group : groups) {
        assertEquals(table.spec().specId(), group.outputSpecId());
        group.setOutputFiles(IcebergRewriteFiles.rewrite(table, group, 0, 0));
        for (var file : group.addedFiles()) {
          assertEquals(table.spec().specId(), file.specId());
          partitions.add(file.partition().get(0, Integer.class));
          assertNativeFile(table, file);
          try (var footer =
              org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(
                  org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                      new org.apache.hadoop.fs.Path(file.location()),
                      new org.apache.hadoop.conf.Configuration()))) {
            assertEquals(
                "GZIP",
                footer.getFooter().getBlocks().get(0).getColumns().get(0).getCodec().name());
          }
        }
        rewritten.add(group);
      }
    }
    assertEquals(java.util.Set.of(0, 1, 2, 3), partitions);
    new RewriteDataFilesCommitManager(table, snapshot).commitOrClean(rewritten);
    table.refresh();
    assertEquals(expected, read(table));
  }

  @org.junit.jupiter.api.Test
  void whollyDeletedInputDoesNotCreateAnEmptyDataFile() throws Exception {
    var table = NativeIcebergReaderTest.fixture(temp, false);
    var keys = new org.apache.iceberg.Schema(table.schema().findField("id"));
    var factory =
        new GenericFileWriterFactory.Builder(table)
            .deleteFileFormat(FileFormat.PARQUET)
            .equalityFieldIds(new int[] {1})
            .equalityDeleteRowSchema(keys)
            .build();
    var deletes =
        factory.newEqualityDeleteWriter(
            table
                .encryption()
                .encrypt(table.io().newOutputFile(table.location() + "/all-deleted.parquet")),
            table.spec(),
            null);
    try (deletes) {
      var row = GenericRecord.create(keys);
      for (int id = 0; id < 101; id++) {
        row.setField("id", id);
        deletes.write(row);
      }
    }
    table.newRowDelta().addDeletes(deletes.toDeleteFile()).commit();
    long snapshot = table.currentSnapshot().snapshotId();
    var planner = new BinPackRewriteFilePlanner(table, Expressions.alwaysTrue(), snapshot, true);
    planner.init(Map.of("rewrite-all", "true"));
    var rewritten = new LinkedHashSet<RewriteFileGroup>();
    try (var groups = planner.plan().groups()) {
      for (var group : groups) {
        group.setOutputFiles(IcebergRewriteFiles.rewrite(table, group, 0, 0));
        assertTrue(group.addedFiles().isEmpty());
        rewritten.add(group);
      }
    }
    assertEquals(1, rewritten.size());
    new RewriteDataFilesCommitManager(table, snapshot).commitOrClean(rewritten);
    table.refresh();
    assertTrue(read(table).isEmpty());
    try (var files = table.newScan().planFiles()) {
      assertFalse(files.iterator().hasNext());
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void rewritePreservesTimeMicrosecondsThroughBorrowedFlinkViews(boolean sort) throws Exception {
    var schema =
        new org.apache.iceberg.Schema(
            org.apache.iceberg.types.Types.NestedField.required(
                1, "id", org.apache.iceberg.types.Types.IntegerType.get()),
            org.apache.iceberg.types.Types.NestedField.optional(
                2, "clock", org.apache.iceberg.types.Types.TimeType.get()));
    var table =
        new org.apache.iceberg.hadoop.HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                org.apache.iceberg.PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"),
                temp.resolve("microseconds").toString());
    var values = new java.util.TreeMap<Integer, java.time.LocalTime>();
    values.put(0, java.time.LocalTime.ofNanoOfDay(1_234_567_000L));
    values.put(1, java.time.LocalTime.ofNanoOfDay(1_000));
    values.put(2, java.time.LocalTime.of(23, 59, 59, 999_999_000));
    values.put(3, null);
    values.put(4, java.time.LocalTime.ofNanoOfDay(1_234_566_000L));
    var factory =
        new GenericFileWriterFactory.Builder(table).dataFileFormat(FileFormat.PARQUET).build();
    var input =
        factory.newDataWriter(
            table
                .encryption()
                .encrypt(table.io().newOutputFile(table.location() + "/input.parquet")),
            table.spec(),
            null);
    try (input) {
      for (var entry : values.entrySet()) {
        var row = GenericRecord.create(schema);
        row.setField("id", entry.getKey());
        row.setField("clock", entry.getValue());
        input.write(row);
      }
    }
    table.newAppend().appendFile(input.toDataFile()).commit();
    if (sort)
      table.replaceSortOrder().asc("clock", org.apache.iceberg.NullOrder.NULLS_LAST).commit();
    long snapshot = table.currentSnapshot().snapshotId();
    var planner = new BinPackRewriteFilePlanner(table, Expressions.alwaysTrue(), snapshot, true);
    planner.init(Map.of("rewrite-all", "true"));
    var rewritten = new LinkedHashSet<RewriteFileGroup>();
    try (var groups = planner.plan().groups()) {
      for (var group : groups) {
        group.setOutputFiles(
            IcebergRewriteFiles.rewrite(table, group, 0, 0, sort, 2L << 20, temp.toString()));
        rewritten.add(group);
      }
    }
    new RewriteDataFilesCommitManager(table, snapshot).commitOrClean(rewritten);
    table.refresh();
    var actual = new java.util.TreeMap<Integer, java.time.LocalTime>();
    var order = new ArrayList<Integer>();
    try (var rows = IcebergGenerics.read(table).build()) {
      for (var row : rows) {
        int id = (Integer) row.getField("id");
        actual.put(id, (java.time.LocalTime) row.getField("clock"));
        order.add(id);
      }
    }
    assertEquals(values, actual);
    assertEquals(sort ? List.of(1, 4, 0, 2, 3) : List.of(0, 1, 2, 3, 4), order);
    table.updateSpec().addField("clock").commit();
    assertThrows(
        IllegalArgumentException.class, () -> IcebergRewriteFiles.validateTable(table, sort));
  }

  private static void assertNativeFile(
      org.apache.iceberg.Table table, org.apache.iceberg.DataFile file) throws Exception {
    try (var footer =
        org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(
            org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                new org.apache.hadoop.fs.Path(file.location()),
                new org.apache.hadoop.conf.Configuration()))) {
      assertTrue(footer.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs"));
    }
  }

  private static List<String> read(org.apache.iceberg.Table table) throws Exception {
    var rows = new ArrayList<String>();
    try (var input = IcebergGenerics.read(table).build()) {
      for (var row : input) rows.add(row.getField("id") + ":" + row.getField("payload"));
    }
    rows.sort(String::compareTo);
    return rows;
  }
}
