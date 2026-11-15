package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BinPackRewriteFilePlanner;
import org.apache.iceberg.actions.RewriteDataFilesCommitManager;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;

class NativeIcebergV3Test {
  @TempDir Path temp;

  @org.junit.jupiter.api.BeforeEach
  void initializeMemory() {
    TaskOffHeapMemory.initialize(
        org.apache.flink.configuration.Configuration.fromMap(
            Map.of("taskmanager.memory.task.off-heap.size", "128mb")));
  }

  @org.junit.jupiter.api.AfterEach
  void resourcesReleased() {
    assertEquals("", NativeIceberg.liveNativeHandles());
    assertEquals(0L, NativeAllocator.SHARED.getAllocatedMemory());
    assertEquals(0L, TaskOffHeapMemory.reservedBytes());
  }

  @org.junit.jupiter.api.Test
  void flip27SourceAdmitsV3AndReadsItsDeletedSnapshotAsArrow() throws Exception {
    var table = fixture(temp);
    var config =
        org.apache.flink.configuration.Configuration.fromMap(
            Map.of(
                "restart-strategy.type", "none", "taskmanager.memory.task.off-heap.size", "128mb"));
    var env =
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
            .getExecutionEnvironment(config);
    env.setParallelism(2);
    var values =
        NativeIcebergSource.read(
                env,
                table,
                org.apache.iceberg.flink.TableLoader.fromHadoopTable(table.location()),
                config,
                Map.of(
                    "streaming",
                    "true",
                    "monitor-interval",
                    "10ms",
                    "starting-strategy",
                    "TABLE_SCAN_THEN_INCREMENTAL"),
                table.schema(),
                7)
            .<Integer>flatMap(
                (batch, output) -> {
                  try (var root = batch.root()) {
                    for (int row = 0; row < root.getRowCount(); row++)
                      output.collect((Integer) root.getVector("id").getObject(row));
                  }
                })
            .returns(org.apache.flink.api.common.typeinfo.Types.INT)
            .executeAndCollect(60);
    assertEquals(javaRows(table).keySet(), new java.util.HashSet<>(values));
    assertEquals(60, values.size());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void readsPuffinAndEqualityDeletesAndPreservesLineageThroughRewrite(boolean sort)
      throws Exception {
    Table table = fixture(temp);
    if (sort) table.replaceSortOrder().desc("id").commit();
    Map<Integer, List<Object>> expected = javaRows(table);
    assertEquals(60, expected.size());
    assertEquals(expected, nativeRows(table));
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
    assertFalse(groups.isEmpty());
    new RewriteDataFilesCommitManager(table, snapshot).commitOrClean(groups);
    table.refresh();
    assertEquals(expected, javaRows(table));
    assertEquals(expected, nativeRows(table));
    try (var tasks = table.newScan().planFiles()) {
      for (var task : tasks) {
        assertTrue(
            task.deletes().stream().noneMatch(org.apache.iceberg.util.ContentFileUtil::isDV));
        if (sort) assertEquals(table.sortOrder().orderId(), task.file().sortOrderId());
      }
    }
  }

  static Table fixture(Path directory) throws Exception {
    var schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "payload", Types.StringType.get()));
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                PartitionSpec.unpartitioned(),
                Map.of("format-version", "3"),
                directory.resolve("v3").toString());
    var keys = new Schema(schema.findField("id"));
    var factory =
        new GenericFileWriterFactory.Builder(table)
            .dataFileFormat(FileFormat.PARQUET)
            .deleteFileFormat(FileFormat.PARQUET)
            .equalityFieldIds(new int[] {1})
            .equalityDeleteRowSchema(keys)
            .build();
    var data = new ArrayList<DataFile>();
    for (int file = 0; file < 2; file++) {
      var writer =
          factory.newDataWriter(
              table
                  .encryption()
                  .encrypt(
                      table.io().newOutputFile(table.location() + "/input-" + file + ".parquet")),
              table.spec(),
              null);
      try (writer) {
        var row = GenericRecord.create(schema);
        for (int id = file * 32; id < (file + 1) * 32; id++) {
          row.setField("id", id);
          row.setField("payload", id % 7 == 0 ? null : "v-é🐍-" + id);
          writer.write(row);
        }
      }
      data.add(writer.toDataFile());
      table.newAppend().appendFile(writer.toDataFile()).commit();
    }
    var dv =
        new BaseDVFileWriter(
            OutputFileFactory.builderFor(table, 0, 0).format(FileFormat.PUFFIN).build(),
            ignored -> null);
    try (dv) {
      dv.delete(data.get(0).location(), 1, table.spec(), null);
      dv.delete(data.get(0).location(), 17, table.spec(), null);
      dv.delete(data.get(1).location(), 0, table.spec(), null);
      dv.delete(data.get(1).location(), 31, table.spec(), null);
    }
    assertEquals(2, dv.result().deleteFiles().size());
    assertEquals(
        dv.result().deleteFiles().get(0).location(), dv.result().deleteFiles().get(1).location());
    var equality =
        factory.newEqualityDeleteWriter(
            table.encryption().encrypt(table.io().newOutputFile(table.location() + "/eq.parquet")),
            table.spec(),
            null);
    try (equality) {
      var key = GenericRecord.create(keys);
      key.setField("id", 5);
      equality.write(key);
    }
    var delta = table.newRowDelta().addDeletes(equality.toDeleteFile());
    dv.result().deleteFiles().forEach(delta::addDeletes);
    delta.commit();
    var later =
        factory.newDataWriter(
            table
                .encryption()
                .encrypt(table.io().newOutputFile(table.location() + "/later.parquet")),
            table.spec(),
            null);
    try (later) {
      var row = GenericRecord.create(schema);
      row.setField("id", 5);
      row.setField("payload", "after-equality-delete");
      later.write(row);
    }
    table.newAppend().appendFile(later.toDataFile()).commit();
    return table;
  }

  @org.junit.jupiter.api.Test
  void deletionVectorProjectionAndResumeCountOnlySurvivingRows() throws Exception {
    var table = fixture(temp);
    long count = 0;
    try (var tasks = table.newScan().planFiles()) {
      for (var task : tasks) {
        var expected = new ArrayList<Integer>();
        try (var reader =
            new IcebergNativeTaskReader(task, table.schema(), List.of(1), table.io(), 7, 0)) {
          org.apache.arrow.vector.VectorSchemaRoot batch;
          while ((batch = reader.next()) != null) {
            try (var root = batch) {
              for (int row = 0; row < root.getRowCount(); row++)
                expected.add((Integer) root.getVector("id").getObject(row));
            }
          }
        }
        var resumed = new ArrayList<Integer>();
        int skip = Math.min(9, expected.size());
        try (var reader =
            new IcebergNativeTaskReader(task, table.schema(), List.of(1), table.io(), 3, skip)) {
          org.apache.arrow.vector.VectorSchemaRoot batch;
          while ((batch = reader.next()) != null) {
            try (var root = batch) {
              for (int row = 0; row < root.getRowCount(); row++)
                resumed.add((Integer) root.getVector("id").getObject(row));
            }
          }
        }
        assertEquals(expected.subList(skip, expected.size()), resumed);
        try (var reader =
            new IcebergNativeTaskReader(task, table.schema(), List.of(), table.io(), 5, 0)) {
          int taskRows = 0;
          org.apache.arrow.vector.VectorSchemaRoot batch;
          while ((batch = reader.next()) != null) {
            try (var root = batch) {
              assertEquals(0, root.getFieldVectors().size());
              taskRows += root.getRowCount();
            }
          }
          assertEquals(expected.size(), taskRows);
          count += taskRows;
        }
      }
    }
    assertEquals(60, count);
  }

  private static Map<Integer, List<Object>> javaRows(Table table) throws Exception {
    var result = new TreeMap<Integer, List<Object>>();
    var schema = MetadataColumns.schemaWithRowLineage(table.schema());
    try (var tasks = table.newScan().planTasks()) {
      for (var task : tasks) {
        try (var rows =
            new org.apache.iceberg.flink.source.DataIterator<>(
                new org.apache.iceberg.flink.source.RowDataFileScanTaskReader(
                    table.schema(), schema, null, false, List.of()),
                task,
                table.io(),
                table.encryption())) {
          while (rows.hasNext()) {
            var row = rows.next();
            result.put(
                row.getInt(0),
                java.util.Arrays.asList(
                    row.isNullAt(1) ? null : row.getString(1).toString(),
                    row.isNullAt(2) ? null : row.getLong(2),
                    row.isNullAt(3) ? null : row.getLong(3)));
          }
        }
      }
    }
    return result;
  }

  private static Map<Integer, List<Object>> nativeRows(Table table) throws Exception {
    var result = new TreeMap<Integer, List<Object>>();
    List<Integer> projection =
        List.of(
            1,
            2,
            MetadataColumns.ROW_ID.fieldId(),
            MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId());
    try (var tasks = table.newScan().planFiles()) {
      for (var task : tasks) {
        try (var reader =
            new IcebergNativeTaskReader(task, table.schema(), projection, table.io(), 7, 0)) {
          org.apache.arrow.vector.VectorSchemaRoot batch;
          while ((batch = reader.next()) != null) {
            try (var root = batch) {
              for (int row = 0; row < root.getRowCount(); row++) {
                Object payload = root.getVector("payload").getObject(row);
                assertNull(
                    result.put(
                        (Integer) root.getVector("id").getObject(row),
                        java.util.Arrays.asList(
                            payload == null ? null : payload.toString(),
                            root.getVector("_row_id").getObject(row),
                            root.getVector("_last_updated_sequence_number").getObject(row))));
              }
            }
          }
        }
      }
    }
    return result;
  }
}
