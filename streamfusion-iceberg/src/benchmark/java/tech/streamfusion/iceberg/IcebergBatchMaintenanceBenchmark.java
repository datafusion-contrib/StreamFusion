package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BinPackRewriteFilePlanner;
import org.apache.iceberg.actions.RewriteDataFilesCommitManager;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.actions.Actions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Times bounded Flink jobs from Iceberg planning through the committed rewrite. */
@EnabledIfEnvironmentVariable(named = "SF_ICEBERG_BATCH_MAINTENANCE_BENCHMARK", matches = "true")
class IcebergBatchMaintenanceBenchmark {
  private static final int PARTITIONS = 4;
  private static final int FILES_PER_PARTITION = 4;
  private static final int DELETE_STRIDE = 512;
  private static final long TARGET_SIZE = 128L << 20;
  private static final long SORT_MEMORY = 64L << 20;
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "payload", Types.StringType.get()),
          Types.NestedField.required(3, "shard", Types.IntegerType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("shard").build();
  private static final Map<String, String> TABLE_PROPERTIES =
      Map.of(
          "format-version", "2",
          "write.parquet.compression-codec", "zstd",
          "write.parquet.compression-level", "3",
          "write.target-file-size-bytes", Long.toString(TARGET_SIZE));
  private static final Map<String, String> NATIVE_OPTIONS =
      Map.of(
          "rewrite-all", "true",
          "target-file-size-bytes", Long.toString(TARGET_SIZE),
          "max-file-group-size-bytes", Long.toString(TARGET_SIZE));

  private record Scenario(String name, boolean withDeletes, boolean sorted) {}

  @TempDir Path temp;

  @Test
  void compareBoundedCompactionThroughCommit() throws Exception {
    assertEquals("false", System.getProperty("streamfusion.exchange.zeroCopyLocal"), "Use -Pbench");
    int rowsPerFile = IcebergMaintenanceBenchmark.rowsPerFile();
    int runs = Integer.parseInt(System.getenv().getOrDefault("SF_ICEBERG_MAINTENANCE_RUNS", "3"));
    assertTrue(rowsPerFile > 0 && runs > 0);
    int inputRows = Math.multiplyExact(PARTITIONS * FILES_PER_PARTITION, rowsPerFile);
    var tables = new HadoopTables(new org.apache.hadoop.conf.Configuration());
    var fixtures = tables.create(SCHEMA, SPEC, TABLE_PROPERTIES, temp.resolve("input").toString());
    var input = writeFixture(fixtures, rowsPerFile);
    int cohort = 0;
    for (var scenario :
        List.of(
            new Scenario("append", false, false),
            new Scenario("position-deletes", true, false),
            new Scenario("sorted-append", false, true),
            new Scenario("sorted-position-deletes", true, true))) {
      boolean withDeletes = scenario.withDeletes();
      cohort++;
      int expectedRows =
          inputRows
              - (withDeletes
                  ? PARTITIONS * FILES_PER_PARTITION * (1 + (rowsPerFile - 1) / DELETE_STRIDE)
                  : 0);
      for (int trial = -1; trial < runs; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          boolean nativeMode = Math.floorMod(trial + turn + cohort, 2) == 0;
          String engine =
              nativeMode ? "streamfusion" : scenario.sorted() ? "flink-row-sort" : "stock-flink";
          var table =
              tables.create(
                  SCHEMA,
                  SPEC,
                  TABLE_PROPERTIES,
                  temp.resolve(scenario.name() + "-" + trial + "-" + engine).toString());
          var append = table.newAppend();
          input.files().forEach(append::appendFile);
          append.commit();
          if (withDeletes) {
            var delta = table.newRowDelta();
            input.deletes().forEach(delta::addDeletes);
            delta.commit();
          }
          if (scenario.sorted())
            table
                .replaceSortOrder()
                .asc("payload", NullOrder.NULLS_FIRST)
                .desc("id", NullOrder.NULLS_LAST)
                .commit();
          long snapshot = table.currentSnapshot().snapshotId();
          IcebergNativeMetrics.reset();
          var env = environment();
          long start = System.nanoTime();
          int rewrittenFiles;
          long elapsed;
          if (nativeMode || scenario.sorted()) {
            var groups =
                nativeMode
                    ? NativeIcebergMaintenance.rewriteDataFiles(
                        env, table, NATIVE_OPTIONS, scenario.sorted())
                    : stockSortedRewrite(env, table);
            elapsed = System.nanoTime() - start;
            assertEquals(PARTITIONS, groups.size());
            rewrittenFiles = groups.stream().mapToInt(group -> group.rewrittenFiles().size()).sum();
          } else {
            var result =
                Actions.forTable(env, table)
                    .rewriteDataFiles()
                    .maxParallelism(PARTITIONS)
                    .targetSizeInBytes(TARGET_SIZE)
                    .splitOpenFileCost(1)
                    .execute();
            elapsed = System.nanoTime() - start;
            rewrittenFiles = result.deletedDataFiles().size();
          }
          assertEquals(
              RuntimeExecutionMode.BATCH,
              env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE));
          table.refresh();
          assertNotEquals(snapshot, table.currentSnapshot().snapshotId());
          assertEquals("replace", table.currentSnapshot().operation());
          assertEquals(input.files().size(), rewrittenFiles);
          var output = outputFiles(table);
          assertEquals(PARTITIONS, output.size());
          assertEquals(
              Set.of(0, 1, 2, 3),
              output.stream()
                  .map(file -> file.partition().get(0, Integer.class))
                  .collect(java.util.stream.Collectors.toSet()));
          IcebergMaintenanceBenchmark.assertEncoder(table, output, nativeMode);
          if (scenario.sorted())
            assertEquals(
                expectedRows, IcebergMaintenanceBenchmark.checksum(table, output, true)[0]);
          verifyRows(table, withDeletes, rowsPerFile, inputRows, expectedRows);
          assertEquals(
              nativeMode ? PARTITIONS : 0, IcebergNativeMetrics.count("nativeRewriteGroups"));
          assertEquals(0, IcebergNativeMetrics.count("stockRewriteGroups"));
          assertEquals(0, IcebergNativeMetrics.count("nativeRewriteErrors"));
          assertEquals("", NativeIceberg.liveNativeHandles());
          assertEquals(0, NativeAllocator.SHARED.getAllocatedMemory());
          assertEquals(0, TaskOffHeapMemory.reservedBytes());

          var observation = new LinkedHashMap<String, Object>();
          observation.put("scenario", scenario.name());
          observation.put("engine", engine);
          observation.put("trial", trial);
          observation.put("elapsed_nanos", elapsed);
          observation.put("input_rows", inputRows);
          observation.put("input_files", input.files().size());
          observation.put("output_rows", expectedRows);
          observation.put("output_files", output.size());
          observation.put("native_groups", IcebergNativeMetrics.count("nativeRewriteGroups"));
          observation.put("sort_memory_bytes", scenario.sorted() ? SORT_MEMORY : 0);
          observation.put(
              "sort_spills",
              IcebergNativeMetrics.count(
                  nativeMode ? "nativeRewriteSortSpills" : "stockRewriteSortSpills"));
          System.out.println(
              "ICEBERG_MAINTENANCE_BATCH " + IcebergJson.mapper().writeValueAsString(observation));
        }
      }
    }
  }

  private static StreamExecutionEnvironment environment() {
    var config =
        Configuration.fromMap(
            Map.of(
                "restart-strategy.type", "none",
                "execution.batch.adaptive.auto-parallelism.enabled", "false",
                "taskmanager.numberOfTaskSlots", Integer.toString(PARTITIONS),
                "taskmanager.memory.task.off-heap.size", "512m",
                "metrics.reporters", "iceberg",
                "metrics.reporter.iceberg.factory.class", IcebergNativeMetrics.class.getName()));
    var env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    env.setParallelism(PARTITIONS);
    return env;
  }

  private record Fixture(List<DataFile> files, List<DeleteFile> deletes) {}

  private static Fixture writeFixture(Table table, int rowsPerFile) throws Exception {
    var factory =
        new GenericFileWriterFactory.Builder(table)
            .dataFileFormat(FileFormat.PARQUET)
            .deleteFileFormat(FileFormat.PARQUET)
            .build();
    var files = new ArrayList<DataFile>();
    var deletes = new ArrayList<DeleteFile>();
    var row = GenericRecord.create(table.schema());
    var delete = PositionDelete.<org.apache.iceberg.data.Record>create();
    for (int shard = 0; shard < PARTITIONS; shard++) {
      var partition = new PartitionKey(table.spec(), table.schema());
      partition.set(0, shard);
      var paths = new ArrayList<String>();
      for (int file = 0; file < FILES_PER_PARTITION; file++) {
        var output =
            table
                .io()
                .newOutputFile(table.location() + "/input-" + shard + "-" + file + ".parquet");
        var writer =
            factory.newDataWriter(table.encryption().encrypt(output), table.spec(), partition);
        try (writer) {
          for (int position = 0; position < rowsPerFile; position++) {
            int id = ((shard * FILES_PER_PARTITION) + file) * rowsPerFile + position;
            row.setField("id", id);
            row.setField("payload", payload(id));
            row.setField("shard", shard);
            writer.write(row);
          }
        }
        var dataFile = writer.toDataFile();
        files.add(dataFile);
        paths.add(dataFile.location());
      }
      var output = table.io().newOutputFile(table.location() + "/deletes-" + shard + ".parquet");
      var writer =
          factory.newPositionDeleteWriter(
              table.encryption().encrypt(output), table.spec(), partition);
      try (writer) {
        for (String path : paths) {
          for (int position = 0; position < rowsPerFile; position += DELETE_STRIDE)
            writer.write(delete.set(path, position));
        }
      }
      deletes.add(writer.toDeleteFile());
    }
    return new Fixture(List.copyOf(files), List.copyOf(deletes));
  }

  /** Same Iceberg planner and commit protocol, with Flink's RowData external sorter. */
  private static List<RewriteFileGroup> stockSortedRewrite(
      StreamExecutionEnvironment env, Table table) throws Exception {
    table.refresh();
    long snapshot = table.currentSnapshot().snapshotId();
    var planner = new BinPackRewriteFilePlanner(table, Expressions.alwaysTrue(), snapshot, false);
    planner.init(NATIVE_OPTIONS);
    var groups = new ArrayList<RewriteFileGroup>();
    try (var planned = planner.plan().groups()) {
      for (var group : planned) groups.add(group);
    }
    var manager = new RewriteDataFilesCommitManager(table, snapshot);
    var completed = new LinkedHashSet<RewriteFileGroup>();
    try (var results =
        env.fromData(groups, TypeInformation.of(RewriteFileGroup.class))
            .transform(
                "Flink RowData sort and rewrite",
                TypeInformation.of(RewriteFileGroup.class),
                new StockSortedOperator(SerializableTable.copyOf(table)))
            .setParallelism(Math.min(groups.size(), env.getParallelism()))
            .executeAndCollect("Flink sorted Iceberg data-file rewrite")) {
      results.forEachRemaining(completed::add);
    } catch (Exception failure) {
      completed.forEach(manager::abortFileGroup);
      throw failure;
    }
    manager.commitOrClean(completed);
    return List.copyOf(completed);
  }

  private static final class StockSortedOperator extends AbstractStreamOperator<RewriteFileGroup>
      implements OneInputStreamOperator<RewriteFileGroup, RewriteFileGroup> {
    private final Table table;
    private transient org.apache.flink.metrics.Counter spills;

    StockSortedOperator(Table table) {
      this.table = table;
    }

    @Override
    public void open() throws Exception {
      super.open();
      spills = getMetricGroup().counter("stockRewriteSortSpills");
    }

    @Override
    public void processElement(StreamRecord<RewriteFileGroup> record) throws Exception {
      var group = record.getValue();
      var statistics = new IcebergStockRewrite.Statistics();
      group.setOutputFiles(
          IcebergStockRewrite.rewrite(
              table,
              group,
              true,
              SORT_MEMORY,
              getContainingTask().getEnvironment().getIOManager().getSpillingDirectoriesPaths()[0],
              statistics));
      spills.inc(statistics.spillFiles);
      output.collect(record);
    }
  }

  private static Set<DataFile> outputFiles(Table table) throws Exception {
    var files = new HashSet<DataFile>();
    try (var tasks = table.newScan().planFiles()) {
      for (var task : tasks) files.add(task.file());
    }
    return files;
  }

  private static void verifyRows(
      Table table, boolean withDeletes, int rowsPerFile, int inputRows, int expectedRows)
      throws Exception {
    var seen = new BitSet(inputRows);
    try (var records = IcebergGenerics.read(table).build()) {
      for (var row : records) {
        int id = (Integer) row.getField("id");
        assertTrue(id >= 0 && id < inputRows && !seen.get(id));
        assertFalse(withDeletes && id % rowsPerFile % DELETE_STRIDE == 0);
        seen.set(id);
        assertEquals(payload(id), row.getField("payload"));
        assertEquals(id / rowsPerFile / FILES_PER_PARTITION, row.getField("shard"));
      }
    }
    assertEquals(expectedRows, seen.cardinality());
  }

  private static String payload(int id) {
    return id % 7 == 0 ? null : "payload-é-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-" + id % 4096;
  }
}
