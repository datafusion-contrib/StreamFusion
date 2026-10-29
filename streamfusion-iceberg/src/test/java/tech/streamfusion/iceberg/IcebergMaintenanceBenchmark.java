package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BinPackRewriteFilePlanner;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** File-task timings are separate from the continuous maintenance graph measurements. */
@EnabledIfEnvironmentVariable(named = "SF_ICEBERG_MAINTENANCE_BENCHMARK", matches = "true")
class IcebergMaintenanceBenchmark {
  @TempDir Path temp;

  static final class Scenario implements Serializable {
    final String name;
    final boolean sorted;
    final boolean nested;
    final int version;
    final String equality;
    final long memory;

    Scenario(String name) {
      if (!java.util.Set.of(
              "sort-memory",
              "sort-spill",
              "equality-int-v2",
              "equality-string-v2",
              "equality-composite-v2",
              "equality-composite-v3",
              "nested-append",
              "nested-sort")
          .contains(name)) throw new IllegalArgumentException("Unknown maintenance case: " + name);
      this.name = name;
      sorted = name.startsWith("sort-") || name.equals("nested-sort");
      nested = name.startsWith("nested-");
      version = name.endsWith("v2") || name.equals("nested-append") ? 2 : 3;
      equality = name.startsWith("equality-") ? name.split("-")[1] : "none";
      memory = name.equals("sort-spill") ? 8L << 20 : 512L << 20;
    }
  }

  static List<Scenario> scenarios() {
    return Arrays.stream(
            System.getenv()
                .getOrDefault(
                    "SF_ICEBERG_MAINTENANCE_CASES",
                    "sort-memory,sort-spill,equality-int-v2,equality-string-v2,equality-composite-v2,equality-composite-v3,nested-append,nested-sort")
                .split(","))
        .map(Scenario::new)
        .toList();
  }

  static int rowsPerFile() {
    return Integer.parseInt(System.getenv().getOrDefault("SF_ICEBERG_MAINTENANCE_ROWS", "100000"));
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new IcebergMaintenanceBenchmark();
    benchmark.temp = java.nio.file.Files.createTempDirectory("iceberg-maintenance-benchmark-");
    try {
      benchmark.compareFileWorkersWithSortingAndEqualityDeletes();
    } finally {
      org.apache.commons.io.FileUtils.deleteDirectory(benchmark.temp.toFile());
    }
  }

  @Test
  void compareFileWorkersWithSortingAndEqualityDeletes() throws Exception {
    TaskOffHeapMemory.initialize(
        org.apache.flink.configuration.Configuration.fromMap(
            Map.of("taskmanager.memory.task.off-heap.size", "1gb")));
    boolean previous = Boolean.parseBoolean(System.getenv("SF_ICEBERG_MAINTENANCE_PREVIOUS"));
    int rows = rowsPerFile();
    int runs = Integer.parseInt(System.getenv().getOrDefault("SF_ICEBERG_MAINTENANCE_RUNS", "3"));
    for (var scenario : scenarios()) {
      if (previous && scenario.nested)
        throw new IllegalArgumentException("Previous native version did not admit nested schemas");
      var table = fixture(temp.resolve(scenario.name), scenario, rows);
      var planner =
          new BinPackRewriteFilePlanner(
              table, Expressions.alwaysTrue(), table.currentSnapshot().snapshotId(), true);
      planner.init(Map.of("rewrite-all", "true"));
      RewriteFileGroup group;
      try (var groups = planner.plan().groups()) {
        var iterator = groups.iterator();
        group = iterator.next();
        assertFalse(iterator.hasNext());
      }
      long[] expected = checksum(table);
      for (int trial = -1; trial < runs; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          boolean nativeMode = (trial + turn) % 2 == 0;
          var nativeStats = new IcebergRewriteFiles.Statistics();
          var stockStats = new IcebergStockRewrite.Statistics();
          long start = System.nanoTime();
          var output =
              nativeMode
                  ? previous
                      ? IcebergRewriteFiles.rewrite(
                          table, group, 0, 0, scenario.sorted, scenario.memory, temp.toString())
                      : IcebergRewriteFiles.rewrite(
                          table,
                          group,
                          0,
                          0,
                          scenario.sorted,
                          scenario.memory,
                          temp.toString(),
                          nativeStats)
                  : IcebergStockRewrite.rewrite(
                      table, group, scenario.sorted, scenario.memory, temp.toString(), stockStats);
          double seconds = (System.nanoTime() - start) / 1e9;
          long spills =
              nativeMode ? (previous ? -1 : nativeStats.sortSpills) : stockStats.spillFiles;
          long spillBytes =
              nativeMode ? (previous ? -1 : nativeStats.sortSpillBytes) : stockStats.spillBytes;
          try {
            assertArrayEquals(expected, checksum(table, output, scenario.sorted));
            assertEncoder(table, output, nativeMode);
            if (!(previous && nativeMode) && scenario.name.equals("sort-spill") && rows >= 100000)
              assertTrue(spills > 0 && spillBytes > 0, "Forced sort spill must execute");
            if (!(previous && nativeMode) && scenario.name.equals("sort-memory"))
              assertEquals(0, spills, "In-memory cohort spilled");
            assertEquals("", NativeIceberg.liveNativeHandles());
            assertEquals(0, NativeAllocator.SHARED.getAllocatedMemory());
            assertEquals(0, TaskOffHeapMemory.reservedBytes());
            var sample = new LinkedHashMap<String, Object>();
            sample.put("case", scenario.name);
            sample.put("native", nativeMode);
            sample.put("previous", previous && nativeMode);
            sample.put("trial", trial);
            sample.put("input_rows", 4L * rows);
            sample.put("output_rows", expected[0]);
            sample.put("seconds", seconds);
            sample.put("sort_memory_bytes", scenario.sorted ? scenario.memory : 0);
            sample.put("spill_count", spills);
            sample.put("spill_bytes", spillBytes);
            sample.put("output_files", output.size());
            System.out.println(
                "ICEBERG_MAINTENANCE_FILE " + IcebergJson.mapper().writeValueAsString(sample));
          } finally {
            output.forEach(file -> table.io().deleteFile(file.location()));
          }
        }
      }
    }
  }

  static Table fixture(Path location, Scenario scenario, int rows) throws Exception {
    var schema = schema(scenario.nested);
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                PartitionSpec.unpartitioned(),
                Map.of(
                    "format-version",
                    Integer.toString(scenario.version),
                    "write.parquet.compression-codec",
                    "zstd",
                    "write.parquet.compression-level",
                    "3"),
                location.toString());
    append(table, scenario, rows, "initial");
    if (scenario.sorted)
      table
          .replaceSortOrder()
          .asc("payload", org.apache.iceberg.NullOrder.NULLS_FIRST)
          .desc("id", org.apache.iceberg.NullOrder.NULLS_LAST)
          .commit();
    return table;
  }

  static void append(Table table, Scenario scenario, int rows, String cohort) throws Exception {
    var prepared = prepare(table, scenario, rows, cohort);
    publishData(table, prepared);
    if (prepared.deletes != null) table.newRowDelta().addDeletes(prepared.deletes).commit();
  }

  static final class Prepared {
    final List<DataFile> files = new ArrayList<>();
    org.apache.iceberg.DeleteFile deletes;
  }

  static void publishData(Table table, Prepared prepared) {
    var append = table.newAppend();
    prepared.files.forEach(append::appendFile);
    append.commit();
  }

  static Prepared prepare(Table table, Scenario scenario, int rows, String cohort)
      throws Exception {
    var factory =
        new GenericFileWriterFactory.Builder(table)
            .dataFileFormat(FileFormat.PARQUET)
            .dataSortOrder(org.apache.iceberg.SortOrder.unsorted())
            .build();
    var prepared = new Prepared();
    for (int file = 0; file < 4; file++) {
      var writer =
          factory.newDataWriter(
              table
                  .encryption()
                  .encrypt(
                      table
                          .io()
                          .newOutputFile(
                              table.location() + "/" + cohort + "-" + file + ".parquet")),
              table.spec(),
              null);
      try (writer) {
        for (int position = 0; position < rows; position++) {
          int id = (int) (((long) file * rows + position) * 104729L % (4L * rows));
          writer.write(record(table.schema(), id, scenario.nested));
        }
      }
      prepared.files.add(writer.toDataFile());
    }
    if (!scenario.equality.equals("none"))
      prepared.deletes = prepareEqualities(table, scenario.equality, 4 * rows, cohort);
    return prepared;
  }

  private static Schema schema(boolean nested) {
    var fields = new ArrayList<Types.NestedField>();
    fields.add(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    fields.add(Types.NestedField.optional(2, "payload", Types.StringType.get()));
    fields.add(Types.NestedField.optional(3, "shard", Types.LongType.get()));
    if (nested) {
      fields.add(
          Types.NestedField.optional(
              4,
              "profile",
              Types.StructType.of(
                  Types.NestedField.optional(5, "bucket", Types.LongType.get()),
                  Types.NestedField.optional(6, "label", Types.StringType.get()))));
      fields.add(
          Types.NestedField.optional(
              7, "samples", Types.ListType.ofOptional(8, Types.LongType.get())));
      fields.add(
          Types.NestedField.optional(
              9,
              "attributes",
              Types.MapType.ofOptional(10, 11, Types.StringType.get(), Types.StringType.get())));
    }
    return new Schema(fields);
  }

  private static String payload(int key) {
    return "payload-é🐍-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-" + key;
  }

  private static GenericRecord record(Schema schema, int id, boolean nested) {
    var row = GenericRecord.create(schema);
    row.setField("id", id);
    row.setField("payload", id % 7 == 0 ? null : payload(id % 4096));
    row.setField("shard", id % 19 == 0 ? null : (long) (id / 4096 % 64));
    if (nested) {
      var profile = GenericRecord.create(schema.findType("profile").asStructType());
      profile.setField("bucket", (long) (id % 64));
      profile.setField("label", id % 5 == 0 ? null : "nested-" + id % 1024);
      row.setField("profile", id % 11 == 0 ? null : profile);
      row.setField(
          "samples",
          id % 13 == 0
              ? null
              : id % 3 == 0 ? List.of() : Arrays.asList((long) id, null, (long) -id));
      var attrs = new LinkedHashMap<String, String>();
      attrs.put("key", "value-" + id % 128);
      attrs.put("nullable", null);
      row.setField("attributes", id % 17 == 0 ? null : attrs);
    }
    return row;
  }

  private static org.apache.iceberg.DeleteFile prepareEqualities(
      Table table, String mode, int rows, String cohort) throws Exception {
    String[] names =
        mode.equals("int")
            ? new String[] {"id"}
            : mode.equals("string") ? new String[] {"payload"} : new String[] {"shard", "payload"};
    var keys = new Schema(Arrays.stream(names).map(table.schema()::findField).toList());
    var factory =
        new GenericFileWriterFactory.Builder(table)
            .dataFileFormat(FileFormat.PARQUET)
            .deleteFileFormat(FileFormat.PARQUET)
            .equalityFieldIds(
                Arrays.stream(names)
                    .mapToInt(name -> table.schema().findField(name).fieldId())
                    .toArray())
            .equalityDeleteRowSchema(keys)
            .build();
    var writer =
        factory.newEqualityDeleteWriter(
            table
                .encryption()
                .encrypt(
                    table
                        .io()
                        .newOutputFile(table.location() + "/" + cohort + "-equalities.parquet")),
            table.spec(),
            null);
    try (writer) {
      if (mode.equals("int")) {
        for (int id = 17; id < rows; id += 101) {
          var row = GenericRecord.create(keys);
          row.setField("id", id);
          writer.write(row);
        }
      } else if (mode.equals("string")) {
        for (int key = -1; key < 128; key++) {
          var row = GenericRecord.create(keys);
          row.setField("payload", key == -1 ? null : payload(key));
          writer.write(row);
        }
      } else {
        for (int shard = -1; shard < 64; shard++) {
          for (int key : new int[] {-1, 0, 64, 128}) {
            var row = GenericRecord.create(keys);
            row.setField("shard", shard == -1 ? null : (long) shard);
            row.setField("payload", key == -1 ? null : payload(key));
            writer.write(row);
          }
        }
      }
    }
    return writer.toDeleteFile();
  }

  static long[] checksum(Table table) throws Exception {
    var sums = new long[3];
    try (var input = IcebergGenerics.read(table).build()) {
      for (var row : input) checksum(sums, row);
    }
    return sums;
  }

  static long[] checksum(Table table, Set<DataFile> files, boolean sorted) throws Exception {
    var sums = new long[3];
    java.util.Comparator<org.apache.iceberg.StructLike> comparator =
        sorted
            ? org.apache.iceberg.SortOrderComparators.forSchema(table.schema(), table.sortOrder())
            : null;
    for (var file : files) {
      if (sorted) assertEquals(table.sortOrder().orderId(), file.sortOrderId());
      org.apache.iceberg.data.Record previous = null;
      try (var input =
          org.apache.iceberg.parquet.Parquet.read(table.io().newInputFile(file.location()))
              .project(table.schema())
              .createReaderFunc(
                  physical ->
                      org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(
                          table.schema(), physical))
              .build()) {
        for (var value : input) {
          var row = (org.apache.iceberg.data.Record) value;
          if (comparator != null && previous != null)
            assertTrue(comparator.compare(previous, row) <= 0, "Incorrect sorted output");
          checksum(sums, row);
          if (sorted) previous = row.copy();
        }
      }
    }
    return sums;
  }

  private static void checksum(long[] sums, org.apache.iceberg.data.Record row) {
    long hash = 0x9e3779b97f4a7c15L;
    for (int field = 0; field < row.size(); field++)
      hash = (hash ^ java.util.Objects.hashCode(row.get(field))) * 0xbf58476d1ce4e5b9L;
    hash = (hash ^ (hash >>> 30)) * 0xbf58476d1ce4e5b9L;
    hash = (hash ^ (hash >>> 27)) * 0x94d049bb133111ebL;
    hash ^= hash >>> 31;
    sums[0]++;
    sums[1] += hash;
    sums[2] ^= hash;
  }

  static void assertEncoder(Table table, Set<DataFile> files, boolean nativeMode) throws Exception {
    for (var file : files) {
      try (var footer =
          org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(
              org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                  new org.apache.hadoop.fs.Path(file.location()),
                  new org.apache.hadoop.conf.Configuration()))) {
        assertEquals(
            nativeMode, footer.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs"));
      }
    }
  }
}
