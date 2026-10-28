package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BinPackRewriteFilePlanner;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

/** Measures production file-task IO, including JNI, Java controls and both file close paths. */
@EnabledIfEnvironmentVariable(named = "SF_ICEBERG_REWRITE_BENCHMARK", matches = "true")
class IcebergRewriteBenchmark {
  @TempDir Path temp;

  @Test
  void compareStockAndNativeRewriteTasks() throws Exception {
    int rowsPerFile =
        Integer.parseInt(System.getenv().getOrDefault("SF_ICEBERG_REWRITE_ROWS", "100000"));
    tech.streamfusion.operator.TaskOffHeapMemory.initialize(
        org.apache.flink.configuration.Configuration.fromMap(
            Map.of("taskmanager.memory.task.off-heap.size", "256mb")));
    for (String mode : List.of("v2-append", "v2-deletes", "v3-deletes")) {
      var table = fixture(mode, rowsPerFile);
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
      long[] expected = tableChecksum(table);
      for (int trial = -1; trial < 3; trial++) {
        for (int turn = 0; turn < 2; turn++) {
          boolean nativeMode = (trial + turn) % 2 == 0;
          long start = System.nanoTime();
          Set<DataFile> output =
              nativeMode ? IcebergRewriteFiles.rewrite(table, group, 0, 0) : stock(table, group);
          double seconds = (System.nanoTime() - start) / 1e9;
          try {
            assertArrayEquals(expected, fileChecksum(table, output));
            for (var file : output) {
              try (var footer =
                  org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(
                      org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile
                          .fromPath(
                              new org.apache.hadoop.fs.Path(file.location()),
                              new org.apache.hadoop.conf.Configuration()))) {
                assertEquals(
                    nativeMode,
                    footer.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs"));
              }
            }
            assertEquals("", NativeIceberg.liveNativeHandles());
            assertEquals(0, tech.streamfusion.operator.NativeAllocator.SHARED.getAllocatedMemory());
            assertEquals(0, tech.streamfusion.operator.TaskOffHeapMemory.reservedBytes());
            System.out.println(
                "ICEBERG_REWRITE "
                    + IcebergJson.mapper()
                        .writeValueAsString(
                            Map.of(
                                "case",
                                mode,
                                "native",
                                nativeMode,
                                "trial",
                                trial,
                                "input_rows",
                                4L * rowsPerFile,
                                "output_rows",
                                expected[0],
                                "seconds",
                                seconds,
                                "output_files",
                                output.size())));
          } finally {
            output.forEach(file -> table.io().deleteFile(file.location()));
          }
        }
      }
    }
  }

  private static Set<DataFile> stock(Table table, RewriteFileGroup group) throws Exception {
    var schema =
        org.apache.iceberg.TableUtil.supportsRowLineage(table)
            ? MetadataColumns.schemaWithRowLineage(table.schema())
            : table.schema();
    var factory =
        new RowDataTaskWriterFactory(
            () -> table,
            FlinkSchemaUtil.convert(schema),
            group.inputSplitSize(),
            FileFormat.PARQUET,
            table.properties(),
            null,
            false,
            schema,
            table.spec());
    factory.initialize(0, 0);
    try (var writer = factory.create();
        var input =
            new DataIterator<>(
                new RowDataFileScanTaskReader(table.schema(), schema, null, false, List.of()),
                new BaseCombinedScanTask(group.fileScanTasks()),
                table.io(),
                table.encryption())) {
      try {
        while (input.hasNext()) writer.write(input.next());
        return new LinkedHashSet<>(List.of(writer.complete().dataFiles()));
      } catch (Throwable failure) {
        try {
          writer.abort();
        } catch (Throwable cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      }
    }
  }

  private Table fixture(String mode, int rowsPerFile) throws Exception {
    int version = mode.startsWith("v3") ? 3 : 2;
    var schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "payload", Types.StringType.get()));
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                PartitionSpec.unpartitioned(),
                Map.of(
                    "format-version",
                    Integer.toString(version),
                    "write.parquet.compression-codec",
                    "zstd",
                    "write.parquet.compression-level",
                    "3"),
                temp.resolve(mode).toString());
    var keys = new Schema(schema.findField("id"));
    var factory =
        new GenericFileWriterFactory.Builder(table)
            .dataFileFormat(FileFormat.PARQUET)
            .deleteFileFormat(FileFormat.PARQUET)
            .equalityFieldIds(new int[] {1})
            .equalityDeleteRowSchema(keys)
            .build();
    var data = new java.util.ArrayList<DataFile>();
    var append = table.newAppend();
    for (int file = 0; file < 4; file++) {
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
        for (int position = 0; position < rowsPerFile; position++) {
          int id = file * rowsPerFile + position;
          row.setField("id", id);
          row.setField(
              "payload",
              id % 7 == 0 ? null : "payload-é🐍-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-" + id % 4096);
          writer.write(row);
        }
      }
      data.add(writer.toDataFile());
      append.appendFile(writer.toDataFile());
    }
    append.commit();
    if (!mode.endsWith("deletes")) return table;
    var delta = table.newRowDelta();
    if (version == 3) {
      var writer =
          new BaseDVFileWriter(
              OutputFileFactory.builderFor(table, 0, 0).format(FileFormat.PUFFIN).build(),
              ignored -> null);
      try (writer) {
        for (var file : data)
          for (int position = 0; position < rowsPerFile; position += 8)
            writer.delete(file.location(), position, table.spec(), null);
      }
      writer.result().deleteFiles().forEach(delta::addDeletes);
    } else {
      var writer =
          factory.newPositionDeleteWriter(
              table
                  .encryption()
                  .encrypt(table.io().newOutputFile(table.location() + "/positions.parquet")),
              table.spec(),
              null);
      try (writer) {
        for (var file :
            data.stream().sorted(java.util.Comparator.comparing(DataFile::location)).toList())
          for (int position = 0; position < rowsPerFile; position += 8)
            writer.write(
                PositionDelete.<org.apache.iceberg.data.Record>create()
                    .set(file.location(), position));
      }
      delta.addDeletes(writer.toDeleteFile());
    }
    var writer =
        factory.newEqualityDeleteWriter(
            table
                .encryption()
                .encrypt(table.io().newOutputFile(table.location() + "/equalities.parquet")),
            table.spec(),
            null);
    try (writer) {
      var key = GenericRecord.create(keys);
      for (int id = 17; id < 4 * rowsPerFile; id += 101) {
        key.setField("id", id);
        writer.write(key);
      }
    }
    delta.addDeletes(writer.toDeleteFile()).commit();
    return table;
  }

  private static void checksum(long[] sums, org.apache.iceberg.data.Record row) {
    sums[0]++;
    Object payload = row.getField("payload");
    long hash =
        (((long) (Integer) row.getField("id")) << 32)
            ^ (payload == null ? 0x9e3779b9L : payload.toString().hashCode());
    hash = (hash ^ (hash >>> 30)) * 0xbf58476d1ce4e5b9L;
    hash = (hash ^ (hash >>> 27)) * 0x94d049bb133111ebL;
    hash ^= hash >>> 31;
    sums[1] += hash;
    sums[2] ^= hash;
  }

  private static long[] tableChecksum(Table table) throws Exception {
    long[] sums = new long[3];
    try (var input = IcebergGenerics.read(table).build()) {
      for (var row : input) checksum(sums, row);
    }
    return sums;
  }

  private static long[] fileChecksum(Table table, Set<DataFile> files) throws Exception {
    long[] sums = new long[3];
    for (var file : files) {
      try (var input =
          org.apache.iceberg.parquet.Parquet.read(table.io().newInputFile(file.location()))
              .project(table.schema())
              .createReaderFunc(
                  physical ->
                      org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(
                          table.schema(), physical))
              .build()) {
        for (var row : input) checksum(sums, (org.apache.iceberg.data.Record) row);
      }
    }
    return sums;
  }
}
