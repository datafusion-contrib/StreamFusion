package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

/** Real file-to-file maintenance, including Java planning, JNI, metadata and writer close. */
class PaimonCompactionBenchmark {
  @TempDir java.nio.file.Path directory;

  @Test
  @EnabledIfEnvironmentVariable(named = "SF_PAIMON_COMPACTION_BENCHMARK", matches = "true")
  void compareReleasedCompactors() throws Exception {
    assertTrue(
        java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
            .anyMatch(arg -> arg.startsWith("-javaagent:")),
        "Use the suite agent for baseline SPI selection");
    int rows =
        Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_COMPACTION_ROWS", "400000"));
    int repeats =
        Integer.parseInt(System.getenv().getOrDefault("SF_PAIMON_COMPACTION_REPEATS", "3"));
    String originalMode = System.getProperty("streamfusion.paimon.compaction.mode");
    try {
      for (String shape : List.of("unique", "versions", "wide", "rolling", "multilevel")) {
        System.setProperty("streamfusion.paimon.compaction.mode", "stock");
        FileStoreTable table = fixture(shape, rows);
        List<String> expected = null;
        var timings = new java.util.LinkedHashMap<String, List<Double>>();
        for (String engine : List.of("stock", "previous", "native"))
          timings.put(engine, new ArrayList<>());
        for (int trial = 0; trial <= repeats; trial++) {
          for (int position = 0; position < 3; position++) {
            String engine = List.of("stock", "previous", "native").get((position + trial) % 3);
            System.setProperty("streamfusion.paimon.compaction.mode", engine);
            var selected = table.copy(java.util.Map.of("write-only", "false"));
            var nativeWriters = new ArrayList<NativePaimonCompactRewriter>();
            List<DataFileMeta> output = new ArrayList<>();
            long start = System.nanoTime();
            try (var io = IOManager.create(directory.resolve("spill").toString());
                var write = selected.newWrite("benchmark")) {
              write.withIOManager(io);
              if (engine.equals("native"))
                write.withCompactRewriterFactory(
                    (partition, bucket, stock) -> {
                      var rewriter =
                          new NativePaimonCompactRewriter(
                              selected, partition, bucket, stock, new UnregisteredMetricsGroup());
                      nativeWriters.add(rewriter);
                      return rewriter;
                    });
              write.compact(BinaryRow.EMPTY_ROW, 0, true);
              for (var message : write.prepareCommit(true, 100))
                output.addAll(((CommitMessageImpl) message).compactIncrement().compactAfter());
            }
            double seconds = (System.nanoTime() - start) / 1e9;
            assertFalse(output.isEmpty());
            var outputPaths =
                table.store().pathFactory().createDataFilePathFactory(BinaryRow.EMPTY_ROW, 0);
            for (var file : output) {
              try (var footer =
                  org.apache.paimon.format.parquet.ParquetUtil.getParquetReader(
                      table.fileIO(),
                      outputPaths.toPath(file),
                      file.fileSize(),
                      new org.apache.paimon.options.Options())) {
                assertTrue(
                    footer
                        .getFooter()
                        .getFileMetaData()
                        .getCreatedBy()
                        .contains(engine.equals("native") ? "parquet-rs" : "parquet-mr"));
              }
            }
            if (engine.equals("native")) {
              assertTrue(
                  nativeWriters.stream().mapToLong(r -> r.nativeGroups.getCount()).sum() > 0);
              assertEquals(
                  0,
                  nativeWriters.stream().mapToLong(r -> r.stockGroups.getCount()).sum(),
                  "benchmark must be admitted");
            }
            var fingerprint = fingerprint(table, output);
            if (expected == null) expected = fingerprint;
            else
              assertEquals(
                  expected,
                  fingerprint,
                  shape + " physical output including stored sequences and kinds");
            System.out.printf(
                "PAIMON_COMPACTION shape=%s rows=%d engine=%s trial=%d seconds=%.6f output_files=%d"
                    + " fingerprint=%s%n",
                shape, rows, engine, trial, seconds, output.size(), fingerprint);
            if (trial > 0) timings.get(engine).add(seconds);
            var paths =
                table.store().pathFactory().createDataFilePathFactory(BinaryRow.EMPTY_ROW, 0);
            for (var file : output) table.fileIO().delete(paths.toPath(file), false);
            assertEquals("", NativePaimon.liveNativeHandles());
          }
        }
        System.out.printf(
            "PAIMON_COMPACTION_RESULT shape=%s rows=%d timings=%s%n", shape, rows, timings);
      }
    } finally {
      if (originalMode == null) System.clearProperty("streamfusion.paimon.compaction.mode");
      else System.setProperty("streamfusion.paimon.compaction.mode", originalMode);
    }
  }

  FileStoreTable fixture(String shape, int rows) throws Exception {
    var path = new Path(directory.resolve(shape).toUri());
    var fileIO = LocalFileIO.create();
    var schema =
        Schema.newBuilder()
            .column("id", DataTypes.INT().notNull())
            .column("v", DataTypes.BIGINT())
            .primaryKey("id")
            .option("bucket", "1")
            .option("file.format", "parquet")
            .option("write-only", "true")
            .option("write-buffer-size", "256 mb")
            .option("sort-spill-buffer-size", "256 mb")
            .option("parquet.block.size", "4194304")
            .option("target-file-size", shape.equals("rolling") ? "512 kb" : "128 mb");
    if (shape.equals("wide"))
      schema
          .column("payload", DataTypes.STRING())
          .column("nested", DataTypes.ARRAY(DataTypes.INT()));
    new FileSystemSchemaManager(fileIO, path).createTable(schema.build());
    var table = FileStoreTableFactory.create(fileIO, path);
    try (var io = IOManager.create(directory.resolve("seed-spill").toString());
        var write = table.newWrite("seed").withIOManager(io);
        var commit = table.newCommit("seed")) {
      for (int run = 0; run < 4; run++) {
        for (int row = 0; row < rows / 4; row++) {
          int key = shape.equals("unique") ? row * 4 + run : row;
          RowKind kind =
              run == 3 && key % 5 == 0 && !shape.equals("unique") ? RowKind.DELETE : RowKind.INSERT;
          long value = (long) key * 17 + run;
          write.write(
              shape.equals("wide")
                  ? GenericRow.ofKind(
                      kind,
                      key,
                      value,
                      key % 7 == 0
                          ? null
                          : BinaryString.fromString(
                              "value-"
                                  + value
                                  + "-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz"),
                      new GenericArray(new Integer[] {key, null, run}))
                  : GenericRow.ofKind(kind, key, value));
        }
        commit.commit(run, write.prepareCommit(true, run));
        if (shape.equals("multilevel") && run == 1) {
          var maintenance = table.copy(java.util.Map.of("write-only", "false"));
          try (var compact = maintenance.newWrite("seed-level").withIOManager(io);
              var levelCommit = maintenance.newCommit("seed-level")) {
            compact.compact(BinaryRow.EMPTY_ROW, 0, true);
            levelCommit.commit(0, compact.prepareCommit(true, 0));
          }
        }
      }
    }
    return table;
  }

  private List<String> fingerprint(FileStoreTable table, List<DataFileMeta> files)
      throws Exception {
    var factory =
        ((KeyValueFileStore) table.store())
            .newReaderFactoryBuilder()
            .buildWithoutDeletionVector(BinaryRow.EMPTY_ROW, 0);
    var layout = PaimonKeyValueLayout.of(table);
    var physical = new org.apache.paimon.KeyValueSerializer(layout.keyType, table.rowType());
    var binary = new org.apache.paimon.data.serializer.InternalRowSerializer(layout.writeType);
    var digest = java.security.MessageDigest.getInstance("SHA-256");
    long count = 0;
    for (var file : files) {
      try (var reader = new RecordReaderIterator<KeyValue>(factory.createRecordReader(file))) {
        while (reader.hasNext()) {
          digest.update(binary.toBinaryRow(physical.toRow(reader.next())).toBytes());
          count++;
        }
      }
    }
    return List.of(
        Long.toString(count), java.util.Base64.getEncoder().encodeToString(digest.digest()));
  }
}
