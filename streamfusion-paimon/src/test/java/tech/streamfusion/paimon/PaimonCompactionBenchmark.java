package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
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
      for (String shape :
          System.getenv()
              .getOrDefault(
                  "SF_PAIMON_COMPACTION_SHAPES",
                  "unique,versions,wide,rolling,multilevel,append,append-wide")
              .split(",")) {
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
            var metrics = new Metrics();
            StoreSinkWrite.Provider plain =
                (resolved, user, state, manager, memory, group) ->
                    new StoreSinkWriteImpl(
                        resolved, user, state, manager, false, false, false, memory, group);
            var provider =
                engine.equals("native")
                    ? PaimonCompactionProvider.wrap(selected, plain, false, false, false)
                    : plain;
            List<DataFileMeta> output = new ArrayList<>();
            long start = System.nanoTime();
            try (var io = new IOManagerAsync()) {
              var write = PaimonTestCompat.provide(provider, selected, io, metrics);
              try (AutoCloseable close = write::close) {
                write.compact(BinaryRow.EMPTY_ROW, 0, true);
                for (var message : write.prepareCommit(true, 100))
                  output.addAll(
                      ((CommitMessageImpl) PaimonTestCompat.message(message))
                          .compactIncrement()
                          .compactAfter());
              }
            }
            double seconds = (System.nanoTime() - start) / 1e9;
            assertFalse(output.isEmpty());
            var outputPaths =
                table.store().pathFactory().createDataFilePathFactory(BinaryRow.EMPTY_ROW, 0);
            if (engine.equals("native")) {
              assertTrue(metrics.counters.get("nativeRewriteGroups").getCount() > 0);
              assertEquals(
                  0,
                  metrics.counters.get("stockRewriteGroups").getCount(),
                  "benchmark must be admitted");
            }
            for (var file : output) {
              assertFileBackend(table, outputPaths.toPath(file), file, engine.equals("native"));
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
                    + " fingerprint=%s format=%s output_bytes=%d native_groups=%d"
                    + " fallback_groups=%d%n",
                shape,
                rows,
                engine,
                trial,
                seconds,
                output.size(),
                fingerprint,
                table.coreOptions().fileFormatString(),
                output.stream().mapToLong(DataFileMeta::fileSize).sum(),
                metrics.counters.containsKey("nativeRewriteGroups")
                    ? metrics.counters.get("nativeRewriteGroups").getCount()
                    : 0,
                metrics.counters.containsKey("stockRewriteGroups")
                    ? metrics.counters.get("stockRewriteGroups").getCount()
                    : 0);
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
    boolean append = shape.startsWith("append");
    boolean wide = shape.contains("wide");
    String mergeEngine =
        shape.startsWith("first-row")
            ? "first-row"
            : shape.startsWith("partial-update")
                ? "partial-update"
                : shape.startsWith("aggregation") ? "aggregation" : "deduplicate";
    var fields =
        new ArrayList<org.apache.paimon.types.DataField>(
            List.of(
                DataTypes.FIELD(0, "id", DataTypes.INT().notNull()),
                DataTypes.FIELD(1, "v", DataTypes.BIGINT())));
    if (wide) {
      fields.add(DataTypes.FIELD(2, "payload", DataTypes.STRING()));
      fields.add(DataTypes.FIELD(3, "nested", DataTypes.ARRAY(DataTypes.INT())));
    }
    var options = new java.util.HashMap<String, String>();
    options.put("bucket", "1");
    options.put("bucket-key", "id");
    options.put("file.format", System.getenv().getOrDefault("SF_PAIMON_FILE_FORMAT", "parquet"));
    options.put("write-only", "true");
    if (!append) options.put("merge-engine", mergeEngine);
    if (mergeEngine.equals("aggregation")) options.put("fields.v.aggregate-function", "sum");
    options.put("write-buffer-size", "256 mb");
    options.put(
        "sort-spill-buffer-size",
        System.getenv().getOrDefault("SF_PAIMON_COMPACTION_SORT_BUFFER", "256 mb"));
    options.put("parquet.block.size", "4194304");
    options.put("target-file-size", shape.equals("rolling") ? "512 kb" : "128 mb");
    PaimonTestCompat.schemaManager(fileIO, path)
        .createTable(
            new Schema(fields, List.of(), append ? List.of() : List.of("id"), options, ""));
    var table = FileStoreTableFactory.create(fileIO, path);
    try (var io = IOManager.create(directory.resolve("seed-spill").toString());
        var write = table.newWrite("seed").withIOManager(io);
        var commit = table.newCommit("seed")) {
      for (int run = 0; run < 4; run++) {
        for (int row = 0; row < rows / 4; row++) {
          int key = shape.equals("unique") ? row * 4 + run : row;
          RowKind kind =
              !append
                      && mergeEngine.equals("deduplicate")
                      && run == 3
                      && key % 5 == 0
                      && !shape.equals("unique")
                  ? RowKind.DELETE
                  : RowKind.INSERT;
          long value = (long) key * 17 + run;
          write.write(
              wide
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
    if (table.primaryKeys().isEmpty()) return appendFingerprint(table, files);
    var factory = PaimonTestCompat.keyValueReaderFactory(table);
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

  private List<String> appendFingerprint(FileStoreTable table, List<DataFileMeta> files)
      throws Exception {
    var split =
        DataSplit.builder()
            .withPartition(BinaryRow.EMPTY_ROW)
            .withBucket(0)
            .withBucketPath(
                table.store().pathFactory().bucketPath(BinaryRow.EMPTY_ROW, 0).toString())
            .withDataFiles(files)
            .rawConvertible(true)
            .isStreaming(false)
            .build();
    var binary = new org.apache.paimon.data.serializer.InternalRowSerializer(table.rowType());
    var digest = java.security.MessageDigest.getInstance("SHA-256");
    long count = 0;
    try (var reader =
        new RecordReaderIterator<>(table.newReadBuilder().newRead().createReader(split))) {
      while (reader.hasNext()) {
        digest.update(binary.toBinaryRow(reader.next()).toBytes());
        count++;
      }
    }
    return List.of(
        Long.toString(count), java.util.Base64.getEncoder().encodeToString(digest.digest()));
  }

  private static void assertFileBackend(
      FileStoreTable table, Path path, DataFileMeta file, boolean nativeWriter) throws Exception {
    if (file.fileFormat().equals("orc")) {
      try (var reader =
          org.apache.paimon.shade.org.apache.orc.OrcFile.createReader(
              new org.apache.hadoop.fs.Path(path.toUri()),
              org.apache.paimon.shade.org.apache.orc.OrcFile.readerOptions(
                  new org.apache.hadoop.conf.Configuration()))) {
        assertEquals(
            nativeWriter,
            reader.hasMetadataValue(tech.streamfusion.orc.OrcVectorWriter.ARROW_WRITER_METADATA));
      }
    } else {
      try (var reader = PaimonTestCompat.parquetReader(table.fileIO(), path, file.fileSize())) {
        assertTrue(
            reader
                .getFooter()
                .getFileMetaData()
                .getCreatedBy()
                .contains(nativeWriter ? "parquet-rs" : "parquet-mr"));
      }
    }
  }

  private static final class Metrics extends UnregisteredMetricsGroup {
    final java.util.Map<String, Counter> counters = new java.util.HashMap<>();

    @Override
    public <C extends Counter> C counter(String name, C counter) {
      if (name.startsWith("nativeRewrite") || name.equals("stockRewriteGroups"))
        counters.put(name, counter);
      return counter;
    }
  }
}
