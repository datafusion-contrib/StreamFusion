package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.sink.CompactorSinkBuilder;
import org.apache.paimon.flink.source.CompactorSourceBuilder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class PaimonMaintenanceTest {
  @TempDir java.nio.file.Path directory;

  @ParameterizedTest
  @MethodSource("coordinatedModes")
  @Timeout(90)
  void boundedFullDataCompactionPreservesTheStockGraph(boolean coordinated) throws Exception {
    var table = table(coordinated);
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    env.setParallelism(1);
    build(env, table, false, true);
    adapt(env);
    env.execute("Paimon bounded full compaction");
    assertCompacted(table);
  }

  @ParameterizedTest
  @MethodSource("coordinatedModes")
  @Timeout(90)
  void continuousMinorCompactionKeepsCheckpointAndCoordinatorLifecycle(boolean coordinated)
      throws Exception {
    var table = table(coordinated);
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(100);
    build(env, table, true, false);
    adapt(env);
    var job = env.executeAsync("Paimon continuous minor compaction");
    try {
      long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
      while (dataFiles(table).stream().anyMatch(file -> file.level() == 0)) {
        if (job.getJobStatus().get().isTerminalState()) job.getJobExecutionResult().get();
        assertTrue(System.nanoTime() < deadline, "minor compaction must commit its output");
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
      }
      assertCompacted(table);
    } finally {
      job.cancel().get();
    }
  }

  private FileStoreTable table(boolean coordinated) throws Exception {
    var io = LocalFileIO.create();
    var path = new Path(directory.resolve("table").toUri());
    PaimonTestCompat.schemaManager(io, path)
        .createTable(
            new Schema(
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT().notNull()),
                        DataTypes.FIELD(1, "v", DataTypes.INT()))
                    .getFields(),
                List.of(),
                List.of("id"),
                Map.of(
                    "bucket",
                    "1",
                    "file.format",
                    System.getenv().getOrDefault("SF_PAIMON_FILE_FORMAT", "parquet"),
                    "write-only",
                    "true",
                    "num-sorted-run.compaction-trigger",
                    "2",
                    "sink.writer-coordinator.enabled",
                    Boolean.toString(coordinated)),
                ""));
    var table = FileStoreTableFactory.create(io, path);
    var builder = table.newStreamWriteBuilder().withCommitUser("seed");
    try (var writer = builder.newWrite();
        var commit = builder.newCommit()) {
      for (int checkpoint = 1; checkpoint <= 2; checkpoint++) {
        for (int id = 0; id < 10; id++)
          writer.write(GenericRow.of(id, id + (checkpoint - 1) * 100));
        commit.commit(checkpoint, writer.prepareCommit(true, checkpoint));
      }
    }
    assertEquals(2, dataFiles(table).size());
    return table.copy(Map.of("write-only", "false"));
  }

  private static void build(
      StreamExecutionEnvironment env, FileStoreTable table, boolean continuous, boolean full) {
    var buckets =
        new CompactorSourceBuilder("maintenance", table)
            .withEnv(env)
            .withContinuousMode(continuous)
            .build();
    new CompactorSinkBuilder(table, full).withInput(buckets).build();
  }

  private static void adapt(StreamExecutionEnvironment env) {
    var before = graph(env);
    assertEquals(1, NativePaimonMaintenance.accelerate(env));
    assertEquals(before, graph(env), "source, commit and state identities must be retained");
    assertEquals(0, NativePaimonMaintenance.accelerate(env), "adapting twice must be inert");
  }

  private static Map<Integer, String> graph(StreamExecutionEnvironment env) {
    Map<Integer, String> graph = new LinkedHashMap<>();
    for (var root : env.getTransformations()) {
      for (var node : root.getTransitivePredecessors()) {
        graph.put(
            node.getId(),
            node.getName()
                + ":"
                + node.getUid()
                + ":"
                + node.getParallelism()
                + ":"
                + node.getMaxParallelism()
                + ":"
                + node.getInputs().stream()
                    .map(input -> input.getId())
                    .collect(Collectors.toList()));
      }
    }
    return graph;
  }

  private static void assertCompacted(FileStoreTable table) throws Exception {
    var files = dataFiles(table);
    assertEquals(1, files.size());
    assertTrue(files.get(0).level() > 0);
    assertEquals(
        IntStream.range(0, 10)
            .mapToObj(id -> id + "|" + (id + 100))
            .sorted()
            .collect(Collectors.toList()),
        readRows(table));
    assertNativeFiles(table);
  }

  static java.util.stream.Stream<Boolean> coordinatedModes() {
    return PaimonTestCompat.coordinatedWriters()
        ? java.util.stream.Stream.of(false, true)
        : java.util.stream.Stream.of(false);
  }

  private static List<org.apache.paimon.io.DataFileMeta> dataFiles(FileStoreTable table) {
    return table.newReadBuilder().newScan().plan().splits().stream()
        .flatMap(split -> ((org.apache.paimon.table.source.DataSplit) split).dataFiles().stream())
        .collect(Collectors.toList());
  }

  private static List<String> readRows(FileStoreTable table) throws Exception {
    var result = new java.util.ArrayList<String>();
    var reader = table.newReadBuilder().newRead();
    for (var split : table.newReadBuilder().newScan().plan().splits()) {
      try (var rows =
          new org.apache.paimon.reader.RecordReaderIterator<>(reader.createReader(split))) {
        while (rows.hasNext()) {
          var row = rows.next();
          result.add(row.getInt(0) + "|" + row.getInt(1));
        }
      }
    }
    result.sort(String::compareTo);
    return result;
  }

  private static void assertNativeFiles(FileStoreTable table) throws Exception {
    var paths =
        table
            .store()
            .pathFactory()
            .createDataFilePathFactory(org.apache.paimon.data.BinaryRow.EMPTY_ROW, 0);
    for (var file : dataFiles(table)) {
      if (file.fileFormat().equals("orc")) {
        try (var reader =
            org.apache.paimon.shade.org.apache.orc.OrcFile.createReader(
                new org.apache.hadoop.fs.Path(paths.toPath(file).toUri()),
                org.apache.paimon.shade.org.apache.orc.OrcFile.readerOptions(
                    new org.apache.hadoop.conf.Configuration()))) {
          assertTrue(
              reader.hasMetadataValue(tech.streamfusion.orc.OrcVectorWriter.ARROW_WRITER_METADATA));
        }
      } else {
        try (var reader =
            PaimonTestCompat.parquetReader(table.fileIO(), paths.toPath(file), file.fileSize())) {
          assertTrue(reader.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs"));
        }
      }
    }
  }
}
