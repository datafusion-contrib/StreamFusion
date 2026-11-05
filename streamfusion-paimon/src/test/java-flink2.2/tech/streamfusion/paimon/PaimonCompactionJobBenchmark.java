package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.CompactorSinkBuilder;
import org.apache.paimon.flink.source.CompactorSourceBuilder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

/** Publishes identical files to an already running stock continuous compaction job. */
class PaimonCompactionJobBenchmark {
  @TempDir java.nio.file.Path directory;

  @Test
  @Timeout(600)
  @EnabledIfEnvironmentVariable(named = "SF_PAIMON_COMPACTION_JOB_BENCHMARK", matches = "true")
  void comparePublicationToCompactedCommit() throws Exception {
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
      var fixtures = new PaimonCompactionBenchmark();
      fixtures.directory = directory.resolve("input");
      for (String shape : List.of("versions", "wide", "multilevel")) {
        System.setProperty("streamfusion.paimon.compaction.mode", "stock");
        var input = fixtures.fixture(shape, rows);
        var expected = PaimonTestTables.readRows(input, input.rowType());
        for (int trial = 0; trial <= repeats; trial++) {
          for (int position = 0; position < 3; position++) {
            String engine = List.of("stock", "previous", "native").get((position + trial) % 3);
            System.setProperty("streamfusion.paimon.compaction.mode", engine);
            var target = emptyTarget(input, shape + "-" + engine + "-" + trial);
            var files =
                input.newScan().plan().splits().stream()
                    .flatMap(split -> ((DataSplit) split).dataFiles().stream())
                    .collect(java.util.stream.Collectors.toList());
            var sourcePaths =
                input.store().pathFactory().createDataFilePathFactory(BinaryRow.EMPTY_ROW, 0);
            var targetPaths =
                target.store().pathFactory().createDataFilePathFactory(BinaryRow.EMPTY_ROW, 0);
            Files.createDirectories(java.nio.file.Path.of(targetPaths.parent().toUri()));
            for (var file : files)
              Files.copy(
                  java.nio.file.Path.of(sourcePaths.toPath(file).toUri()),
                  java.nio.file.Path.of(targetPaths.toPath(file).toUri()));
            var env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.enableCheckpointing(100);
            var source =
                new CompactorSourceBuilder("benchmark", target)
                    .withEnv(env)
                    .withContinuousMode(true)
                    .build();
            new CompactorSinkBuilder(target, true).withInput(source).build();
            assertEquals(engine.equals("native") ? 1 : 0, NativePaimonMaintenance.accelerate(env));
            long submitted = System.nanoTime();
            var job = env.executeAsync("Paimon compaction commit latency");
            try {
              long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
              while (job.getJobStatus().get() != org.apache.flink.api.common.JobStatus.RUNNING) {
                if (job.getJobStatus().get().isTerminalState()) job.getJobExecutionResult().get();
                assertTrue(System.nanoTime() < deadline, "job did not start");
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
              }
              // Let the empty source poll and checkpoints run before publishing benchmark files.
              LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500));
              double startup = (System.nanoTime() - submitted) / 1e9;
              long start = System.nanoTime();
              try (var commit = target.newCommit("publish")) {
                commit.commit(
                    0,
                    List.of(
                        new CommitMessageImpl(
                            BinaryRow.EMPTY_ROW,
                            0,
                            1,
                            new DataIncrement(files, List.of(), List.of()),
                            new CompactIncrement(List.of(), List.of(), List.of()))));
              }
              for (; ; ) {
                var snapshot = target.snapshotManager().latestSnapshot();
                if (snapshot != null && snapshot.commitKind() == Snapshot.CommitKind.COMPACT) break;
                if (job.getJobStatus().get().isTerminalState()) job.getJobExecutionResult().get();
                assertTrue(System.nanoTime() < deadline, "compaction did not commit");
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
              }
              double seconds = (System.nanoTime() - start) / 1e9;
              assertEquals(expected, PaimonTestTables.readRows(target, target.rowType()));
              if (engine.equals("native")) NativeAppendSinkWriteTest.assertNativeFiles(target);
              System.out.printf(
                  "PAIMON_COMPACTION_JOB shape=%s rows=%d engine=%s trial=%d seconds=%.6f"
                      + " startup_s=%.6f%n",
                  shape, rows, engine, trial, seconds, startup);
            } finally {
              job.cancel().get();
              long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
              while (!job.getJobStatus().get().isTerminalState()) {
                assertTrue(System.nanoTime() < deadline, "compaction job did not cancel");
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
              }
            }
            assertEquals("", NativePaimon.liveNativeHandles());
          }
        }
      }
    } finally {
      if (originalMode == null) System.clearProperty("streamfusion.paimon.compaction.mode");
      else System.setProperty("streamfusion.paimon.compaction.mode", originalMode);
    }
  }

  private FileStoreTable emptyTarget(FileStoreTable input, String name) throws Exception {
    var path = new Path(directory.resolve(name).toUri());
    var io = LocalFileIO.create();
    var options = new java.util.HashMap<>(input.options());
    options.remove("path");
    options.putAll(Map.of("write-only", "false", "continuous.discovery-interval", "50 ms"));
    PaimonTestCompat.schemaManager(io, path)
        .createTable(
            new Schema(
                input.schema().fields(), input.partitionKeys(), input.primaryKeys(), options, ""));
    return FileStoreTableFactory.create(io, path);
  }
}
