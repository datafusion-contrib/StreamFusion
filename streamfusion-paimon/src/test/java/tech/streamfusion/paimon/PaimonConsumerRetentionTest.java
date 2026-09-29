package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.flink.source.ContinuousFileStoreSource;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.PendingSplitsCheckpoint;
import org.apache.paimon.flink.source.ReaderConsumeProgressEvent;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class PaimonConsumerRetentionTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void committedProgressResumesWithoutReplayingConsumedSnapshots(boolean primaryKey)
      throws Exception {
    List<List<String>> twins = new ArrayList<>();
    for (boolean nativeSource : new boolean[] {false, true}) {
      var warehouse = Files.createTempDirectory("paimon-consumer");
      var sql = environment(warehouse);
      sql.executeSql(
          "CREATE TABLE t (id INT NOT NULL, v STRING"
              + (primaryKey ? ", PRIMARY KEY(id) NOT ENFORCED" : "")
              + ") WITH ('bucket'='"
              + (primaryKey ? "2" : "-1")
              + "', 'file.format'='"
              + PaimonTestTables.fileFormat()
              + "', 'changelog-producer'='"
              + (primaryKey ? "input" : "none")
              + "', 'write-only'='true', 'consumer-id'='reader', 'consumer.mode'='at-least-once',"
              + " 'consumer.expiration-time'='1 d', 'continuous.discovery-interval'='10 ms')");
      FileStoreTable table =
          FileStoreTableFactory.create(
              LocalFileIO.create(), new Path(warehouse.resolve("default.db/t").toUri()));
      var consumers = new ConsumerManager(table.fileIO(), table.location());
      var builder = table.newStreamWriteBuilder().withCommitUser("retention-test");
      List<String> values = new ArrayList<>();
      try (var io = new IOManagerImpl(Files.createTempDirectory("consumer-writer-io").toString());
          var writer = builder.newWrite();
          var commit = builder.newCommit()) {
        writer.withIOManager(io);
        for (int phase = 0; phase < 2; phase++) {
          for (int i = phase * 40; i < (phase + 1) * 40; i++) {
            writer.write(GenericRow.of(i, i % 7 == 0 ? null : BinaryString.fromString("v" + i)));
          }
          commit.commit(phase + 1, writer.prepareCommit(true, phase + 1));
          long nextSnapshot = table.snapshotManager().latestSnapshotId() + 1;
          String query = "SELECT id, v FROM t";
          if (nativeSource)
            assertTrue(NativePlanner.explain(sql, query).contains("NativePaimonSource"));
          var scan = nativeSource ? NativePlanner.install(sql) : null;
          var result = sql.executeSql(query);
          var executor = Executors.newSingleThreadExecutor();
          try (var rows = result.collect()) {
            var collected =
                executor.submit(
                    () -> {
                      List<String> batch = new ArrayList<>();
                      while (batch.size() < 40 && rows.hasNext()) batch.add(rows.next().toString());
                      return batch;
                    });
            var batch = collected.get(45, TimeUnit.SECONDS);
            assertEquals(40, batch.size());
            values.addAll(batch);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
            while (consumers
                    .consumer("reader")
                    .map(c -> c.nextSnapshot() < nextSnapshot)
                    .orElse(true)
                && System.nanoTime() < deadline) {
              Thread.sleep(20);
            }
            assertEquals(nextSnapshot, consumers.consumer("reader").orElseThrow().nextSnapshot());
            if (scan != null) {
              assertEquals(1, scan.substitutions(), scan.explainSummary());
            }
          } finally {
            result.getJobClient().orElseThrow().cancel().get(30, TimeUnit.SECONDS);
            executor.shutdownNow();
          }
          sql = environment(warehouse);
        }
      }
      values.sort(String::compareTo);
      assertEquals(80, new HashSet<>(values).size(), "consumer restart must not replay rows");
      twins.add(values);
    }
    assertEquals(twins.get(0), twins.get(1));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void onlyCompletedCheckpointsAdvanceRetentionAcrossEnumeratorRestore(boolean nativeSource)
      throws Exception {
    var table =
        PaimonTestTables.createTable(
            Files.createTempDirectory("consumer-checkpoint"),
            Map.of(
                "bucket",
                "-1",
                "file.format",
                PaimonTestTables.fileFormat(),
                "consumer-id",
                "reader",
                "consumer.mode",
                "at-least-once",
                "consumer.expiration-time",
                "1 d"));
    var builder = table.newStreamWriteBuilder().withCommitUser("consumer-checkpoint");
    try (var writer = builder.newWrite();
        var commit = builder.newCommit()) {
      writer.write(PaimonTestTables.paimonRow(PaimonTestTables.values(1).get(0)));
      commit.commit(1, writer.prepareCommit(true, 1));
    }
    table =
        table.copy(
            Map.of(
                "snapshot.num-retained.min",
                "1",
                "snapshot.num-retained.max",
                "1",
                "snapshot.time-retained",
                "1 ms",
                "snapshot.expire.execution-mode",
                "sync"));
    long snapshot = table.snapshotManager().latestSnapshotId();
    var consumers = new ConsumerManager(table.fileIO(), table.location());
    Source<?, FileStoreSourceSplit, PendingSplitsCheckpoint> source =
        nativeSource
            ? new NativePaimonSource(table, new int[] {0}, 7, -1)
            : new ContinuousFileStoreSource(
                table.newReadBuilder().withProjection(new int[] {0}), table.options(), null);
    var context = new EnumeratorContext();
    PendingSplitsCheckpoint saved;
    try (var enumerator = source.createEnumerator(context)) {
      enumerator.start();
      context.tasks.remove().run();
      enumerator.handleSplitRequest(0, "localhost");
      assertFalse(context.assigned.isEmpty());
      saved = enumerator.snapshotState(10);
      assertTrue(consumers.consumer("reader").isEmpty());
      enumerator.handleSplitRequest(0, "localhost");
      enumerator.handleSourceEvent(0, new ReaderConsumeProgressEvent(snapshot));
      enumerator.snapshotState(11);
      assertTrue(consumers.consumer("reader").isEmpty());
      enumerator.notifyCheckpointComplete(10);
      assertEquals(
          snapshot,
          consumers.consumer("reader").orElseThrow().nextSnapshot(),
          "later uncompleted checkpoints must not advance the retained snapshot");
    }
    appendRetentionSnapshot(table, 2);
    assertTrue(
        table.snapshotManager().snapshotExists(snapshot),
        "the persisted consumer must protect its snapshot from expiration");
    var serializer = source.getEnumeratorCheckpointSerializer();
    saved = serializer.deserialize(serializer.getVersion(), serializer.serialize(saved));
    context = new EnumeratorContext();
    try (var restored = source.restoreEnumerator(context, saved)) {
      restored.start();
      context.tasks.remove().run();
      restored.snapshotState(20);
      restored.notifyCheckpointComplete(20);
      assertEquals(
          snapshot,
          consumers.consumer("reader").orElseThrow().nextSnapshot(),
          "a restored reader with unreported progress must retain its snapshot");
      restored.handleSplitRequest(0, "localhost");
      restored.handleSourceEvent(0, new ReaderConsumeProgressEvent(snapshot));
      restored.snapshotState(21);
      assertEquals(snapshot, consumers.consumer("reader").orElseThrow().nextSnapshot());
      restored.notifyCheckpointComplete(21);
      assertEquals(snapshot + 1, consumers.consumer("reader").orElseThrow().nextSnapshot());
    }
    appendRetentionSnapshot(table, 3);
    assertFalse(
        table.snapshotManager().snapshotExists(snapshot),
        "completed consumer progress must release the old snapshot for expiration");
  }

  private static void appendRetentionSnapshot(FileStoreTable table, long checkpoint)
      throws Exception {
    var builder = table.newStreamWriteBuilder().withCommitUser("retention-expiration");
    try (var writer = builder.newWrite();
        var commit = builder.newCommit()) {
      writer.write(PaimonTestTables.paimonRow(PaimonTestTables.values(1).get(0)));
      commit.commit(checkpoint, writer.prepareCommit(true, checkpoint));
    }
  }

  private static final class EnumeratorContext
      implements SplitEnumeratorContext<FileStoreSourceSplit> {
    final Queue<Runnable> tasks = new ArrayDeque<>();
    final List<FileStoreSourceSplit> assigned = new ArrayList<>();

    @Override
    public SplitEnumeratorMetricGroup metricGroup() {
      return null;
    }

    @Override
    public int currentParallelism() {
      return 1;
    }

    @Override
    public Map<Integer, ReaderInfo> registeredReaders() {
      return Map.of(0, new ReaderInfo(0, "localhost"));
    }

    @Override
    public void assignSplits(SplitsAssignment<FileStoreSourceSplit> splits) {
      assigned.addAll(splits.assignment().getOrDefault(0, List.of()));
    }

    @Override
    public void signalNoMoreSplits(int subtask) {}

    @Override
    public void sendEventToSourceReader(int subtask, SourceEvent event) {
      fail("Unexpected coordinator event: " + event);
    }

    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
      tasks.add(
          () -> {
            T result;
            try {
              result = callable.call();
            } catch (Exception e) {
              handler.accept(null, e);
              return;
            }
            handler.accept(result, null);
          });
    }

    @Override
    public <T> void callAsync(
        Callable<T> callable, BiConsumer<T, Throwable> handler, long delay, long period) {
      callAsync(callable, handler);
    }

    @Override
    public void runInCoordinatorThread(Runnable action) {
      action.run();
    }
  }

  private static StreamTableEnvironment environment(java.nio.file.Path warehouse) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    env.enableCheckpointing(100);
    var sql = StreamTableEnvironment.create(env);
    sql.executeSql(
        "CREATE CATALOG p WITH ('type'='paimon', 'warehouse'='" + warehouse.toUri() + "')");
    sql.executeSql("USE CATALOG p");
    return sql;
  }
}
