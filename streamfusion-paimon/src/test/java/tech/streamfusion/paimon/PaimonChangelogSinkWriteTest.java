package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.flink.sink.StoreSinkWriteState;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamTableScan;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.RowDataArrowConverter;

class PaimonChangelogSinkWriteTest {

  static Stream<Map<String, String>> modes() {
    return Stream.of(
        Map.of("changelog-producer", "input"),
        Map.of("changelog-producer", "input", "ignore-delete", "true"),
        Map.of(
            "changelog-producer",
            "input",
            "changelog-file.compression",
            "zstd",
            "changelog-file.stats-mode",
            "counts"),
        Map.of("changelog-producer", "input", "write-only", "true"),
        Map.of("changelog-producer", "lookup"),
        Map.of("changelog-producer", "lookup", "changelog-file.format", "avro"),
        Map.of("force-lookup", "true"),
        Map.of("changelog-producer", "input", "force-lookup", "true"),
        Map.of("changelog-producer", "full-compaction", "full-compaction.delta-commits", "3"),
        Map.of(
            "changelog-producer",
            "full-compaction",
            "changelog-producer.compaction-interval",
            "3 s"),
        Map.of("full-compaction.delta-commits", "3"),
        Map.of("changelog-producer", "input", "full-compaction.delta-commits", "3"),
        Map.of("deletion-vectors.enabled", "true"),
        Map.of("changelog-producer", "input", "deletion-vectors.enabled", "true"),
        Map.of("changelog-producer", "lookup", "deletion-vectors.enabled", "true"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"lookup", "full-compaction", "deletion-vectors"})
  void writeOnlyLeavesMaintenanceToTheDedicatedCompactor(String mode) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put("bucket", "2");
    options.put("write-only", "true");
    options.put(
        mode.equals("deletion-vectors") ? "deletion-vectors.enabled" : "changelog-producer",
        mode.equals("deletion-vectors") ? "true" : mode);
    List<List<String>> contents = new ArrayList<>();
    List<List<String>> changes = new ArrayList<>();
    for (boolean nativeWriter : new boolean[] {false, true}) {
      FileStoreTable table =
          PaimonTestTables.createPrimaryKeyTable(
              Files.createTempDirectory("pk-write-only-producer"), options);
      try (Writer writer = new Writer(table, nativeWriter, new MemoryState())) {
        writer.write(PaimonTestTables.changelog(240, 90));
        writer.commit(true, 1);
      }
      var entries = table.store().newScan().plan().files();
      assertFalse(entries.isEmpty());
      assertTrue(entries.stream().allMatch(e -> e.file().level() == 0));
      if (!mode.equals("deletion-vectors")) {
        assertTrue(changelogRows(table).isEmpty());
      }
      FileStoreTable compacting = table.copy(Map.of("write-only", "false"));
      try (Writer compactor = new Writer(compacting, false, new MemoryState())) {
        for (var entry : entries) {
          compactor.write.compact(entry.partition(), entry.bucket(), true);
        }
        compactor.commit(true, 2);
      }
      contents.add(PaimonTestTables.readRows(table, table.rowType()));
      if (!mode.equals("deletion-vectors")) {
        changes.add(changelogRows(table));
      }
    }
    assertFalse(contents.get(0).isEmpty());
    assertEquals(contents.get(0), contents.get(1));
    if (!changes.isEmpty()) {
      assertFalse(changes.get(0).isEmpty());
      assertEquals(changes.get(0), changes.get(1));
    }
  }

  @ParameterizedTest
  @MethodSource("modes")
  void checkpointsAndRestartsPreserveRowsAndChangelogs(Map<String, String> mode) throws Exception {
    Map<String, String> options = new HashMap<>(mode);
    options.put("bucket", "2");
    // Keep ordinary size-based compaction from obscuring the scheduled/lookup contracts.
    options.put("num-sorted-run.compaction-trigger", "100");
    options.put("num-levels", "4");
    FileStoreTable table =
        PaimonTestTables.createPrimaryKeyTable(Files.createTempDirectory("pk-native-log"), options);
    FileStoreTable twin =
        PaimonTestTables.createPrimaryKeyTable(Files.createTempDirectory("pk-stock-log"), options);
    MemoryState nativeState = new MemoryState();
    MemoryState stockState = new MemoryState();
    List<Object[]> rows = PaimonTestTables.changelog(720, 90);
    boolean sawChangelog = false;
    // Restart after checkpoint 2, leaving buckets awaiting scheduled compaction at checkpoint 3.
    for (int run = 0; run < 2; run++) {
      try (Writer ours = new Writer(table, true, nativeState);
          Writer stock = new Writer(twin, false, stockState)) {
        for (int checkpoint = run * 2 + 1; checkpoint <= run * 2 + 2; checkpoint++) {
          // Checkpoint 3 is idle: restored state must still trigger full compaction.
          List<Object[]> input =
              checkpoint == 3
                  ? List.of()
                  : rows.subList(
                      (checkpoint == 4 ? 2 : checkpoint - 1) * 240,
                      (checkpoint == 4 ? 3 : checkpoint) * 240);
          ours.write(input);
          stock.write(input);
          ours.commit(checkpoint == 4, checkpoint);
          stock.commit(checkpoint == 4, checkpoint);
          assertEquals(
              PaimonTestTables.readRows(twin, twin.rowType()),
              PaimonTestTables.readRows(table, table.rowType()),
              "rows at checkpoint " + checkpoint);
          if (table.coreOptions().changelogProducer() != CoreOptions.ChangelogProducer.NONE) {
            List<String> changes = changelogRows(table);
            assertEquals(changelogRows(twin), changes, "changelog at checkpoint " + checkpoint);
            sawChangelog |= !changes.isEmpty();
          }
          if (checkpoint == 2
              && mode.getOrDefault("changelog-producer", "").equals("full-compaction")) {
            assertTrue(
                nativeState.values.keySet().stream()
                    .anyMatch(k -> k.endsWith("paimon_written_buckets")));
            assertFalse(nativeState.values.values().stream().allMatch(List::isEmpty));
          }
        }
      }
    }
    if (table.coreOptions().changelogProducer() != CoreOptions.ChangelogProducer.NONE) {
      assertTrue(sawChangelog, "the test must observe actual changelog records");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"none", "input", "lookup"})
  void sparseUpdatesProduceDeletionVectorsAndRestoreThem(String producer) throws Exception {
    Map<String, String> options =
        Map.of(
            "bucket",
            "2",
            "changelog-producer",
            producer,
            "deletion-vectors.enabled",
            "true",
            "num-sorted-run.compaction-trigger",
            "100",
            "num-levels",
            "4");
    FileStoreTable table =
        PaimonTestTables.createPrimaryKeyTable(Files.createTempDirectory("pk-native-dv"), options);
    FileStoreTable twin =
        PaimonTestTables.createPrimaryKeyTable(Files.createTempDirectory("pk-stock-dv"), options);
    List<Object[]> rows = PaimonTestTables.changelog(4128, 4096);
    for (int run = 0; run < 2; run++) {
      try (Writer ours = new Writer(table, true, new MemoryState());
          Writer stock = new Writer(twin, false, new MemoryState())) {
        if (run == 0) {
          ours.write(rows.subList(0, 4096));
          stock.write(rows.subList(0, 4096));
          ours.commit(true, 1);
          stock.commit(true, 1);
        }
        List<Object[]> updates = rows.subList(4096 + run * 16, 4112 + run * 16);
        ours.write(updates);
        stock.write(updates);
        for (List<CommitMessage> messages :
            List.of(ours.commit(true, run + 2), stock.commit(true, run + 2))) {
          assertTrue(
              messages.stream()
                  .map(m -> (CommitMessageImpl) m)
                  .anyMatch(m -> !m.compactIncrement().newIndexFiles().isEmpty()),
              "sparse updates must produce deletion-vector index files");
        }
        assertEquals(
            PaimonTestTables.readRows(twin, twin.rowType()),
            PaimonTestTables.readRows(table, table.rowType()));
        if (!producer.equals("none")) {
          assertEquals(changelogRows(twin), changelogRows(table));
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void asynchronousLookupRestoresUncompactedBucketsWithoutNewInput(boolean deletionVectors)
      throws Exception {
    Map<String, String> options =
        Map.of(
            "bucket",
            "2",
            "changelog-producer",
            "lookup",
            "lookup-wait",
            "false",
            "deletion-vectors.enabled",
            String.valueOf(deletionVectors));
    List<List<String>> changes = new ArrayList<>();
    List<List<String>> contents = new ArrayList<>();
    for (boolean nativeWriter : new boolean[] {false, true}) {
      FileStoreTable table =
          PaimonTestTables.createPrimaryKeyTable(
              Files.createTempDirectory("pk-async-lookup"), options);
      MemoryState state = new MemoryState();
      ExecutorService executor = Executors.newSingleThreadExecutor();
      CountDownLatch blocked = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      executor.submit(
          () -> {
            blocked.countDown();
            try {
              release.await();
            } catch (InterruptedException interrupted) {
              Thread.currentThread().interrupt();
            }
          });
      blocked.await();
      try (Writer writer = new Writer(table, nativeWriter, state)) {
        writer.delegate.withCompactExecutor(executor);
        writer.write(PaimonTestTables.changelog(240, 90));
        writer.commit(false, 1);
        assertTrue(changelogRows(table).isEmpty(), "compaction is still blocked");
        assertTrue(
            state.values.entrySet().stream()
                .anyMatch(
                    e ->
                        e.getKey().endsWith("paimon_async_lookup_active_buckets")
                            && !e.getValue().isEmpty()));
      } finally {
        release.countDown();
        executor.shutdownNow();
      }
      try (Writer restored = new Writer(table, nativeWriter, state)) {
        restored.commit(true, 2);
      }
      changes.add(changelogRows(table));
      contents.add(PaimonTestTables.readRows(table, table.rowType()));
    }
    assertFalse(changes.get(0).isEmpty(), "restored lookup produced changelog without new input");
    assertEquals(changes.get(0), changes.get(1));
    assertEquals(contents.get(0), contents.get(1));
  }

  static List<String> changelogRows(FileStoreTable table) throws Exception {
    Long latest = table.snapshotManager().latestSnapshotId();
    if (latest == null) {
      return List.of();
    }
    ReadBuilder builder = table.newReadBuilder();
    StreamTableScan scan = builder.newStreamScan();
    scan.restore(1L);
    List<String> rows = new ArrayList<>();
    while (scan.checkpoint() <= latest) {
      long before = scan.checkpoint();
      try (var reader = builder.newRead().createReader(scan.plan())) {
        reader.forEachRemaining(
            row ->
                rows.add(
                    row.getRowKind().shortString()
                        + " "
                        + PaimonTestTables.render(row, table.rowType())));
      }
      assertTrue(scan.checkpoint() > before, "stream scan must advance");
    }
    rows.sort(String::compareTo);
    return rows;
  }

  static final class MemoryState implements StoreSinkWriteState {
    final Map<String, List<StateValue>> values = new HashMap<>();

    @Override
    public List<StateValue> get(String table, String key) {
      return values.get(table + "/" + key);
    }

    @Override
    public void put(String table, String key, List<StateValue> state) {
      values.put(table + "/" + key, new ArrayList<>(state));
    }

    @Override
    public void snapshotState() {}

    @Override
    public int getSubtaskId() {
      return 0;
    }
  }

  static final class Writer implements AutoCloseable {
    private final IOManagerAsync io = new IOManagerAsync();
    private final BufferAllocator allocator = new RootAllocator();
    private final int batchRows;
    private final StreamTableWrite router;
    private final StoreSinkWriteImpl delegate;
    final StoreSinkWrite write;
    private final StreamTableCommit commit;

    Writer(FileStoreTable table, boolean nativeWriter, MemoryState state) {
      this(table, nativeWriter, state, 23);
    }

    Writer(FileStoreTable table, boolean nativeWriter, MemoryState state, int batchRows) {
      this.batchRows = batchRows;
      CheckpointConfig checkpoints = new CheckpointConfig();
      checkpoints.setCheckpointInterval(1000);
      CoreOptions options = table.coreOptions();
      delegate =
          (StoreSinkWriteImpl)
              StoreSinkWrite.createWriteProvider(table, checkpoints, true, false, false)
                  .provide(
                      table,
                      "writer",
                      state,
                      io,
                      new MemoryPoolFactory(
                          new HeapMemorySegmentPool(options.writeBufferSize(), options.pageSize())),
                      null);
      write = nativeWriter ? new NativeKeyValueSinkWrite(table, delegate) : delegate;
      router = table.newStreamWriteBuilder().newWrite();
      commit = table.newStreamWriteBuilder().withCommitUser("writer").newCommit();
    }

    void write(List<Object[]> rows) throws Exception {
      Map<BinaryRow, Map<Integer, List<RowData>>> routed = new LinkedHashMap<>();
      for (Object[] row : rows) {
        InternalRow paimonRow = PaimonTestTables.primaryKeyPaimonRow(row);
        if (!(write instanceof NativeKeyValueSinkWrite)) {
          write.write(paimonRow);
          continue;
        }
        BinaryRow partition = router.getPartition(paimonRow).copy();
        int bucket = router.getBucket(paimonRow);
        List<RowData> batch =
            routed
                .computeIfAbsent(partition, p -> new LinkedHashMap<>())
                .computeIfAbsent(bucket, b -> new ArrayList<>());
        batch.add(PaimonTestTables.primaryKeyFlinkRow(row));
        if (batch.size() == batchRows) {
          writeBatch(partition, bucket, batch);
          batch.clear();
        }
      }
      for (var partition : routed.entrySet()) {
        for (var bucket : partition.getValue().entrySet()) {
          if (!bucket.getValue().isEmpty()) {
            writeBatch(partition.getKey(), bucket.getKey(), bucket.getValue());
          }
        }
      }
    }

    private void writeBatch(BinaryRow partition, int bucket, List<RowData> rows) throws Exception {
      ((NativeKeyValueSinkWrite) write)
          .writeBundle(
              partition,
              bucket,
              RowDataArrowConverter.write(
                  rows, PaimonTestTables.PRIMARY_KEY_FLINK_TYPE, allocator, true));
    }

    List<CommitMessage> commit(boolean wait, long checkpoint) throws Exception {
      List<CommitMessage> messages = new ArrayList<>();
      for (Committable committable : write.prepareCommit(wait, checkpoint)) {
        messages.add(committable.commitMessage());
      }
      write.snapshotState();
      commit.commit(checkpoint, messages);
      return messages;
    }

    @Override
    public void close() throws Exception {
      try {
        write.close();
        router.close();
        commit.close();
      } finally {
        io.close();
        allocator.close();
      }
    }
  }
}
