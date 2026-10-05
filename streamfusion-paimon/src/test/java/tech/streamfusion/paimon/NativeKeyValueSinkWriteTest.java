package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.NoopStoreSinkWriteState;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.operation.RestoreFiles;
import org.apache.paimon.operation.WriteRestore;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.RowDataArrowConverter;

/**
 * The primary-key sink write against Paimon's own: a changelog fed as routed Arrow batches over
 * several checkpoints and separate runs must leave the same rows as Paimon's row-at-a-time writer,
 * continue its sequence numbering from the committed files, and get compacted by the Paimon writer
 * it feeds its files to.
 */
class NativeKeyValueSinkWriteTest {

  private static final int BATCH_ROWS = 23;

  private static Map<String, String> options(String... keyValues) {
    Map<String, String> options = new LinkedHashMap<>();
    options.put("bucket", "2");
    for (int i = 0; i < keyValues.length; i += 2) {
      options.put(keyValues[i], keyValues[i + 1]);
    }
    return options;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void compactsNativeFilesInJobAndContinuesSequenceNumbersAcrossRuns(boolean thin)
      throws Exception {
    Map<String, String> options =
        options(
            "num-sorted-run.compaction-trigger",
            "2",
            "data-file.thin-mode",
            Boolean.toString(thin));
    FileStoreTable table =
        PaimonTestTables.createPrimaryKeyTable(Files.createTempDirectory("pk-sink"), options);
    FileStoreTable twin =
        PaimonTestTables.createPrimaryKeyTable(Files.createTempDirectory("pk-sink-twin"), options);
    List<Object[]> first = PaimonTestTables.changelog(240, 80);
    List<Object[]> second = PaimonTestTables.changelog(400, 120).subList(240, 400);

    List<CommitMessage> firstRun = runNatively(table, 1, first);
    assertEquals(List.of(), compactedFiles(firstRun), "a single run has nothing to compact");
    List<CommitMessage> secondRun = runNatively(table, 2, second);
    assertTrue(
        !compactedFiles(secondRun).isEmpty(), "Paimon's writer compacted the notified files");

    List<CommitMessage> twinFirstRun = runStock(twin, 1, first);
    List<CommitMessage> twinSecondRun = runStock(twin, 2, second);
    assertEquals(sequenceRanges(twinFirstRun), sequenceRanges(firstRun));
    assertEquals(
        sequenceRanges(twinSecondRun),
        sequenceRanges(secondRun),
        "the second run numbers its rows from the committed files like Paimon's writer");

    RowType rowType = table.rowType();
    assertEquals(
        PaimonTestTables.readRows(twin, rowType), PaimonTestTables.readRows(table, rowType));
    assertTrue(
        PaimonTestTables.dataFiles(table).values().stream()
            .flatMap(List::stream)
            .anyMatch(file -> file.level() > 0),
        "compaction left files above level 0");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void snapshotSequenceInitializationMatchesStockIncludingMigration(boolean writeOnly)
      throws Exception {
    Map<String, String> settings = options("write-only", Boolean.toString(writeOnly));
    FileStoreTable table =
        PaimonTestTables.createPrimaryKeyTable(
            Files.createTempDirectory("pk-sequence-snapshot"), settings);
    FileStoreTable twin =
        PaimonTestTables.createPrimaryKeyTable(
            Files.createTempDirectory("pk-sequence-snapshot-twin"), settings);
    List<Object[]> rows = PaimonTestTables.changelog(600, 120);
    for (int run = 0; run < 3; run++) {
      if (run == 1) {
        table = table.copy(Map.of("write.sequence-number-init-mode", "snapshot"));
        twin = twin.copy(Map.of("write.sequence-number-init-mode", "snapshot"));
      }
      List<Object[]> chunk = rows.subList(run * 200, (run + 1) * 200);
      List<CommitMessage> actual = runNatively(table, run + 1, chunk);
      List<CommitMessage> expected = runStock(twin, run + 1, chunk);
      assertEquals(sequenceRanges(expected), sequenceRanges(actual), "run " + run);
      assertEquals(
          PaimonTestTables.readRows(twin, twin.rowType()),
          PaimonTestTables.readRows(table, table.rowType()));
      if (run > 0) {
        assertEquals(
            twin.snapshotManager().latestSnapshot().properties(),
            table.snapshotManager().latestSnapshot().properties());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void restoredSequenceDoesNotRegressBehindCoordinatedFiles(boolean idleCheckpoint) throws Exception {
    var type =
        new RowType(
            List.of(
                new DataField(0, "id", DataTypes.INT().notNull()),
                new DataField(1, "v", DataTypes.BIGINT())));
    var table = PaimonMergeEngineTest.table(Map.of("bucket", "1", "write-only", "true"), type);
    var state = new PaimonChangelogSinkWriteTest.MemoryState();
    List<DataFileMeta> cachedFiles;
    try (var writer = new PaimonMergeEngineTest.Writer(table, true, state, 7)) {
      writer.write(List.of(GenericRow.of(1, 0L)));
      cachedFiles = newFiles(writer.commit(1)).stream().map(Map.Entry::getValue).toList();
      var updates = new ArrayList<InternalRow>();
      for (long value = 1; value <= 40; value++) {
        updates.add(GenericRow.of(1, value));
      }
      writer.write(updates);
      writer.commit(2);
    }
    if (idleCheckpoint) {
      try (var idle = new PaimonMergeEngineTest.Writer(table, true, state, 7)) {
        idle.writer.snapshotState();
      }
    }
    try (var restored = new PaimonMergeEngineTest.Writer(table, true, state, 7)) {
      restored.writer.setWriteRestore(
          new WriteRestore() {
            @Override
            public long latestCommittedIdentifier(String user) {
              return 2;
            }

            @Override
            public RestoreFiles restoreFiles(
                BinaryRow partition, int bucket, boolean index, boolean deletes, boolean source) {
              return new RestoreFiles(null, 1, cachedFiles, null, null, null);
            }
          });
      restored.write(List.of(GenericRow.of(1, 100L)));
      restored.commit(3);
    }
    assertEquals(List.of("1|100"), PaimonTestTables.readRows(table, type));
  }

  @Test
  void spillsTheLargestBucketOnceBuffersExceedTheWriteBufferSize() throws Exception {
    Map<String, String> options = options("write-buffer-size", "64 kb", "page-size", "4 kb");
    FileStoreTable table =
        PaimonTestTables.createPrimaryKeyTable(Files.createTempDirectory("pk-spill"), options);
    FileStoreTable twin =
        PaimonTestTables.createPrimaryKeyTable(Files.createTempDirectory("pk-spill-twin"), options);
    List<Object[]> changelog = PaimonTestTables.changelog(3_000, 500);

    List<CommitMessage> run = runNatively(table, 1, changelog);
    runStock(twin, 1, changelog);

    Map<String, Long> filesPerBucket =
        newFiles(run).stream()
            .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.counting()));
    assertTrue(filesPerBucket.values().stream().anyMatch(count -> count > 1), "a bucket spilled");
    RowType rowType = table.rowType();
    assertEquals(
        PaimonTestTables.readRows(twin, rowType), PaimonTestTables.readRows(table, rowType));
  }

  @Test
  void managedMemorySpillsAndReleasesReservationsAfterCommit() throws Exception {
    Map<String, String> settings = options("sink.use-managed-memory-allocator", "true");
    FileStoreTable table =
        PaimonTestTables.createPrimaryKeyTable(Files.createTempDirectory("pk-managed"), settings);
    FileStoreTable twin =
        PaimonTestTables.createPrimaryKeyTable(
            Files.createTempDirectory("pk-managed-twin"), settings);
    List<Object[]> changes = PaimonTestTables.changelog(3000, 500);
    List<CommitMessage> committed = runNatively(table, 1, changes);
    runStock(twin, 1, changes);
    assertTrue(
        newFiles(committed).stream()
            .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.counting()))
            .values()
            .stream()
            .anyMatch(n -> n > 1),
        "managed budget forced a bucket to flush");
    assertEquals(
        PaimonTestTables.readRows(twin, twin.rowType()),
        PaimonTestTables.readRows(table, table.rowType()));
  }

  @Test
  void writeOnlyLeavesTheFilesAtLevelZeroForADedicatedCompaction() throws Exception {
    Map<String, String> options =
        options("write-only", "true", "num-sorted-run.compaction-trigger", "2");
    FileStoreTable table =
        PaimonTestTables.createPrimaryKeyTable(Files.createTempDirectory("pk-wo"), options);
    FileStoreTable twin =
        PaimonTestTables.createPrimaryKeyTable(Files.createTempDirectory("pk-wo-twin"), options);
    List<Object[]> first = PaimonTestTables.changelog(200, 60);
    List<Object[]> second = PaimonTestTables.changelog(300, 60).subList(200, 300);

    List<CommitMessage> run = runNatively(table, 1, first);
    run.addAll(runNatively(table, 2, second));
    assertEquals(List.of(), compactedFiles(run));
    runStock(twin, 1, first);
    runStock(twin, 2, second);
    RowType rowType = table.rowType();
    assertTrue(
        PaimonTestTables.dataFiles(table).values().stream()
            .flatMap(List::stream)
            .allMatch(file -> file.level() == 0));
    assertEquals(
        PaimonTestTables.readRows(twin, rowType), PaimonTestTables.readRows(table, rowType));

    compactDedicated(table.copy(Map.of("write-only", "false")));
    assertTrue(
        PaimonTestTables.dataFiles(table).values().stream()
            .flatMap(List::stream)
            .allMatch(file -> file.level() > 0),
        "the dedicated compaction read our level-0 files");
    assertEquals(
        PaimonTestTables.readRows(twin, rowType), PaimonTestTables.readRows(table, rowType));
  }

  private static List<CommitMessage> runNatively(
      FileStoreTable table, long checkpointId, List<Object[]> changelog) throws Exception {
    CoreOptions options = table.coreOptions();
    NativeKeyValueSinkWrite write =
        new NativeKeyValueSinkWrite(
            table,
            new org.apache.paimon.flink.sink.StoreSinkWriteImpl(
                table,
                "native",
                new NoopStoreSinkWriteState(0),
                new IOManagerAsync(),
                false,
                !options.writeOnly() && options.prepareCommitWaitCompaction(),
                true,
                new MemoryPoolFactory(
                    new HeapMemorySegmentPool(options.writeBufferSize(), options.pageSize())),
                null));
    var manager =
        org.apache.flink.runtime.memory.MemoryManagerBuilder.newBuilder()
            .setMemorySize(64 * 1024)
            .setPageSize(4 * 1024)
            .build();
    if (Boolean.parseBoolean(table.options().get("sink.use-managed-memory-allocator"))) {
      write.managedMemory(manager, 64 * 1024);
    }
    StreamTableWrite router = table.newStreamWriteBuilder().withCommitUser("router").newWrite();
    Map<String, List<RowData>> pending = new LinkedHashMap<>();
    Map<String, BinaryRow> partitions = new LinkedHashMap<>();
    Map<String, Integer> buckets = new LinkedHashMap<>();
    try (BufferAllocator allocator = new RootAllocator()) {
      for (Object[] row : changelog) {
        InternalRow paimonRow = PaimonTestTables.primaryKeyPaimonRow(row);
        BinaryRow partition = router.getPartition(paimonRow).copy();
        int bucket = router.getBucket(paimonRow);
        String key = partition + "@" + bucket;
        partitions.put(key, partition);
        buckets.put(key, bucket);
        List<RowData> rows = pending.computeIfAbsent(key, k -> new ArrayList<>());
        rows.add(PaimonTestTables.primaryKeyFlinkRow(row));
        if (rows.size() == BATCH_ROWS) {
          write.writeBundle(partition, bucket, batch(allocator, rows));
          rows.clear();
        }
      }
      for (String key : pending.keySet()) {
        if (!pending.get(key).isEmpty()) {
          write.writeBundle(
              partitions.get(key), buckets.get(key), batch(allocator, pending.get(key)));
        }
      }
      router.close();
      List<CommitMessage> messages = new ArrayList<>();
      for (Committable committable : write.prepareCommit(true, checkpointId)) {
        messages.add(committable.commitMessage());
      }
      write.close();
      assertTrue(manager.verifyEmpty(), "commit and close return all native reservations");
      manager.shutdown();
      commit(table, "native", checkpointId, messages);
      return messages;
    }
  }

  private static VectorSchemaRoot batch(BufferAllocator allocator, List<RowData> rows) {
    return RowDataArrowConverter.write(
        rows, PaimonTestTables.PRIMARY_KEY_FLINK_TYPE, allocator, true);
  }

  private static List<CommitMessage> runStock(
      FileStoreTable table, long checkpointId, List<Object[]> changelog) throws Exception {
    StreamTableWrite write = table.newStreamWriteBuilder().withCommitUser("twin").newWrite();
    for (Object[] row : changelog) {
      write.write(PaimonTestTables.primaryKeyPaimonRow(row));
    }
    List<CommitMessage> messages = write.prepareCommit(true, checkpointId);
    commit(table, "twin", checkpointId, messages);
    write.close();
    return messages;
  }

  private static void compactDedicated(FileStoreTable table) throws Exception {
    StreamTableWrite write = table.newStreamWriteBuilder().withCommitUser("compactor").newWrite();
    for (Split split : table.newReadBuilder().newScan().plan().splits()) {
      DataSplit dataSplit = (DataSplit) split;
      write.compact(dataSplit.partition(), dataSplit.bucket(), true);
    }
    commit(table, "compactor", 1, write.prepareCommit(true, 1));
    write.close();
  }

  private static void commit(
      FileStoreTable table, String user, long checkpointId, List<CommitMessage> messages)
      throws Exception {
    StreamTableCommit commit = table.newStreamWriteBuilder().withCommitUser(user).newCommit();
    commit.commit(checkpointId, messages);
    commit.close();
  }

  private static List<Map.Entry<String, DataFileMeta>> newFiles(List<CommitMessage> messages) {
    List<Map.Entry<String, DataFileMeta>> files = new ArrayList<>();
    for (CommitMessage message : messages) {
      CommitMessageImpl impl = (CommitMessageImpl) message;
      for (DataFileMeta file : impl.newFilesIncrement().newFiles()) {
        files.add(Map.entry(impl.partition() + "@" + impl.bucket(), file));
      }
    }
    return files;
  }

  private static List<DataFileMeta> compactedFiles(List<CommitMessage> messages) {
    List<DataFileMeta> files = new ArrayList<>();
    for (CommitMessage message : messages) {
      files.addAll(((CommitMessageImpl) message).compactIncrement().compactAfter());
    }
    return files;
  }

  /** The sequence range of every bucket's new files, keyed by destination. */
  private static Map<String, String> sequenceRanges(List<CommitMessage> messages) {
    Map<String, long[]> ranges = new TreeMap<>();
    for (Map.Entry<String, DataFileMeta> file : newFiles(messages)) {
      long[] range =
          ranges.computeIfAbsent(file.getKey(), k -> new long[] {Long.MAX_VALUE, Long.MIN_VALUE});
      range[0] = Math.min(range[0], file.getValue().minSequenceNumber());
      range[1] = Math.max(range[1], file.getValue().maxSequenceNumber());
    }
    Map<String, String> rendered = new TreeMap<>();
    ranges.forEach((bucket, range) -> rendered.put(bucket, range[0] + ".." + range[1]));
    return rendered;
  }
}
