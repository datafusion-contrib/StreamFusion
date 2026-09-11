package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.HashBucketAssigner;
import org.apache.paimon.index.HashIndexFile;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.BucketedArrowBatch;
import tech.streamfusion.operator.BucketedArrowBatchSerializer;
import tech.streamfusion.operator.RowDataArrowConverter;

class PaimonDynamicBucketRecoveryTest {
  @Test
  void restoreDiscardsUncommittedAssignmentsAndReplaysIntoPersistedBuckets() throws Exception {
    Map<String, String> options =
        Map.of(
            "bucket",
            "-1",
            "changelog-producer",
            "input",
            "dynamic-bucket.target-row-num",
            "10",
            "write-only",
            "true");
    FileStoreTable ours =
        PaimonTestTables.createPrimaryKeyTable(
            Files.createTempDirectory("dynamic-recovery-native"), options);
    FileStoreTable stock =
        PaimonTestTables.createPrimaryKeyTable(
            Files.createTempDirectory("dynamic-recovery-stock"), options);
    List<Object[]> initial = PaimonTestTables.changelog(300, 90);
    List<Object[]> next = PaimonTestTables.changelog(540, 180).subList(300, 540);
    OperatorSubtaskState saved;
    try (BufferAllocator allocator = new RootAllocator();
        var assigner = harness(ours, null);
        var writer =
            new PaimonChangelogSinkWriteTest.Writer(
                ours, true, new PaimonChangelogSinkWriteTest.MemoryState())) {
      feed(ours, assigner, writer, initial, allocator);
      assigner.prepareSnapshotPreBarrier(1);
      saved = assigner.snapshot(1, 1);
      writer.commit(true, 1);
      long committed = ours.snapshotManager().latestSnapshotId();

      feed(ours, assigner, writer, next, allocator);
      assigner.prepareSnapshotPreBarrier(2);
      // Data and hash-index files exist, but checkpoint 2 never reaches the committer.
      assertFalse(writer.write.prepareCommit(true, 2).isEmpty());
      assertEquals(committed, ours.snapshotManager().latestSnapshotId());
    }
    try (BufferAllocator allocator = new RootAllocator();
        var assigner = harness(ours, saved);
        var writer =
            new PaimonChangelogSinkWriteTest.Writer(
                ours, true, new PaimonChangelogSinkWriteTest.MemoryState())) {
      feed(ours, assigner, writer, next, allocator);
      assigner.prepareSnapshotPreBarrier(2);
      writer.commit(true, 2);
    }
    writeStock(stock, initial, 1);
    writeStock(stock, next, 2);
    assertEquals(
        PaimonTestTables.readRows(stock, stock.rowType()),
        PaimonTestTables.readRows(ours, ours.rowType()));
    assertEquals(
        PaimonChangelogSinkWriteTest.changelogRows(stock),
        PaimonChangelogSinkWriteTest.changelogRows(ours));
    assertEquals(indexes(stock), indexes(ours), "exact hash-to-bucket assignment after replay");
  }

  private static OneInputStreamOperatorTestHarness<BucketedArrowBatch, BucketedArrowBatch> harness(
      FileStoreTable table, OperatorSubtaskState restored) throws Exception {
    PaimonKeyValueLayout layout = PaimonKeyValueLayout.of(table);
    var harness =
        new OneInputStreamOperatorTestHarness<BucketedArrowBatch, BucketedArrowBatch>(
            new NativePaimonBucketAssigner(
                table,
                restored == null ? "writer" : "new-job-user",
                1,
                layout.keyColumns,
                layout.keyTimestampPrecisions),
            new BucketedArrowBatchSerializer());
    harness.setup(new BucketedArrowBatchSerializer());
    if (restored != null) {
      harness.initializeState(restored);
    }
    harness.open();
    return harness;
  }

  private static void feed(
      FileStoreTable table,
      OneInputStreamOperatorTestHarness<BucketedArrowBatch, BucketedArrowBatch> assigner,
      PaimonChangelogSinkWriteTest.Writer writer,
      List<Object[]> rows,
      BufferAllocator allocator)
      throws Exception {
    RowPartitionKeyExtractor keys = new RowPartitionKeyExtractor(table.schema());
    Map<BinaryRow, List<RowData>> partitions = new LinkedHashMap<>();
    for (Object[] row : rows) {
      BinaryRow partition = keys.partition(PaimonTestTables.primaryKeyPaimonRow(row)).copy();
      partitions
          .computeIfAbsent(partition, ignored -> new ArrayList<>())
          .add(PaimonTestTables.primaryKeyFlinkRow(row));
    }
    for (var partition : partitions.entrySet()) {
      assigner.processElement(
          new StreamRecord<>(
              new BucketedArrowBatch(
                  RowDataArrowConverter.write(
                      partition.getValue(),
                      PaimonTestTables.PRIMARY_KEY_FLINK_TYPE,
                      allocator,
                      true),
                  partition.getKey().toBytes(),
                  0)));
    }
    Object value;
    while ((value = assigner.getOutput().poll()) != null) {
      if (value instanceof StreamRecord<?>) {
        BucketedArrowBatch batch = (BucketedArrowBatch) ((StreamRecord<?>) value).getValue();
        ((NativeKeyValueSinkWrite) writer.write)
            .writeBundle(
                PaimonPartitions.fromBytes(batch.partition(), table.partitionKeys().size()),
                batch.bucket(),
                batch.root());
      }
    }
  }

  private static void writeStock(FileStoreTable table, List<Object[]> rows, long checkpoint)
      throws Exception {
    HashBucketAssigner assigner =
        new HashBucketAssigner(
            table.snapshotManager(),
            "writer",
            table.store().newIndexFileHandler(),
            1,
            1,
            0,
            10,
            -1);
    RowPartitionKeyExtractor keys = new RowPartitionKeyExtractor(table.schema());
    try (var writer =
        new PaimonChangelogSinkWriteTest.Writer(
            table, false, new PaimonChangelogSinkWriteTest.MemoryState())) {
      for (Object[] row : rows) {
        InternalRow record = PaimonTestTables.primaryKeyPaimonRow(row);
        int bucket =
            assigner.assign(keys.partition(record), keys.trimmedPrimaryKey(record).hashCode());
        writer.write.write(record, bucket);
      }
      assigner.prepareCommit(checkpoint);
      writer.commit(true, checkpoint);
    }
  }

  private static Map<String, List<Integer>> indexes(FileStoreTable table) throws Exception {
    Map<String, List<Integer>> result = new LinkedHashMap<>();
    var handler = table.store().newIndexFileHandler();
    for (var entry : handler.scan(HashIndexFile.HASH_INDEX)) {
      List<Integer> hashes =
          handler.hashIndex(entry.partition(), entry.bucket()).readList(entry.indexFile());
      hashes.sort(Integer::compareTo);
      result.put(entry.partition() + "@" + entry.bucket(), hashes);
    }
    return result;
  }
}
