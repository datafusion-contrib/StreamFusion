package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ArrowFileBatch;
import tech.streamfusion.operator.TaskOffHeapMemory;

class PaimonCompactionMemoryTest {
  @TempDir java.nio.file.Path directory;
  private static final int ROWS = 128 + 48 * 1024;

  @ParameterizedTest
  @CsvSource({
    "134217728,1024,false",
    "8388608,128,false",
    "8388608,8192,false",
    "134217728,1024,true"
  })
  void nativeWrittenWideFilesCompactUnderDefaultSortBudget(
      int blockBytes, int batchRows, boolean dictionary) throws Exception {
    var fixture = fixture(blockBytes, ROWS, dictionary);
    var table = fixture.table.copy(Map.of("read.batch-size", Integer.toString(batchRows)));
    var type = LogicalTypeConversion.toLogicalType(table.rowType());
    var input = ArrowFileBatch.schema(ArrowConversion.toArrowSchema(type));
    var names = table.rowType().getFieldNames().toArray(String[]::new);
    var estimate =
        PaimonReadPreflight.appendEstimate(
            table, fixture.files, input, names, fixture.bucketPath, batchRows);
    assertEquals(2, fixture.files.size());
    assertEquals(ROWS, fixture.files.stream().mapToLong(DataFileMeta::rowCount).sum());
    for (var file : fixture.files)
      try (var footer =
          PaimonTestCompat.parquetReader(
              table.fileIO(), new Path(fixture.bucketPath, file.fileName()), file.fileSize())) {
        assertTrue(footer.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs"));
        if (file.rowCount() > 128)
          assertEquals(blockBytes == 134217728, footer.getFooter().getBlocks().size() == 1);
      }
    assertEquals(
        blockBytes != 134217728,
        PaimonReadPreflight.rawWithinBudget(
            table, fixture.files, input, names, fixture.bucketPath, batchRows));
    assertTrue(estimate.bytes > 0);
    if (blockBytes == 134217728)
      assertTrue(estimate.bytes > table.coreOptions().sortSpillBufferSize() / 2);
    var counters = new NativePaimonCompactRewriter.Counters(new UnregisteredMetricsGroup());
    var write = ((AppendOnlyFileStore) table.store()).newWrite("rewrite");
    List<DataFileMeta> output;
    long before = TaskOffHeapMemory.reservedBytes();
    try (AutoCloseable close = write::close;
        var pressure =
            TaskOffHeapMemory.reserve(
                "test",
                "matched compact budget",
                TaskOffHeapMemory.availableBytes() - 1024L * 1024 * 1024)) {
      var rewriter =
          new NativePaimonAppendRewriter(
              table,
              write,
              fixture.partition,
              0,
              files -> {
                throw new AssertionError("Unexpected stock rewrite");
              },
              counters);
      output = rewriter.rewrite(fixture.files);
      assertNull(rewriter.fallbackReason, rewriter.admissionEvidence);
    }
    assertEquals(before, TaskOffHeapMemory.reservedBytes());
    assertEquals(1, counters.nativeGroups.getCount());
    assertEquals(0, counters.stockGroups.getCount());
    assertEquals(ROWS, counters.rows.getCount());
    long next = fixture.files.get(0).minSequenceNumber();
    for (var file : output) {
      assertEquals(next, file.minSequenceNumber());
      next += file.rowCount();
      assertEquals(next - 1, file.maxSequenceNumber());
      assertNotNull(file.valueStats());
    }
    assertEquals(fixture.files.get(1).maxSequenceNumber() + 1, next);
    assertValues(fixture, output);
    var messages =
        List.of(
            PaimonTestCompat.compactMessage(
                fixture.partition,
                new org.apache.paimon.compact.CompactResult(fixture.files, output)));
    try (var commit = table.newCommit("compact")) {
      commit.commit(3, messages);
    }
    long snapshot = table.snapshotManager().latestSnapshotId();
    // A recovered checkpoint must recognize the already committed compact transaction.
    try (var recovered = table.newCommit("compact")) {
      recovered.filterAndCommit(Map.of(3L, messages));
    }
    assertEquals(snapshot, table.snapshotManager().latestSnapshotId());
    assertEquals(
        org.apache.paimon.Snapshot.CommitKind.COMPACT,
        table.snapshotManager().latestSnapshot().commitKind());
    var reopened = FileStoreTableFactory.create(table.fileIO(), table.location());
    var committed =
        reopened.newReadBuilder().newScan().plan().splits().stream()
            .flatMap(split -> ((DataSplit) split).dataFiles().stream())
            .collect(java.util.stream.Collectors.toList());
    assertEquals(
        output.stream()
            .map(DataFileMeta::fileName)
            .sorted()
            .collect(java.util.stream.Collectors.toList()),
        committed.stream()
            .map(DataFileMeta::fileName)
            .sorted()
            .collect(java.util.stream.Collectors.toList()));
    assertValues(
        new Fixture(reopened, fixture.partition, committed, fixture.bucketPath), committed);
  }

  @Test
  void oversizedInputDelegatesTheWholeSelectedGroupBeforeOutput() throws Exception {
    var fixture = fixture(128 * 1024 * 1024);
    var table = fixture.table;
    var counters = new NativePaimonCompactRewriter.Counters(new UnregisteredMetricsGroup());
    var write = ((AppendOnlyFileStore) table.store()).newWrite("denied");
    var bucket = new Path(fixture.bucketPath);
    int filesBefore = table.fileIO().listStatus(bucket).length;
    try (AutoCloseable close = write::close;
        var pressure =
            TaskOffHeapMemory.reserve(
                "test",
                "colocated work",
                TaskOffHeapMemory.availableBytes() - 384L * 1024 * 1024)) {
      var rewriter =
          new NativePaimonAppendRewriter(
              table,
              write,
              fixture.partition,
              0,
              selected -> {
                assertSame(fixture.files, selected);
                return List.of();
              },
              counters);
      assertTrue(rewriter.rewrite(fixture.files).isEmpty());
      assertEquals("projected-row-group-budget", rewriter.fallbackReason);
      assertTrue(rewriter.admissionEvidence.contains(fixture.files.get(1).fileName()));
      assertFalse(rewriter.admissionEvidence.contains("threshold=0,"));
      assertEquals(1, counters.stockGroups.getCount());
      assertEquals(0, counters.nativeGroups.getCount());
      assertEquals(0, counters.errors.getCount());
      assertEquals(filesBefore, table.fileIO().listStatus(bucket).length);
    }
  }

  @org.junit.jupiter.params.provider.ValueSource(
      strings = {"4160 kb", "4352 kb", "5 mb", "20 mb", "1 gb"})
  @ParameterizedTest
  void reportedThresholdMatchesAtomicAdmissionAcrossStagingSizes(String budget) {
    long available = org.apache.flink.configuration.MemorySize.parse(budget).getBytes();
    try (var pressure =
        TaskOffHeapMemory.reserve(
            "test", "threshold boundary", TaskOffHeapMemory.availableBytes() - available)) {
      long threshold = PaimonCompactMemory.threshold(available, 1024 * 1024);
      try (var memory = PaimonCompactMemory.acquire(threshold, 1024 * 1024)) {
        assertNotNull(memory);
      }
      assertNull(PaimonCompactMemory.acquire(threshold + 1, 1024 * 1024));
    }
  }

  @Test
  void colocatedWorkersCannotSpendTheSameTaskBudget() throws Exception {
    long before = TaskOffHeapMemory.reservedBytes();
    try (var pressure =
        TaskOffHeapMemory.reserve(
            "test", "colocated workers", TaskOffHeapMemory.availableBytes() - 24 * 1024 * 1024)) {
      var first = PaimonCompactMemory.acquire(1024 * 1024, 4 * 1024 * 1024);
      assertNotNull(first);
      try (first) {
        assertNull(PaimonCompactMemory.acquire(1024 * 1024, 4 * 1024 * 1024));
        assertTrue(first.tryReserveReader(1024 * 1024));
        assertFalse(first.tryReserveReader(1));
        assertEquals(1024 * 1024, first.readerReservedBytes());
        first.releaseReader(1024 * 1024);
      }
      assertNull(PaimonCompactMemory.current());
      try (var second = PaimonCompactMemory.acquire(1024 * 1024, 4 * 1024 * 1024)) {
        assertNotNull(second);
      }
    }
    assertEquals(before, TaskOffHeapMemory.reservedBytes());
  }

  @Test
  void cancelledWorkersKeepTheirLeaseUntilDecoderRelease() {
    long before = TaskOffHeapMemory.reservedBytes();
    var memory = PaimonCompactMemory.acquire(1024, 1024);
    assertNotNull(memory);
    assertTrue(memory.tryReserveReader(512));
    memory.close();
    assertNull(PaimonCompactMemory.current());
    assertFalse(memory.tryReserveReader(1));
    assertTrue(TaskOffHeapMemory.reservedBytes() > before);
    memory.releaseReader(512);
    assertEquals(before, TaskOffHeapMemory.reservedBytes());
    memory.close();
  }

  @Test
  void concurrentCompactorsReserveAtomicallyBeforeOpeningWriters() throws Exception {
    long before = TaskOffHeapMemory.reservedBytes();
    var workers = java.util.concurrent.Executors.newFixedThreadPool(2);
    try (var pressure =
        TaskOffHeapMemory.reserve(
            "test", "other tasks", TaskOffHeapMemory.availableBytes() - 24 * 1024 * 1024)) {
      var start = new java.util.concurrent.CyclicBarrier(2);
      var admitted = new java.util.concurrent.CyclicBarrier(2);
      java.util.concurrent.Callable<Boolean> task =
          () -> {
            start.await(10, java.util.concurrent.TimeUnit.SECONDS);
            try (var memory = PaimonCompactMemory.acquire(1024 * 1024, 4 * 1024 * 1024)) {
              admitted.await(10, java.util.concurrent.TimeUnit.SECONDS);
              assertTrue(TaskOffHeapMemory.reservedBytes() <= TaskOffHeapMemory.capacityBytes());
              return memory != null;
            }
          };
      var first = workers.submit(task);
      var second = workers.submit(task);
      assertNotEquals(
          first.get(15, java.util.concurrent.TimeUnit.SECONDS),
          second.get(15, java.util.concurrent.TimeUnit.SECONDS));
    } finally {
      workers.shutdownNow();
      assertTrue(workers.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS));
    }
    assertEquals(before, TaskOffHeapMemory.reservedBytes());
  }

  @Test
  void compactReaderDenialPrecedesDataAndOrdinaryRawReadsStayIndependent() throws Exception {
    var fixture = fixture(8 * 1024 * 1024);
    var table = fixture.table;
    var type = LogicalTypeConversion.toLogicalType(table.rowType());
    var split =
        DataSplit.builder()
            .withPartition(fixture.partition)
            .withBucket(0)
            .withBucketPath(fixture.bucketPath)
            .withDataFiles(fixture.files)
            .isStreaming(false)
            .rawConvertible(true)
            .build();
    long before = TaskOffHeapMemory.reservedBytes();
    try (var memory = PaimonCompactMemory.acquire(1, 1024);
        var reader = PaimonRustReader.appendCompaction(table, split, type, 128, memory)) {
      assertNotNull(memory);
      var failure = assertThrows(RuntimeException.class, reader::next);
      assertTrue(failure.getMessage().contains("reserve"), failure.toString());
      assertEquals(0, memory.readerReservedBytes());
    }
    assertEquals(before, TaskOffHeapMemory.reservedBytes());
    try (var reader =
        new PaimonRustReader(
            table.copy(Map.of("sort-spill-buffer-size", "1 b")), split, type, 128, true)) {
      try (var batch = reader.next()) {
        assertNotNull(batch);
        reader.close();
        assertEquals(128, batch.getRowCount());
        assertTrue(batch.getVector(0).isNull(0));
      }
    }
    assertEquals(before, TaskOffHeapMemory.reservedBytes());
    var estimate =
        PaimonReadPreflight.appendEstimate(
            table,
            fixture.files,
            ArrowFileBatch.schema(ArrowConversion.toArrowSchema(type)),
            table.rowType().getFieldNames().toArray(String[]::new),
            fixture.bucketPath,
            128);
    try (var occupied = PaimonCompactMemory.acquire(estimate.bytes, 1024)) {
      assertNotNull(occupied);
      assertTrue(occupied.tryReserveReader(estimate.bytes));
      try (var reader = PaimonRustReader.appendCompaction(table, split, type, 128, occupied)) {
        var failure = assertThrows(RuntimeException.class, reader::next);
        assertTrue(failure.getMessage().contains("denied"), failure.toString());
        assertEquals(0, reader.reservedBytes());
        assertEquals(estimate.bytes, occupied.readerReservedBytes());
      } finally {
        occupied.releaseReader(estimate.bytes);
      }
    }
    assertEquals(before, TaskOffHeapMemory.reservedBytes());
    var memory = PaimonCompactMemory.acquire(estimate.bytes, 1024);
    assertNotNull(memory);
    var compact = PaimonRustReader.appendCompaction(table, split, type, 128, memory);
    try (var batch = compact.next()) {
      assertNotNull(batch);
      assertTrue(compact.reservedBytes() > 0);
      assertEquals(compact.reservedBytes(), memory.readerReservedBytes());
      compact.close();
      memory.close();
      assertTrue(
          TaskOffHeapMemory.reservedBytes() > before,
          "exported Arrow buffers retain their independent task reservation");
      assertTrue(batch.getVector(0).isNull(0));
    } finally {
      compact.close();
      memory.close();
    }
    assertEquals(before, TaskOffHeapMemory.reservedBytes());
  }

  Fixture fixture(int blockBytes) throws Exception {
    return fixture(blockBytes, ROWS);
  }

  Fixture fixture(int blockBytes, int totalRows) throws Exception {
    return fixture(blockBytes, totalRows, false);
  }

  private Fixture fixture(int blockBytes, int totalRows, boolean dictionary) throws Exception {
    return fixture(blockBytes, totalRows, dictionary, LocalFileIO.create());
  }

  Fixture fixture(int blockBytes, int totalRows, org.apache.paimon.fs.FileIO io) throws Exception {
    return fixture(blockBytes, totalRows, false, io);
  }

  private Fixture fixture(
      int blockBytes, int totalRows, boolean dictionary, org.apache.paimon.fs.FileIO io)
      throws Exception {
    var path = new Path(directory.resolve("table").toUri());
    var rowType =
        DataTypes.ROW(
            DataTypes.FIELD(0, "content", DataTypes.STRING()),
            DataTypes.FIELD(1, "ds", DataTypes.STRING()),
            DataTypes.FIELD(2, "hh", DataTypes.STRING()),
            DataTypes.FIELD(3, "mm", DataTypes.STRING()));
    PaimonTestCompat.schemaManager(io, path)
        .createTable(
            new Schema(
                rowType.getFields(),
                List.of("ds", "hh", "mm"),
                List.of(),
                Map.of(
                    "bucket",
                    "1",
                    "bucket-key",
                    "content",
                    "file.format",
                    "parquet",
                    "file.compression",
                    "zstd",
                    "write-only",
                    "true",
                    "parquet.enable.dictionary",
                    Boolean.toString(dictionary),
                    "parquet.block.size",
                    Integer.toString(blockBytes)),
                ""));
    var table = FileStoreTableFactory.create(io, path);
    var logical = LogicalTypeConversion.toLogicalType(rowType);
    var arrow = ArrowFileBatch.schema(ArrowConversion.toArrowSchema(logical));
    StoreSinkWrite.Provider plain =
        (resolved, user, state, spill, memory, metrics) ->
            new StoreSinkWriteImpl(
                resolved, user, state, spill, false, false, true, memory, metrics);
    var files = new ArrayList<DataFileMeta>();
    BinaryRow partition;
    try (var allocator = new RootAllocator();
        var disk = new IOManagerAsync(directory.resolve("spill").toString());
        var router = table.newWrite("partition");
        var commit = table.newCommit("compact")) {
      partition =
          router
              .getPartition(
                  GenericRow.of(
                      BinaryString.fromString("sample"),
                      BinaryString.fromString("20261111"),
                      BinaryString.fromString("19"),
                      BinaryString.fromString("00")))
              .copy();
      var write =
          (NativeAppendSinkWrite)
              PaimonTestCompat.provide(
                  NativeAppendSinkWrite.provider(plain),
                  table,
                  disk,
                  new UnregisteredMetricsGroup());
      try (AutoCloseable close = write::close) {
        int offset = 0;
        long checkpoint = 0;
        for (int count : new int[] {128, totalRows - 128}) {
          for (int begin = 0; begin < count; begin += 1024) {
            int size = Math.min(1024, count - begin);
            var root = VectorSchemaRoot.create(arrow, allocator);
            root.allocateNew();
            for (int row = 0; row < size; row++) {
              var content = (VarCharVector) root.getVector(0);
              String value = payload(offset + begin + row);
              if (value == null) content.setNull(row);
              else content.setSafe(row, value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
              for (int c = 1; c < 4; c++)
                ((VarCharVector) root.getVector(c))
                    .setSafe(
                        row,
                        new String[] {"", "20261111", "19", "00"}
                            [c].getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            root.setRowCount(size);
            write.writeBundle(partition, 0, root);
          }
          var messages =
              write.prepareCommit(true, ++checkpoint).stream()
                  .map(PaimonTestCompat::message)
                  .collect(java.util.stream.Collectors.toList());
          for (var message : messages)
            files.addAll(
                ((org.apache.paimon.table.sink.CommitMessageImpl) message)
                    .newFilesIncrement()
                    .newFiles());
          commit.commit(checkpoint, messages);
          offset += count;
        }
      }
    }
    return new Fixture(
        table, partition, files, table.store().pathFactory().bucketPath(partition, 0).toString());
  }

  static void assertValues(Fixture fixture, List<DataFileMeta> files) throws Exception {
    var split =
        DataSplit.builder()
            .withPartition(fixture.partition)
            .withBucket(0)
            .withBucketPath(fixture.bucketPath)
            .withDataFiles(files)
            .isStreaming(false)
            .rawConvertible(true)
            .build();
    int index = 0;
    try (var rows =
        new RecordReaderIterator<>(fixture.table.newReadBuilder().newRead().createReader(split))) {
      while (rows.hasNext()) {
        var row = rows.next();
        assertEquals(payload(index++), row.isNullAt(0) ? null : row.getString(0).toString());
        assertEquals("20261111", row.getString(1).toString());
        assertEquals("19", row.getString(2).toString());
        assertEquals("00", row.getString(3).toString());
      }
    }
    assertEquals(fixture.files.stream().mapToLong(DataFileMeta::rowCount).sum(), index);
  }

  private static String payload(int id) {
    if (id % 499 == 0) return null;
    char[] chars = new char[1024];
    long x = id + 123456789L;
    for (int i = 0; i < chars.length; i++) {
      x ^= x << 13;
      x ^= x >>> 7;
      x ^= x << 17;
      chars[i] =
          "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-".charAt((int) (x & 63));
    }
    return new String(chars);
  }

  static final class Fixture {
    final FileStoreTable table;
    final BinaryRow partition;
    final List<DataFileMeta> files;
    final String bucketPath;

    Fixture(
        FileStoreTable table, BinaryRow partition, List<DataFileMeta> files, String bucketPath) {
      this.table = table;
      this.partition = partition;
      this.files = files;
      this.bucketPath = bucketPath;
    }
  }
}
