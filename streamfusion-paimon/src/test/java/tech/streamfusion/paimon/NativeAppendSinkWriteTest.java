package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.NoopStoreSinkWriteState;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.format.parquet.ParquetUtil;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.RowDataArrowConverter;

class NativeAppendSinkWriteTest {
  private record Bucket(BinaryRow partition, int bucket) {}

  @ParameterizedTest
  @ValueSource(strings = {"0 b", "1 b", "1 gb"})
  void spillsAcrossCheckpointsRefreshAndReopenWithoutLosingRows(String diskLimit) throws Exception {
    Map<String, String> options =
        Map.of(
            "bucket",
            "4",
            "bucket-key",
            "id,ts",
            "write-only",
            "true",
            "write-buffer-size",
            "16 kb",
            "page-size",
            "4 kb",
            "write-buffer-spill.max-disk-size",
            diskLimit);
    FileStoreTable table =
        PaimonTestTables.createTable(Files.createTempDirectory("append-native"), options);
    FileStoreTable stock =
        PaimonTestTables.createTable(Files.createTempDirectory("append-stock"), options);
    List<Object[]> values = PaimonTestTables.values(1200);
    java.nio.file.Path temporary = Files.createTempDirectory("append-spill-local");
    try (RootAllocator allocator = new RootAllocator();
        IOManagerAsync io = new IOManagerAsync(temporary.toString());
        var stockWrite = stock.newStreamWriteBuilder().withCommitUser("stock").newWrite();
        var stockCommit = stock.newStreamWriteBuilder().withCommitUser("stock").newCommit();
        var nativeCommit = table.newStreamWriteBuilder().withCommitUser("native").newCommit()) {
      for (int run = 0; run < 2; run++) {
        try (NativeAppendSinkWrite write = create(table, io)) {
          for (int checkpoint = run * 2; checkpoint < run * 2 + 2; checkpoint++) {
            for (int start = checkpoint * 300; start < (checkpoint + 1) * 300; start += 60) {
              Map<Bucket, List<RowData>> routed = new LinkedHashMap<>();
              for (Object[] value : values.subList(start, start + 60)) {
                var row = PaimonTestTables.paimonRow(value);
                Bucket key =
                    new Bucket(stockWrite.getPartition(row).copy(), stockWrite.getBucket(row));
                routed
                    .computeIfAbsent(key, ignored -> new ArrayList<>())
                    .add(PaimonTestTables.flinkRow(value));
                stockWrite.write(row);
              }
              for (var entry : routed.entrySet()) {
                write.writeBundle(
                    entry.getKey().partition(),
                    entry.getKey().bucket(),
                    RowDataArrowConverter.write(
                        entry.getValue(), PaimonTestTables.FLINK_TYPE, allocator, false));
              }
            }
            if (diskLimit.equals("1 gb")) {
              try (var files = Files.walk(temporary)) {
                assertTrue(
                    files.anyMatch(Files::isRegularFile),
                    "memory pressure created an actual local spill");
              }
            }
            List<CommitMessage> messages =
                write.prepareCommit(true, checkpoint + 1).stream()
                    .map(committable -> committable.commitMessage())
                    .toList();
            nativeCommit.commit(checkpoint + 1, messages);
            stockCommit.commit(checkpoint + 1, stockWrite.prepareCommit(true, checkpoint + 1));
            write.snapshotState();
            write.replace(table.copy(Map.of("spill-compression", "lz4")));
            assertEquals(
                PaimonTestTables.readRows(stock, stock.rowType()),
                PaimonTestTables.readRows(table, table.rowType()));
          }
        }
      }
      assertNativeFiles(table);
      // Each bucket's sequence range continues monotonically, including across a restored writer.
      for (List<DataFileMeta> files : PaimonTestTables.dataFiles(table).values()) {
        List<DataFileMeta> ordered =
            files.stream()
                .sorted(java.util.Comparator.comparingLong(DataFileMeta::minSequenceNumber))
                .toList();
        long next = 0;
        for (DataFileMeta file : ordered) {
          assertEquals(next, file.minSequenceNumber());
          next += file.rowCount();
          assertEquals(next - 1, file.maxSequenceNumber());
        }
      }
    }
    try (var files = Files.walk(temporary)) {
      assertEquals(0, files.filter(Files::isRegularFile).count(), "all spill files were removed");
    }
  }

  private static NativeAppendSinkWrite create(FileStoreTable table, IOManagerAsync io)
      throws Exception {
    CoreOptions options = table.coreOptions();
    return (NativeAppendSinkWrite)
        NativeAppendSinkWrite.provider(
                (t, user, state, manager, memory, metrics) ->
                    new StoreSinkWriteImpl(
                        t, user, state, manager, false, false, true, memory, metrics))
            .provide(
                table,
                "native",
                new NoopStoreSinkWriteState(0),
                io,
                new MemoryPoolFactory(
                    new HeapMemorySegmentPool(options.writeBufferSize(), options.pageSize())),
                null);
  }

  static void assertNativeFiles(FileStoreTable table) throws Exception {
    int count = 0;
    for (var split : table.newReadBuilder().newScan().plan().splits()) {
      DataSplit data = (DataSplit) split;
      for (DataFileMeta file : data.dataFiles()) {
        try (var reader =
            ParquetUtil.getParquetReader(
                table.fileIO(),
                new Path(data.bucketPath(), file.fileName()),
                file.fileSize(),
                new Options())) {
          assertTrue(
              reader.getFooter().getFileMetaData().getCreatedBy().contains("parquet-rs"),
              "every append file must use the native encoder");
          count++;
        }
      }
    }
    assertTrue(count > 0);
  }
}
