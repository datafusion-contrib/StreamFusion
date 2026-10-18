package tech.streamfusion.iceberg;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class NativeIcebergReaderTest {
  @TempDir Path temp;

  @org.junit.jupiter.api.Test
  void cancellationRecyclesQueuedBatchesWithoutClosingClaimedBatches() throws Exception {
    var table = fixture(temp, false);
    org.apache.iceberg.FileScanTask task;
    try (var files = table.newScan().planFiles()) {
      task = files.iterator().next();
    }
    var split =
        org.apache.iceberg.flink.source.split.IcebergSourceSplit.fromCombinedScanTask(
            new org.apache.iceberg.BaseCombinedScanTask(List.of(task)));
    var function = new IcebergArrowReaderFunction(table.schema(), List.of(1), table.io(), 7);
    try (var input = function.apply(split)) {
      var queued = input.next();
      var claimed = input.next().nextRecordFromSplit();
      try (var root = claimed.record().root()) {
        function.cancelIo();
        queued.recycle();
        assertNull(queued.nextRecordFromSplit());
        assertEquals(7, root.getRowCount());
        assertEquals(7, root.getVector("id").getObject(0));
      }
      function.cancelIo();
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
    assertEquals(0L, tech.streamfusion.operator.NativeAllocator.SHARED.getAllocatedMemory());
    assertEquals(0L, tech.streamfusion.operator.TaskOffHeapMemory.reservedBytes());
  }

  @org.junit.jupiter.api.Test
  void sourceSplitResumeUsesRowsAndFileOrdinalsAcrossDifferentBatchSizes() throws Exception {
    var table = fixture(temp, false);
    org.apache.iceberg.FileScanTask task;
    try (var files = table.newScan().planFiles()) {
      task = files.iterator().next();
    }
    var combined = new org.apache.iceberg.BaseCombinedScanTask(List.of(task, task));
    var split =
        org.apache.iceberg.flink.source.split.IcebergSourceSplit.fromCombinedScanTask(combined);
    var function = new IcebergArrowReaderFunction(table.schema(), List.of(1), table.io(), 7);
    try (var input = function.apply(split)) {
      var batch = input.next();
      assertEquals(split.splitId(), batch.nextSplit());
      var positioned = batch.nextRecordFromSplit();
      assertEquals(0, positioned.fileOffset());
      assertEquals(7, positioned.recordOffset());
      try (var root = positioned.record().root()) {
        assertEquals(7, root.getRowCount());
      }
      split.updatePosition(positioned.fileOffset(), positioned.recordOffset());
      batch.recycle();
      assertTrue(input.hasNext()); // Closing a prefetched but unemitted batch must reclaim it.
    }
    function = new IcebergArrowReaderFunction(table.schema(), List.of(1), table.io(), 13);
    int count = 7;
    try (var input = function.apply(split)) {
      while (input.hasNext()) {
        var batch = input.next();
        assertEquals(split.splitId(), batch.nextSplit());
        var positioned = batch.nextRecordFromSplit();
        int before = count;
        try (var root = positioned.record().root()) {
          for (int row = 0; row < root.getRowCount(); row++) {
            assertEquals(count % 101, root.getVector("id").getObject(row));
            count++;
          }
        }
        assertEquals(before / 101, positioned.fileOffset());
        assertEquals(count - positioned.fileOffset() * 101L, positioned.recordOffset());
        batch.recycle();
      }
    }
    assertEquals(202, count);
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @org.junit.jupiter.api.Test
  void zeroColumnProjectionRetainsDeletedRowCounts() throws Exception {
    var table = fixture(temp, true);
    int count = 0;
    try (var tasks = table.newScan().planFiles()) {
      for (var task : tasks) {
        try (var reader =
            new IcebergNativeTaskReader(task, table.schema(), List.of(), table.io(), 7, 0)) {
          org.apache.arrow.vector.VectorSchemaRoot batch;
          while ((batch = reader.next()) != null) {
            try (var root = batch) {
              assertEquals(0, root.getFieldVectors().size());
              count += root.getRowCount();
            }
          }
        }
      }
    }
    assertEquals(99, count);
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @org.junit.jupiter.api.Test
  void hostReadFailuresPropagateAndReleaseTheNativeReader() throws Exception {
    var table = fixture(temp, false);
    org.apache.iceberg.io.FileIO failing =
        new org.apache.iceberg.io.FileIO() {
          @Override
          public org.apache.iceberg.io.InputFile newInputFile(String path) {
            throw new java.io.UncheckedIOException(
                new java.io.IOException("injected host read failure"));
          }

          @Override
          public org.apache.iceberg.io.OutputFile newOutputFile(String path) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void deleteFile(String path) {
            throw new UnsupportedOperationException();
          }
        };
    try (var files = table.newScan().planFiles()) {
      for (var task : files) {
        try (var reader =
            new IcebergNativeTaskReader(task, table.schema(), List.of(1), failing, 7, 0)) {
          var failure = assertThrows(RuntimeException.class, reader::next);
          assertTrue(failure.getMessage().contains("injected host read failure"));
        }
      }
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void javaTasksReadNativelyWithProjectionDeletesAndLogicalResume(boolean deletes, boolean project)
      throws Exception {
    Table table = fixture(temp, deletes);
    var expected =
        java.util.stream.IntStream.range(0, 101)
            .filter(id -> !deletes || (id != 17 && id != 23))
            .boxed()
            .toList();
    try (var tasks = table.newScan().planFiles()) {
      for (var task : tasks) {
        List<Integer> projection = project ? List.of(2) : List.of(2, 1);
        int offset = 13;
        try (var reader =
            new IcebergNativeTaskReader(task, table.schema(), projection, table.io(), 7, offset)) {
          org.apache.arrow.vector.VectorSchemaRoot batch;
          while ((batch = reader.next()) != null) {
            try (var root = batch) {
              assertTrue(root.getRowCount() <= 7);
              assertEquals(
                  project ? List.of("payload") : List.of("payload", "id"),
                  root.getSchema().getFields().stream().map(f -> f.getName()).toList());
              for (int row = 0; row < root.getRowCount(); row++) {
                int id = expected.get(offset++);
                if (!project) assertEquals(id, root.getVector("id").getObject(row));
                Object actual = root.getVector("payload").getObject(row);
                assertEquals(
                    id % 7 == 0 ? null : "v-é🐍-" + id, actual == null ? null : actual.toString());
              }
            }
          }
        }
        assertEquals(expected.size(), offset);
        try (var reader =
            new IcebergNativeTaskReader(
                task, table.schema(), projection, table.io(), 128, expected.size())) {
          assertNull(reader.next());
        }
        try (var reader =
            new IcebergNativeTaskReader(
                task, table.schema(), projection, table.io(), 128, expected.size() + 1)) {
          assertThrows(RuntimeException.class, reader::next);
        }
      }
    }
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  @org.junit.jupiter.api.Test
  void cancellationClosesBlockedHostInputWithoutRacingTheNativeHandle() throws Exception {
    var table = fixture(temp, false);
    var entered = new java.util.concurrent.CountDownLatch(1);
    var released = new java.util.concurrent.CountDownLatch(1);
    var closes = new java.util.concurrent.atomic.AtomicInteger();
    var io =
        new org.apache.iceberg.io.FileIO() {
          @Override
          public org.apache.iceberg.io.InputFile newInputFile(String path) {
            return new org.apache.iceberg.io.InputFile() {
              public long getLength() {
                return 4096;
              }

              public boolean exists() {
                return true;
              }

              public String location() {
                return path;
              }

              public org.apache.iceberg.io.SeekableInputStream newStream() {
                return new org.apache.iceberg.io.SeekableInputStream() {
                  public long getPos() {
                    return 0;
                  }

                  public void seek(long position) {}

                  public int read() throws java.io.IOException {
                    entered.countDown();
                    try {
                      released.await();
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw new java.io.IOException(e);
                    }
                    throw new java.io.IOException("cancelled blocked host input");
                  }

                  public void close() {
                    closes.incrementAndGet();
                    released.countDown();
                  }
                };
              }
            };
          }

          public org.apache.iceberg.io.OutputFile newOutputFile(String path) {
            throw new UnsupportedOperationException();
          }

          public void deleteFile(String path) {
            throw new UnsupportedOperationException();
          }
        };
    var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
    try (var tasks = table.newScan().planFiles();
        var cancellation = new IcebergReadCancellation()) {
      var task = tasks.iterator().next();
      try (var reader =
          new IcebergNativeTaskReader(
              task, table.schema(), List.of(1), io, 7, 0, List.of(), true, null, cancellation)) {
        var blocked = executor.submit(reader::next);
        try {
          assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
          assertTimeoutPreemptively(java.time.Duration.ofSeconds(2), cancellation::close);
          var failure =
              assertThrows(
                  java.util.concurrent.ExecutionException.class,
                  () -> blocked.get(5, java.util.concurrent.TimeUnit.SECONDS));
          assertTrue(failure.getCause().getMessage().contains("cancelled blocked host input"));
        } finally {
          released.countDown();
        }
      }
    } finally {
      executor.shutdownNow();
    }
    assertEquals(1, closes.get());
    assertEquals("", NativeIceberg.liveNativeHandles());
  }

  static Table fixture(Path temp, boolean deletes) throws Exception {
    var schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "payload", Types.StringType.get()));
    var table =
        new HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                PartitionSpec.unpartitioned(),
                Map.of(TableProperties.FORMAT_VERSION, "2"),
                temp.resolve("read").toUri().toString());
    var keys = new Schema(schema.findField("id"));
    var factory =
        new GenericFileWriterFactory.Builder(table)
            .dataFileFormat(FileFormat.PARQUET)
            .deleteFileFormat(FileFormat.PARQUET)
            .equalityFieldIds(new int[] {1})
            .equalityDeleteRowSchema(keys)
            .build();
    var writer =
        factory.newDataWriter(
            table
                .encryption()
                .encrypt(table.io().newOutputFile(table.location() + "/data.parquet")),
            table.spec(),
            null);
    try (writer) {
      var record = GenericRecord.create(schema);
      for (int id = 0; id < 101; id++) {
        record.setField("id", id);
        record.setField("payload", id % 7 == 0 ? null : "v-é🐍-" + id);
        writer.write(record);
      }
    }
    table.newAppend().appendFile(writer.toDataFile()).commit();
    if (deletes) {
      var equality =
          factory.newEqualityDeleteWriter(
              table
                  .encryption()
                  .encrypt(table.io().newOutputFile(table.location() + "/eq.parquet")),
              table.spec(),
              null);
      try (equality) {
        var key = GenericRecord.create(keys);
        key.setField("id", 17);
        equality.write(key);
      }
      var position =
          factory.newPositionDeleteWriter(
              table
                  .encryption()
                  .encrypt(table.io().newOutputFile(table.location() + "/pos.parquet")),
              table.spec(),
              null);
      try (position) {
        position.write(
            PositionDelete.<org.apache.iceberg.data.Record>create()
                .set(writer.toDataFile().location(), 23));
      }
      table
          .newRowDelta()
          .addDeletes(equality.toDeleteFile())
          .addDeletes(position.toDeleteFile())
          .commit();
    }
    return table;
  }
}
