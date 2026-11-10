package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.NativeExtensionLoader;
import tech.streamfusion.NativeMemoryLimitException;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.parquet.NativeParquetBulkFormat;

class NativeParquetSourceTest {
  private static final RowType TYPE =
      RowType.of(
          new org.apache.flink.table.types.logical.LogicalType[] {
            new IntType(), new VarCharType(), new DecimalType(15, 2)
          },
          new String[] {"id", "text", "value"});
  @TempDir java.nio.file.Path directory;

  @AfterEach
  void noLeakedNativeBuffersOrDecoderHandles() {
    assertEquals(0, NativeAllocator.SHARED.getAllocatedMemory());
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
    assertEquals("", NativeExtensionLoader.liveNativeHandles());
    TaskOffHeapMemory.resetForTests(-1);
  }

  @Test
  void projectionRestorationAndLogicalPositionsSurviveDifferentBatchSizes() throws Exception {
    var split = fixture(17);
    TaskOffHeapMemory.resetForTests(16L << 20);
    var projected =
        RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
              new DecimalType(15, 2), new IntType()
            },
            new String[] {"value", "id"});
    var format = format(projected, 4);
    assertFalse(format.isSplittable());
    long checkpoint;
    try (var reader = format.createReader(new Configuration(), split)) {
      var batch = reader.readBatch();
      var record = batch.next();
      checkpoint = record.getRecordSkipCount();
      assertEquals(4, checkpoint);
      try (var root = record.getRecord().root()) {
        assertEquals(4, root.getRowCount());
        assertEquals(new BigDecimal("-12.34"), ((DecimalVector) root.getVector(0)).getObject(0));
        assertTrue(root.getVector(0).isNull(3));
        assertEquals(3, ((IntVector) root.getVector(1)).get(3));
      }
      assertNull(batch.next());
      batch.releaseBatch();
    }
    // Restore inside a native batch with another batch size, not only at old batch boundaries.
    var restored =
        split.updateWithCheckpointedPosition(
            new CheckpointedPosition(CheckpointedPosition.NO_OFFSET, checkpoint));
    var ids = new ArrayList<Integer>();
    try (var reader = format(projected, 7).restoreReader(new Configuration(), restored)) {
      org.apache.flink.connector.file.src.reader.BulkFormat.RecordIterator<ArrowBatch> batch;
      while ((batch = reader.readBatch()) != null) {
        try (var root = batch.next().getRecord().root()) {
          for (int i = 0; i < root.getRowCount(); i++)
            ids.add(((IntVector) root.getVector(1)).get(i));
        } finally {
          batch.releaseBatch();
        }
      }
    }
    assertEquals(
        java.util.stream.IntStream.range(4, 17)
            .boxed()
            .collect(java.util.stream.Collectors.toList()),
        ids);
  }

  @Test
  void abandonedFetchBatchAndMemoryDenialReleaseOwnership() throws Exception {
    var split = fixture(17);
    TaskOffHeapMemory.resetForTests(16L << 20);
    try (var reader = format(TYPE, 5).createReader(new Configuration(), split)) {
      var batch = reader.readBatch();
      assertNotNull(batch);
      batch.releaseBatch(); // Cancellation before the task consumes a queued batch.
      batch.releaseBatch();
    }
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
    TaskOffHeapMemory.resetForTests(1);
    assertThrows(
        NativeMemoryLimitException.class,
        () -> format(TYPE, 5).createReader(new Configuration(), split));
  }

  @Test
  void nullableUtf8AndEmptyFilesRetainExactRows() throws Exception {
    TaskOffHeapMemory.resetForTests(16L << 20);
    var split = fixture(17);
    int rows = 0;
    try (var reader = format(TYPE, 5).createReader(new Configuration(), split)) {
      org.apache.flink.connector.file.src.reader.BulkFormat.RecordIterator<ArrowBatch> batch;
      while ((batch = reader.readBatch()) != null) {
        try (var root = batch.next().getRecord().root()) {
          for (int i = 0; i < root.getRowCount(); i++, rows++) {
            assertEquals(rows, ((IntVector) root.getVector(0)).get(i));
            if (rows % 4 == 3) assertTrue(root.getVector(1).isNull(i));
            else
              assertEquals(
                  "résumé-" + rows, ((VarCharVector) root.getVector(1)).getObject(i).toString());
          }
        } finally {
          batch.releaseBatch();
        }
      }
    }
    assertEquals(17, rows);
    try (var reader = format(TYPE, 5).createReader(new Configuration(), fixture(0))) {
      assertNull(reader.readBatch());
    }
  }

  @Test
  void stockSourceReaderRestoresLogicalRowsAndClosesPrefetchedBatches() throws Exception {
    var split = fixture(103);
    TaskOffHeapMemory.resetForTests(16L << 20);
    var config = new Configuration();
    config.set(
        org.apache.flink.configuration.TaskManagerOptions.TASK_OFF_HEAP_MEMORY,
        new org.apache.flink.configuration.MemorySize(16L << 20));
    var source =
        new tech.streamfusion.parquet.NativeParquetSource(
            new org.apache.flink.core.fs.Path[] {split.path()},
            ArrowConversion.toArrowInputSchema(TYPE, false).serializeAsMessage(),
            TYPE.getFieldNames().toArray(new String[0]),
            4);
    var output = new Output();
    List<FileSourceSplit> checkpoint;
    try (var reader = source.createReader(context(config))) {
      reader.addSplits(List.of(split));
      reader.notifyNoMoreSplits();
      reader.start();
      while (output.rows.isEmpty()) {
        reader.pollNext(output);
        if (output.rows.isEmpty())
          reader.isAvailable().get(10, java.util.concurrent.TimeUnit.SECONDS);
      }
      checkpoint = reader.snapshotState(1);
      assertEquals(4, checkpoint.get(0).getReaderPosition().orElseThrow().getRecordsAfterOffset());
    }
    assertEquals(List.of(0, 1, 2, 3), output.rows);
    assertEquals(
        0, TaskOffHeapMemory.reservedBytes(), "cancelled prefetched batches must close promptly");
    try (var reader = source.createReader(context(config))) {
      reader.addSplits(checkpoint);
      reader.notifyNoMoreSplits();
      reader.start();
      while (reader.pollNext(output) != org.apache.flink.core.io.InputStatus.END_OF_INPUT)
        reader.isAvailable().get(10, java.util.concurrent.TimeUnit.SECONDS);
    }
    assertEquals(
        java.util.stream.IntStream.range(0, 103)
            .boxed()
            .collect(java.util.stream.Collectors.toList()),
        output.rows);
  }

  @Test
  void decoderWorkspaceOnlyChargesProjectedColumnChunks() throws Exception {
    var split = fixture(17);
    TaskOffHeapMemory.resetForTests(16L << 20);
    long full;
    try (var reader = format(TYPE, 4).createReader(new Configuration(), split)) {
      full = TaskOffHeapMemory.reservedBytes();
      assertTrue(full > 0);
    }
    var narrow =
        RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {new IntType()},
            new String[] {"id"});
    try (var reader = format(narrow, 4).createReader(new Configuration(), split)) {
      assertTrue(
          TaskOffHeapMemory.reservedBytes() < full,
          "an id-only scan must not reserve string and decimal decoder chunks");
    }
  }

  private static org.apache.flink.api.connector.source.SourceReaderContext context(
      Configuration config) {
    return new org.apache.flink.api.connector.source.SourceReaderContext() {
      public org.apache.flink.metrics.groups.SourceReaderMetricGroup metricGroup() {
        return org.apache.flink.metrics.groups.UnregisteredMetricsGroup
            .createSourceReaderMetricGroup();
      }

      public Configuration getConfiguration() {
        return config;
      }

      public String getLocalHostName() {
        return "localhost";
      }

      public int getIndexOfSubtask() {
        return 0;
      }

      public void sendSplitRequest() {}

      public void sendSourceEventToCoordinator(
          org.apache.flink.api.connector.source.SourceEvent event) {}

      public org.apache.flink.util.UserCodeClassLoader getUserCodeClassLoader() {
        return null;
      }
    };
  }

  private static final class Output
      implements org.apache.flink.api.connector.source.ReaderOutput<ArrowBatch> {
    final List<Integer> rows = new ArrayList<>();

    public void collect(ArrowBatch batch) {
      try (var root = batch.root()) {
        for (int row = 0; row < root.getRowCount(); row++)
          rows.add(((IntVector) root.getVector(0)).get(row));
      }
    }

    public void collect(ArrowBatch batch, long timestamp) {
      collect(batch);
    }

    public void emitWatermark(org.apache.flink.api.common.eventtime.Watermark watermark) {}

    public void markIdle() {}

    public void markActive() {}

    public org.apache.flink.api.connector.source.SourceOutput<ArrowBatch> createOutputForSplit(
        String split) {
      return this;
    }

    public void releaseOutputForSplit(String split) {}
  }

  private NativeParquetBulkFormat format(RowType type, int rows) {
    return new NativeParquetBulkFormat(
        ArrowConversion.toArrowInputSchema(type, false).serializeAsMessage(),
        type.getFieldNames().toArray(new String[0]),
        rows,
        null);
  }

  private FileSourceSplit fixture(int count) throws Exception {
    var file = directory.resolve("rows-" + count + ".parquet");
    var path = new org.apache.flink.core.fs.Path(file.toUri());
    try (var output = path.getFileSystem().create(path, WriteMode.NO_OVERWRITE)) {
      var writer =
          ParquetRowDataBuilder.createWriterFactory(
                  TYPE, new org.apache.hadoop.conf.Configuration(false), false)
              .create(output);
      for (int i = 0; i < count; i++)
        writer.addElement(
            GenericRowData.of(
                i,
                i % 4 == 3 ? null : StringData.fromString("résumé-" + i),
                i % 4 == 3 ? null : DecimalData.fromBigDecimal(new BigDecimal("-12.34"), 15, 2)));
      writer.finish();
    }
    long bytes = Files.size(file);
    return new FileSourceSplit("file", path, 0, bytes, 0, bytes);
  }
}
