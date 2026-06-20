package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class NativeParquetSinkOperatorTest {

  private static final RowType SCHEMA =
      RowType.of(new LogicalType[] {new BigIntType(), new IntType()}, new String[] {"k", "v"});

  private static RowData row(long k, int v) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, k);
    row.setField(1, v);
    return row;
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    VectorSchemaRoot root = RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator);
    return new ArrowBatch(root);
  }

  private static File[] visibleFiles(Path directory) {
    File[] files = directory.toFile().listFiles((dir, name) -> name.endsWith(".parquet"));
    return files == null ? new File[0] : files;
  }

  @Test
  void commitsFilesOnlyWhenTheCheckpointCompletes() throws Exception {
    Path directory = Files.createTempDirectory("streamfusion-sink");
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, Object> harness =
            new OneInputStreamOperatorTestHarness<>(
                new NativeParquetSinkOperator(directory.toString()), new ArrowBatchSerializer())) {
      harness.setup();
      harness.open();
      // Three batches → three in-progress files, but nothing visible before a checkpoint.
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10), row(2, 20))));
      harness.processElement(new StreamRecord<>(batch(allocator, row(3, 30), row(4, 40))));
      harness.processElement(new StreamRecord<>(batch(allocator, row(5, 50))));
      assertEquals(0, visibleFiles(directory).length, "nothing visible before checkpoint");

      harness.snapshot(1L, 1L);
      assertEquals(0, visibleFiles(directory).length, "still nothing visible at snapshot");

      harness.notifyOfCompletedCheckpoint(1L);
      assertEquals(3, visibleFiles(directory).length, "all recorded files committed on completion");
      for (File file : visibleFiles(directory)) {
        assertTrue(file.length() > 0, "committed parquet file should be non-empty");
      }
    }
  }

  @Test
  void commitsPendingFilesOnRecovery() throws Exception {
    Path directory = Files.createTempDirectory("streamfusion-sink");
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, Object> harness =
            new OneInputStreamOperatorTestHarness<>(
                new NativeParquetSinkOperator(directory.toString()), new ArrowBatchSerializer())) {
      harness.setup();
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, row(1, 10), row(2, 20))));
      harness.processElement(new StreamRecord<>(batch(allocator, row(3, 30))));
      snapshot = harness.snapshot(1L, 1L); // records the in-progress files; no completion follows
    }
    assertEquals(0, visibleFiles(directory).length, "a crash before completion leaves nothing visible");

    // A fresh operator restored from the snapshot commits the files the checkpoint recorded.
    try (OneInputStreamOperatorTestHarness<ArrowBatch, Object> restored =
        new OneInputStreamOperatorTestHarness<>(
            new NativeParquetSinkOperator(directory.toString()), new ArrowBatchSerializer())) {
      restored.initializeState(snapshot);
      restored.open();
      assertEquals(2, visibleFiles(directory).length, "recovered checkpoint's files are committed");
    }
  }
}
