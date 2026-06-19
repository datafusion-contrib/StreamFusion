package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
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

  private static File[] visibleFiles(Path directory) {
    File[] files = directory.toFile().listFiles((dir, name) -> name.endsWith(".parquet"));
    return files == null ? new File[0] : files;
  }

  @Test
  void commitsFilesOnlyWhenTheCheckpointCompletes() throws Exception {
    Path directory = Files.createTempDirectory("streamfusion-sink");
    NativeParquetSinkOperator operator =
        new NativeParquetSinkOperator(SCHEMA, directory.toString(), 2);
    try (OneInputStreamOperatorTestHarness<RowData, Void> harness =
        new OneInputStreamOperatorTestHarness<>(operator)) {
      harness.open();
      for (int i = 0; i < 5; i++) {
        harness.processElement(new StreamRecord<>(row(i, i * 10)));
      }
      // Two batches flushed to in-progress files, but nothing is visible before a checkpoint.
      assertEquals(0, visibleFiles(directory).length, "nothing visible before checkpoint");

      harness.snapshot(1L, 1L); // flushes the remainder and records the in-progress set
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
    try (OneInputStreamOperatorTestHarness<RowData, Void> harness =
        new OneInputStreamOperatorTestHarness<>(
            new NativeParquetSinkOperator(SCHEMA, directory.toString(), 2))) {
      harness.open();
      for (int i = 0; i < 3; i++) {
        harness.processElement(new StreamRecord<>(row(i, i * 10)));
      }
      snapshot = harness.snapshot(1L, 1L); // records the in-progress files; no completion follows
    }
    assertEquals(0, visibleFiles(directory).length, "a crash before completion leaves nothing visible");

    // A fresh operator restored from the snapshot commits the files the checkpoint recorded.
    try (OneInputStreamOperatorTestHarness<RowData, Void> restored =
        new OneInputStreamOperatorTestHarness<>(
            new NativeParquetSinkOperator(SCHEMA, directory.toString(), 2))) {
      restored.initializeState(snapshot);
      restored.open();
      assertEquals(2, visibleFiles(directory).length, "recovered checkpoint's files are committed");
    }
  }
}
