package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NativeBatchSortOperatorTest {
  private static final RowType TYPE = RowType.of(new BigIntType(), new IntType());
  private long previousCapacity;

  @BeforeEach
  void configureMemory() {
    previousCapacity = TaskOffHeapMemory.capacityBytes();
    TaskOffHeapMemory.resetForTests(2L << 20);
  }

  @AfterEach
  void restoreMemory() throws Exception {
    for (int attempt = 0; attempt < 100 && TaskOffHeapMemory.reservedBytes() != 0; attempt++) {
      Thread.sleep(10);
    }
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
    TaskOffHeapMemory.resetForTests(previousCapacity == 0 ? -1 : previousCapacity);
  }

  @Test
  void sortsMultipleKeysAndNullsAfterEof() throws Exception {
    var rows =
        List.<RowData>of(
            GenericRowData.of(2L, 1),
            GenericRowData.of(null, 5),
            GenericRowData.of(1L, null),
            GenericRowData.of(2L, 3),
            GenericRowData.of(1L, 4));
    try (var harness = harness(new int[] {0, 1}, new int[] {2, 1}, 2)) {
      for (int offset = 0; offset < rows.size(); offset += 2) {
        push(harness, rows.subList(offset, Math.min(rows.size(), offset + 2)));
        assertTrue(harness.getOutput().isEmpty());
      }
      harness.endInput();
      var actual = drain(harness);
      assertEquals(List.of("null:5", "1:4", "1:null", "2:3", "2:1"), actual);
    }
  }

  @Test
  void sortsPastTheTaskMemoryLimitWithSpill() throws Exception {
    try (var harness = harness(new int[] {0}, new int[] {0}, 1024)) {
      int count = 262144;
      int[] expected = {1};
      var output =
          org.apache.flink.streaming.api.operators.AbstractStreamOperator.class.getDeclaredField(
              "output");
      output.setAccessible(true);
      output.set(
          harness.getOperator(),
          new tech.streamfusion.compat.FlinkOutput<ArrowBatch>(null) {
            @Override
            public void collect(StreamRecord<ArrowBatch> record) {
              try (var root = record.getValue().root()) {
                assertTrue(root.getRowCount() <= 1024);
                var key = (org.apache.arrow.vector.BigIntVector) root.getVector(0);
                var value = (org.apache.arrow.vector.IntVector) root.getVector(1);
                for (int row = 0; row < root.getRowCount(); row++) {
                  assertEquals(expected[0], key.get(row));
                  assertEquals(expected[0] % 37, value.get(row));
                  expected[0]++;
                }
              }
            }
          });
      pushDescending(harness, count);
      assertTrue(harness.getOutput().isEmpty());
      assertTrue(spillFiles(harness) > 0, "must spill before EOF");
      var statsField = NativeBatchSortOperator.class.getDeclaredField("executionStats");
      statsField.setAccessible(true);
      long[] stats = (long[]) statsField.get(harness.getOperator());
      assertTrue(
          stats[0] > 0 && stats[1] > 0, "spill metrics must report actual native spill work");
      harness.endInput();
      assertEquals(count + 1, expected[0]);
      assertEquals(0, spillFiles(harness));
    }
  }

  @Test
  void cancellationReleasesRetainedInput() throws Exception {
    try (var harness = harness(new int[] {0}, new int[] {0}, 1024)) {
      push(harness, List.of(GenericRowData.of(2L, 3), GenericRowData.of(1L, 4)));
      assertTrue(TaskOffHeapMemory.reservedBytes() > 0);
      assertEquals(
          0,
          NativeAllocator.SHARED.getAllocatedMemory(),
          "retained sort input must release its borrowed Java buffers");
      assertTrue(harness.getOutput().isEmpty());
    }
  }

  @Test
  void copiedOutputOutlivesItsProducerAndIsChargedOnlyToArrow() throws Exception {
    List<org.apache.arrow.vector.VectorSchemaRoot> outputs = new ArrayList<>();
    try {
      try (var harness = harness(new int[] {0}, new int[] {0}, 2)) {
        push(
            harness,
            List.of(
                GenericRowData.of(3L, 30), GenericRowData.of(1L, 10), GenericRowData.of(2L, 20)));
        harness.endInput();
        for (Object event : harness.getOutput()) {
          if (event instanceof StreamRecord) {
            outputs.add(((ArrowBatch) ((StreamRecord<?>) event).getValue()).root());
          }
        }
        harness.getOutput().clear();
      }
      assertTrue(NativeAllocator.SHARED.getAllocatedMemory() > 0);
      assertEquals(NativeAllocator.SHARED.getAllocatedMemory(), TaskOffHeapMemory.reservedBytes());
      long expected = 1;
      for (var root : outputs) {
        var keys = (org.apache.arrow.vector.BigIntVector) root.getVector(0);
        for (int row = 0; row < root.getRowCount(); row++) assertEquals(expected++, keys.get(row));
      }
      assertEquals(4, expected);
    } finally {
      outputs.forEach(org.apache.arrow.vector.VectorSchemaRoot::close);
    }
  }

  @Test
  void cancellationRemovesSpillFiles() throws Exception {
    var harness = harness(new int[] {0}, new int[] {0}, 1024);
    var directories = harness.getEnvironment().getIOManager().getSpillingDirectories();
    try (harness) {
      pushDescending(harness, 262144);
      assertTrue(spillFiles(harness) > 0);
    }
    for (var directory : directories) {
      if (!directory.exists()) continue;
      try (var paths = java.nio.file.Files.walk(directory.toPath())) {
        assertEquals(0, paths.filter(java.nio.file.Files::isRegularFile).count());
      }
    }
  }

  @Test
  void failedCreationReleasesExportedSchemaAndInput() throws Exception {
    try (var harness = harness(new int[] {99}, new int[] {0}, 1024)) {
      assertThrows(
          tech.streamfusion.NativeException.class,
          () -> push(harness, List.of(GenericRowData.of(1L, 2))));
    }
  }

  @Test
  void interruptedTaskStopsPollingAndReleasesItsPendingInput() throws Exception {
    try (var harness = harness(new int[] {0}, new int[] {0}, 1024)) {
      Thread.currentThread().interrupt();
      try {
        assertThrows(
            java.util.concurrent.CancellationException.class,
            () -> push(harness, List.of(GenericRowData.of(1L, 2))));
      } finally {
        Thread.interrupted();
      }
    }
  }

  @Test
  void emptyInputCompletesWithoutANativeHandle() throws Exception {
    try (var harness = harness(new int[] {0}, new int[] {0}, 1024)) {
      harness.endInput();
      assertTrue(harness.getOutput().isEmpty());
    }
  }

  private static OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness(
      int[] keys, int[] flags, int batchRows) throws Exception {
    var harness =
        new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(
            new NativeBatchSortOperator(keys, flags, batchRows), new ArrowBatchSerializer());
    harness.setup(new ArrowBatchSerializer());
    harness.open();
    return harness;
  }

  private static void push(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness, List<RowData> rows)
      throws Exception {
    var root = RowDataArrowConverter.write(rows, TYPE, NativeAllocator.SHARED, false);
    harness.processElement(new StreamRecord<>(new ArrowBatch(root)));
  }

  private static void pushDescending(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness, int count)
      throws Exception {
    for (int high = count; high > 0; high -= 1024) {
      List<RowData> rows = new ArrayList<>();
      for (int value = high; value > high - 1024; value--) {
        rows.add(GenericRowData.of((long) value, value % 37));
      }
      push(harness, rows);
    }
  }

  private static long spillFiles(OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness)
      throws Exception {
    long count = 0;
    for (var directory : harness.getEnvironment().getIOManager().getSpillingDirectories()) {
      try (var paths = java.nio.file.Files.walk(directory.toPath())) {
        count += paths.filter(java.nio.file.Files::isRegularFile).count();
      }
    }
    return count;
  }

  private static List<String> drain(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<String> rows = new ArrayList<>();
    for (Object event : harness.getOutput()) {
      if (!(event instanceof StreamRecord)) continue;
      var batch = (ArrowBatch) ((StreamRecord<?>) event).getValue();
      try (var root = batch.root()) {
        for (int row = 0; row < root.getRowCount(); row++) {
          rows.add(root.getVector(0).getObject(row) + ":" + root.getVector(1).getObject(row));
        }
      }
    }
    harness.getOutput().clear();
    return rows;
  }
}
