package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class OrderedKeyGroupReassemblerTest {

  private static final RowType SCHEMA =
      RowType.of(new LogicalType[] {new BigIntType()}, new String[] {"v"});

  @Test
  void restoresOriginalOrderFromOutOfOrderKeyGroupFragments() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness = harness()) {
      harness.open();
      harness.processElement(new StreamRecord<>(fragment(allocator, 9, new long[] {1, 3}, new int[] {1, 3})));
      harness.processElement(new StreamRecord<>(fragment(allocator, 7, new long[] {0, 2}, new int[] {0, 2})));
      assertEquals(List.of(0L, 1L, 2L, 3L), values(harness));
    }
  }

  @Test
  void mergesSeveralInterleavedSortedKeyGroupStreams() throws Exception {
    int[] keyGroups = {7, 9, 11};
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness = harness()) {
      harness.open();
      harness.processElement(
          new StreamRecord<>(
              fragment(
                  allocator, 11, new long[] {2, 5, 8}, new int[] {2, 5, 8}, keyGroups)));
      harness.processElement(
          new StreamRecord<>(
              fragment(
                  allocator, 7, new long[] {0, 3, 6}, new int[] {0, 3, 6}, keyGroups)));
      harness.processElement(
          new StreamRecord<>(
              fragment(
                  allocator, 9, new long[] {1, 4, 7}, new int[] {1, 4, 7}, keyGroups)));
      assertEquals(List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L), values(harness));
    }
  }

  @Test
  void checkpointsAnIncompleteParentAndReplaysOldAttemptFragmentsExactlyOnce() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> before = harness()) {
      before.open();
      before.processElement(
          new StreamRecord<>(fragment(allocator, 7, new long[] {0, 2}, new int[] {0, 2})));
      snapshot = before.snapshot(1, 1);
    }
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> restored = harness()) {
      restored.initializeState(snapshot);
      restored.open();
      restored.processElement(
          new StreamRecord<>(fragment(allocator, 9, new long[] {1, 3}, new int[] {1, 3})));
      assertEquals(List.of(0L, 1L, 2L, 3L), values(restored).stream().sorted().toList());
    }
  }

  @Test
  void holdsAWatermarkUntilTheParentIsComplete() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness = harness()) {
      harness.open();
      harness.processElement(
          new StreamRecord<>(fragment(allocator, 7, new long[] {0, 2}, new int[] {0, 2})));
      harness.processWatermark(new Watermark(42));
      assertEquals(0, harness.getOutput().size());
      harness.processElement(
          new StreamRecord<>(fragment(allocator, 9, new long[] {1, 3}, new int[] {1, 3})));
      assertEquals(List.of(0L, 1L, 2L, 3L), values(harness));
      Watermark watermark =
          (Watermark) harness.getOutput().stream().skip(1).findFirst().orElseThrow();
      assertEquals(42, watermark.getTimestamp());
    }
  }

  @Test
  void restoredWatermarkWaitsForAReplayedInputWatermark() throws Exception {
    OperatorSubtaskState snapshot;
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> before = harness()) {
      before.open();
      before.processElement(
          new StreamRecord<>(fragment(allocator, 7, new long[] {0, 2}, new int[] {0, 2})));
      before.processWatermark(new Watermark(42));
      snapshot = before.snapshot(1, 1);
    }
    try (OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> restored = harness()) {
      restored.initializeState(snapshot);
      restored.open();
      assertEquals(1, restored.getOutput().size(), "restore must not overtake replayed channel data");
      restored.processWatermark(new Watermark(50));
      assertEquals(2, restored.getOutput().size());
      Watermark watermark =
          (Watermark) restored.getOutput().stream().skip(1).findFirst().orElseThrow();
      assertEquals(50, watermark.getTimestamp());
    }
  }

  @Test
  void rejectsEndOfInputWithAnIncompleteLiveParent() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness = harness()) {
      harness.open();
      harness.processElement(
          new StreamRecord<>(fragment(allocator, 7, new long[] {0, 2}, new int[] {0, 2})));
      assertThrows(IllegalStateException.class, harness::endInput);
    }
  }

  private static OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness()
      throws Exception {
    OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(new OrderedKeyGroupReassembler(128));
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }

  private static ArrowBatch fragment(
      BufferAllocator allocator, int keyGroup, long[] values, int[] ordinals) {
    return fragment(allocator, keyGroup, values, ordinals, new int[] {7, 9});
  }

  private static ArrowBatch fragment(
      BufferAllocator allocator,
      int keyGroup,
      long[] values,
      int[] ordinals,
      int[] parentKeyGroups) {
    List<RowData> rows = new ArrayList<>();
    for (long value : values) {
      rows.add(GenericRowData.of(value));
    }
    return new ArrowBatch(
        RowDataArrowConverter.write(rows, SCHEMA, allocator),
        keyGroup,
        ArrowBatch.NO_HANDLE_OWNER,
        null,
        1,
        2,
        3,
        ordinals,
        parentKeyGroups);
  }

  @SuppressWarnings("unchecked")
  private static List<Long> values(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<Long> values = new ArrayList<>();
    for (Object item : harness.getOutput()) {
      if (item instanceof StreamRecord<?>) {
        ArrowBatch batch = ((StreamRecord<ArrowBatch>) item).getValue();
        try (VectorSchemaRoot root = batch.root()) {
          RowDataArrowConverter.read(root, SCHEMA).stream()
              .map(row -> row.getLong(0))
              .forEach(values::add);
        }
      }
    }
    return values;
  }
}
