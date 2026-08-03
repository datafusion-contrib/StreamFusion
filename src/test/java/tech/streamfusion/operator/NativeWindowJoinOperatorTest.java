package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class NativeWindowJoinOperatorTest {

  private static final int MAX_PARALLELISM = 128;

  private static final RowType INPUT =
      RowType.of(
          new LogicalType[] {
            new BigIntType(),
            new BigIntType(),
            new LocalZonedTimestampType(3),
            new LocalZonedTimestampType(3)
          },
          new String[] {"k", "v", "window_start", "window_end"});

  @Test
  void rawKeyedProctimeWindowJoinRearmsTimerAfterRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> before =
        harness()) {
      before.setup(new ArrowBatchSerializer());
      before.open();
      before.setProcessingTime(500);
      before.processElement1(new StreamRecord<>(batch(row(1, 10, 0, 1000))));
      before.processElement2(new StreamRecord<>(batch(row(1, 100, 0, 1000))));
      snapshot = before.snapshot(1L, 1L);
      collectPairs(before);
    }

    try (KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> restored =
        harness()) {
      restored.setup(new ArrowBatchSerializer());
      restored.initializeState(snapshot);
      restored.open();
      // Both inputs are already buffered. The restored timer closes their window without new input.
      restored.setProcessingTime(1000);
      assertEquals(List.of(List.of(1L, 10L, 1L, 100L)), collectPairs(restored));
    }
  }

  private static KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
      harness() throws Exception {
    return new KeyedTwoInputStreamOperatorTestHarness<>(
        new NativeWindowJoinOperator(
            new int[] {0},
            new int[] {0},
            2,
            3,
            2,
            3,
            0,
            INPUT,
            INPUT,
            EncodedPredicate.NONE,
            true,
            1000,
            1000,
            false,
            new int[] {-1},
            MAX_PARALLELISM),
        batch -> 0,
        batch -> 0,
        Types.INT,
        MAX_PARALLELISM,
        1,
        0);
  }

  private static ArrowBatch batch(RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), INPUT, NativeAllocator.SHARED));
  }

  private static RowData row(long key, long value, long start, long end) {
    GenericRowData row = new GenericRowData(4);
    row.setField(0, key);
    row.setField(1, value);
    row.setField(2, TimestampData.fromEpochMillis(start));
    row.setField(3, TimestampData.fromEpochMillis(end));
    return row;
  }

  private static List<List<Long>> collectPairs(
      KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, outputType())) {
            rows.add(List.of(row.getLong(0), row.getLong(1), row.getLong(4), row.getLong(5)));
          }
        }
      }
    }
    return rows;
  }

  private static RowType outputType() {
    return RowType.of(
        new LogicalType[] {
          new BigIntType(),
          new BigIntType(),
          new LocalZonedTimestampType(3),
          new LocalZonedTimestampType(3),
          new BigIntType(),
          new BigIntType(),
          new LocalZonedTimestampType(3),
          new LocalZonedTimestampType(3)
        },
        new String[] {
          "lk", "lv", "left_start", "left_end", "rk", "rv", "right_start", "right_end"
        });
  }
}
