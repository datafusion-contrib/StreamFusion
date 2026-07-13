package io.github.jordepic.streamfusion.operator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class NativeColumnarLocalGroupAggregateOperatorTest {

  private static final RowType INPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType()},
          new String[] {"key", "value"});
  private static final RowType PARTIAL =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType()},
          new String[] {"key", "sum"});

  @Test
  void flushesAtExactCountWhenAPhysicalBatchStraddlesTheBoundary() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness = harness(2)) {
      harness.open();

      harness.processElement(
          new StreamRecord<>(batch(allocator, row(1, 1), row(1, 2), row(1, 4), row(1, 8), row(1, 16))));

      assertThat(collect(harness)).containsExactly(List.of(1L, 3L), List.of(1L, 12L));

      ((NativeColumnarLocalGroupAggregateOperator) harness.getOneInputOperator()).finish();
      assertThat(collect(harness)).containsExactly(List.of(1L, 16L));
    }
  }

  private static OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness(long size)
      throws Exception {
    NativeColumnarLocalGroupAggregateOperator operator =
        new NativeColumnarLocalGroupAggregateOperator(
            new int[] {0},
            new int[] {0},
            new int[] {1},
            new int[] {-1},
            new int[] {0},
            new int[0],
            size);
    OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(operator);
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }

  private static RowData row(long key, long value) {
    return GenericRowData.of(key, value);
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    return new ArrowBatch(RowDataArrowConverter.write(List.of(rows), INPUT, allocator, false));
  }

  private static List<List<Long>> collect(
      OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness) {
    List<List<Long>> rows = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord<?> record) {
        try (VectorSchemaRoot root = ((ArrowBatch) record.getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, PARTIAL)) {
            rows.add(List.of(row.getLong(0), row.getLong(1)));
          }
        }
      }
    }
    return rows;
  }
}
