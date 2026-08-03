package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class NativeFilterOperatorTest {

  private static final RowType SCHEMA =
      RowType.of(new LogicalType[] {new BigIntType(), new IntType()}, new String[] {"k", "v"});

  private static RowData row(long k, int v) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, k);
    row.setField(1, v);
    return row;
  }

  @Test
  void filtersABatchKeepingSurvivors() throws Exception {
    // Predicate v > 20 (column index 1, int literal): CALL gt ( INPUT_REF 1, LIT_INT 20 ).
    NativeFilterOperator operator =
        new NativeFilterOperator(
            new int[] {0, 1}, // identity projection
            new int[] {6, 0, 7},
            new int[] {10, 1, 0},
            new int[] {2, 0, 0},
            new long[] {20},
            new double[] {},
            new String[] {},
            NativeUdf.Binding.EMPTY);
    List<RowData> output = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      VectorSchemaRoot in =
          RowDataArrowConverter.write(List.of(row(1L, 10), row(2L, 30), row(3L, 20)), SCHEMA, allocator);
      harness.processElement(new StreamRecord<>(new ArrowBatch(in)));

      for (Object record : harness.getOutput()) {
        if (record instanceof StreamRecord) {
          ArrowBatch batch = ((StreamRecord<ArrowBatch>) record).getValue();
          try (VectorSchemaRoot root = batch.root()) {
            output.addAll(RowDataArrowConverter.read(root, SCHEMA));
          }
        }
      }
    }
    assertEquals(1, output.size(), "only v=30 survives v>20");
    assertEquals(30, output.get(0).getInt(1));
  }
}
