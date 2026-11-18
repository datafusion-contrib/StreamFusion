package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

/**
 * The expand operator fans each input row out to one row per grouping set — copying the grouped-in
 * columns, nulling the grouped-out ones, and stamping the per-set expand id. End-to-end coverage
 * (CUBE/ROLLUP feeding a GROUP BY) is in the SQL harness; this isolates the fan-out shape.
 */
class NativeColumnarExpandOperatorTest {

  // Input [k BIGINT, s STRING]; output [k BIGINT, s STRING, $e INT] for GROUPING SETS ((k), (s)).
  private static final RowType INPUT =
      RowType.of(new LogicalType[] {new BigIntType(), new VarCharType()}, new String[] {"k", "s"});

  private static final RowType OUTPUT =
      RowType.of(
          new LogicalType[] {new BigIntType(), new VarCharType(), new IntType()},
          new String[] {"k", "s", "$e"});

  @Test
  void fansEachRowIntoOneRowPerGroupingSet() throws Exception {
    // Projects: {k, null AS s, 1}, {null AS k, s, 2}. copyIndices flattened [row0..][row1..].
    NativeColumnarExpandOperator operator =
        new NativeColumnarExpandOperator(
            2, 3, 2, false, new int[] {0, -1, -1, -1, 1, -1}, new long[] {1L, 2L});

    // One input row (k=7, s="a") expands to [7, null, 1] and [null, "a", 2].
    assertEquals(
        List.of(Arrays.asList(7L, null, 1), Arrays.asList(null, "a", 2)), run(operator, 7L, "a"));
  }

  private static List<List<Object>> run(NativeColumnarExpandOperator operator, long k, String s)
      throws Exception {
    List<List<Object>> rows = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      GenericRowData row = new GenericRowData(2);
      row.setField(0, k);
      row.setField(1, StringData.fromString(s));
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(RowDataArrowConverter.write(List.of(row), INPUT, allocator))));

      while (!harness.getOutput().isEmpty()) {
        Object event = harness.getOutput().poll();
        if (event instanceof StreamRecord) {
          try (VectorSchemaRoot out = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
            for (RowData r : RowDataArrowConverter.read(out, OUTPUT)) {
              rows.add(
                  Arrays.asList(
                      r.isNullAt(0) ? null : r.getLong(0),
                      r.isNullAt(1) ? null : r.getString(1).toString(),
                      r.isNullAt(2) ? null : r.getInt(2)));
            }
          }
        }
      }
    }
    rows.sort(Comparator.comparing(r -> String.valueOf(r.get(2))));
    return rows;
  }
}
