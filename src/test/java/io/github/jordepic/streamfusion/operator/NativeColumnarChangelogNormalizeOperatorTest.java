package io.github.jordepic.streamfusion.operator;

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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

/**
 * The changelog normalizer keeps the last row per key and emits the normalized changelog: the first
 * row inserts, a changed row retracts the previous then appends the new (when generateUpdateBefore),
 * an unchanged row is suppressed, and a delete retracts the stored full row. This verifies the exact
 * emitted change sequence, which the SQL harness's collapsed comparison cannot see.
 */
class NativeColumnarChangelogNormalizeOperatorTest {

  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {new BigIntType(), new BigIntType()}, new String[] {"f0", "f1"});

  @Test
  void emitsNormalizedChangelog() throws Exception {
    List<List<Object>> out =
        run(
            new NativeColumnarChangelogNormalizeOperator(new int[] {0}, true),
            row(RowKind.INSERT, 1L, 10L), // first row for key 1 → +I
            row(RowKind.UPDATE_AFTER, 1L, 20L), // changed → -U(prev) +U(new)
            row(RowKind.INSERT, 2L, 5L), // first row for key 2 → +I
            row(RowKind.UPDATE_AFTER, 2L, 5L), // unchanged → suppressed
            row(RowKind.DELETE, 1L, 20L)); // delete key 1 → -D(stored row)

    assertEquals(
        List.of(
            List.of("+I", 1L, 10L),
            List.of("-U", 1L, 10L),
            List.of("+U", 1L, 20L),
            List.of("+I", 2L, 5L),
            List.of("-D", 1L, 20L)),
        out);
  }

  @Test
  void withoutUpdateBeforeOmitsRetraction() throws Exception {
    List<List<Object>> out =
        run(
            new NativeColumnarChangelogNormalizeOperator(new int[] {0}, false),
            row(RowKind.INSERT, 1L, 10L),
            row(RowKind.UPDATE_AFTER, 1L, 20L)); // changed → +U only (no -U)

    assertEquals(List.of(List.of("+I", 1L, 10L), List.of("+U", 1L, 20L)), out);
  }

  private static GenericRowData row(RowKind kind, long f0, long f1) {
    GenericRowData row = new GenericRowData(2);
    row.setRowKind(kind);
    row.setField(0, f0);
    row.setField(1, f1);
    return row;
  }

  private static List<List<Object>> run(
      NativeColumnarChangelogNormalizeOperator operator, GenericRowData... rows) throws Exception {
    List<List<Object>> emitted = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();

      for (GenericRowData row : rows) {
        harness.processElement(
            new StreamRecord<>(
                new ArrowBatch(
                    RowDataArrowConverter.write(List.of(row), SCHEMA, allocator, true))));
      }

      while (!harness.getOutput().isEmpty()) {
        Object event = harness.getOutput().poll();
        if (event instanceof StreamRecord) {
          try (VectorSchemaRoot vsr = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
            for (RowData r : RowDataArrowConverter.read(vsr, SCHEMA)) {
              emitted.add(List.of(r.getRowKind().shortString(), r.getLong(0), r.getLong(1)));
            }
          }
        }
      }
    }
    return emitted;
  }
}
