package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class TransposeOperatorsTest {

  private static final RowType SCHEMA =
      RowType.of(new LogicalType[] {new BigIntType(), new IntType()}, new String[] {"k", "v"});

  private static RowData row(long k, int v) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, k);
    row.setField(1, v);
    return row;
  }

  @SuppressWarnings("unchecked")
  private static <T> List<T> values(OneInputStreamOperatorTestHarness<?, T> harness) {
    List<T> out = new ArrayList<>();
    for (Object record : harness.getOutput()) {
      if (record instanceof StreamRecord) {
        out.add((T) ((StreamRecord<T>) record).getValue());
      }
    }
    return out;
  }

  @Test
  void rowsTransposeToBatchesAndBack() throws Exception {
    List<RowData> input = List.of(row(1L, 10), row(2L, 20), row(3L, 30), row(4L, 40), row(5L, 50));

    // Declared first so it closes last: its allocator outlives the batches the second harness frees.
    try (OneInputStreamOperatorTestHarness<RowData, ArrowBatch> toArrow =
            new OneInputStreamOperatorTestHarness<>(new RowDataToArrowOperator(SCHEMA, 2, false));
        OneInputStreamOperatorTestHarness<ArrowBatch, RowData> toRows =
            new OneInputStreamOperatorTestHarness<>(
                new ArrowToRowDataOperator(SCHEMA), new ArrowBatchSerializer())) {
      // The harness copies records by serializer; tell it to use the batch serializer (identity copy)
      // for the ArrowBatch edges rather than falling back to Kryo, which cannot copy an Arrow batch.
      toArrow.setup(new ArrowBatchSerializer());
      // ArrowToRowData emits a reused, lazy ColumnarRowData valid only until the batch closes; like a
      // real downstream, the harness must copy each emitted row on arrival — give it a RowData
      // serializer so getOutput() holds independent rows rather than aliases of freed buffers.
      toRows.setup(new RowDataSerializer(SCHEMA));
      toArrow.open();
      toRows.open();
      for (RowData r : input) {
        toArrow.processElement(new StreamRecord<>(r));
      }
      toArrow.endInput();

      // Batch size 2 over 5 rows → batches of 2, 2, 1.
      List<ArrowBatch> batches = values(toArrow);
      assertEquals(3, batches.size());
      assertEquals(5, batches.stream().mapToInt(ArrowBatch::rowCount).sum());

      for (ArrowBatch batch : batches) {
        toRows.processElement(new StreamRecord<>(batch));
      }

      List<RowData> output = values(toRows);
      assertEquals(input.size(), output.size());
      for (int i = 0; i < input.size(); i++) {
        assertEquals(input.get(i).getLong(0), output.get(i).getLong(0), "k row " + i);
        assertEquals(input.get(i).getInt(1), output.get(i).getInt(1), "v row " + i);
      }
    }
  }
}
