package tech.streamfusion.paimon;

import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.mergetree.compact.aggregate.FieldAggregator;
import org.apache.paimon.types.DataField;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;

/** Executes a column's specialized Paimon field operations in one C Data call per flush. */
public final class PaimonFieldAggregator {
  private final FieldAggregator aggregate;
  private final org.apache.flink.table.types.logical.RowType type;
  private final InternalRow.FieldGetter getter;

  PaimonFieldAggregator(DataField field, String function, CoreOptions options) {
    aggregate = PaimonVersion.fieldAggregator(field.type(), field.name(), function, options);
    type =
        LogicalTypeConversion.toLogicalType(
            new org.apache.paimon.types.RowType(
                List.of(new DataField(field.id(), "value", field.type().copy(true)))));
    getter = InternalRow.createFieldGetter(field.type(), 0);
  }

  /** References are input rows (-row-2), NULL (-1), or prior results (nonnegative). */
  public void merge(
      long inputArray,
      long inputSchema,
      long outputArray,
      long outputSchema,
      int[] program,
      int[] outputs) {
    try (var input =
            Data.importVectorSchemaRoot(
                NativeAllocator.SHARED,
                ArrowArray.wrap(inputArray),
                ArrowSchema.wrap(inputSchema),
                NativeAllocator.DICTIONARIES);
        var result = VectorSchemaRoot.create(input.getSchema(), NativeAllocator.SHARED)) {
      var view =
          new ColumnarRowData(
              new VectorizedColumnBatch(
                  ArrowConversion.createArrowReader(input, type).getColumnVectors()));
      var row = new FlinkRowWrapper(view);
      Object[] computed = new Object[program.length / 3];
      int[] uses = new int[computed.length];
      for (int i = 0; i < program.length; i += 3) {
        if (program[i] >= 0) uses[program[i]]++;
        if (program[i + 1] >= 0) uses[program[i + 1]]++;
      }
      for (int reference : outputs) {
        if (reference >= 0) uses[reference]++;
      }
      aggregate.reset();
      for (int i = 0; i < computed.length; i++) {
        int accumulator = program[3 * i];
        int incoming = program[3 * i + 1];
        Object a = value(accumulator, computed, view, row);
        Object b = value(incoming, computed, view, row);
        Object value =
            switch (program[3 * i + 2]) {
              case 1 -> aggregate.retract(a, b);
              case 2 -> aggregate.aggReversed(a, b);
              default -> aggregate.agg(a, b);
            };
        if (uses[i] > 0) computed[i] = value;
        release(accumulator, uses, computed);
        release(incoming, uses, computed);
      }
      var writer = ArrowConversion.createRowDataArrowWriter(result, type);
      var valueRow = new GenericRow(1);
      var flinkRow = new FlinkRowData(valueRow);
      for (int reference : outputs) {
        valueRow.setField(0, value(reference, computed, view, row));
        writer.write(flinkRow);
        release(reference, uses, computed);
      }
      writer.finish();
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED,
          result,
          NativeAllocator.DICTIONARIES,
          ArrowArray.wrap(outputArray),
          ArrowSchema.wrap(outputSchema));
    }
  }

  private Object value(int reference, Object[] computed, ColumnarRowData view, InternalRow row) {
    if (reference == -1) return null;
    if (reference >= 0) return computed[reference];
    view.setRowId(-reference - 2);
    return getter.getFieldOrNull(row);
  }

  private static void release(int reference, int[] uses, Object[] computed) {
    if (reference >= 0 && --uses[reference] == 0) computed[reference] = null;
  }
}
