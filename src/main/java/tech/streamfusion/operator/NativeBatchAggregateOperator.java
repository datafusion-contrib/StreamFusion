package tech.streamfusion.operator;

import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;

/** Flink accumulator layouts expressed as bounded DataFusion aggregates and projections. */
public final class NativeBatchAggregateOperator extends NativeBoundedBatchOperator {
  private final String inputSchemaJson;
  private final String outputSchemaJson;
  private final int[] kinds;
  private final int[] columns;
  private final int[] filters;
  private final boolean merge;
  private final boolean finalOutput;
  private final int batchRows;

  public NativeBatchAggregateOperator(
      RowType input,
      RowType output,
      int[] kinds,
      int[] columns,
      int[] filters,
      boolean merge,
      boolean finalOutput,
      int batchRows) {
    inputSchemaJson = ArrowConversion.toArrowInputSchema(input, false).toJson();
    outputSchemaJson = ArrowConversion.toArrowInputSchema(output, false).toJson();
    this.kinds = kinds.clone();
    this.columns = columns.clone();
    this.filters = filters.clone();
    this.merge = merge;
    this.finalOutput = finalOutput;
    this.batchRows = batchRows;
  }

  @Override
  protected String metricPrefix() {
    return "batchAggregate";
  }

  @Override
  protected Schema outputSchema(Schema input) {
    return schema(outputSchemaJson);
  }

  @Override
  protected Schema emptyInputSchema() {
    return schema(inputSchemaJson);
  }

  private static Schema schema(String json) {
    try {
      return Schema.fromJSON(json);
    } catch (java.io.IOException failure) {
      throw new IllegalStateException("invalid aggregate schema", failure);
    }
  }

  @Override
  protected long createTask(long inputSchemaAddress) {
    try (ArrowSchema output = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportSchema(
          NativeAllocator.SHARED, schema(outputSchemaJson), NativeAllocator.DICTIONARIES, output);
      try {
        return Native.createBoundedAggregate(
            inputSchemaAddress,
            output.memoryAddress(),
            kinds,
            columns,
            filters,
            merge,
            finalOutput,
            getContainingTask().getEnvironment().getIOManager().getSpillingDirectoriesPaths(),
            batchRows,
            tech.streamfusion.compat.RuntimeCompat.taskName(getRuntimeContext()));
      } finally {
        output.release();
      }
    }
  }
}
