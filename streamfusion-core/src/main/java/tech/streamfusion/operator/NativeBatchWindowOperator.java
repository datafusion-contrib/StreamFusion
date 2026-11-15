package tech.streamfusion.operator;

import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;

/** EOF-driven partitioned DataFusion windows over Arrow batches. */
public final class NativeBatchWindowOperator extends NativeBoundedBatchOperator {
  private final String inputJson, outputJson;
  private final int[] partition, order, flags, kinds, columns;
  private final boolean rows, whole;
  private final int batchRows;
  private final long rankStart, rankEnd;
  private final boolean outputRank;

  public NativeBatchWindowOperator(
      RowType input,
      RowType output,
      int[] partition,
      int[] order,
      int[] flags,
      int[] kinds,
      int[] columns,
      boolean rows,
      boolean whole,
      long rankStart,
      long rankEnd,
      boolean outputRank,
      int batchRows) {
    inputJson = ArrowConversion.toArrowInputSchema(input, false).toJson();
    outputJson = ArrowConversion.toArrowInputSchema(output, false).toJson();
    this.partition = partition.clone();
    this.order = order.clone();
    this.flags = flags.clone();
    this.kinds = kinds.clone();
    this.columns = columns.clone();
    this.rows = rows;
    this.whole = whole;
    this.batchRows = batchRows;
    this.rankStart = rankStart;
    this.rankEnd = rankEnd;
    this.outputRank = outputRank;
  }

  @Override
  protected String metricPrefix() {
    return "batchWindow";
  }

  @Override
  protected Schema outputSchema(Schema input) {
    return schema(outputJson);
  }

  @Override
  protected Schema emptyInputSchema() {
    return schema(inputJson);
  }

  private static Schema schema(String json) {
    try {
      return Schema.fromJSON(json);
    } catch (java.io.IOException failure) {
      throw new IllegalStateException("Invalid window schema", failure);
    }
  }

  @Override
  protected long createTask(long inputAddress) {
    try (var output = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportSchema(
          NativeAllocator.SHARED, schema(outputJson), NativeAllocator.DICTIONARIES, output);
      try {
        long reservation =
            Math.min(64L << 20, Math.max(64L << 10, TaskOffHeapMemory.capacityBytes() / 8));
        return Native.createBoundedWindow(
            inputAddress,
            output.memoryAddress(),
            partition,
            order,
            flags,
            kinds,
            columns,
            rows,
            whole,
            rankStart,
            rankEnd,
            outputRank,
            getContainingTask().getEnvironment().getIOManager().getSpillingDirectoriesPaths(),
            batchRows,
            reservation,
            tech.streamfusion.compat.RuntimeCompat.taskName(getRuntimeContext()));
      } finally {
        output.release();
      }
    }
  }
}
