package tech.streamfusion.operator;

import tech.streamfusion.Native;

/** Bounded columnar sort; input demand returns control to Flink's mailbox. */
public final class NativeBatchSortOperator extends NativeBoundedBatchOperator {
  private final int[] columns;
  private final int[] flags;
  private final int batchRows;
  private final long offset;
  private final long end;

  public NativeBatchSortOperator(int[] columns, int[] flags, int batchRows) {
    this(columns, flags, batchRows, 0, -1);
  }

  public NativeBatchSortOperator(int[] columns, int[] flags, int batchRows, long offset, long end) {
    this.columns = columns.clone();
    this.flags = flags.clone();
    this.batchRows = batchRows;
    this.offset = offset;
    this.end = end;
  }

  @Override
  protected String metricPrefix() {
    return "batchSort";
  }

  @Override
  protected long createTask(long schemaAddress) {
    String[] directories =
        getContainingTask().getEnvironment().getIOManager().getSpillingDirectoriesPaths();
    long reservation =
        Math.min(64L << 20, Math.max(64L << 10, TaskOffHeapMemory.capacityBytes() / 8));
    String task = tech.streamfusion.compat.RuntimeCompat.taskName(getRuntimeContext());
    return offset == 0 && end == -1
        ? Native.createBoundedSorter(
            schemaAddress, columns, flags, directories, batchRows, reservation, task)
        : Native.createBoundedLimitedSorter(
            schemaAddress, columns, flags, directories, batchRows, reservation, task, offset, end);
  }
}
