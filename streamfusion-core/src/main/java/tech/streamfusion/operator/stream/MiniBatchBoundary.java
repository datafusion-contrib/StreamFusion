package tech.streamfusion.operator.stream;

/** Owns the exact row-count boundary for a native logical mini-batch. */
@org.apache.flink.annotation.Internal
public final class MiniBatchBoundary {

  private final long maxRows;
  private long bufferedRows;

  public MiniBatchBoundary(long maxRows) {
    this.maxRows = maxRows;
  }

  /** Returns how many of the available contiguous rows fit before the next count boundary. */
  public int nextSliceLength(int availableRows) {
    if (availableRows <= 0) {
      throw new IllegalArgumentException("availableRows must be positive");
    }
    if (maxRows <= 0) {
      return availableRows;
    }
    return (int) Math.min(availableRows, maxRows - bufferedRows);
  }

  /** Records a consumed slice and returns whether it completed the logical mini-batch. */
  public boolean onSlice(int rows) {
    if (rows <= 0 || (maxRows > 0 && rows > maxRows - bufferedRows)) {
      throw new IllegalArgumentException("slice crosses the mini-batch boundary");
    }
    bufferedRows += rows;
    if (maxRows > 0 && bufferedRows == maxRows) {
      bufferedRows = 0;
      return true;
    }
    return false;
  }

  /** Resets the count after a marker, watermark, checkpoint, or end-of-input flush. */
  public void reset() {
    bufferedRows = 0;
  }

  public long bufferedRows() {
    return bufferedRows;
  }
}
