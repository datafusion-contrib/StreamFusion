package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Reads Parquet files natively into {@link ArrowBatch}es. Parquet is self-describing, so the reader
 * derives the schema from the file; splitting is at row-group granularity (a row group is read by the
 * split whose byte range contains its start). Timestamp columns are normalized to the host reader's
 * wall-clock encoding so results match exactly.
 */
public class ParquetArrowBulkFormat extends NativeFileArrowBulkFormat {

  // The host's utc-timezone setting; decides how timestamp columns are interpreted on read.
  private final boolean utcTimestamp;

  public ParquetArrowBulkFormat(String[] projection, boolean utcTimestamp) {
    super(projection);
    this.utcTimestamp = utcTimestamp;
  }

  @Override
  protected long open(String path, String[] projection, long rangeStart, long rangeLength) {
    return Native.openParquet(path, projection, rangeStart, rangeLength);
  }

  @Override
  protected VectorSchemaRoot adapt(VectorSchemaRoot root, BufferAllocator allocator) {
    return ParquetSourceTimestamps.normalize(root, allocator, utcTimestamp);
  }
}
