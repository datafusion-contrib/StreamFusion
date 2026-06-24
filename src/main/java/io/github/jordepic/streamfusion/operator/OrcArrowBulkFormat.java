package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;

/**
 * Reads ORC files natively into {@link ArrowBatch}es. ORC is self-describing, so the reader derives
 * the schema from the file; splitting is at stripe granularity (a stripe is read by the split whose
 * byte range contains its start).
 */
public class OrcArrowBulkFormat extends NativeFileArrowBulkFormat {

  public OrcArrowBulkFormat(String[] projection) {
    super(projection);
  }

  @Override
  protected long open(String path, String[] projection, long rangeStart, long rangeLength) {
    return Native.openOrc(path, projection, rangeStart, rangeLength);
  }
}
