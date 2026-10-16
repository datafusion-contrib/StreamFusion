package tech.streamfusion.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.binary.BinaryStringData;

/** Mutable UTF-8 lookup views valid only while the owning Arrow batch remains open. */
@Internal
public final class BorrowedStringValues {
  private final VarCharVector input;
  private final MemorySegment[] segments;
  private final BinaryStringData value;

  public static BorrowedStringValues create(FieldVector input) {
    if (!(input instanceof VarCharVector) || input.getDataBuffer().capacity() > Integer.MAX_VALUE)
      return null;
    return new BorrowedStringValues((VarCharVector) input);
  }

  private BorrowedStringValues(VarCharVector input) {
    this.input = input;
    int bytes = Math.toIntExact(input.getDataBuffer().capacity());
    segments =
        bytes == 0
            ? null
            : new MemorySegment[] {
              MemorySegmentFactory.wrapOffHeapMemory(input.getDataBuffer().nioBuffer(0, bytes))
            };
    value = segments == null ? null : BinaryStringData.fromAddress(segments, 0, 0);
  }

  public BinaryStringData value(int row) {
    if (input.isNull(row)) return null;
    int start = input.getOffsetBuffer().getInt(row * 4L);
    int end = input.getOffsetBuffer().getInt((row + 1L) * 4);
    if (start == end) return BinaryStringData.EMPTY_UTF8;
    value.setJavaObject(null);
    value.getBinarySection().pointTo(segments, start, end - start);
    return value;
  }
}
