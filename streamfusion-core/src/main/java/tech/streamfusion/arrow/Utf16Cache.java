package tech.streamfusion.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.flink.core.memory.MemoryUtils;

/** Copies original UTF-16 code units without normalizing malformed surrogate pairs. */
public final class Utf16Cache {
  private static final long CHAR_BASE = MemoryUtils.UNSAFE.arrayBaseOffset(char[].class);
  private static final int MAX_SCRATCH_CHARS = 4096;

  private Utf16Cache() {}

  /** Owns reusable heap scratch; the destination vector owns every written value. */
  public static final class Writer {
    private final ListVector vector;
    private char[] scratch = new char[0];

    public Writer(ListVector vector) {
      this.vector = vector;
    }

    public void write(int position, String text) {
      if (text == null) {
        vector.setNull(position);
        return;
      }
      int start = vector.startNewValue(position);
      int length = text.length();
      int end = Math.addExact(start, length);
      var units = (UInt2Vector) vector.getDataVector();
      while (end > units.getValueCapacity()) units.reAlloc();
      if (length > 0) {
        int scratchLength = Math.min(length, MAX_SCRATCH_CHARS);
        if (scratch.length < scratchLength) scratch = new char[scratchLength];
        ArrowBuf data = units.getDataBuffer();
        long offset = (long) start * Character.BYTES;
        long bytes = (long) length * Character.BYTES;
        data.checkBytes(offset, offset + bytes);
        for (int copied = 0; copied < length; ) {
          int count = Math.min(length - copied, scratch.length);
          text.getChars(copied, copied + count, scratch, 0);
          MemoryUtils.UNSAFE.copyMemory(
              scratch,
              CHAR_BASE,
              null,
              data.memoryAddress() + offset + (long) copied * Character.BYTES,
              (long) count * Character.BYTES);
          copied += count;
        }
        // This child contains only valid code units; spare bits are outside its value count.
        long firstByte = start >>> 3;
        units.getValidityBuffer().setOne(firstByte, ((end + 7L) >>> 3) - firstByte);
      }
      vector.endValue(position, length);
    }
  }

  /** Returns an owned String; the Arrow buffer is borrowed only during the checked copy. */
  static String read(ListVector vector, int position) {
    int start = vector.getElementStartIndex(position);
    int length = vector.getElementEndIndex(position) - start;
    char[] text = new char[length];
    if (length > 0) {
      ArrowBuf data = vector.getDataVector().getDataBuffer();
      long offset = (long) start * Character.BYTES;
      long bytes = (long) length * Character.BYTES;
      data.checkBytes(offset, offset + bytes);
      MemoryUtils.UNSAFE.copyMemory(null, data.memoryAddress() + offset, text, CHAR_BASE, bytes);
    }
    return new String(text);
  }
}
