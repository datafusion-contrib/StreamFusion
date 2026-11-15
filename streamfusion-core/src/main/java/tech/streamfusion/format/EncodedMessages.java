package tech.streamfusion.format;

import java.util.Arrays;

/** One encoded message per Arrow row, stored in an owned JVM byte slab. */
public final class EncodedMessages {
  public final byte[] bytes;
  public final int[] offsets;
  public final int[] lengths;

  public EncodedMessages(byte[] bytes, int[] offsets, int[] lengths) {
    if (offsets.length != lengths.length)
      throw new IllegalArgumentException("inconsistent encoded row counts");
    for (int i = 0; i < offsets.length; i++) {
      if (offsets[i] < 0 || lengths[i] < 0 || offsets[i] > bytes.length - lengths[i])
        throw new IllegalArgumentException("encoded row is outside its byte slab");
    }
    this.bytes = bytes;
    this.offsets = offsets;
    this.lengths = lengths;
  }

  public int size() {
    return offsets.length;
  }

  public byte[] message(int row) {
    return Arrays.copyOfRange(bytes, offsets[row], offsets[row] + lengths[row]);
  }
}
