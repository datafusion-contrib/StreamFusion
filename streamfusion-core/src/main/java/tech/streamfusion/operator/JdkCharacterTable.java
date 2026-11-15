package tech.streamfusion.operator;

/** Worker-JDK character properties copied once when native classification is compiled. */
public final class JdkCharacterTable {
  private JdkCharacterTable() {}

  public static long[] letters() {
    return properties(false);
  }

  public static long[] digits() {
    return properties(true);
  }

  public static long[] decimalDigits() {
    long[] bits = new long[4096];
    java.util.Arrays.fill(bits, -1L);
    for (int unit = 0; unit <= Character.MAX_VALUE; unit++) {
      int digit = Character.digit((char) unit, 10);
      if (digit >= 0) {
        int shift = (unit & 15) * 4;
        bits[unit >>> 4] = (bits[unit >>> 4] & ~(15L << shift)) | ((long) digit << shift);
      }
    }
    return bits;
  }

  private static long[] properties(boolean digits) {
    long[] bits = new long[1024];
    for (int unit = 0; unit <= Character.MAX_VALUE; unit++) {
      if (digits ? Character.isDigit((char) unit) : Character.isLetter((char) unit)) {
        bits[unit >>> 6] |= 1L << (unit & 63);
      }
    }
    return bits;
  }
}
