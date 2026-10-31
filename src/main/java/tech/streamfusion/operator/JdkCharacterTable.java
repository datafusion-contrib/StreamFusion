package tech.streamfusion.operator;

/** Worker-JDK character properties copied once when native classification is compiled. */
public final class JdkCharacterTable {
  private JdkCharacterTable() {}

  public static long[] letters() {
    long[] bits = new long[1024];
    for (int unit = 0; unit <= Character.MAX_VALUE; unit++) {
      if (Character.isLetter((char) unit)) bits[unit >>> 6] |= 1L << (unit & 63);
    }
    return bits;
  }
}
