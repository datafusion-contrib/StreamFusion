package tech.streamfusion.operator;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.text.BreakIterator;
import java.util.Locale;
import java.util.stream.LongStream;

/** Worker-JDK character properties captured while compiling native expressions. */
public final class JdkCharacterTable {
  private static final Locale TURKISH = Locale.forLanguageTag("tr");
  private static final Locale LITHUANIAN = Locale.forLanguageTag("lt");
  private static final String DOT_ABOVE = "\u0307";
  private static volatile CaseMappings upperMappings;
  private static volatile CaseMappings lowerMappings;
  private static volatile CaseMappings wordBreakTables;

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

  public static long[] appendCaseMappings(long[] pool, boolean upper) {
    Locale locale = Locale.getDefault();
    long[] mappings = caseMappings(upper, locale);
    long[] wordBreak = upper ? new long[0] : lowerCaseWordBreakData(locale);
    int offset = pool.length;
    long[] result = java.util.Arrays.copyOf(pool, offset + 3 + mappings.length + wordBreak.length);
    result[offset] = upper ? 1 : 0;
    result[offset + 1] = mappings.length;
    result[offset + 2] = wordBreak.length;
    System.arraycopy(mappings, 0, result, offset + 3, mappings.length);
    System.arraycopy(wordBreak, 0, result, offset + 3 + mappings.length, wordBreak.length);
    return result;
  }

  private static long[] lowerCaseWordBreakData(Locale locale) {
    var cached = wordBreakTables;
    if (cached != null && cached.locale.equals(locale)) return cached.words;
    long[] data = readWordBreakData(locale);
    wordBreakTables = new CaseMappings(locale, data);
    return data;
  }

  private static long[] readWordBreakData(Locale locale) {
    BreakIterator iterator = BreakIterator.getWordInstance(locale);
    String resource;
    switch (iterator.getClass().getName()) {
      case "sun.text.RuleBasedBreakIterator":
        resource = "/modules/java.base/sun/text/resources/WordBreakIteratorData";
        break;
      case "sun.text.DictionaryBasedBreakIterator":
        resource = "/modules/jdk.localedata/sun/text/resources/ext/WordBreakIteratorData_th";
        break;
      default:
        return new long[0];
    }
    try {
      var jrt = FileSystems.getFileSystem(URI.create("jrt:/"));
      byte[] data = Files.readAllBytes(jrt.getPath(resource));
      if (data.length < 48 || (int) ByteBuffer.wrap(data).getLong(40) != iterator.hashCode()) {
        return new long[0];
      }
      long[] words = new long[1 + (data.length + 7) / 8];
      words[0] = data.length;
      for (int i = 0; i < data.length; i++) {
        words[1 + (i >>> 3)] |= (long) Byte.toUnsignedInt(data[i]) << ((i & 7) * 8);
      }
      return words;
    } catch (IOException | RuntimeException unavailable) {
      return new long[0];
    }
  }

  private static long[] caseMappings(boolean upper, Locale locale) {
    CaseMappings cached = upper ? upperMappings : lowerMappings;
    if (cached != null && cached.locale.equals(locale)) return cached.words;
    var words = LongStream.builder();
    int mode = caseMode(locale);
    words.accept(mode);
    for (int point = 0; point <= Character.MAX_CODE_POINT; point++) {
      int type = Character.getType(point);
      if (type == Character.UNASSIGNED || type == Character.SURROGATE) continue;
      String original = new String(Character.toChars(point));
      String mapped = upper ? original.toUpperCase(locale) : original.toLowerCase(locale);
      int properties = caseContextProperties(point, type, original, mode, upper);
      if (!upper && isCasedForFinalSigma(point, type)) properties |= 8;
      boolean changes = !original.equals(mapped);
      if (upper && mode == 2 && !changes && point <= Character.MAX_VALUE
          && !(original + "\u1d62" + DOT_ABOVE).toUpperCase(locale).endsWith(DOT_ABOVE)) {
        // Some unchanged uppercase mappings still enter StringUTF16's special-casing loop.
        properties |= 16;
      }
      if (!changes && properties == 0) continue;
      long units = 0;
      if (mapped.length() > 4) return new long[0];
      if (changes) {
        for (int i = 0; i < mapped.length(); i++) units |= (long) mapped.charAt(i) << (i * 16);
      }
      words.accept(point | ((long) properties << 21));
      words.accept(units);
    }
    var mappings = new CaseMappings(locale, words.build().toArray());
    if (upper) upperMappings = mappings;
    else lowerMappings = mappings;
    return mappings.words;
  }

  private static int caseMode(Locale locale) {
    switch (locale.getLanguage()) {
      case "tr":
      case "az":
        return 1;
      case "lt":
        return 2;
      default:
        return 0;
    }
  }

  private static int caseContextProperties(
      int point, int type, String original, int mode, boolean upper) {
    if (mode == 0 || (mode == 1 && upper)) return 0;
    boolean mark = type == Character.NON_SPACING_MARK
        || type == Character.COMBINING_SPACING_MARK
        || type == Character.ENCLOSING_MARK;
    // These probes expose the running JDK's combining and soft-dot rules through its
    // public casing API. Unicode defines nonzero combining classes only for marks.
    if (mark) {
      if (("I" + original).toLowerCase(LITHUANIAN).length()
          > ("a" + original).toLowerCase(LITHUANIAN).length()) return 1;
      if (point != '\u0307'
          && ("I" + original + DOT_ABOVE).toLowerCase(TURKISH).length()
              == ("I" + original).toLowerCase(TURKISH).length()) return 2;
    }
    if (mode == 2 && upper
        && ("a" + original + DOT_ABOVE).toUpperCase(LITHUANIAN).length()
            == ("a" + original).toUpperCase(LITHUANIAN).length()) return 4;
    return 0;
  }

  private static boolean isCasedForFinalSigma(int point, int type) {
    return type == Character.UPPERCASE_LETTER
        || type == Character.LOWERCASE_LETTER
        || type == Character.TITLECASE_LETTER
        || between(point, 0x02b0, 0x02b8)
        || between(point, 0x02c0, 0x02c1)
        || between(point, 0x02e0, 0x02e4)
        || point == 0x0345
        || point == 0x037a
        || between(point, 0x1d2c, 0x1d61)
        || between(point, 0x2160, 0x217f)
        || between(point, 0x24b6, 0x24e9);
  }

  private static boolean between(int point, int start, int end) {
    return point >= start && point <= end;
  }

  private static final class CaseMappings {
    private final Locale locale;
    private final long[] words;

    private CaseMappings(Locale locale, long[] words) {
      this.locale = locale;
      this.words = words;
    }
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
