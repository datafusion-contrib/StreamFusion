package tech.streamfusion.paimon;

import java.util.Set;
import tech.streamfusion.format.ColumnarFileCodec;

/** Optional codec discovery is an admission check, including the native payload's build stamp. */
public final class PaimonCodecs {
  private PaimonCodecs() {}

  public static boolean available(String format) {
    if (!Set.of("parquet", "orc").contains(format)) return false;
    String name = format.equals("orc") ? "Orc" : "Parquet";
    try {
      return (boolean)
          Class.forName("tech.streamfusion." + format + ".Native" + name)
              .getMethod("isLoaded")
              .invoke(null);
    } catch (ReflectiveOperationException | LinkageError absent) {
      return false;
    }
  }

  static ColumnarFileCodec reader(String format) {
    return reader(format, true);
  }

  static ColumnarFileCodec reader(String format, boolean legacyTimestamp) {
    switch (format) {
      case "parquet":
        return new tech.streamfusion.parquet.ParquetCodec();
      case "orc":
        return new tech.streamfusion.orc.OrcCodec(null, legacyTimestamp);
      default:
        throw new IllegalArgumentException("Unsupported file format " + format);
    }
  }
}
