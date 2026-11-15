package tech.streamfusion.paimon;

import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;

/** Registers the native Arrow writer under Paimon's stock ORC identifier. */
public final class NativePaimonOrcFormatFactory implements FileFormatFactory {
  public String identifier() {
    return "orc";
  }

  public FileFormat create(FormatContext context) {
    return new NativePaimonOrcFormat(context);
  }
}
