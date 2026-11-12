package tech.streamfusion.paimon;

import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;

/**
 * Registers StreamFusion's Parquet writer under Paimon's own {@code parquet} identifier. Paimon
 * resolves a format by the first matching factory on the classpath, so this factory replaces the
 * stock one only when its jar is discovered first; the planner checks that it did and declines the
 * native sink otherwise, so a losing discovery is a logged fallback rather than a silent stock write.
 */
public final class NativePaimonParquetFormatFactory implements FileFormatFactory {

  public static final String IDENTIFIER = "parquet";

  @Override
  public String identifier() {
    return IDENTIFIER;
  }

  @Override
  public FileFormat create(FormatContext context) {
    return new NativePaimonParquetFormat(context);
  }
}
