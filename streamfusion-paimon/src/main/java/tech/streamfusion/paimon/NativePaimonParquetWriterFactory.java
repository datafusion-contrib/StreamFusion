package tech.streamfusion.paimon;

import java.io.IOException;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;

/**
 * Opens one {@link NativePaimonFileWriter} per data file. A compression Paimon asks for that the
 * native encoder cannot reproduce keeps that file on the stock writer.
 */
final class NativePaimonParquetWriterFactory implements FormatWriterFactory {

  private final RowType writeType;
  private final Options parquetOptions;
  private final FormatWriterFactory stock;

  NativePaimonParquetWriterFactory(
      RowType writeType, Options parquetOptions, FormatWriterFactory stock) {
    this.writeType = writeType;
    this.parquetOptions = parquetOptions;
    this.stock = stock;
  }

  @Override
  public FormatWriter create(PositionOutputStream out, String compression) throws IOException {
    PaimonParquetSettings settings = PaimonParquetSettings.translate(parquetOptions, compression);
    if (settings.fallbackReason() != null) {
      return stock.create(out, compression);
    }
    return new NativePaimonFileWriter(
            writeType,
            new tech.streamfusion.parquet.ParquetCodec(),
            settings.keys(),
            settings.values(),
            out,
            compression,
            stock)
        .withParquetFloatingStats();
  }
}
