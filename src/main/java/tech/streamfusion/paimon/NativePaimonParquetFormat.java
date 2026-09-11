package tech.streamfusion.paimon;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.format.SupportsFieldMetadata;
import org.apache.paimon.format.parquet.ParquetFileFormat;
import org.apache.paimon.format.parquet.ParquetWriterFactory;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;

/**
 * Paimon's Parquet format with only the data-file writer replaced. Reads, schema validation, and
 * footer statistics stay with Paimon's own implementation, so a table written natively is read and
 * described exactly as one written by parquet-mr. The writer factory it hands out writes Arrow
 * bundles through the native encoder and delegates every row-fed file (compaction rewrites, batch
 * inserts, foreign bundles) to the stock writer, so no table ever mixes writer behaviour by
 * accident.
 */
public final class NativePaimonParquetFormat extends FileFormat implements SupportsFieldMetadata {

  private final ParquetFileFormat delegate;
  private final Options parquetOptions;
  private final String fileCompression;

  public NativePaimonParquetFormat(FormatContext context) {
    super(NativePaimonParquetFormatFactory.IDENTIFIER);
    this.delegate = new ParquetFileFormat(context);
    this.parquetOptions = parquetOptions(context);
    this.fileCompression = context.options().get(CoreOptions.FILE_COMPRESSION);
  }

  /**
   * The stock format's writer options: the {@code parquet.*} keys plus Paimon's level and block
   * size.
   */
  private Options parquetOptions(FormatContext context) {
    Options options = getIdentifierPrefixOptions(context.options());
    if (!options.containsKey(PaimonParquetSettings.ZSTD_LEVEL_KEY)) {
      options.set(PaimonParquetSettings.ZSTD_LEVEL_KEY, String.valueOf(context.zstdLevel()));
    }
    MemorySize blockSize = context.blockSize();
    if (blockSize != null) {
      options.set(PaimonParquetSettings.BLOCK_SIZE_KEY, String.valueOf(blockSize.getBytes()));
    }
    return options;
  }

  /**
   * Why the table's default write settings keep the stock writer, or null when the native one
   * applies.
   */
  @Nullable
  public String nativeWriterFallbackReason(RowType type) {
    return nativeWriterFallbackReason(type, fileCompression);
  }

  @Nullable
  public String nativeWriterFallbackReason(RowType type, String compression) {
    String unsupportedType = PaimonArrowFields.unsupportedTypeReason(type);
    if (unsupportedType != null) {
      return unsupportedType;
    }
    return PaimonParquetSettings.translate(parquetOptions, compression).fallbackReason();
  }

  @Override
  public FormatWriterFactory createWriterFactory(RowType type) {
    FormatWriterFactory stock = delegate.createWriterFactory(type);
    if (!(stock instanceof ParquetWriterFactory)
        || PaimonArrowFields.unsupportedTypeReason(type) != null) {
      return stock;
    }
    return new NativePaimonParquetWriterFactory(type, parquetOptions, stock);
  }

  @Override
  public FormatReaderFactory createReaderFactory(
      RowType dataSchemaRowType, RowType projectedRowType, @Nullable List<Predicate> filters) {
    return delegate.createReaderFactory(dataSchemaRowType, projectedRowType, filters);
  }

  @Override
  public Map<String, Map<String, String>> readFieldMetadata(FormatReaderFactory.Context context)
      throws IOException {
    return delegate.readFieldMetadata(context);
  }

  @Override
  public void validateDataFields(RowType rowType) {
    delegate.validateDataFields(rowType);
  }

  @Override
  public Optional<SimpleStatsExtractor> createStatsExtractor(
      RowType type, SimpleColStatsCollector.Factory[] statsCollectors) {
    return delegate.createStatsExtractor(type, statsCollectors);
  }
}
