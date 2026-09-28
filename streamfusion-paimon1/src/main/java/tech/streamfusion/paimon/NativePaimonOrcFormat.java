package tech.streamfusion.paimon;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.format.*;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.orc.OrcFileFormat;
import org.apache.paimon.format.orc.OrcTypeUtil;
import org.apache.paimon.format.orc.OrcWriterFactory;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.shade.org.apache.orc.OrcConf;
import org.apache.paimon.shade.org.apache.orc.TypeDescription;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;
import tech.streamfusion.orc.OrcCodec;
import tech.streamfusion.orc.OrcWriterSettings;

/**
 * ORC encoding consumes Arrow columns; Paimon retains schema validation, row writers and footer
 * statistics.
 */
public final class NativePaimonOrcFormat extends FileFormat implements NativePaimonFileFormat {
  private final OrcFileFormat delegate;
  private final Map<String, String> properties = new LinkedHashMap<>();
  private final String compression;

  public NativePaimonOrcFormat(FormatContext context) {
    super("orc");
    delegate = new OrcFileFormat(context);
    delegate.orcProperties().forEach((k, v) -> properties.put(k.toString(), v.toString()));
    compression = context.options().get(CoreOptions.FILE_COMPRESSION);
  }

  private TypeDescription schema(RowType type) {
    return OrcTypeUtil.convertToOrcSchema((RowType) OrcFileFormat.refineDataType(type));
  }

  private OrcWriterSettings settings(RowType type, String compression) {
    Map<String, String> defaults = new LinkedHashMap<>(),
        requested = new LinkedHashMap<>(properties);
    for (OrcConf key : OrcConf.values())
      defaults.put(key.getAttribute(), String.valueOf(key.getDefaultValue()));
    // An explicit orc.compress overrides the compression requested by Paimon for this file.
    if (compression != null) requested.putIfAbsent("orc.compress", compression);
    Map<String, String> effective = new LinkedHashMap<>(defaults);
    effective.putAll(requested);
    // 1.0 uses java.sql.Timestamp for both timestamp kinds, even though LTZ has
    // timestamp-instant physical storage. Preserve its pre-1582 calendar conversion.
    effective.put("orc.timestamp-ltz.legacy.type", "true");
    String columns = effective.getOrDefault("orc.bloom.filter.columns", "");
    TypeDescription schema = schema(type);
    if (!columns.isEmpty())
      java.util.Arrays.stream(columns.split(",")).forEach(name -> schema.findSubtype(name.trim()));
    return OrcWriterSettings.translate(effective, requested, defaults, columns);
  }

  public String nativeWriterFallbackReason(RowType type) {
    return nativeWriterFallbackReason(type, compression);
  }

  public String nativeWriterFallbackReason(RowType type, String compression) {
    if (!PaimonCodecs.available("orc")) return "streamfusion-orc is not installed";
    String reason = PaimonArrowFields.unsupportedTypeReason(type);
    if (reason != null) return reason;
    reason =
        OrcWriterSettings.timestampFallback(
            org.apache.paimon.flink.LogicalTypeConversion.toLogicalType(type));
    if (reason != null) return reason;
    try {
      return settings(type, compression).fallbackReason();
    } catch (RuntimeException unsupported) {
      return unsupported.toString();
    }
  }

  public FormatWriterFactory createWriterFactory(RowType type) {
    FormatWriterFactory stock = delegate.createWriterFactory(type);
    if (!(stock instanceof OrcWriterFactory) || nativeWriterFallbackReason(type) != null)
      return stock;
    String description = schema(type).toString();
    // Capture a serializable format wrapper, as Paimon's own format factories do.
    return (out, compression) -> {
      OrcWriterSettings settings = settings(type, compression);
      if (settings.fallbackReason() != null) return stock.create(out, compression);
      return new NativePaimonFileWriter(
          type,
          new OrcCodec(description, false, new PaimonOrcWriter()),
          settings.keys(),
          settings.values(),
          out,
          compression,
          stock);
    };
  }

  public FormatReaderFactory createReaderFactory(RowType data, List<Predicate> filters) {
    return delegate.createReaderFactory(data, filters);
  }

  public void validateDataFields(RowType type) {
    delegate.validateDataFields(type);
  }

  public Optional<SimpleStatsExtractor> createStatsExtractor(
      RowType type, SimpleColStatsCollector.Factory[] collectors) {
    return delegate.createStatsExtractor(type, collectors);
  }
}
