package tech.streamfusion.planner;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.orc.OrcSplitReaderUtil;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.orc.OrcWriterSettings;

/** Resolves the same DDL-over-Hadoop settings as Flink's ORC format factory. */
final class OrcSinkTranslator {
  static RowType writeType(RowType type, List<String> partitions) {
    return new RowType(
        type.getFields().stream()
            .filter(f -> !partitions.contains(f.getName()))
            .collect(ListCollectors.toList()));
  }

  static String schema(RowType type, List<String> partitions) {
    return OrcSplitReaderUtil.logicalTypeToOrcType(writeType(type, partitions)).toString();
  }

  static OrcWriterSettings translate(
      Map<String, String> options, RowType type, List<String> partitions) {
    if ("true".equalsIgnoreCase(options.get("auto-compaction")))
      return OrcWriterSettings.fallback("auto-compaction requires Flink's compaction topology");
    for (String key : options.keySet()) {
      if (!key.startsWith("orc.")
          && !FileSinkOptions.HOST_HONORED.contains(key)
          && !FileSinkOptions.HOST_IGNORED.contains(key)
          && !key.equals("auto-compaction"))
        return OrcWriterSettings.fallback("unrecognized filesystem sink option " + key);
    }
    try {
      RowType written = writeType(type, partitions);
      String timestampReason = OrcWriterSettings.timestampFallback(written);
      if (timestampReason != null) return OrcWriterSettings.fallback(timestampReason);
      if (written.getFieldCount() == 0) return OrcWriterSettings.fallback("zero-column ORC file");
      TypeDescription schema = OrcSplitReaderUtil.logicalTypeToOrcType(written);
      Map<String, String> requested = new LinkedHashMap<>(),
          defaults = new LinkedHashMap<>(),
          effective = new LinkedHashMap<>();
      Configuration conf = new Configuration();
      for (OrcConf key : OrcConf.values()) {
        String name = key.getAttribute();
        defaults.put(name, String.valueOf(key.getDefaultValue()));
        String cluster = conf.get(name);
        if (cluster == null && key.getHiveConfName() != null)
          cluster = conf.get(key.getHiveConfName());
        if (cluster != null) requested.put(name, cluster);
      }
      options.forEach(
          (k, v) -> {
            if (k.startsWith("orc.")) requested.put(k, v);
          });
      effective.putAll(defaults);
      effective.putAll(requested);
      // Flink 2.2's ORC 1.5.6 reader cannot read ZSTD files; Paimon's newer shaded ORC can.
      org.apache.orc.CompressionKind.valueOf(
          effective.get("orc.compress").toUpperCase(java.util.Locale.ROOT));
      String columns = effective.getOrDefault("orc.bloom.filter.columns", "");
      if (!columns.isEmpty())
        java.util.Arrays.stream(columns.split(","))
            .forEach(name -> schema.findSubtype(name.trim()));
      return OrcWriterSettings.translate(effective, requested, defaults, columns);
    } catch (RuntimeException unsupported) {
      return OrcWriterSettings.fallback(unsupported.toString());
    }
  }
}
