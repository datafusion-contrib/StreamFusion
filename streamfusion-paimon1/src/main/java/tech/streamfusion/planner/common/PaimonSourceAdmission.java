package tech.streamfusion.planner.common;

import java.util.Set;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.source.DataTableSource;
import org.apache.paimon.table.FileStoreTable;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.paimon.NativePaimonSource;
import tech.streamfusion.paimon.PaimonCodecs;

/** Shared table, projection and reader-option admission for bounded and continuous Paimon scans. */
public final class PaimonSourceAdmission {
  private PaimonSourceAdmission() {}

  public static String fallback(TableSourceTable resolved, RowType type, boolean streaming) {
    if (resolved == null || !(resolved.tableSource() instanceof DataTableSource))
      return "not a Paimon data-table source";
    var source = (DataTableSource) resolved.tableSource();
    if (resolved.isStreamingMode() != streaming)
      return "source execution mode does not match the planner";
    if (!(source.getTable() instanceof FileStoreTable)) return "not a Paimon file-store table";
    var table = (FileStoreTable) source.getTable();
    CoreOptions options = table.coreOptions();
    if (options.mergeEngine() == CoreOptions.MergeEngine.PARTIAL_UPDATE) {
      if (table.options().keySet().stream().anyMatch(k -> k.endsWith("sequence-group")))
        return "Paimon 1.0 partial-update sequence groups retain the stock source";
      if (options.changelogProducer() == CoreOptions.ChangelogProducer.LOOKUP
          && options.toConfiguration().get(CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE))
        return "Paimon 1.0 lookup delete-removal reads retain the stock source";
    }
    if (table.options().keySet().stream()
        .anyMatch(k -> k.startsWith("fields.") && k.endsWith(".default-value"))) {
      return "Paimon 1.0 applies field defaults while reading; retain the stock source";
    }
    if (streaming && table.options().containsKey("scan.bounded.watermark"))
      return "bounded reads retain the stock source";
    String typeReason = NativePaimonSource.unsupportedTypeReason(table);
    if (typeReason != null) {
      return typeReason;
    }
    if (!PaimonCodecs.available(options.fileFormatString())) {
      return "file.format requires an installed native Parquet or ORC module";
    }
    if (streaming
        && !table.primaryKeys().isEmpty()
        && options.changelogProducer() == CoreOptions.ChangelogProducer.NONE) {
      return "primary-key tailing requires a changelog producer";
    }

    if (streaming && table.options().containsKey("consumer-id")) {
      return "consumer retention requires the stock source";
    }
    for (String option :
        Set.of(
            "source.checkpoint-align.enabled",
            "chain-table.enabled",
            "postpone.merge-on-read",
            "data-file.thin-mode",
            "key-value-sequence-number.enabled")) {
      if (Boolean.parseBoolean(table.options().getOrDefault(option, "false"))) {
        return option + " requires the stock source";
      }
    }

    for (String key : table.options().keySet()) {
      if (Set.of(
              "scan.dedicated-split-generation",
              "scan.mode",
              "scan.snapshot-id",
              "scan.timestamp-millis",
              "scan.timestamp",
              "scan.tag-name",
              "scan.watermark",
              "scan.bounded.watermark",
              "scan.parallelism",
              "scan.infer-parallelism",
              "scan.infer-parallelism.max",
              "scan.remove-normalize",
              "scan.watermark.idle-timeout",
              "scan.watermark.emit.strategy")
          .contains(key)) {
        continue;
      }
      if (key.equals("orc.timestamp-ltz.legacy.type")
          && java.util.Set.of("true", "false").contains(table.options().get(key))) continue;
      if (key.startsWith("scan.")
          || key.startsWith("streaming-read-")
          || key.startsWith("log.")
          || key.startsWith("parquet.")
          || key.startsWith("orc.")) {
        return "source option " + key + " is not supported";
      }
    }
    if (Boolean.parseBoolean(
        table.options().getOrDefault("scan.dedicated-split-generation", "false"))) {
      return "dedicated split generation is not supported";
    }
    for (var ability : resolved.abilitySpecs()) {
      String name = ability.getClass().getSimpleName();
      if (!Set.of("ProjectPushDownSpec", "FilterPushDownSpec", "WatermarkPushDownSpec")
          .contains(name)) {
        return "source ability " + name + " is not supported";
      }
    }
    if ("orc".equals(options.fileFormatString())) {
      String timestampReason =
          tech.streamfusion.orc.OrcWriterSettings.timestampFallback(
              LogicalTypeConversion.toLogicalType(table.rowType()));
      if (timestampReason != null) return timestampReason;
    }
    if (type.getFieldCount() == 0) {
      return "zero-column projection is not supported";
    }
    if (!RowDataArrowConverter.supports(type)) {
      return "unsupported Arrow source type";
    }
    for (int i = 0; i < type.getFieldCount(); i++) {
      int column = table.rowType().getFieldNames().indexOf(type.getFieldNames().get(i));
      if (column < 0
          || !type.getTypeAt(i)
              .copy(true)
              .equals(
                  LogicalTypeConversion.toLogicalType(table.rowType().getTypeAt(column))
                      .copy(true))) {
        return "nested projection or metadata columns are not supported";
      }
    }
    return null;
  }
}
