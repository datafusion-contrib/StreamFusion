package tech.streamfusion.planner.stream;

import java.util.Map;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.planner.plan.abilities.sink.OverwriteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.RowLevelDeleteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.RowLevelUpdateSpec;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSink;
import org.apache.paimon.flink.sink.FlinkTableSink;
import tech.streamfusion.compat.FlinkCompat;
import tech.streamfusion.paimon.NativePaimonParquetFormat;
import tech.streamfusion.planner.NativeConfig;

/** Refines the file encoding without replacing Flink's mutation or Paimon's clustering topology. */
final class PaimonBatchEncoding {
  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(PaimonBatchEncoding.class);

  private PaimonBatchEncoding() {}

  static BatchPhysicalSink prepare(BatchPhysicalSink sink, ReadableConfig config) {
    if (!NativeConfig.operatorEnabled("paimonSink")
        || !NativeConfig.operatorEnabled("paimonBatchEncoding")
        || !(sink.tableSink() instanceof FlinkTableSink)) return sink;
    var table =
        PaimonSinkMatcher.resolveTable(
            sink, Map.of(NativePaimonParquetFormat.BATCH_ROW_ENCODING, "true"));
    if (table == null || !"parquet".equals(table.coreOptions().fileFormatString())) return sink;
    var abilities = FlinkCompat.sinkAbilities(sink);
    boolean operation =
        abilities.stream()
            .anyMatch(
                ability ->
                    ability instanceof OverwriteSpec
                        || ability instanceof RowLevelDeleteSpec
                        || ability instanceof RowLevelUpdateSpec);
    operation |= table.primaryKeys().isEmpty() && PaimonSinkMatcher.clustering(table);
    if (!operation) return sink;
    String fallback =
        tech.streamfusion.paimon.PaimonCodecs.specialWriteFeatures(table.coreOptions());
    if (fallback == null
        && !new org.apache.paimon.fileindex.FileIndexOptions(table.coreOptions()).isEmpty())
      fallback = "file indexes";
    if (fallback == null
        && !table
            .coreOptions()
            .toConfiguration()
            .get(org.apache.paimon.CoreOptions.FILE_FORMAT_PER_LEVEL)
            .isEmpty()) fallback = "per-level file formats";
    if (fallback == null) fallback = PaimonSinkMatcher.formatFallbackReason(table);
    if (fallback != null) {
      LOG.info("StreamFusion batch Paimon Parquet encoding fallback: {}", fallback);
      return sink;
    }
    var resolved = sink.contextResolvedTable();
    var context =
        new FactoryUtil.DefaultDynamicTableContext(
            resolved.getIdentifier(),
            (ResolvedCatalogTable) resolved.getResolvedTable(),
            Map.of(),
            config,
            Thread.currentThread().getContextClassLoader(),
            resolved.isTemporary());
    var encodingSink = PaimonSinkMatcher.stockSink(table, context);
    if (encodingSink == null) {
      LOG.info("StreamFusion batch Paimon Parquet encoding fallback: external log store");
      return sink;
    }
    for (var ability : abilities) ability.apply(encodingSink);
    LOG.info(
        "StreamFusion batch Paimon native Parquet encoding with Java sink topology: {}",
        resolved.getIdentifier().asSummaryString());
    return new BatchPhysicalSink(
        sink.getCluster(),
        sink.getTraitSet(),
        sink.getInput(),
        sink.hints(),
        resolved,
        encodingSink,
        sink.targetColumns(),
        abilities.toArray(new SinkAbilitySpec[0]));
  }
}
