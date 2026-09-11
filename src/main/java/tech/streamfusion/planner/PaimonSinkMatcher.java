package tech.streamfusion.planner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.planner.plan.abilities.sink.OverwriteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.CoreOptions.PartitionSinkStrategy;
import org.apache.paimon.CoreOptions.SequenceNumberInitMode;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.flink.DataCatalogTable;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.FlinkFileIOLoader;
import org.apache.paimon.flink.sink.FlinkTableSink;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import tech.streamfusion.paimon.NativePaimonParquetFormat;
import tech.streamfusion.paimon.PaimonKeyValueLayout;

/**
 * Whitelist-first admission for the columnar Paimon sink. The table is resolved the way Paimon's
 * own Flink factory resolves it, so every option the stock sink would see — DDL, hints, and the
 * table-scoped dynamic options — shapes the native topology identically. An append table takes an
 * insert-only input; a primary-key table takes a changelog, and is admitted only in the shape the
 * native level-0 writer reproduces: fixed buckets, the deduplicate merge engine with Paimon's own
 * compaction and changelog production.
 */
final class PaimonSinkMatcher {
  private PaimonSinkMatcher() {}

  static final class Planned {
    final FileStoreTable table;
    final boolean primaryKey;
    final int[] partitionColumns;
    final int[] partitionTimestampPrecisions;
    final int[] bucketColumns;
    final int[] bucketTimestampPrecisions;
    final String fallbackReason;

    private Planned(
        FileStoreTable table,
        boolean primaryKey,
        int[] partitionColumns,
        int[] partitionTimestampPrecisions,
        int[] bucketColumns,
        int[] bucketTimestampPrecisions,
        String fallbackReason) {
      this.table = table;
      this.primaryKey = primaryKey;
      this.partitionColumns = partitionColumns;
      this.partitionTimestampPrecisions = partitionTimestampPrecisions;
      this.bucketColumns = bucketColumns;
      this.bucketTimestampPrecisions = bucketTimestampPrecisions;
      this.fallbackReason = fallbackReason;
    }

    static Planned fallback(String reason) {
      return new Planned(null, false, null, null, null, null, reason);
    }
  }

  static boolean appliesTo(StreamPhysicalSink sink) {
    return sink.tableSink() instanceof FlinkTableSink;
  }

  static Planned plan(StreamPhysicalSink sink) {
    for (SinkAbilitySpec ability : sink.abilitySpecs()) {
      if (ability instanceof OverwriteSpec) {
        return Planned.fallback("INSERT OVERWRITE is not supported");
      }
    }
    if (sink.upsertMaterialize()) {
      return Planned.fallback(
          "an upsert-materialized sink (SinkUpsertMaterializer) is not natively reproduced");
    }
    FileStoreTable table = resolveTable(sink);
    if (table == null) {
      return Planned.fallback("the sink is not a Paimon data table");
    }
    boolean primaryKey = !table.primaryKeys().isEmpty();
    if (!primaryKey && !ChangelogPlanUtils.isInsertOnly((StreamPhysicalRel) sink.getInput())) {
      return Planned.fallback("an append table takes an insert-only input");
    }
    CoreOptions coreOptions = table.coreOptions();
    Options options = coreOptions.toConfiguration();
    BucketMode bucketMode = table.bucketMode();
    if (bucketMode != BucketMode.HASH_FIXED && bucketMode != BucketMode.BUCKET_UNAWARE) {
      return Planned.fallback("bucket mode " + bucketMode + " is not supported");
    }
    if (primaryKey) {
      String reason = primaryKeyFallbackReason(table, coreOptions, options);
      if (reason != null) {
        return Planned.fallback(reason);
      }
    }
    if (!"parquet".equalsIgnoreCase(coreOptions.fileFormatString())) {
      return Planned.fallback(
          "file.format " + coreOptions.fileFormatString() + " is not supported");
    }
    if (!options.get(CoreOptions.FILE_FORMAT_PER_LEVEL).isEmpty()) {
      return Planned.fallback("file.format.per.level is not supported");
    }
    if (options.get(CoreOptions.WRITE_BUFFER_FOR_APPEND)) {
      return Planned.fallback("write-buffer-for-append is not supported");
    }
    if (!new FileIndexOptions(coreOptions).isEmpty()) {
      return Planned.fallback("file indexes are not supported");
    }
    if (coreOptions.rowTrackingEnabled()) {
      return Planned.fallback("row tracking is not supported");
    }
    if (coreOptions.dataEvolutionEnabled()) {
      return Planned.fallback("data evolution is not supported");
    }
    if (!CoreOptions.blobField(table.options()).isEmpty()) {
      return Planned.fallback("BLOB columns are not supported");
    }
    if (!coreOptions.clusteringColumns().isEmpty()) {
      return Planned.fallback("sink clustering is not supported");
    }
    if (coreOptions.partitionSinkStrategy() == PartitionSinkStrategy.PARTITION_DYNAMIC) {
      return Planned.fallback("partition.sink-strategy PARTITION_DYNAMIC is not supported");
    }
    if (options.get(FlinkConnectorOptions.SINK_WRITER_COORDINATOR_ENABLED)) {
      return Planned.fallback("sink.writer-coordinator.enabled is not supported");
    }
    if (options.get(FlinkConnectorOptions.SINK_COORDINATOR_COMMIT_ENABLED)) {
      return Planned.fallback("sink.coordinator-commit.enabled is not supported");
    }
    RelDataType inputType = sink.getInput().getRowType();
    List<String> fieldNames = table.rowType().getFieldNames();
    if (inputType.getFieldCount() != fieldNames.size()) {
      return Planned.fallback(
          "the sink input has "
              + inputType.getFieldCount()
              + " columns for a table with "
              + fieldNames.size());
    }
    String constraintFallback = SinkConstraintGate.fallbackReason(sink);
    if (constraintFallback != null) {
      return Planned.fallback(constraintFallback);
    }
    String formatFallback = formatFallbackReason(table);
    if (formatFallback != null) {
      return Planned.fallback(formatFallback);
    }
    int[] partitionColumns = ordinals(fieldNames, table.partitionKeys());
    int[] bucketColumns =
        bucketMode == BucketMode.HASH_FIXED
            ? ordinals(fieldNames, table.schema().bucketKeys())
            : new int[0];
    return new Planned(
        table,
        primaryKey,
        partitionColumns,
        FlinkKeyGroupUtils.timestampPrecisions(inputType, partitionColumns),
        bucketColumns,
        FlinkKeyGroupUtils.timestampPrecisions(inputType, bucketColumns),
        null);
  }

  /**
   * The primary-key shapes whose files the native level-0 writer reproduces exactly: rows merged by
   * last arrival (Paimon's deduplicate engine, optionally ignoring deletes), compaction left to the
   * Paimon writer selected for the table's changelog producer, and sequence numbers continued from
   * the committed files. Input changelog shares the native sort and encoding; lookup, full
   * compaction, and deletion vectors retain Paimon's writer lifecycle.
   */
  private static String primaryKeyFallbackReason(
      FileStoreTable table, CoreOptions coreOptions, Options options) {
    if (coreOptions.mergeEngine() != MergeEngine.DEDUPLICATE) {
      return "merge-engine " + coreOptions.mergeEngine() + " is not supported";
    }
    if (coreOptions.changelogProducer() != ChangelogProducer.NONE
        && coreOptions.changelogProducer() != ChangelogProducer.INPUT
        && coreOptions.changelogProducer() != ChangelogProducer.LOOKUP
        && coreOptions.changelogProducer() != ChangelogProducer.FULL_COMPACTION) {
      return "changelog-producer " + coreOptions.changelogProducer() + " is not supported";
    }
    if (coreOptions.changelogProducer() == ChangelogProducer.INPUT
        && coreOptions.changelogFileFormat() != null
        && !"parquet".equalsIgnoreCase(coreOptions.changelogFileFormat())) {
      return "changelog-file.format " + coreOptions.changelogFileFormat() + " is not supported";
    }
    if (coreOptions.primaryKeyVectorIndexEnabled()
        || coreOptions.primaryKeyFullTextIndexEnabled()
        || !coreOptions.primaryKeyBTreeIndexColumns().isEmpty()
        || !coreOptions.primaryKeyBitmapIndexColumns().isEmpty()) {
      return "primary-key indexes are not supported";
    }
    if (!coreOptions.sequenceField().isEmpty()) {
      return "sequence.field is not supported";
    }
    if (coreOptions.rowkindField().isPresent()) {
      return "rowkind.field is not supported";
    }
    if (coreOptions.localMergeEnabled()) {
      return "local-merge-buffer-size is not supported";
    }
    if (coreOptions.dataFileThinMode()) {
      return "data-file.thin-mode is not supported";
    }
    if (coreOptions.dataFileExternalPaths() != null) {
      return "data-file.external-paths is not supported for primary-key tables";
    }
    if (options.get(FlinkConnectorOptions.SINK_KEY_ONLY_DELETES_ENABLED)) {
      return "sink.key-only-deletes.enabled is not supported";
    }
    if (options.get(FlinkConnectorOptions.PRECOMMIT_COMPACT)) {
      return FlinkConnectorOptions.PRECOMMIT_COMPACT.key() + " is not supported";
    }
    if (options.get(CoreOptions.WRITE_SEQUENCE_NUMBER_INIT_MODE) != SequenceNumberInitMode.SCAN) {
      return CoreOptions.WRITE_SEQUENCE_NUMBER_INIT_MODE.key()
          + " "
          + options.get(CoreOptions.WRITE_SEQUENCE_NUMBER_INIT_MODE)
          + " is not supported";
    }
    if (options.get(FlinkConnectorOptions.SINK_USE_MANAGED_MEMORY)) {
      return FlinkConnectorOptions.SINK_USE_MANAGED_MEMORY.key() + " is not supported";
    }
    return PaimonKeyValueLayout.unsupportedKeyReason(table);
  }

  /**
   * Paimon picks the first {@code parquet} format factory on the classpath. Writing natively needs
   * ours to have won that discovery, so a table whose files would still be written by Paimon's own
   * format is declined loudly rather than silently left on the stock path.
   */
  static String formatFallbackReason(FileStoreTable table) {
    Options options = table.coreOptions().toConfiguration();
    FileFormat format = FileFormat.fromIdentifier("parquet", options);
    if (!(format instanceof NativePaimonParquetFormat)) {
      return "Paimon resolved the parquet format to "
          + format.getClass().getName()
          + "; streamfusion-paimon must precede paimon-flink on the classpath"
          + " (deploy it as 01-streamfusion-paimon.jar)";
    }
    NativePaimonParquetFormat nativeFormat = (NativePaimonParquetFormat) format;
    String reason = nativeFormat.nativeWriterFallbackReason(table.rowType());
    if (reason == null && !table.primaryKeys().isEmpty()) {
      CoreOptions core = table.coreOptions();
      String compression = core.fileCompressionPerLevel().getOrDefault(0, core.fileCompression());
      reason = nativeFormat.nativeWriterFallbackReason(table.rowType(), compression);
      if (reason == null && core.changelogProducer() == ChangelogProducer.INPUT) {
        reason =
            nativeFormat.nativeWriterFallbackReason(
                table.rowType(),
                core.changelogFileCompression() == null
                    ? compression
                    : core.changelogFileCompression());
      }
    }
    return reason;
  }

  private static FileStoreTable resolveTable(StreamPhysicalSink sink) {
    ContextResolvedTable resolved = sink.contextResolvedTable();
    ResolvedCatalogBaseTable<?> resolvedTable = resolved.getResolvedTable();
    CatalogBaseTable origin = resolvedTable.getOrigin();
    Map<String, String> options = new HashMap<>(resolvedTable.getOptions());
    options.putAll(PaimonDynamicOptions.forTable(sink, resolved.getIdentifier()));
    FileStoreTable table;
    if (origin instanceof DataCatalogTable) {
      Table paimonTable = ((DataCatalogTable) origin).table();
      if (!(paimonTable instanceof FileStoreTable)) {
        return null;
      }
      table = (FileStoreTable) paimonTable;
    } else if (options.containsKey(CoreOptions.PATH.key())) {
      table =
          FileStoreTableFactory.create(
              CatalogContext.create(Options.fromMap(options), new FlinkFileIOLoader()));
    } else {
      return null;
    }
    return table.copyWithoutTimeTravel(options);
  }

  private static int[] ordinals(List<String> fieldNames, List<String> columns) {
    return columns.stream().mapToInt(fieldNames::indexOf).toArray();
  }

  static RelNode substitute(StreamPhysicalSink sink, PlanContext context) {
    if (!NativeConfig.operatorEnabled("paimonSink")) {
      context.decline(Substitution.disabledReason("paimonSink"));
      return null;
    }
    Planned planned = plan(sink);
    if (planned.fallbackReason != null) {
      context.decline("paimon sink: " + planned.fallbackReason);
      return null;
    }
    return new StreamPhysicalNativePaimonSink(
        sink.getCluster(), sink.getTraitSet(), sink.getInput(), sink.getRowType(), planned);
  }
}
