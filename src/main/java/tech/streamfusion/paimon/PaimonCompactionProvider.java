package tech.streamfusion.paimon;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

/** Selects the released writer adapter before bucket compaction is submitted. */
@FunctionalInterface
interface PaimonCompactionProvider extends StoreSinkWrite.Provider {

  static StoreSinkWrite.Provider wrap(
      FileStoreTable table,
      StoreSinkWrite.Provider stock,
      boolean streaming,
      boolean ignorePreviousFiles,
      boolean materializer) {
    if (stock instanceof PaimonCompactionProvider) return stock;
    var options = table.coreOptions();
    var configuration = options.toConfiguration();
    if (materializer || PaimonVersion.compactionFeatures(options) != null) return stock;
    if (table.primaryKeys().isEmpty()) {
      if (table.bucketMode() != BucketMode.HASH_FIXED
          || !NativePaimonAppendRewriter.available()
          || !NativePaimonStoreSinkWrite.compactionProviderAvailable()) return stock;
    } else if (!PaimonVersion.compactionProviderAvailable()
        || (table.bucketMode() != BucketMode.HASH_FIXED
            && table.bucketMode() != BucketMode.HASH_DYNAMIC)
        || !NativePaimonCompactRewriter.supportsMergeEngine(options.mergeEngine())
        || (options.changelogProducer() != CoreOptions.ChangelogProducer.NONE
            && options.changelogProducer() != CoreOptions.ChangelogProducer.INPUT)
        || (options.needLookup() && options.mergeEngine() != CoreOptions.MergeEngine.FIRST_ROW)
        || configuration.contains(CoreOptions.FULL_COMPACTION_DELTA_COMMITS)
        || configuration.contains(
            FlinkConnectorOptions.CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL))
      return stock;
    boolean waitCompaction = !options.writeOnly() && options.prepareCommitWaitCompaction();
    return PaimonVersion.compactionProvider(stock, streaming, ignorePreviousFiles, waitCompaction);
  }

  static NativePaimonCompactRewriter.Counters counters(MetricGroup metrics) {
    return new NativePaimonCompactRewriter.Counters(
        metrics == null ? new UnregisteredMetricsGroup() : metrics);
  }
}
