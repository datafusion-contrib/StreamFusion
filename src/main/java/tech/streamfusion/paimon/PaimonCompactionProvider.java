package tech.streamfusion.paimon;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteState;
import org.apache.paimon.flink.sink.StreamFusionStoreSinkWrite;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

/** Installs the rewriter before the stock writer creates or restores any buckets. */
final class PaimonCompactionProvider implements StoreSinkWrite.Provider {
  private static final long serialVersionUID = 1L;
  private final StoreSinkWrite.Provider stock;
  private final boolean streaming, ignorePreviousFiles, waitCompaction;

  private PaimonCompactionProvider(
      StoreSinkWrite.Provider stock,
      boolean streaming,
      boolean ignorePreviousFiles,
      boolean waitCompaction) {
    this.stock = stock;
    this.streaming = streaming;
    this.ignorePreviousFiles = ignorePreviousFiles;
    this.waitCompaction = waitCompaction;
  }

  static StoreSinkWrite.Provider wrap(
      FileStoreTable table,
      StoreSinkWrite.Provider stock,
      boolean streaming,
      boolean ignorePreviousFiles,
      boolean materializer) {
    if (stock instanceof PaimonCompactionProvider) return stock;
    var options = table.coreOptions();
    var configuration = options.toConfiguration();
    if (!EnvironmentInformation.getVersion().startsWith("2.2.")
        || !StreamFusionStoreSinkWrite.available()
        || materializer
        || table.primaryKeys().isEmpty()
        || (table.bucketMode() != BucketMode.HASH_FIXED
            && table.bucketMode() != BucketMode.HASH_DYNAMIC)
        || options.mergeEngine() != CoreOptions.MergeEngine.DEDUPLICATE
        || (options.changelogProducer() != CoreOptions.ChangelogProducer.NONE
            && options.changelogProducer() != CoreOptions.ChangelogProducer.INPUT)
        || options.needLookup()
        || !CoreOptions.blobField(table.options()).isEmpty()
        || configuration.contains(CoreOptions.FULL_COMPACTION_DELTA_COMMITS)
        || configuration.contains(
            FlinkConnectorOptions.CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL))
      return stock;
    boolean waitCompaction = !options.writeOnly() && options.prepareCommitWaitCompaction();
    return new PaimonCompactionProvider(stock, streaming, ignorePreviousFiles, waitCompaction);
  }

  @Override
  public StoreSinkWrite provide(
      FileStoreTable resolved,
      String user,
      StoreSinkWriteState state,
      IOManager io,
      MemoryPoolFactory memory,
      MetricGroup metricGroup) {
    if (!StreamFusionStoreSinkWrite.available())
      return stock.provide(resolved, user, state, io, memory, metricGroup);
    var metrics =
        new NativePaimonCompactRewriter.Counters(
            metricGroup == null ? new UnregisteredMetricsGroup() : metricGroup);
    return StreamFusionStoreSinkWrite.create(
        resolved,
        user,
        state,
        io,
        ignorePreviousFiles,
        waitCompaction,
        streaming,
        memory,
        metricGroup,
        (fresh, write) ->
            write.withCompactRewriterFactory(
                (partition, bucket, original) ->
                    new NativePaimonCompactRewriter(fresh, partition, bucket, original, metrics)));
  }
}
