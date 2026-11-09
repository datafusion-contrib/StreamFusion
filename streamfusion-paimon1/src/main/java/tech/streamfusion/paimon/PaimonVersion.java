package tech.streamfusion.paimon;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.flink.sink.StoreCompactOperator;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteState;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RowDataRollingFileWriter;
import org.apache.paimon.io.SingleFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.mergetree.compact.aggregate.FieldAggregator;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldAggregatorFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.AppendOnlyFileStoreWrite;
import org.apache.paimon.options.Options;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.StatsCollectorFactories;

/** Released API differences used by the shared columnar reader and writer. */
final class PaimonVersion {
  static boolean compactionSumWraps() {
    return true;
  }

  static boolean partialUpdateDeleteRemovalNeedsJava() {
    return false;
  }

  private PaimonVersion() {}

  static boolean appendWorkerStreaming(StreamOperatorFactory<?> factory, boolean streaming)
      throws ReflectiveOperationException {
    return streaming;
  }

  static boolean supportsAppendWrite(Object write) {
    return write.getClass()
            == org.apache.paimon.operation.AppendOnlyUnawareBucketFileStoreWrite.class
        || write.getClass()
            == org.apache.paimon.operation.AppendOnlyFixedBucketFileStoreWrite.class;
  }

  static java.lang.reflect.Method appendWriterFactory() {
    if (!PaimonAppendSequence.supported()) return null;
    return NativePaimonAppendRewriter.writerFactory(
        AppendOnlyFileStoreWrite.class, LongCounter.class);
  }

  static Object appendSequenceCounter(LongCounter counter) {
    return counter;
  }

  static void writeAppendBundle(
      RowDataRollingFileWriter output, LongCounter counter, ArrowBatchBundle bundle)
      throws Exception {
    if (!PaimonAppendSequence.supported())
      throw new IllegalStateException("Paimon append sequence contract unavailable");
    PaimonAppendSequence.write(counter, bundle.rowCount(), () -> output.writeBundle(bundle));
  }

  static boolean isCompactionFactory(StreamOperatorFactory<?> factory) {
    return factory.getClass() == StoreCompactOperator.Factory.class;
  }

  static StreamOperatorFactory<?> compactionFactory(
      StreamOperatorFactory<?> original,
      FileStoreTable table,
      StoreSinkWrite.Provider provider,
      String user,
      boolean full) {
    return new StoreCompactOperator.Factory(table, provider, user, full);
  }

  static StoreSinkWrite.Provider compactionProvider(
      StoreSinkWrite.Provider stock,
      boolean streaming,
      boolean ignorePreviousFiles,
      boolean waitCompaction) {
    return (PaimonCompactionProvider)
        (resolved, user, state, io, memory, metrics) ->
            (resolved.primaryKeys().isEmpty()
                    ? NativePaimonStoreSinkWrite.compactionProviderAvailable()
                    : compactionProviderAvailable())
                ? compactionWrite(
                    resolved,
                    user,
                    state,
                    io,
                    ignorePreviousFiles,
                    waitCompaction,
                    streaming,
                    memory == null ? null : new MemoryPoolFactory(memory),
                    metrics,
                    PaimonCompactionProvider.counters(metrics))
                : stock.provide(resolved, user, state, io, memory, metrics);
  }

  static boolean compactionProviderAvailable() {
    return NativePaimonStoreSinkWrite.compactionProviderAvailable();
  }

  static StoreSinkWrite compactionWrite(
      FileStoreTable table,
      String user,
      StoreSinkWriteState state,
      IOManager io,
      boolean ignorePreviousFiles,
      boolean waitCompaction,
      boolean streaming,
      MemoryPoolFactory memory,
      MetricGroup metrics,
      NativePaimonCompactRewriter.Counters counters) {
    return NativePaimonStoreSinkWrite.compactionWrite(
        table,
        user,
        state,
        io,
        ignorePreviousFiles,
        waitCompaction,
        streaming,
        memory,
        metrics,
        counters);
  }

  static boolean supportsPartitionPruningHandoff() {
    return false;
  }

  static boolean legacyOrcTimestamp(Options options) {
    return false;
  }

  static TableRead withMetrics(TableRead read, MetricRegistry metrics) {
    return read;
  }

  static boolean hasKeyValueStore(FileStoreTable table) {
    return table.store() instanceof KeyValueFileStore;
  }

  static Comparator<InternalRow> keyComparator(FileStoreTable table) {
    return ((KeyValueFileStore) table.store()).newKeyComparator();
  }

  static boolean hasBeforeFiles(DataSplit split) {
    return !split.beforeFiles().isEmpty() || split.beforeDeletionFiles().isPresent();
  }

  static Object writerMetadata(FormatWriter writer) {
    return null;
  }

  static boolean snapshotSequenceOrdering(CoreOptions options) {
    return false;
  }

  static boolean specialFileMetadata(DataFileMeta file) {
    return false;
  }

  static String compactionFeatures(CoreOptions options) {
    return null;
  }

  static long targetFileRows(CoreOptions options) {
    return Long.MAX_VALUE;
  }

  static String changelogCompression(CoreOptions options) {
    return null;
  }

  static String changelogFormat(CoreOptions options) {
    return null;
  }

  static String externalPath(DataFilePathFactory paths, Path path) {
    return null;
  }

  static RollingFileWriter<InternalRow, DataFileMeta> rollingWriter(
      Supplier<? extends SingleFileWriter<InternalRow, DataFileMeta>> factory,
      long size,
      long rows,
      Runnable closed) {
    return new RollingFileWriter<>(factory, size);
  }

  static DataFileMeta fileMeta(
      String name,
      long size,
      long rows,
      BinaryRow minKey,
      BinaryRow maxKey,
      SimpleStats keyStats,
      SimpleStats valueStats,
      long minSequence,
      long maxSequence,
      long schemaId,
      int level,
      Timestamp time,
      long deletes,
      FileSource source,
      List<String> statsColumns,
      String externalPath) {
    return new DataFileMeta(
        name,
        size,
        rows,
        minKey,
        maxKey,
        keyStats,
        valueStats,
        minSequence,
        maxSequence,
        schemaId,
        level,
        Collections.emptyList(),
        time,
        deletes,
        null,
        source,
        statsColumns,
        externalPath);
  }

  static PaimonFileStats fileStats(
      CoreOptions options, RowType type, int level, boolean changelog, List<String> keyNames) {
    var collectors =
        StatsCollectorFactories.createStatsFactories(options, type.getFieldNames(), keyNames);
    boolean disabled = true;
    for (var collector : SimpleColStatsCollector.create(collectors))
      disabled &= collector instanceof NoneSimpleColStatsCollector;
    String format = options.fileFormatPerLevel().getOrDefault(level, options.fileFormatString());
    if (changelog && changelogFormat(options) != null) format = changelogFormat(options);
    var extractor =
        disabled
            ? null
            : FileFormat.fromIdentifier(format, options.toConfiguration())
                .createStatsExtractor(type, collectors)
                .orElse(null);
    return (io, path, size, metadata) -> {
      if (extractor != null) return extractor.extract(io, path);
      var empty = new SimpleColStats[type.getFieldCount()];
      Arrays.fill(empty, SimpleColStats.NONE);
      return empty;
    };
  }

  static FieldAggregator fieldAggregator(
      DataType type, String field, String function, CoreOptions options) {
    return FieldAggregatorFactory.create(
        type, function, options.fieldAggIgnoreRetract(field), false, options, field);
  }
}
