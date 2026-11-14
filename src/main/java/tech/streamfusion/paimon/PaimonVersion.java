package tech.streamfusion.paimon;

import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.flink.sink.StoreCompactOperator;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteState;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.OrcOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RollingFileWriterImpl;
import org.apache.paimon.io.RowDataRollingFileWriter;
import org.apache.paimon.io.SimpleStatsProducer;
import org.apache.paimon.io.SingleFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.mergetree.compact.aggregate.FieldAggregator;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldAggregatorFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.options.Options;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.StatsCollectorFactories;

/** Released API differences used by the shared columnar reader and writer. */
final class PaimonVersion {
  private PaimonVersion() {}

  static org.apache.flink.api.connector.source.Source<
          tech.streamfusion.operator.ArrowBatch,
          org.apache.paimon.flink.source.FileStoreSourceSplit,
          org.apache.paimon.flink.source.PendingSplitsCheckpoint>
      withSourceLineage(NativePaimonSource source, FileStoreTable table) {
    return PaimonApiCompat.withSourceLineage(source, table);
  }

  static boolean compactionSumWraps() {
    return PaimonApiCompat.compactionSumWraps();
  }

  static boolean appendWorkerStreaming(StreamOperatorFactory<?> factory, boolean streaming)
      throws ReflectiveOperationException {
    return NativePaimonMaintenance.field(factory.getClass(), "isStreaming").getBoolean(factory);
  }

  static boolean supportsAppendWrite(Object write) {
    return write.getClass() == org.apache.paimon.operation.AppendFileStoreWrite.class
        || write.getClass() == org.apache.paimon.operation.BucketedAppendFileStoreWrite.class;
  }

  static java.lang.reflect.Method appendWriterFactory() {
    return NativePaimonAppendRewriter.writerFactory(
        BaseAppendFileStoreWrite.class, java.util.function.Supplier.class);
  }

  static Object appendSequenceCounter(LongCounter counter) {
    return (java.util.function.Supplier<LongCounter>) () -> new LongCounter(counter.getValue());
  }

  static void writeAppendBundle(
      RowDataRollingFileWriter output, LongCounter counter, ArrowBatchBundle bundle)
      throws Exception {
    output.writeBundle(bundle);
  }

  static boolean isCompactionFactory(StreamOperatorFactory<?> factory) {
    return factory.getClass() == StoreCompactOperator.Factory.class
        || factory.getClass() == StoreCompactOperator.CoordinatedFactory.class;
  }

  static StreamOperatorFactory<?> compactionFactory(
      StreamOperatorFactory<?> original,
      FileStoreTable table,
      StoreSinkWrite.Provider provider,
      String user,
      boolean full) {
    return original.getClass() == StoreCompactOperator.CoordinatedFactory.class
        ? new StoreCompactOperator.CoordinatedFactory(table, provider, user, full)
        : new StoreCompactOperator.Factory(table, provider, user, full);
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
                    memory,
                    metrics,
                    PaimonCompactionProvider.counters(metrics))
                : stock.provide(resolved, user, state, io, memory, metrics);
  }

  static boolean compactionProviderAvailable() {
    return PaimonApiCompat.compactionProviderAvailable();
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
    return PaimonApiCompat.compactionWrite(
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
    return true;
  }

  static boolean legacyOrcTimestamp(Options options) {
    return options.get(OrcOptions.ORC_TIMESTAMP_LTZ_LEGACY_TYPE);
  }

  static TableRead withMetrics(TableRead read, MetricRegistry metrics) {
    return read.withMetricRegistry(metrics);
  }

  static boolean hasKeyValueStore(FileStoreTable table) {
    return table instanceof PrimaryKeyFileStoreTable;
  }

  static Comparator<InternalRow> keyComparator(FileStoreTable table) {
    return ((PrimaryKeyFileStoreTable) table).store().newKeyComparator();
  }

  static boolean hasBeforeFiles(DataSplit split) {
    return false;
  }

  static boolean partialUpdateDeleteRemovalNeedsJava() {
    return PaimonApiCompat.retainsPartialDelete();
  }

  static Object writerMetadata(FormatWriter writer) {
    return writer.writerMetadata();
  }

  static boolean snapshotSequenceOrdering(CoreOptions options) {
    return options.snapshotSequenceOrdering();
  }

  static boolean specialFileMetadata(DataFileMeta file) {
    return file.firstRowId() != null || file.writeCols() != null;
  }

  static String compactionFeatures(CoreOptions options) {
    if (options.rowTrackingEnabled()) return "row-tracking";
    if (options.dataEvolutionEnabled()) return "data-evolution";
    if (!CoreOptions.blobField(options.toConfiguration().toMap()).isEmpty()) return "blob-fields";
    if (options.primaryKeyVectorIndexEnabled()
        || options.primaryKeyFullTextIndexEnabled()
        || !options.primaryKeyBTreeIndexColumns().isEmpty()
        || !options.primaryKeyBitmapIndexColumns().isEmpty()
        || PaimonApiCompat.extraIndexes(options)) return "file-index";
    return null;
  }

  static long targetFileRows(CoreOptions options) {
    return options.targetFileRowNum();
  }

  static String changelogCompression(CoreOptions options) {
    return options.changelogFileCompression();
  }

  static String changelogFormat(CoreOptions options) {
    return options.changelogFileFormat();
  }

  static String externalPath(DataFilePathFactory paths, Path path) {
    return paths.isExternalPath() ? path.toString() : null;
  }

  static RollingFileWriter<InternalRow, DataFileMeta> rollingWriter(
      Supplier<? extends SingleFileWriter<InternalRow, DataFileMeta>> factory,
      long size,
      long rows,
      Runnable closed) {
    return new RollingFileWriterImpl<InternalRow, DataFileMeta>(factory, size, rows) {
      @Override
      protected void closeCurrentWriter() throws java.io.IOException {
        super.closeCurrentWriter();
        closed.run();
      }
    };
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
    return PaimonApiCompat.fileMeta(
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
        time,
        deletes,
        source,
        statsColumns,
        externalPath);
  }

  static PaimonFileStats fileStats(
      CoreOptions options, RowType type, int level, boolean changelog, List<String> keyNames) {
    String mode =
        changelog && options.changelogFileStatsMode() != null
            ? options.changelogFileStatsMode()
            : options.statsModePerLevel().getOrDefault(level, options.statsMode());
    var collectors =
        StatsCollectorFactories.createStatsFactories(mode, options, type.getFieldNames(), keyNames);
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
    var producer = SimpleStatsProducer.fromExtractor(extractor);
    return producer::extract;
  }

  static FieldAggregator fieldAggregator(
      DataType type, String field, String function, CoreOptions options) {
    return FieldAggregatorFactory.create(type, field, function, options);
  }
}
