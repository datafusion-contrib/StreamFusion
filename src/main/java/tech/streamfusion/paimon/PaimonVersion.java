package tech.streamfusion.paimon;

import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RollingFileWriterImpl;
import org.apache.paimon.io.SingleFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.mergetree.compact.aggregate.FieldAggregator;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldAggregatorFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.options.Options;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StatsCollectorFactories;

/** Released API differences used by the shared columnar reader and writer. */
final class PaimonVersion {
  private PaimonVersion() {}

  static boolean supportsPartitionPruningHandoff() {
    return true;
  }

  static boolean legacyOrcTimestamp(Options options) {
    return options.get(org.apache.paimon.format.OrcOptions.ORC_TIMESTAMP_LTZ_LEGACY_TYPE);
  }

  static TableRead withMetrics(TableRead read, MetricRegistry metrics) {
    return read.withMetricRegistry(metrics);
  }

  static boolean hasKeyValueStore(FileStoreTable table) {
    return table instanceof org.apache.paimon.table.PrimaryKeyFileStoreTable;
  }

  static Comparator<InternalRow> keyComparator(FileStoreTable table) {
    return ((org.apache.paimon.table.PrimaryKeyFileStoreTable) table).store().newKeyComparator();
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
    var producer = org.apache.paimon.io.SimpleStatsProducer.fromExtractor(extractor);
    return producer::extract;
  }

  static FieldAggregator fieldAggregator(
      DataType type, String field, String function, CoreOptions options) {
    return FieldAggregatorFactory.create(type, field, function, options);
  }
}
