package tech.streamfusion.paimon;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.RollingFileWriter;
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
  static boolean partialUpdateDeleteRemovalNeedsJava() {
    return false;
  }

  private PaimonVersion() {}

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
    return table.store() instanceof org.apache.paimon.KeyValueFileStore;
  }

  static Comparator<InternalRow> keyComparator(FileStoreTable table) {
    return ((org.apache.paimon.KeyValueFileStore) table.store()).newKeyComparator();
  }

  static boolean hasBeforeFiles(DataSplit split) {
    return !split.beforeFiles().isEmpty() || split.beforeDeletionFiles().isPresent();
  }

  static Object writerMetadata(FormatWriter writer) {
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
