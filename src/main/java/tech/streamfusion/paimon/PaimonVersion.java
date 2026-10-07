package tech.streamfusion.paimon;

import java.util.Comparator;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.mergetree.compact.aggregate.FieldAggregator;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldAggregatorFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataType;

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

  static Object writerMetadata(FormatWriter writer) {
    return writer.writerMetadata();
  }

  static FieldAggregator fieldAggregator(
      DataType type, String field, String function, CoreOptions options) {
    return FieldAggregatorFactory.create(type, field, function, options);
  }
}
