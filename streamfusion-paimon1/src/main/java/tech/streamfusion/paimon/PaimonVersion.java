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

  static FieldAggregator fieldAggregator(
      DataType type, String field, String function, CoreOptions options) {
    return FieldAggregatorFactory.create(
        type, function, options.fieldAggIgnoreRetract(field), false, options, field);
  }
}
