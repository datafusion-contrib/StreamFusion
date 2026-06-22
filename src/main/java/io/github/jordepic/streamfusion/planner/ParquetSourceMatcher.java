package io.github.jordepic.streamfusion.planner;

import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

/**
 * Recognizes a source the native reader can run: a filesystem connector with the Parquet format,
 * reading from a local path. The read-side mirror of {@link ParquetSinkMatcher}.
 */
final class ParquetSourceMatcher {

  private ParquetSourceMatcher() {}

  static boolean matches(RelNode node) {
    if (!(node instanceof StreamPhysicalTableSourceScan)) {
      return false;
    }
    Map<String, String> options = options((StreamPhysicalTableSourceScan) node);
    return options != null
        && "filesystem".equals(options.get("connector"))
        && "parquet".equals(options.get("format"))
        && ParquetTables.localPath(options.get("path")) != null;
  }

  /** The matched source's input directory as a local filesystem path. */
  static String path(StreamPhysicalTableSourceScan scan) {
    return ParquetTables.localPath(options(scan).get("path"));
  }

  /**
   * The format's {@code utc-timezone} setting (default false), which decides how the host reader
   * interprets timestamp columns; the native reader replays the same conversion.
   */
  static boolean utcTimestamp(StreamPhysicalTableSourceScan scan) {
    return Boolean.parseBoolean(options(scan).getOrDefault("utc-timezone", "false"));
  }

  private static Map<String, String> options(StreamPhysicalTableSourceScan scan) {
    try {
      TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
      if (table == null) {
        return null;
      }
      ResolvedCatalogBaseTable<?> resolved = table.contextResolvedTable().getResolvedTable();
      if (!(resolved instanceof ResolvedCatalogTable)) {
        return null;
      }
      return ((ResolvedCatalogTable) resolved).getOptions();
    } catch (RuntimeException e) {
      return null;
    }
  }
}
