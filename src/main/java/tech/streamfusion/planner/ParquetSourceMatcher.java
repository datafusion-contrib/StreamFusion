package tech.streamfusion.planner;

import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;

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
    Map<String, String> options = FilesystemTables.options((StreamPhysicalTableSourceScan) node);
    return options != null
        && "filesystem".equals(options.get("connector"))
        && "parquet".equals(options.get("format"))
        && FilesystemTables.localPath(options.get("path")) != null;
  }

  /** The matched source's input directory as a local filesystem path. */
  static String path(StreamPhysicalTableSourceScan scan) {
    return FilesystemTables.localPath(FilesystemTables.options(scan).get("path"));
  }

  /**
   * The format's {@code utc-timezone} setting (default false), which decides how the host reader
   * interprets timestamp columns; the native reader replays the same conversion.
   */
  static boolean utcTimestamp(StreamPhysicalTableSourceScan scan) {
    return Boolean.parseBoolean(FilesystemTables.options(scan).getOrDefault("utc-timezone", "false"));
  }

  static RelNode substitute(StreamPhysicalTableSourceScan scan, PlanContext ctx) {
    return new StreamPhysicalNativeParquetSource(
        scan.getCluster(),
        scan.getTraitSet(),
        scan.getRowType(),
        path(scan),
        utcTimestamp(scan));
  }
}
