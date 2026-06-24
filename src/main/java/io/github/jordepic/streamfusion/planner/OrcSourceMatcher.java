package io.github.jordepic.streamfusion.planner;

import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;

/**
 * Recognizes a source the native reader can run: a filesystem connector with the ORC format, reading
 * from a local path. ORC files are self-describing, so unlike a text format no schema is passed to the
 * reader — it derives the columns from each file.
 */
final class OrcSourceMatcher {

  private OrcSourceMatcher() {}

  static boolean matches(RelNode node) {
    if (!(node instanceof StreamPhysicalTableSourceScan)) {
      return false;
    }
    Map<String, String> options = FilesystemTables.options((StreamPhysicalTableSourceScan) node);
    return options != null
        && "filesystem".equals(options.get("connector"))
        && "orc".equals(options.get("format"))
        && FilesystemTables.localPath(options.get("path")) != null;
  }

  /** The matched source's input directory as a local filesystem path. */
  static String path(StreamPhysicalTableSourceScan scan) {
    return FilesystemTables.localPath(FilesystemTables.options(scan).get("path"));
  }
}
