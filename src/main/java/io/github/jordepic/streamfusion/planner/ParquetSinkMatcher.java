package io.github.jordepic.streamfusion.planner;

import java.util.Map;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;

/**
 * Recognizes a sink the native writer can run: a filesystem connector with the Parquet format,
 * writing to a local path. The native sink writes Arrow batches to local Parquet files directly, so
 * it stands in only for that connector, format, and a {@code file:} (or scheme-less) path; any other
 * sink — including remote filesystems the native writer does not speak — is left to the host.
 */
final class ParquetSinkMatcher {

  private ParquetSinkMatcher() {}

  static boolean matches(StreamPhysicalSink sink) {
    Map<String, String> options = options(sink);
    return options != null
        && "filesystem".equals(options.get("connector"))
        && "parquet".equals(options.get("format"))
        && FilesystemTables.localPath(options.get("path")) != null;
  }

  /** The matched sink's output directory as a local filesystem path. */
  static String path(StreamPhysicalSink sink) {
    return FilesystemTables.localPath(options(sink).get("path"));
  }

  private static Map<String, String> options(StreamPhysicalSink sink) {
    // Not every sink is a catalog table — `collect()`, `print`, and anonymous sinks reach here too,
    // so resolve options defensively and treat anything unexpected as "not ours" (fall back).
    try {
      ContextResolvedTable context = sink.contextResolvedTable();
      if (context == null) {
        return null;
      }
      ResolvedCatalogBaseTable<?> resolved = context.getResolvedTable();
      if (!(resolved instanceof ResolvedCatalogTable)) {
        return null;
      }
      return ((ResolvedCatalogTable) resolved).getOptions();
    } catch (RuntimeException e) {
      return null;
    }
  }
}
