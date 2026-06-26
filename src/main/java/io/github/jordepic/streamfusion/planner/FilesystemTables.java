package io.github.jordepic.streamfusion.planner;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

/** Shared handling of a filesystem table's {@code path} option for the native file sources/sink. */
final class FilesystemTables {

  private FilesystemTables() {}

  /** The matched scan's connector options (connector/format/path/…), or null if unavailable. */
  static Map<String, String> options(StreamPhysicalTableSourceScan scan) {
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

  /**
   * Whether every column of the matched scan's table is physical — i.e. there are no metadata or
   * computed columns. A native value decode produces only the physical columns straight from the
   * message, so a table that adds metadata/computed columns (e.g. CDC source timestamps) must fall back
   * to Flink, which fills those columns. Returns false if the schema can't be resolved (fail safe).
   */
  static boolean allPhysicalColumns(StreamPhysicalTableSourceScan scan) {
    try {
      TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
      if (table == null) {
        return false;
      }
      ResolvedCatalogBaseTable<?> resolved = table.contextResolvedTable().getResolvedTable();
      if (!(resolved instanceof ResolvedCatalogTable)) {
        return false;
      }
      return ((ResolvedCatalogTable) resolved)
          .getResolvedSchema().getColumns().stream().allMatch(Column::isPhysical);
    } catch (RuntimeException e) {
      return false;
    }
  }

  /**
   * A raw {@code path} option as a local filesystem path, or null if it is not local. The native file
   * readers/writer work on local files, so a {@code file:} URI is reduced to its path and remote
   * schemes (e.g. {@code hdfs:}/{@code s3:}) are rejected.
   */
  static String localPath(String raw) {
    if (raw == null) {
      return null;
    }
    URI uri;
    try {
      uri = new URI(raw);
    } catch (URISyntaxException e) {
      return raw; // not a URI — treat as a plain filesystem path
    }
    String scheme = uri.getScheme();
    if (scheme == null) {
      return raw;
    }
    if (!"file".equals(scheme)) {
      return null;
    }
    String local = uri.getPath();
    return local.length() > 1 && local.endsWith("/")
        ? local.substring(0, local.length() - 1)
        : local;
  }
}
