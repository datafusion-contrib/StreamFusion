package tech.streamfusion.planner;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
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

  /** Whether the table declares no computed column; false if the schema can't be resolved. */
  static boolean noComputedColumns(StreamPhysicalTableSourceScan scan) {
    ResolvedSchema schema = resolvedSchema(scan);
    return schema != null
        && schema.getColumns().stream()
            .noneMatch(column -> column instanceof Column.ComputedColumn);
  }

  /** The matched scan's resolved table schema, or null if it can't be resolved (fail safe). */
  static ResolvedSchema resolvedSchema(StreamPhysicalTableSourceScan scan) {
    try {
      TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
      if (table == null) {
        return null;
      }
      ResolvedCatalogBaseTable<?> resolved = table.contextResolvedTable().getResolvedTable();
      return resolved instanceof ResolvedCatalogTable
          ? ((ResolvedCatalogTable) resolved).getResolvedSchema()
          : null;
    } catch (RuntimeException e) {
      return null;
    }
  }

  /**
   * The matched scan's physical row type (the columns a value decode produces), or null if the
   * schema can't be resolved (fail safe — callers treat null as unsupported).
   */
  static org.apache.flink.table.types.logical.RowType physicalRowType(
      StreamPhysicalTableSourceScan scan) {
    try {
      TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
      if (table == null) {
        return null;
      }
      ResolvedCatalogBaseTable<?> resolved = table.contextResolvedTable().getResolvedTable();
      if (!(resolved instanceof ResolvedCatalogTable)) {
        return null;
      }
      return (org.apache.flink.table.types.logical.RowType)
          ((ResolvedCatalogTable) resolved)
              .getResolvedSchema().toPhysicalRowDataType().getLogicalType();
    } catch (RuntimeException e) {
      return null;
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
