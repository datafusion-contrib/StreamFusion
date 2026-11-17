package tech.streamfusion.iceberg;

import java.util.Arrays;
import org.apache.flink.util.Collector;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;

/** Refreshes a stale negative branch lookup before any record has been emitted. */
final class IcebergDynamicBranchRecovery {
  private static final String UPDATER = "org.apache.iceberg.flink.sink.dynamic.TableUpdater";
  private static final String PROCESSOR =
      "org.apache.iceberg.flink.sink.dynamic.DynamicRecordProcessor";

  static void collect(Collector<DynamicRecord> collector, DynamicRecord record) throws Exception {
    try {
      collector.collect(record);
    } catch (IllegalArgumentException failure) {
      if (!collector.getClass().getName().equals(PROCESSOR)) throw failure;
      Object updater = IcebergSinkAdapter.field(collector.getClass(), "updater").get(collector);
      if (!refresh(updater, record.tableIdentifier(), record.branch(), failure)) throw failure;
      collector.collect(record);
    }
  }

  static boolean refresh(
      Object updater, TableIdentifier identifier, String branch, IllegalArgumentException failure)
      throws ReflectiveOperationException {
    if (branch == null
        || !updater.getClass().getName().equals(UPDATER)
        || !("Ref " + branch + " already exists").equals(failure.getMessage())
        || Arrays.stream(failure.getStackTrace())
            .noneMatch(frame -> frame.getClassName().equals(UPDATER)
                && frame.getMethodName().equals("findOrCreateBranch"))) return false;
    Catalog catalog = (Catalog) IcebergSinkAdapter.field(updater.getClass(), "catalog").get(updater);
    var table = catalog.loadTable(identifier);
    table.refresh();
    if (!table.refs().containsKey(branch) || !table.refs().get(branch).isBranch()) return false;
    Object cache = IcebergSinkAdapter.field(updater.getClass(), "cache").get(updater);
    var invalidate = cache.getClass().getDeclaredMethod("invalidate", TableIdentifier.class);
    invalidate.setAccessible(true);
    invalidate.invoke(cache, identifier);
    return true;
  }
}
