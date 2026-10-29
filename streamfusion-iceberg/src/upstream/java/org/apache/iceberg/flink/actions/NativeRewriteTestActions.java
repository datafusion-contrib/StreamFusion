package org.apache.iceberg.flink.actions;

import java.util.List;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import tech.streamfusion.iceberg.IcebergRewriteUpstreamTestHooks;

/** Keeps the released action's planning, options and optimistic commit implementation. */
final class NativeRewriteTestActions {
  private final Table table;
  private final boolean nativeMode;

  private NativeRewriteTestActions(Table table, boolean nativeMode) {
    this.table = table;
    this.nativeMode = nativeMode;
  }

  static NativeRewriteTestActions forTable(Table table, boolean nativeMode) {
    return new NativeRewriteTestActions(table, nativeMode);
  }

  RewriteDataFilesAction rewriteDataFiles() {
    var environment = StreamExecutionEnvironment.getExecutionEnvironment(Actions.CONFIG);
    environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
    if (!nativeMode) return Actions.forTable(environment, table).rewriteDataFiles();
    return new RewriteDataFilesAction(environment, table) {
      @Override
      protected List<DataFile> rewriteDataForTasks(List<CombinedScanTask> tasks) {
        if (tasks.stream()
            .flatMap(t -> t.files().stream())
            .anyMatch(t -> t.file().format() != org.apache.iceberg.FileFormat.PARQUET)) {
          return super.rewriteDataForTasks(tasks);
        }
        try {
          return IcebergRewriteUpstreamTestHooks.rewrite(environment, table(), spec(), tasks);
        } catch (Exception failure) {
          throw new RuntimeException("Native upstream rewrite failed", failure);
        }
      }
    };
  }
}
