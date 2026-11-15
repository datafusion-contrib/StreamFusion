package org.apache.iceberg.flink.maintenance.api;

import java.util.function.Supplier;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.maintenance.operator.CollectingSink;
import org.apache.iceberg.flink.maintenance.operator.ManualSource;
import tech.streamfusion.iceberg.IcebergRewriteUpstreamTestHooks;

/** Inherits the published streaming tests and their assertions without copying them. */
class IcebergNativeRewriteDataFilesUpstreamTest extends TestRewriteDataFiles {
  private org.apache.iceberg.Table witnessTable;
  private java.util.Set<String> originalFiles;

  @Override
  protected void dropTable() {
    try {
      IcebergRewriteUpstreamTestHooks.assertNewFiles(witnessTable, originalFiles, true);
    } catch (Exception failure) {
      throw new RuntimeException("Missing native work before injected planner failure", failure);
    }
    super.dropTable();
  }

  @Override
  void runAndWaitForResult(
      StreamExecutionEnvironment env,
      ManualSource<Trigger> source,
      CollectingSink<TaskResult> sink,
      boolean generateFailure,
      Supplier<Boolean> condition,
      boolean success)
      throws Exception {
    try (var loader = tableLoader()) {
      loader.open();
      witnessTable = loader.loadTable();
      originalFiles = IcebergRewriteUpstreamTestHooks.paths(witnessTable);
      IcebergRewriteUpstreamTestHooks.install(env);
      super.runAndWaitForResult(env, source, sink, generateFailure, condition, success);
      if (!generateFailure)
        IcebergRewriteUpstreamTestHooks.assertNewFiles(witnessTable, originalFiles, true);
    }
  }
}
