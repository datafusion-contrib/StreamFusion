package tech.streamfusion.planner.common;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import tech.streamfusion.planner.NativeConfig;

/** Shared source fan-out requires both native retained views and host subplan reuse. */
public final class SourceReusePolicy {
  private SourceReusePolicy() {}

  public static boolean enabled(RelNode root) {
    var config = ShortcutUtils.unwrapTableConfig(root);
    return NativeConfig.shareSources()
        && config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED)
        && config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED);
  }
}
