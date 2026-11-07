package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.table.planner.plan.optimize.Optimizer;
import org.apache.flink.table.planner.plan.optimize.StreamCommonSubGraphBasedOptimizer;
import org.apache.flink.table.planner.plan.reuse.ScanReuser;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import scala.collection.Seq;

/** Retains Flink's streaming optimizer, with native substitution over all expanded sink roots. */
final class NativeStreamPlanner extends StreamPlanner {
  private final PhysicalPlanScan scan;

  NativeStreamPlanner(PlannerFactory.Context context) {
    super(
        context.getExecutor(),
        context.getTableConfig(),
        context.getModuleManager(),
        context.getFunctionCatalog(),
        context.getCatalogManager(),
        context.getClassLoader());
    scan = NativePlanner.install(context.getTableConfig());
    scan.executionEnvironment(getExecEnv());
    scan.deferToCompletePlan();
  }

  @Override
  public Optimizer getOptimizer() {
    return new StreamCommonSubGraphBasedOptimizer(this) {
      @Override
      public Seq<RelNode> postOptimize(Seq<RelNode> expanded) {
        List<RelNode> roots = JavaScalaConversionUtil.toJava(super.postOptimize(expanded));
        if (roots.isEmpty()) {
          return expanded;
        }
        try (NativeConfig.Scope ignored =
            NativeConfig.usePlannerConfig(getTableConfig().getConfiguration())) {
          if (NativeConfig.nativeEnabled() && PhysicalPlanScan.sourceSharingEnabled(roots.get(0))) {
            // Union source projections while Flink's scan/ability contracts are still visible.
            // Its final subplan reuse pass then remerges only our explicitly retained Arrow edges.
            roots =
                new ScanReuser(
                        ShortcutUtils.unwrapContext(roots.get(0)),
                        ShortcutUtils.unwrapTypeFactory(roots.get(0)))
                    .reuseDuplicatedScan(roots);
          }
          return JavaScalaConversionUtil.toScala(scan.optimizeRoots(roots));
        }
      }
    };
  }
}
