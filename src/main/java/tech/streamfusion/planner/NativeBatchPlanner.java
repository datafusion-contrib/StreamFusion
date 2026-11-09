package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.planner.delegation.BatchPlanner;
import org.apache.flink.table.planner.plan.optimize.BatchCommonSubGraphBasedOptimizer;
import org.apache.flink.table.planner.plan.optimize.Optimizer;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import scala.collection.Seq;

/** Retains Flink's batch planning and substitutes native computation over expanded sink roots. */
final class NativeBatchPlanner extends BatchPlanner {
  private final BatchPlanScan scan;
  private boolean compilingPlan;

  NativeBatchPlanner(PlannerFactory.Context context) {
    super(
        context.getExecutor(),
        context.getTableConfig(),
        context.getModuleManager(),
        context.getFunctionCatalog(),
        context.getCatalogManager(),
        context.getClassLoader());
    scan = NativePlanner.installBatch(context.getTableConfig());
    scan.deferToCompletePlan();
  }

  @Override
  public org.apache.flink.table.delegation.InternalPlan compilePlan(
      List<org.apache.flink.table.operations.ModifyOperation> operations) {
    compilingPlan = true;
    try {
      return super.compilePlan(operations);
    } finally {
      compilingPlan = false;
    }
  }

  @Override
  public Optimizer getOptimizer() {
    return new BatchCommonSubGraphBasedOptimizer(this) {
      @Override
      public Seq<RelNode> postOptimize(Seq<RelNode> expanded) {
        List<RelNode> roots = JavaScalaConversionUtil.toJava(super.postOptimize(expanded));
        if (compilingPlan) return JavaScalaConversionUtil.toScala(roots);
        try (NativeConfig.Scope ignored =
            NativeConfig.usePlannerConfig(getTableConfig().getConfiguration())) {
          return JavaScalaConversionUtil.toScala(scan.optimizeRoots(roots));
        }
      }
    };
  }
}
