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
    scan.allowNativeJoinTasks();
  }

  @Override
  public Seq<org.apache.flink.table.planner.plan.nodes.exec.processor.ExecNodeGraphProcessor>
      getExecNodeGraphProcessors() {
    var processors = JavaScalaConversionUtil.toJava(super.getExecNodeGraphProcessors());
    var result =
        new java.util.ArrayList<
            org.apache.flink.table.planner.plan.nodes.exec.processor.ExecNodeGraphProcessor>();
    for (var processor : processors) {
      result.add(
          (graph, context) -> {
            boolean arrowTasks = hasNativeTasks(graph);
            // Host multiple-input fusion hardcodes RowData serializers and cannot contain Arrow
            // tasks.
            if (arrowTasks
                && processor
                    instanceof
                    org.apache.flink.table.planner.plan.nodes.exec.processor
                        .MultipleInputNodeCreationProcessor) return graph;
            var processed = processor.process(graph, context);
            return arrowTasks
                ? BatchArrowGraphBoundaries.preserve(processed, getTableConfig())
                : processed;
          });
    }
    result.add((graph, context) -> BatchArrowSourceConversion.prepare(graph, getTableConfig()));
    return JavaScalaConversionUtil.toScala(result);
  }

  private static boolean hasNativeTasks(
      org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph graph) {
    var pending =
        new java.util.ArrayDeque<org.apache.flink.table.planner.plan.nodes.exec.ExecNode<?>>(
            graph.getRootNodes());
    var visited =
        java.util.Collections.newSetFromMap(
            new java.util.IdentityHashMap<
                org.apache.flink.table.planner.plan.nodes.exec.ExecNode<?>, Boolean>());
    while (!pending.isEmpty()) {
      var node = pending.removeFirst();
      if (!visited.add(node)) continue;
      if (node instanceof BatchColumnarExecNode) return true;
      for (var edge : node.getInputEdges()) pending.add(edge.getSource());
    }
    return false;
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
