package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.delegation.InternalPlan;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.optimize.Optimizer;
import org.apache.flink.table.planner.plan.optimize.StreamCommonSubGraphBasedOptimizer;
import org.apache.flink.table.planner.plan.reuse.ScanReuser;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import scala.collection.Seq;

/** Retains Flink's streaming optimizer, with native substitution over all expanded sink roots. */
final class NativeStreamPlanner extends StreamPlanner {
  private final PhysicalPlanScan scan;
  private boolean compilingPortablePlan;
  private SqlPlanOrigins sqlOrigins;

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
  public InternalPlan compilePlan(List<ModifyOperation> operations) {
    compilingPortablePlan = true;
    try {
      return super.compilePlan(operations);
    } finally {
      compilingPortablePlan = false;
    }
  }

  @Override
  public List<org.apache.flink.api.dag.Transformation<?>> translateToPlan(ExecNodeGraph graph) {
    try (NativeConfig.Scope ignored =
        NativeConfig.usePlannerConfig(getTableConfig().getConfiguration())) {
      return super.translateToPlan(runtimeGraph(graph));
    }
  }

  @Override
  public ExecNodeGraph translateToExecNodeGraph(Seq<RelNode> roots, boolean compiled) {
    if (compiled || sqlOrigins == null) return super.translateToExecNodeGraph(roots, compiled);
    var captured = sqlOrigins.captureRuntime(JavaScalaConversionUtil.toJava(roots));
    return sqlOrigins.runtimeGraph(
        super.translateToExecNodeGraph(JavaScalaConversionUtil.toScala(captured), false));
  }

  private ExecNodeGraph stockSqlGraph(List<RelNode> roots) {
    return super.translateToExecNodeGraph(JavaScalaConversionUtil.toScala(roots), false);
  }

  @Override
  public String explainPlan(
      InternalPlan plan, Seq<org.apache.flink.table.api.ExplainDetail> details) {
    try (NativeConfig.Scope ignored =
        NativeConfig.usePlannerConfig(getTableConfig().getConfiguration())) {
      var portable = (org.apache.flink.table.planner.plan.ExecNodeGraphInternalPlan) plan;
      var runtime =
          new org.apache.flink.table.planner.plan.ExecNodeGraphInternalPlan(
              plan::asJsonString, plan::asSmileBytes, runtimeGraph(portable.getExecNodeGraph()));
      return super.explainPlan(runtime, details);
    }
  }

  private ExecNodeGraph runtimeGraph(ExecNodeGraph graph) {
    if (graph instanceof StockCompiledPlanLowering.RuntimeGraph) return graph;
    if (graph instanceof SqlPlanOrigins.RuntimeGraph)
      return ordinaryRuntimeGraph((SqlPlanOrigins.RuntimeGraph) graph);
    resetInterop();
    if (!NativeConfig.nativeEnabled() || StockCompiledPlanLowering.containsNativeNodes(graph)) {
      return graph;
    }
    var stockTransformations = super.translateToPlan(graph);
    var executor = new org.apache.flink.table.planner.delegation.DefaultExecutor(getExecEnv());
    var stockStreamGraph =
        (org.apache.flink.streaming.api.graph.StreamGraph)
            executor.createPipeline(
                stockTransformations, getTableConfig().getConfiguration(), null);
    var stockJobGraph = stockStreamGraph.getJobGraph(getFlinkContext().getClassLoader());
    var lowering = new StockCompiledPlanLowering(this, stockStreamGraph, stockJobGraph);
    var runtimeGraph = lowering.lower(graph);
    if (!StockCompiledPlanLowering.containsNativeNodes(runtimeGraph)) return graph;
    var runtimeTransformations = super.translateToPlan(runtimeGraph);
    lowering.configureOperators();
    var runtimeStreamGraph =
        (org.apache.flink.streaming.api.graph.StreamGraph)
            executor.createPipeline(
                runtimeTransformations, getTableConfig().getConfiguration(), null);
    lowering.configureStateBackend(runtimeStreamGraph);
    tech.streamfusion.state.CanonicalCheckpointStorageFactory.install(
        getExecEnv(),
        getTableConfig().getConfiguration(),
        lowering.identityProjection(stockStreamGraph, runtimeStreamGraph, runtimeGraph));
    return runtimeGraph;
  }

  private ExecNodeGraph ordinaryRuntimeGraph(SqlPlanOrigins.RuntimeGraph graph) {
    resetInterop();
    if (!NativeConfig.nativeEnabled()) return graph;
    if (!StockCompiledPlanLowering.containsNativeNodes(graph))
      return runtimeGraph(new ExecNodeGraph(graph.getFlinkVersion(), graph.getRootNodes()));
    if (!StockCompiledPlanLowering.canBindOrdinary(graph, getFlinkContext().getClassLoader())) {
      // State contracts with internal helper roles bind from the original stock template.
      return StockCompiledPlanLowering.requiresStockTemplate(graph)
          ? runtimeGraph(graph.stock)
          : graph;
    }
    var executor = new org.apache.flink.table.planner.delegation.DefaultExecutor(getExecEnv());
    var stockTransformations = super.translateToPlan(graph.stock);
    var stockStreamGraph =
        (org.apache.flink.streaming.api.graph.StreamGraph)
            executor.createPipeline(
                stockTransformations, getTableConfig().getConfiguration(), null);
    var lowering =
        new StockCompiledPlanLowering(
            this,
            stockStreamGraph,
            stockStreamGraph.getJobGraph(getFlinkContext().getClassLoader()));
    ExecNodeGraph bound = lowering.bindOrdinary(graph);
    if (bound == null)
      return StockCompiledPlanLowering.requiresStockTemplate(graph)
          ? runtimeGraph(graph.stock)
          : graph;
    var transformations = super.translateToPlan(bound);
    lowering.configureOperators();
    var runtimeStreamGraph =
        (org.apache.flink.streaming.api.graph.StreamGraph)
            executor.createPipeline(transformations, getTableConfig().getConfiguration(), null);
    lowering.configureStateBackend(runtimeStreamGraph);
    tech.streamfusion.state.CanonicalCheckpointStorageFactory.install(
        getExecEnv(),
        getTableConfig().getConfiguration(),
        lowering.identityProjection(stockStreamGraph, runtimeStreamGraph, bound));
    return bound;
  }

  private void resetInterop() {
    tech.streamfusion.state.CanonicalStateBackend.reset(
        getExecEnv(), getTableConfig().getConfiguration());
    tech.streamfusion.state.CanonicalCheckpointStorageFactory.reset(
        getTableConfig().getConfiguration());
  }

  @Override
  public Optimizer getOptimizer() {
    return new StreamCommonSubGraphBasedOptimizer(this) {
      @Override
      public Seq<RelNode> postOptimize(Seq<RelNode> expanded) {
        resetInterop();
        sqlOrigins = null;
        List<RelNode> roots = JavaScalaConversionUtil.toJava(super.postOptimize(expanded));
        if (roots.isEmpty()) {
          return expanded;
        }
        try (NativeConfig.Scope ignored =
            NativeConfig.usePlannerConfig(getTableConfig().getConfiguration())) {
          if (NativeConfig.nativeEnabled() && !compilingPortablePlan) {
            var shuttle = new org.apache.flink.table.planner.plan.utils.SameRelObjectShuttle();
            var copied =
                roots.stream()
                    .map(root -> root.accept(shuttle))
                    .collect(java.util.stream.Collectors.toList());
            roots =
                JavaScalaConversionUtil.toJava(
                    org.apache.flink.table.planner.plan.reuse.SubplanReuser.reuseDuplicatedSubplan(
                        JavaScalaConversionUtil.toScala(copied),
                        getTableConfig().getConfiguration(),
                        ShortcutUtils.unwrapContext(roots.get(0)),
                        ShortcutUtils.unwrapTypeFactory(roots.get(0))));
            sqlOrigins = new SqlPlanOrigins();
            sqlOrigins.stockGraph(stockSqlGraph(sqlOrigins.captureStock(roots)));
            roots = sqlOrigins.mark(roots);
          }
          if (NativeConfig.nativeEnabled() && PhysicalPlanScan.sourceSharingEnabled(roots.get(0))) {
            // Union source projections while Flink's scan/ability contracts are still visible.
            // Its final subplan reuse pass then remerges only our explicitly retained Arrow edges.
            roots =
                new ScanReuser(
                        ShortcutUtils.unwrapContext(roots.get(0)),
                        ShortcutUtils.unwrapTypeFactory(roots.get(0)))
                    .reuseDuplicatedScan(roots);
          }
          if (compilingPortablePlan) {
            return JavaScalaConversionUtil.toScala(roots);
          }
          return JavaScalaConversionUtil.toScala(scan.optimizeRoots(roots));
        }
      }
    };
  }
}
