package io.github.jordepic.streamfusion.planner;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.calcite.CalciteConfig$;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkStreamProgram;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;

/**
 * Hooks native execution into the host engine's SQL optimizer. The engine exposes no append-only
 * extension point, so this rebuilds the default streaming optimization chain, adds a native stage
 * at the end, and installs the result as the configured planner program before any query runs.
 */
public final class NativePlanner {

  private NativePlanner() {}

  /**
   * Installs the native optimizer stage on a streaming table environment and returns it.
   *
   * <p>Must be called before the first query, since the planner reads its configuration when it
   * first optimizes.
   */
  public static PhysicalPlanScan install(TableEnvironment tableEnv) {
    TableConfig config = tableEnv.getConfig();
    // Disable sub-plan reuse. The columnar island rests on the "produced fresh, consumed once"
    // invariant — each Arrow batch is handed off to exactly one consumer, which closes its off-heap
    // buffers after reading (ArrowBatchSerializer.copy is therefore a zero-cost identity). Sub-plan
    // reuse breaks that: it makes a shared branch fan its batches to two consumers, the first of
    // which closes the VectorSchemaRoot, leaving the second reading freed memory. Disabling reuse
    // keeps the physical plan a tree (every node has one consumer), so the invariant holds. Reuse
    // and no-reuse compute the same result, so this only affects the execution graph, never output.
    config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, false);
    config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, false);
    FlinkChainedProgram<StreamOptimizeContext> program = FlinkStreamProgram.buildProgram(config);
    PhysicalPlanScan scan = new PhysicalPlanScan();
    program.addLast("streamfusion_native", scan);
    config.setPlannerConfig(
        CalciteConfig$.MODULE$.createBuilder().replaceStreamProgram(program).build());
    return scan;
  }

  /**
   * Installs native execution, then returns Flink's {@code explainSql} output for {@code sql} with a
   * native-acceleration section appended — the optimized physical plan shows the substituted native
   * operators by name, and the appended section lists how many ran natively and why any candidate
   * fell back (ticket 29). A convenience for inspecting whether and why a query accelerates.
   */
  public static String explain(TableEnvironment tableEnv, String sql) {
    PhysicalPlanScan scan = install(tableEnv);
    String plan = tableEnv.explainSql(sql);
    return plan + "\n" + scan.explainSummary();
  }
}
