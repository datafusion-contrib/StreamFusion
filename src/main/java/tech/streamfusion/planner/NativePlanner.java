package tech.streamfusion.planner;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.PlannerConfig;
import org.apache.flink.table.planner.calcite.CalciteConfig;
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

  private static final String PROGRAM_NAME = "streamfusion_native";

  private NativePlanner() {}

  /** Installs the native optimizer stage into a streaming Table configuration. */
  public static PhysicalPlanScan install(TableConfig config) {
    // Sub-plan reuse stays enabled, scoped away from the island by digests: reuse runs after this
    // program's substitution stage and merges subtrees by digest, and every native rel carries a
    // per-instance digest term (NativeRelDigests), so Flink can never merge two native nodes — an
    // Arrow batch is handed to exactly one consumer, which closes its off-heap buffers after
    // reading (ArrowBatchSerializer.copy is a zero-cost identity), and a blindly merged native
    // branch would fan one batch to two consumers, the second reading freed memory. The rowwise
    // plan around the islands merges normally. Where sharing a native subtree IS safe — identical
    // native sources — the substitution stage merges them itself under an explicit share node
    // whose declared consumer count makes every branch take a retained view (PhysicalPlanScan's
    // share pass), so a multi-view query reads and decodes its topic once on the native path too.
    PlannerConfig plannerConfig = config.getPlannerConfig();
    CalciteConfig calciteConfig =
        plannerConfig instanceof CalciteConfig
            ? (CalciteConfig) plannerConfig
            : CalciteConfig$.MODULE$.DEFAULT();
    FlinkChainedProgram<StreamOptimizeContext> program =
        calciteConfig.getStreamProgram().isDefined()
            ? calciteConfig.getStreamProgram().get()
            : FlinkStreamProgram.buildProgram(config);
    if (program.get(PROGRAM_NAME).isDefined()) {
      Object installed = program.get(PROGRAM_NAME).get();
      if (installed instanceof PhysicalPlanScan) {
        return (PhysicalPlanScan) installed;
      }
      throw new IllegalStateException(
          "Planner program name '" + PROGRAM_NAME + "' is already owned by " + installed.getClass());
    }
    PhysicalPlanScan scan = new PhysicalPlanScan();
    program.addLast(PROGRAM_NAME, scan);
    config.setPlannerConfig(
        CalciteConfig$.MODULE$
            .createBuilder(calciteConfig)
            .replaceStreamProgram(program)
            .build());
    return scan;
  }

  /**
   * Installs the native optimizer stage on a streaming table environment and returns it.
   *
   * <p>Must be called before the first query, since the planner reads its configuration when it
   * first optimizes.
   */
  public static PhysicalPlanScan install(TableEnvironment tableEnv) {
    return install(tableEnv.getConfig());
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
