package io.github.jordepic.streamfusion.planner;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
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
    FlinkChainedProgram<StreamOptimizeContext> program = FlinkStreamProgram.buildProgram(config);
    PhysicalPlanScan scan = new PhysicalPlanScan();
    program.addLast("streamfusion_native", scan);
    config.setPlannerConfig(
        CalciteConfig$.MODULE$.createBuilder().replaceStreamProgram(program).build());
    return scan;
  }
}
