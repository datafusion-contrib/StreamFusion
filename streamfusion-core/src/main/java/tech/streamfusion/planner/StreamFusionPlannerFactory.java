package tech.streamfusion.planner;

import java.util.Collections;
import java.util.Set;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.planner.delegation.DefaultPlannerFactory;
import tech.streamfusion.planner.batch.NativeBatchPlanner;
import tech.streamfusion.planner.stream.NativeStreamPlanner;

/**
 * Installs native substitution after Flink expands the complete SQL plan.
 *
 * <p>This class is instantiated by StreamFusion's planner-loader shim inside Flink's isolated
 * planner classloader. Both execution modes retain Flink's planners with mode-specific
 * post-optimization hooks; disabled acceleration uses its stock factory.
 */
public final class StreamFusionPlannerFactory implements PlannerFactory {

  private final DefaultPlannerFactory delegate = new DefaultPlannerFactory();

  @Override
  public String factoryIdentifier() {
    return delegate.factoryIdentifier();
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.emptySet();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Collections.emptySet();
  }

  @Override
  public Planner create(Context context) {
    try (NativeConfig.Scope ignored =
        NativeConfig.usePlannerConfig(context.getTableConfig().getConfiguration())) {
      if (NativeConfig.nativeEnabled()) {
        RuntimeExecutionMode mode = context.getTableConfig().get(ExecutionOptions.RUNTIME_MODE);
        if (mode == RuntimeExecutionMode.STREAMING) return createStreamingPlanner(context);
        if (mode == RuntimeExecutionMode.BATCH) return createBatchPlanner(context);
      }
    }
    return delegate.create(context);
  }

  /** Also used by the upstream SQL harness to exercise the deployed planner construction path. */
  public static Planner createStreamingPlanner(Context context) {
    return new NativeStreamPlanner(context);
  }

  public static Planner createBatchPlanner(Context context) {
    return new NativeBatchPlanner(context);
  }
}
