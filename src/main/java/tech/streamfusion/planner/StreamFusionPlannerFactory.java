package tech.streamfusion.planner;

import java.util.Collections;
import java.util.Set;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.planner.delegation.DefaultPlannerFactory;

/**
 * Installs StreamFusion's stream optimizer stage, then delegates planner construction to Flink.
 *
 * <p>This class is instantiated by StreamFusion's planner-loader shim inside Flink's isolated
 * planner classloader. It intentionally composes the stock factory instead of copying Flink's
 * planner implementation.
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
      if (NativeConfig.nativeEnabled()
          && context.getTableConfig().get(ExecutionOptions.RUNTIME_MODE)
              == RuntimeExecutionMode.STREAMING) {
        NativePlanner.install(context.getTableConfig());
      }
    }
    return delegate.create(context);
  }
}
