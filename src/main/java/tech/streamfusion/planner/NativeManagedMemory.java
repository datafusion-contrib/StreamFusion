package tech.streamfusion.planner;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;

/**
 * Declares a native stateful operator's operator-scope managed-memory weight on its transformation,
 * so Flink carves the operator a share of the slot's managed memory. At open the operator reserves
 * that share and passes it to the native side as a state budget (see {@code ManagedMemoryBudget});
 * without a declared weight the computed fraction is zero and the operator runs unaccounted.
 */
public final class NativeManagedMemory {

  private NativeManagedMemory() {}

  public static void declareOperatorWeight(Transformation<?> transformation) {
    if (NativeConfig.memoryAccountingEnabled()) {
      ExecNodeUtil.setManagedMemoryWeight(
          transformation, ((long) NativeConfig.operatorMemoryWeightMb()) << 20);
    }
  }
}
