package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.RelNode;

/** Contributes planner rewrites from an optional connector artifact. */
public interface NativePlannerExtension {

  /** Adds this artifact's substitutions in planner priority order. */
  void addSubstitutions(List<Substitution<?>> substitutions);

  /** Returns a pre-substitution source-sharing key, or {@code null} for an unrelated node. */
  default String sourceSharingKey(RelNode node) {
    return null;
  }

  /**
   * Whether a substituted source keeps the original connector's reader/coordinator state schema.
   */
  default boolean preservesStockSourceState(
      org.apache.flink.table.planner.plan.nodes.exec.ExecNode<?> source) {
    return false;
  }

  /** Reuses this artifact's source admission policy when loading a portable stock plan. */
  default org.apache.flink.table.planner.plan.nodes.exec.ExecNode<
          tech.streamfusion.operator.ArrowBatch>
      lowerCompiledSource(
          org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan
              scan,
          org.apache.flink.configuration.ReadableConfig config) {
    return null;
  }
}
