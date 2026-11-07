package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.RelNode;

/** Contributes planner rewrites from an optional connector artifact. */
public interface NativePlannerExtension {

  /**
   * Adds this artifact's substitutions in planner priority order to its private contribution.
   * The rules and source-sharing hook become active only if registration completes successfully.
   */
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

  /** Whether a substituted sink keeps the original writer/committer state contract. */
  default boolean preservesStockSinkState(
      org.apache.flink.table.planner.plan.nodes.exec.ExecNode<?> sink) {
    return false;
  }

  /** Synchronous representation operators with no durable or buffered work at a barrier. */
  default java.util.Set<Class<?>> canonicalStatelessOperators() {
    return java.util.Set.of();
  }

  /** Reuses the sink admission policy for a portable stock plan's physical table contract. */
  default org.apache.flink.table.planner.plan.nodes.exec.ExecNode<?> lowerCompiledSink(
      org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink sink,
      org.apache.flink.configuration.ReadableConfig config,
      org.apache.flink.table.connector.ChangelogMode inputMode) {
    return null;
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
