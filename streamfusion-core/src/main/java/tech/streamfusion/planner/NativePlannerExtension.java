package tech.streamfusion.planner;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import tech.streamfusion.planner.common.StreamRule;

/** Contributes planner rewrites from an optional connector artifact. */
public interface NativePlannerExtension {

  /**
   * Adds this artifact's substitutions in planner priority order to its private contribution. The
   * rules and source-sharing hook become active only if registration completes successfully.
   */
  void addSubstitutions(List<StreamRule<?>> substitutions);

  /** An optional connector's bounded source, with host discovery and source state retained. */
  default BatchSource planBatchSource(
      org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalTableSourceScan scan,
      org.apache.flink.configuration.ReadableConfig config) {
    return null;
  }

  /** An optional connector's bounded Arrow sink; null retains the host sink. */
  default BatchSink planBatchSink(
      org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSink sink,
      org.apache.flink.configuration.ReadableConfig config) {
    return null;
  }

  interface BatchSource
      extends tech.streamfusion.planner.common.CommonRowDataToArrowExecNode.SourceConversion {}

  @FunctionalInterface
  interface BatchSink {
    org.apache.flink.api.dag.Transformation<?> translate(
        org.apache.flink.table.planner.delegation.PlannerBase planner,
        org.apache.flink.api.dag.Transformation<tech.streamfusion.operator.ArrowBatch> input,
        String description);
  }

  /** Optional columnar file source; null retains the host source and ordinary transpose. */
  default org.apache.flink.api.dag.Transformation<tech.streamfusion.operator.ArrowBatch>
      lowerBatchFileSource(
          org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecTableSourceScan scan,
          org.apache.flink.api.dag.Transformation<org.apache.flink.table.data.RowData> original,
          org.apache.flink.table.types.logical.RowType output,
          int batchRows,
          org.apache.flink.configuration.ReadableConfig config) {
    return null;
  }

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
