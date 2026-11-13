package tech.streamfusion.planner.batch;

import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory$;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.planner.ColumnarInput;
import tech.streamfusion.planner.NativePlannerExtension;
import tech.streamfusion.planner.common.NativePlanDetails;

/** Bounded connector sinks consume the same Arrow representation as native batch computation. */
final class BatchPhysicalNativeConnectorSink extends BatchPhysicalNativeSingleRel
    implements ColumnarInput {
  private final NativePlannerExtension.BatchSink sink;

  BatchPhysicalNativeConnectorSink(
      RelNode original, RelNode input, NativePlannerExtension.BatchSink sink) {
    this(original, input, sink, NativePlanDetails.from(original));
  }

  private BatchPhysicalNativeConnectorSink(
      RelNode original,
      RelNode input,
      NativePlannerExtension.BatchSink sink,
      NativePlanDetails details) {
    super(original, input, details);
    this.sink = sink;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    var result = new BatchPhysicalNativeConnectorSink(this, inputs.get(0), sink, details);
    result.traitSet = traits;
    return result;
  }

  @Override
  public ExecNode<?> translateToExecNode() {
    return new Sink(
        ShortcutUtils.unwrapTableConfig(this),
        FlinkTypeFactory$.MODULE$.toLogicalRowType(getRowType()),
        getRelDetailedDescription(),
        sink);
  }

  private static final class Sink extends ExecNodeBase<Object>
      implements BatchExecNode<Object>,
          SingleTransformationTranslator<Object>,
          BatchColumnarExecNode {
    private final NativePlannerExtension.BatchSink sink;

    Sink(
        ReadableConfig config,
        RowType output,
        String description,
        NativePlannerExtension.BatchSink sink) {
      super(
          ExecNodeContext.newNodeId(),
          new ExecNodeContext("batch-exec-native-connector-sink_1"),
          config,
          Collections.singletonList(InputProperty.DEFAULT),
          output,
          description);
      this.sink = sink;
    }

    @Override
    public boolean outputsArrow() {
      return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<Object> translateToPlanInternal(
        PlannerBase planner, ExecNodeConfig config) {
      var input = (Transformation<ArrowBatch>) getInputEdges().get(0).translateToPlan(planner);
      return (Transformation<Object>) sink.translate(planner, input, getDescription());
    }
  }
}
