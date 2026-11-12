package tech.streamfusion.planner.batch;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import tech.streamfusion.planner.common.CommonArrowToRowDataExecNode;
import tech.streamfusion.planner.common.CommonRowDataToArrowExecNode;

/**
 * Shares stateless transformation construction without exposing streaming nodes to batch
 * processors.
 */
final class BatchNativeExecNode<T> extends ExecNodeBase<T>
    implements BatchExecNode<T>, SingleTransformationTranslator<T>, BatchColumnarExecNode {
  private final ExecNode<T> delegate;

  BatchNativeExecNode(ReadableConfig config, ExecNode<T> transformation) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("batch-exec-native_1"),
        config,
        transformation.getInputProperties(),
        transformation.getOutputType(),
        transformation.getDescription());
    this.delegate = transformation;
  }

  @Override
  public boolean outputsArrow() {
    return !(delegate instanceof CommonArrowToRowDataExecNode);
  }

  boolean acceptsNativeFileSource() {
    return delegate instanceof CommonRowDataToArrowExecNode
        && ((CommonRowDataToArrowExecNode) delegate).acceptsNativeFileSource();
  }

  void allowNativeFileSource(BatchArrowSourceConversion.FileSourceGroup group) {
    ((CommonRowDataToArrowExecNode) delegate).allowNativeFileSource(group);
  }

  @Override
  protected Transformation<T> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
    delegate.setInputEdges(getInputEdges());
    return delegate.translateToPlan(planner);
  }
}
