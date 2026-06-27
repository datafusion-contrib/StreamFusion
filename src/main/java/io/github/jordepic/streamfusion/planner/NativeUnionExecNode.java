package io.github.jordepic.streamfusion.planner;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.UnionTransformation;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.types.logical.RowType;

/**
 * Lowers the native union into the plan. Like Flink's own {@code CommonExecUnion} this is not a
 * physical operator — it merges the inputs' transformations into a single {@link UnionTransformation},
 * which forwards every input record (here, every Arrow batch) downstream unchanged and aligns
 * watermarks across the inputs. No operator, no native code.
 */
public class NativeUnionExecNode extends ExecNodeBase<ArrowBatch>
    implements StreamExecNode<ArrowBatch>, SingleTransformationTranslator<ArrowBatch> {

  public NativeUnionExecNode(
      ReadableConfig tableConfig,
      List<InputProperty> inputProperties,
      RowType outputType,
      String description) {
    super(
        ExecNodeContext.newNodeId(),
        new ExecNodeContext("stream-exec-native-union_1"),
        tableConfig,
        inputProperties,
        outputType,
        description);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transformation<ArrowBatch> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    List<Transformation<ArrowBatch>> inputTransforms = new ArrayList<>();
    for (ExecEdge inputEdge : getInputEdges()) {
      inputTransforms.add((Transformation<ArrowBatch>) inputEdge.translateToPlan(planner));
    }
    return new UnionTransformation<>(inputTransforms);
  }
}
