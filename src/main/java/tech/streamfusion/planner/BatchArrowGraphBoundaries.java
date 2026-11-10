package tech.streamfusion.planner;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange;
import org.apache.flink.table.types.logical.RowType;

/** Keeps the host's deadlock-breaking boundaries while retaining each edge's physical wire type. */
final class BatchArrowGraphBoundaries {
  private BatchArrowGraphBoundaries() {}

  static ExecNodeGraph preserve(ExecNodeGraph graph, ReadableConfig config) {
    var pending = new ArrayDeque<ExecNode<?>>(graph.getRootNodes());
    var visited = Collections.newSetFromMap(new IdentityHashMap<ExecNode<?>, Boolean>());
    while (!pending.isEmpty()) {
      var target = pending.removeFirst();
      if (!visited.add(target)) continue;
      for (int index = 0; index < target.getInputEdges().size(); index++) {
        var edge = target.getInputEdges().get(index);
        ExecNode<?> source = edge.getSource();
        if (source instanceof BatchExecExchange) {
          var original = (BatchExecExchange) source;
          var input = original.getInputEdges().get(0).getSource();
          if (input instanceof BatchColumnarExecNode
              && ((BatchColumnarExecNode) input).outputsArrow()) {
            if (original.getInputProperties().get(0).getRequiredDistribution().getType()
                != InputProperty.DistributionType.ANY)
              throw new IllegalStateException("unverified inserted Arrow exchange distribution");
            var replacement =
                new BatchForwardExchangeExecNode(
                    config, (RowType) original.getOutputType(), original.getDescription());
            replacement.setInputEdges(
                List.of(ExecEdge.builder().source(input).target(replacement).build()));
            target.replaceInputEdge(
                index, ExecEdge.builder().source(replacement).target(target).build());
            source = replacement;
          }
        }
        pending.add(source);
      }
    }
    return graph;
  }
}
