package tech.streamfusion.planner;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecTableSourceScan;
import tech.streamfusion.planner.common.CommonRowDataToArrowExecNode;

/** Selects source-local conversion only after the host's complete reuse graph is known. */
final class BatchArrowSourceConversion {
  private BatchArrowSourceConversion() {}

  static ExecNodeGraph prepare(ExecNodeGraph graph, ReadableConfig config) {
    try (NativeConfig.Scope ignored = NativeConfig.usePlannerConfig(config)) {
      if (!NativeConfig.operatorEnabled("fileSource")) return graph;
    }
    if (config
        .getOptional(
            ConfigOptions.key("execution.batch.adaptive.auto-parallelism.enabled")
                .booleanType()
                .noDefaultValue())
        .orElse(true)) return graph;
    var consumers = new IdentityHashMap<ExecNode<?>, List<ExecNode<?>>>();
    var visited = Collections.newSetFromMap(new IdentityHashMap<ExecNode<?>, Boolean>());
    var pending = new ArrayDeque<ExecNode<?>>(graph.getRootNodes());
    while (!pending.isEmpty()) {
      var node = pending.removeFirst();
      if (!visited.add(node)) continue;
      for (var edge : node.getInputEdges()) {
        consumers.computeIfAbsent(edge.getSource(), ignored -> new ArrayList<>()).add(node);
        pending.add(edge.getSource());
      }
    }
    consumers.forEach(
        (source, readers) -> {
          if (!eligible(source)
              || !readers.stream()
                  .allMatch(
                      reader ->
                          reader instanceof BatchNativeExecNode
                              && reader.getInputEdges().size() == 1
                              && ((BatchNativeExecNode<?>) reader).acceptsNativeFileSource()))
            return;
          var group =
              new BatchArrowSourceConversion.FileSourceGroup(
                  (BatchExecTableSourceScan) source, readers.size());
          for (var reader : readers) ((BatchNativeExecNode<?>) reader).allowNativeFileSource(group);
        });
    return graph;
  }

  static final class FileSourceGroup implements CommonRowDataToArrowExecNode.SourceConversion {
    private final BatchExecTableSourceScan scan;
    private final int consumers;
    private boolean attempted;
    private org.apache.flink.api.dag.Transformation<tech.streamfusion.operator.ArrowBatch>
        converted;

    FileSourceGroup(BatchExecTableSourceScan scan, int consumers) {
      this.scan = scan;
      this.consumers = consumers;
    }

    @Override
    public org.apache.flink.api.dag.Transformation<tech.streamfusion.operator.ArrowBatch> translate(
        org.apache.flink.table.planner.delegation.PlannerBase planner,
        org.apache.flink.api.dag.Transformation<org.apache.flink.table.data.RowData> input,
        org.apache.flink.table.types.logical.RowType output,
        int rows) {
      if (attempted) return converted;
      attempted = true;
      for (var extension :
          NativePlannerExtensions.batchFileSourceProviders(
              planner.getFlinkContext().getClassLoader())) {
        converted =
            extension.lowerBatchFileSource(
                scan, input, output, rows, planner.getTableConfig().getConfiguration());
        if (converted == null) continue;
        if (consumers > 1)
          converted =
              new org.apache.flink.streaming.api.transformations.OneInputTransformation<>(
                  converted,
                  "NativeFileSourceShare",
                  new tech.streamfusion.operator.NativeColumnarShareOperator(consumers),
                  tech.streamfusion.operator.ArrowBatchTypeInformation.INSTANCE,
                  input.getParallelism());
        return converted;
      }
      return null;
    }
  }

  private static boolean eligible(ExecNode<?> node) {
    return node.getClass() == BatchExecTableSourceScan.class && node.getInputEdges().isEmpty();
  }
}
