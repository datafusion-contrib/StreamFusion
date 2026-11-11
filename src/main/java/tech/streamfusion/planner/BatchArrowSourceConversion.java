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
              new RowDataToArrowExecNode.FileSourceGroup(
                  (BatchExecTableSourceScan) source, readers.size());
          for (var reader : readers) ((BatchNativeExecNode<?>) reader).allowNativeFileSource(group);
        });
    return graph;
  }

  private static boolean eligible(ExecNode<?> node) {
    return node.getClass() == BatchExecTableSourceScan.class && node.getInputEdges().isEmpty();
  }
}
