package tech.streamfusion.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.util.StringUtils;
import tech.streamfusion.operator.ArrowToRowDataOperator;
import tech.streamfusion.operator.NativeColumnarShareOperator;
import tech.streamfusion.operator.RowDataToArrowOperator;
import tech.streamfusion.operator.SplitByKeyGroupOperator;

/** Matches unchanged connector topology around explicit logical-operator bindings. */
final class CanonicalGraphIdentities {
  private CanonicalGraphIdentities() {}

  static Map<String, String> bind(
      StreamGraph stock,
      StreamGraph runtime,
      Map<Integer, Integer> runtimeToStock,
      Set<Integer> nativeOnly,
      ClassLoader loader) {
    Map<Integer, StreamNode> stockNodes = nodes(stock, Set.of());
    Map<Integer, StreamNode> runtimeNodes = nodes(runtime, nativeOnly);
    for (int id : nativeOnly) {
      StreamNode node = runtime.getStreamNode(id);
      if (node == null || node.getInEdges().isEmpty() || node.getOperatorFactory() == null) {
        throw new IllegalArgumentException("Invalid native-only operator identity: " + id);
      }
      Class<?> operator = node.getOperatorFactory().getStreamOperatorClass(loader);
      if (operator != RowDataToArrowOperator.class
          && operator != ArrowToRowDataOperator.class
          && operator != SplitByKeyGroupOperator.class
          && operator != NativeColumnarShareOperator.class
          && !isExtensionStatelessOperator(operator, loader)) {
        throw new IllegalArgumentException(
            "Only stateless representation and exchange operators may be omitted: " + operator);
      }
    }
    Map<Integer, Integer> bindings = new HashMap<>();
    Set<Integer> assigned = new HashSet<>();
    runtimeToStock.forEach(
        (nativeId, stockId) -> {
          if (!runtimeNodes.containsKey(nativeId) || !stockNodes.containsKey(stockId)) {
            throw new IllegalArgumentException("Operator binding is not a physical stream node");
          }
          if (!assigned.add(stockId))
            throw new IllegalArgumentException("Duplicate stock operator binding");
          bindings.put(nativeId, stockId);
        });
    List<Edge> stockEdges = edges(stock, Set.of());
    List<Edge> runtimeEdges = edges(runtime, nativeOnly);
    boolean progress;
    do {
      progress = false;
      for (StreamNode original : stockNodes.values()) {
        if (assigned.contains(original.getId())) continue;
        List<Integer> candidates = new ArrayList<>();
        for (StreamNode replacement : runtimeNodes.values()) {
          if (!bindings.containsKey(replacement.getId())
              && sameRole(original, replacement, loader)
              && neighbours(original.getId(), stockEdges, null, assigned)
                  .equals(
                      neighbours(replacement.getId(), runtimeEdges, bindings, bindings.keySet()))) {
            candidates.add(replacement.getId());
          }
        }
        if (candidates.size() == 1) {
          bindings.put(candidates.get(0), original.getId());
          assigned.add(original.getId());
          progress = true;
        }
      }
    } while (progress);
    if (bindings.size() != stockNodes.size() || bindings.size() != runtimeNodes.size()) {
      throw new IllegalArgumentException(
          "Cannot unambiguously bind the complete source/operator/sink topology; unmatched stock="
              + stockNodes.values().stream()
                  .filter(node -> !assigned.contains(node.getId()))
                  .map(node -> node.getId() + ":" + node.getOperatorName())
                  .collect(java.util.stream.Collectors.toList())
              + ", unmatched native="
              + runtimeNodes.values().stream()
                  .filter(node -> !bindings.containsKey(node.getId()))
                  .map(node -> node.getId() + ":" + node.getOperatorName())
                  .collect(java.util.stream.Collectors.toList()));
    }
    Set<Integer> allStock = stockNodes.keySet();
    for (var binding : bindings.entrySet()) {
      if (!neighbours(binding.getValue(), stockEdges, null, allStock)
          .equals(neighbours(binding.getKey(), runtimeEdges, bindings, runtimeNodes.keySet()))) {
        throw new IllegalArgumentException(
            "Native lowering changed the logical topology at operator " + binding.getValue());
      }
    }
    Map<Integer, byte[]> stockHashes =
        new StreamGraphHasherV2().traverseStreamGraphAndGenerateHashes(stock);
    Map<Integer, byte[]> nativeHashes =
        new StreamGraphHasherV2().traverseStreamGraphAndGenerateHashes(runtime);
    Map<String, String> identities = new HashMap<>();
    bindings.forEach(
        (nativeId, stockId) ->
            identities.put(
                StringUtils.byteToHexString(nativeHashes.get(nativeId)),
                StringUtils.byteToHexString(stockHashes.get(stockId))));
    return Map.copyOf(identities);
  }

  static boolean isExtensionStatelessOperator(Class<?> operator, ClassLoader loader) {
    return java.util.ServiceLoader.load(NativePlannerExtension.class, loader).stream()
        .map(java.util.ServiceLoader.Provider::get)
        .anyMatch(extension -> extension.canonicalStatelessOperators().contains(operator));
  }

  private static Map<Integer, StreamNode> nodes(StreamGraph graph, Set<Integer> omitted) {
    Map<Integer, StreamNode> nodes = new HashMap<>();
    for (StreamNode node : graph.getStreamNodes()) {
      if (!omitted.contains(node.getId())) nodes.put(node.getId(), node);
    }
    return nodes;
  }

  private static boolean sameRole(StreamNode stock, StreamNode runtime, ClassLoader loader) {
    var original = stock.getOperatorFactory();
    var replacement = runtime.getOperatorFactory();
    if (original == null || replacement == null || original.getClass() != replacement.getClass())
      return false;
    if (original instanceof org.apache.flink.streaming.api.operators.SimpleOperatorFactory) {
      Class<?> implementation = original.getStreamOperatorClass(loader);
      if (implementation != replacement.getStreamOperatorClass(loader)) return false;
      if (implementation
          == org.apache.flink.table.runtime.operators.sink.StreamRecordTimestampInserter.class)
        return java.util.Arrays.equals(
            timestampDefinition(original), timestampDefinition(replacement));
      if (implementation
          == org.apache.flink.table.runtime.operators.sink.constraint.ConstraintEnforcer.class) {
        // Ordinary translation assigns fresh display IDs to equivalent sink helper stages.
        // Compare the complete immutable executor, including ERROR/DROP and nested constraints.
        return java.util.Arrays.equals(
            constraintDefinition(original), constraintDefinition(replacement));
      }
    }
    // Generated factories compile equivalent stock stages into distinct classes.
    return stock.getOperatorName().equals(runtime.getOperatorName());
  }

  private static int[] timestampDefinition(
      org.apache.flink.streaming.api.operators.StreamOperatorFactory<?> factory) {
    try {
      var type = org.apache.flink.table.runtime.operators.sink.StreamRecordTimestampInserter.class;
      var index = type.getDeclaredField("rowtimeIndex");
      var precision = type.getDeclaredField("precision");
      index.setAccessible(true);
      precision.setAccessible(true);
      Object operator =
          ((org.apache.flink.streaming.api.operators.SimpleOperatorFactory<?>) factory)
              .getOperator();
      return new int[] {index.getInt(operator), precision.getInt(operator)};
    } catch (ReflectiveOperationException failure) {
      throw new IllegalArgumentException(
          "Cannot compare released sink timestamp definitions", failure);
    }
  }

  private static byte[] constraintDefinition(
      org.apache.flink.streaming.api.operators.StreamOperatorFactory<?> factory) {
    try {
      var field =
          org.apache.flink.table.runtime.operators.sink.constraint.ConstraintEnforcer.class
              .getDeclaredField("executor");
      field.setAccessible(true);
      Object operator =
          ((org.apache.flink.streaming.api.operators.SimpleOperatorFactory<?>) factory)
              .getOperator();
      return org.apache.flink.util.InstantiationUtil.serializeObject(field.get(operator));
    } catch (ReflectiveOperationException | java.io.IOException failure) {
      throw new IllegalArgumentException(
          "Cannot compare released sink constraint definitions", failure);
    }
  }

  private static List<Edge> edges(StreamGraph graph, Set<Integer> omitted) {
    List<Edge> edges = new ArrayList<>();
    for (StreamNode node : graph.getStreamNodes()) {
      if (omitted.contains(node.getId())) continue;
      for (var edge : node.getInEdges()) {
        collectEdges(
            graph,
            omitted,
            edge.getSourceId(),
            node.getId(),
            edge.getTypeNumber(),
            new HashSet<>(),
            edges);
      }
    }
    return edges;
  }

  private static void collectEdges(
      StreamGraph graph,
      Set<Integer> omitted,
      int source,
      int target,
      int input,
      Set<Integer> path,
      List<Edge> edges) {
    if (!omitted.contains(source)) {
      edges.add(new Edge(source, target, input));
      return;
    }
    if (!path.add(source)) throw new IllegalArgumentException("Cycle through transposes");
    for (var edge : graph.getStreamNode(source).getInEdges()) {
      collectEdges(graph, omitted, edge.getSourceId(), target, input, path, edges);
    }
    path.remove(source);
  }

  private static List<String> neighbours(
      int node, List<Edge> edges, Map<Integer, Integer> projection, Set<Integer> known) {
    List<String> neighbours = new ArrayList<>();
    for (Edge edge : edges) {
      if (edge.target == node && known.contains(edge.source)) {
        neighbours.add(
            "in:"
                + (projection == null ? edge.source : projection.get(edge.source))
                + ":"
                + edge.input);
      }
      if (edge.source == node && known.contains(edge.target)) {
        neighbours.add(
            "out:"
                + (projection == null ? edge.target : projection.get(edge.target))
                + ":"
                + edge.input);
      }
    }
    neighbours.sort(String::compareTo);
    return neighbours;
  }

  private static final class Edge {
    final int source;
    final int target;
    final int input;

    Edge(int source, int target, int input) {
      this.source = source;
      this.target = target;
      this.input = input;
    }
  }
}
