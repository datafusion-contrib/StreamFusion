package tech.streamfusion.suite;

import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Submitted-graph evidence for Flink's ordering-only PROCTIME passthrough fixture. */
final class ElidedTemporalSort {
  static final String CONTRACT = "@elided:TemporalSort";
  static final String TEST =
      "org.apache.flink.table.planner.plan.nodes.exec.stream.TemporalSortRestoreTest#testRestore";
  static final String VARIANT = "program=temporal_sort_proctime";

  private ElidedTemporalSort() {}

  static Map<String, Object> graph(Object graph) throws ReflectiveOperationException {
    var nodes = new ArrayList<Map<String, Object>>();
    for (Object node : (Iterable<?>) call(graph, "getStreamNodes")) {
      var entry = new LinkedHashMap<String, Object>();
      entry.put("id", call(node, "getId"));
      Object factory = call(node, "getOperatorFactory");
      if (factory == null) throw new AssertionError("Missing submitted operator factory");
      entry.put("operator", SqlInventory.operatorClass(factory));
      var inputs = new ArrayList<Object>();
      for (Object edge : (Iterable<?>) call(node, "getInEdges"))
        inputs.add(call(edge, "getSourceId"));
      var outputs = new ArrayList<Object>();
      for (Object edge : (Iterable<?>) call(node, "getOutEdges"))
        outputs.add(call(edge, "getTargetId"));
      entry.put("inputs", inputs);
      entry.put("outputs", outputs);
      nodes.add(entry);
    }
    return Map.of("nodes", nodes);
  }

  static boolean valid(Map<String, Object> graph) {
    if (!(graph.get("nodes") instanceof List<?> nodes) || nodes.size() < 2) return false;
    var indexed = new LinkedHashMap<Integer, Map<?, ?>>();
    for (Object value : nodes) {
      if (!(value instanceof Map<?, ?> node)
          || !(node.get("id") instanceof Integer id)
          || !(node.get("operator") instanceof String operator)
          || operator.isBlank()
          || operator.contains("SortOperator")
          || !(node.get("inputs") instanceof List<?>)
          || !(node.get("outputs") instanceof List<?>)
          || indexed.put(id, node) != null) return false;
    }
    int sources = 0, sinks = 0, edges = 0;
    for (var entry : indexed.entrySet()) {
      var inputs = (List<?>) entry.getValue().get("inputs");
      var outputs = (List<?>) entry.getValue().get("outputs");
      if (inputs.isEmpty()) sources++;
      if (outputs.isEmpty()) sinks++;
      for (Object source : inputs)
        if (!indexed.containsKey(source)
            || java.util.Collections.frequency(
                    (List<?>) indexed.get(source).get("outputs"), entry.getKey())
                != java.util.Collections.frequency(inputs, source)) return false;
      for (Object target : outputs) {
        if (!indexed.containsKey(target)
            || java.util.Collections.frequency(
                    (List<?>) indexed.get(target).get("inputs"), entry.getKey())
                != java.util.Collections.frequency(outputs, target)) return false;
        edges++;
      }
    }
    var degrees = new LinkedHashMap<Integer, Integer>();
    var ready = new ArrayDeque<Integer>();
    indexed.forEach(
        (id, node) -> {
          int degree = ((List<?>) node.get("inputs")).size();
          degrees.put(id, degree);
          if (degree == 0) ready.add(id);
        });
    int visited = 0;
    while (!ready.isEmpty()) {
      int id = ready.remove();
      visited++;
      for (Object child : (List<?>) indexed.get(id).get("outputs")) {
        int degree = degrees.compute((Integer) child, (key, old) -> old - 1);
        if (degree == 0) ready.add((Integer) child);
      }
    }
    return sources > 0 && sinks > 0 && edges > 0 && visited == indexed.size();
  }

  static Object call(Object value, String name) throws ReflectiveOperationException {
    Method method = value.getClass().getMethod(name);
    method.setAccessible(true);
    return method.invoke(value);
  }
}
