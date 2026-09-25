package tech.streamfusion.suite;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Observes every upstream invocation without changing its SQL, assertions, or route contracts. */
public final class SqlInventory {
  public static final String MARKER = "StreamFusion SQL inventory: ";
  private static Scope active;

  private SqlInventory() {}

  public static synchronized void started(Object identifier) throws Exception {
    if (directory() == null || !(boolean) call(identifier, "isTest")) return;
    String uniqueId = call(identifier, "getUniqueId").toString();
    if (active != null)
      throw new AssertionError("SQL inventory requires serial tests: " + uniqueId);
    active = new Scope();
    active.data.put("schema_version", 1);
    active.data.put("invocation_id", active.id);
    active.data.put("junit_id", uniqueId);
    active.data.put("display_name", call(identifier, "getDisplayName").toString());
    active.data.put("source", call(identifier, "getSource").toString());
    active.data.put("flink_line", System.getProperty("streamfusion.flink-suite.flink-line", "2.2"));
    active.data.put("planners", active.planners);
    active.data.put("plans", active.plans);
    active.data.put("sql", active.sql);
    active.data.put("translations", active.translations);
    active.data.put("operation_failures", active.failures);
    System.out.println(MARKER + active.id);
  }

  public static synchronized void finished(Object identifier, Object result) throws Exception {
    if (directory() == null || !(boolean) call(identifier, "isTest")) return;
    if (active == null || !active.data.get("junit_id").equals(call(identifier, "getUniqueId"))) {
      throw new AssertionError("Missing or mismatched SQL inventory invocation");
    }
    active.data.put("junit_status", call(result, "getStatus").toString());
    active.data.put("junit_failure", call(result, "getThrowable").toString());
    Files.createDirectories(directory());
    Files.writeString(
        directory().resolve(active.id + ".json"), json(active.data) + "\n", StandardCharsets.UTF_8);
    System.out.println(MARKER + active.id);
    active = null;
  }

  public static synchronized void planner(Object context, boolean unmodified) throws Exception {
    if (active == null) return;
    Object config = call(context, "getTableConfig");
    ClassLoader loader = context.getClass().getClassLoader();
    Class<?> optionType =
        Class.forName("org.apache.flink.configuration.ConfigOption", true, loader);
    Object modeOption =
        Class.forName("org.apache.flink.configuration.ExecutionOptions", true, loader)
            .getField("RUNTIME_MODE")
            .get(null);
    Object mode = config.getClass().getMethod("get", optionType).invoke(config, modeOption);
    active.planners.add(Map.of("mode", mode.toString(), "unmodified", unmodified));
  }

  public static synchronized void plan(Object scan, Object roots) throws Exception {
    if (active == null) return;
    Map<String, Object> plan = new LinkedHashMap<>();
    plan.put("substitutions", call(scan, "substitutions"));
    plan.put("operators", new ArrayList<>((List<?>) call(scan, "operatorTypes")));
    plan.put("fallback_reasons", new ArrayList<>((List<?>) call(scan, "fallbackReasons")));
    plan.put("during_translation", active.translationDepth > 0);
    List<Object> finalRoots = new ArrayList<>();
    for (Object root : (List<?>) roots) {
      List<String> operators = new ArrayList<>();
      operators(root, operators, new IdentityHashMap<>());
      finalRoots.add(
          Map.of(
              "operators",
              operators,
              "native_operators",
              operators.stream().filter(name -> name.startsWith("StreamPhysicalNative")).count()));
    }
    plan.put("roots", finalRoots);
    active.plans.add(plan);
  }

  private static void operators(
      Object node, List<String> names, IdentityHashMap<Object, Boolean> seen) throws Exception {
    if (seen.put(node, Boolean.TRUE) != null) return;
    names.add(node.getClass().getSimpleName());
    for (Object input : (List<?>) call(node, "getInputs")) operators(input, names, seen);
  }

  public static synchronized void sql(String method, String statement) {
    if (active != null) active.sql.add(Map.of("method", method, "statement", statement));
  }

  public static synchronized void translating(Object planner) {
    if (active != null) {
      active.translationDepth++;
      active.translations.add(planner.getClass().getName());
    }
  }

  public static synchronized void translated() {
    if (active != null) active.translationDepth--;
  }

  public static synchronized void failed(String operation, Throwable failure) {
    if (active != null && failure != null) {
      String message = failure.toString();
      active.failures.add(
          Map.of(
              "operation",
              operation,
              "error",
              message.substring(0, Math.min(message.length(), 4000))));
    }
  }

  private static Path directory() {
    String path = System.getProperty("streamfusion.flink-suite.sql-inventory");
    return path == null || path.isBlank() ? null : Path.of(path);
  }

  private static Object call(Object value, String name) throws Exception {
    Method method = value.getClass().getMethod(name);
    method.setAccessible(true);
    return method.invoke(value);
  }

  static String json(Object value) {
    if (value == null) return "null";
    if (value instanceof Number || value instanceof Boolean) return value.toString();
    if (value instanceof Map<?, ?> map) {
      List<String> entries = new ArrayList<>();
      map.forEach((key, item) -> entries.add(json(key.toString()) + ":" + json(item)));
      return "{" + String.join(",", entries) + "}";
    }
    if (value instanceof Iterable<?> items) {
      List<String> entries = new ArrayList<>();
      items.forEach(item -> entries.add(json(item)));
      return "[" + String.join(",", entries) + "]";
    }
    StringBuilder out = new StringBuilder("\"");
    for (char c : value.toString().toCharArray()) {
      if (c == '"' || c == '\\') out.append('\\').append(c);
      else if (c < 32 || Character.isSurrogate(c)) out.append(String.format("\\u%04x", (int) c));
      else out.append(c);
    }
    return out.append('"').toString();
  }

  private static final class Scope {
    final String id = UUID.randomUUID().toString();
    final Map<String, Object> data = new LinkedHashMap<>();
    final List<Object> planners = new ArrayList<>();
    final List<Object> plans = new ArrayList<>();
    final List<Object> sql = new ArrayList<>();
    final List<String> translations = new ArrayList<>();
    final List<Object> failures = new ArrayList<>();
    int translationDepth;
  }
}
