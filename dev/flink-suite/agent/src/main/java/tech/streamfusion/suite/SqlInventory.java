package tech.streamfusion.suite;

import java.lang.reflect.InvocationTargetException;
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
  private static final Map<Object, OperatorBinding> OPERATORS = new java.util.WeakHashMap<>();
  private static final java.util.Set<String> FINISHED_JOBS = new java.util.HashSet<>();

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
    active.data.put("native_work", active.nativeWork);
    active.data.put("execution_contracts", active.contracts);
    active.data.put("jobs", active.jobs);
    active.data.put("unattributed_native_work", active.unattributedWork);
    active.data.put("unmatched_native_jobs", active.unmatchedJobs);
    System.out.println(MARKER + active.id);
  }

  public static synchronized void finished(Object identifier, Object result) throws Exception {
    if (directory() == null || !(boolean) call(identifier, "isTest")) return;
    if (active == null || !active.data.get("junit_id").equals(call(identifier, "getUniqueId"))) {
      throw new AssertionError("Missing or mismatched SQL inventory invocation");
    }
    snapshotJobs(active);
    active.data.put("junit_status", call(result, "getStatus").toString());
    active.data.put("junit_failure", call(result, "getThrowable").toString());
    Files.createDirectories(directory());
    Files.writeString(
        directory().resolve(active.id + ".json"), json(active.data) + "\n", StandardCharsets.UTF_8);
    System.out.println(MARKER + active.id);
    FINISHED_JOBS.addAll(active.jobs.keySet());
    FINISHED_JOBS.addAll(active.workByJob.keySet());
    OPERATORS.values().removeIf(binding -> binding.scope() == active);
    active = null;
  }

  public static synchronized Object submitting() {
    return submitting(new Object[0]);
  }

  public static synchronized Object submitting(Object[] arguments) {
    if (active == null) return null;
    Map<String, Object> graph = new LinkedHashMap<>();
    for (Object argument : arguments) {
      if (argument == null
          || !argument
              .getClass()
              .getName()
              .equals("org.apache.flink.streaming.api.graph.StreamGraph")) continue;
      try {
        graph.put("job_type", call(argument, "getJobType").toString());
        List<Map<String, Object>> nodes = new ArrayList<>();
        for (Object node : (Iterable<?>) call(argument, "getStreamNodes")) {
          Map<String, Object> entry = new LinkedHashMap<>();
          entry.put("id", call(node, "getId"));
          entry.put("name", call(node, "getOperatorName"));
          entry.put("parallelism", call(node, "getParallelism"));
          Object factory = call(node, "getOperatorFactory");
          if (factory != null) {
            entry.put("factory", factory.getClass().getName());
            try {
              Method type =
                  factory.getClass().getMethod("getStreamOperatorClass", ClassLoader.class);
              type.setAccessible(true);
              entry.put(
                  "operator_class",
                  ((Class<?>) type.invoke(factory, Thread.currentThread().getContextClassLoader()))
                      .getName());
            } catch (ReflectiveOperationException | RuntimeException unavailable) {
              entry.put("operator_class_error", unavailable.toString());
            }
          }
          nodes.add(entry);
        }
        nodes.sort(java.util.Comparator.comparingInt(node -> ((Number) node.get("id")).intValue()));
        graph.put("nodes", nodes);
      } catch (ReflectiveOperationException | RuntimeException unavailable) {
        graph.put("observation_error", unavailable.toString());
      }
    }
    return new Submission(active, graph);
  }

  private record Submission(Scope owner, Map<String, Object> graph) {}

  public static synchronized void submitted(Object token, Object client, Throwable failure)
      throws Exception {
    if (!(token instanceof Submission submission) || submission.owner() != active) return;
    if (failure != null) {
      failed("executeAsync", failure);
      return;
    }
    if (client == null) return;
    String job = call(client, "getJobID").toString();
    if (active.jobs.containsKey(job)) {
      if (!submission.graph().isEmpty())
        active.jobs.get(job).putIfAbsent("graph", submission.graph());
      return;
    }
    active.jobs.put(job, new LinkedHashMap<>(Map.of("status", "SUBMITTED")));
    if (!submission.graph().isEmpty()) active.jobs.get(job).put("graph", submission.graph());
    try {
      active.jobResults.put(
          job, (java.util.concurrent.CompletableFuture<?>) call(client, "getJobExecutionResult"));
    } catch (ReflectiveOperationException | ClassCastException unavailable) {
      active.jobs.get(job).put("status", "UNAVAILABLE");
      active.jobs.get(job).put("failure", unavailable.toString());
    }
  }

  private static void snapshotJobs(Scope scope) {
    scope.workByJob.forEach(
        (job, work) -> {
          if (scope.jobs.containsKey(job)) scope.jobs.get(job).put("native_work", work);
          else scope.unmatchedJobs.put(job, work);
        });
    scope.jobResults.forEach(
        (job, completion) -> {
          if (!completion.isDone()) return;
          Map<String, Object> observation = scope.jobs.get(job);
          try {
            completion.join();
            observation.put("status", "SUCCEEDED");
          } catch (java.util.concurrent.CompletionException
              | java.util.concurrent.CancellationException error) {
            Throwable cause = error;
            while (cause.getCause() != null) cause = cause.getCause();
            observation.put("status", "RESULT_FAILED");
            observation.put("failure", cause.toString());
          }
        });
  }

  private record OperatorBinding(Scope scope, String job) {}

  public static synchronized void opened(Object operator) {
    if (active == null) return;
    String job = null;
    try {
      Object group = call(operator, "getMetricGroup");
      Object id = ((Map<?, ?>) call(group, "getAllVariables")).get("<job_id>");
      if (id != null) job = id.toString();
    } catch (ReflectiveOperationException | RuntimeException unavailable) {
      // Some existing writer callbacks have no operator metric group. Keep their work
      // visible, but never invent a job association from the currently active invocation.
    }
    if (job != null && FINISHED_JOBS.contains(job)) return;
    OPERATORS.putIfAbsent(operator, new OperatorBinding(active, job));
  }

  public static synchronized boolean tracked(Object operator) {
    OperatorBinding binding = OPERATORS.get(operator);
    return active != null && binding != null && binding.scope() == active;
  }

  public static synchronized void completed(Object operator, int rows) {
    if (rows <= 0 || !tracked(operator)) return;
    String name = operator.getClass().getSimpleName();
    active.nativeWork.merge(name, (long) rows, Long::sum);
    String job = OPERATORS.get(operator).job();
    Map<String, Long> work =
        job == null
            ? active.unattributedWork
            : active.workByJob.computeIfAbsent(job, unused -> new java.util.TreeMap<>());
    work.merge(name, (long) rows, Long::sum);
  }

  public static synchronized void contract(String recordId, String test, String variant) {
    if (active != null)
      active.contracts.add(Map.of("record_id", recordId, "test", test, "variant", variant));
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
      List<Object> boundaries = new ArrayList<>();
      operators(root, operators, boundaries, new IdentityHashMap<>());
      finalRoots.add(
          Map.of(
              "operators",
              operators,
              "host_boundaries",
              boundaries,
              "native_operators",
              operators.stream().filter(name -> name.startsWith("StreamPhysicalNative")).count()));
    }
    plan.put("roots", finalRoots);
    active.plans.add(plan);
  }

  private static void operators(
      Object node,
      List<String> names,
      List<Object> boundaries,
      IdentityHashMap<Object, Boolean> seen)
      throws Exception {
    if (seen.put(node, Boolean.TRUE) != null) return;
    names.add(node.getClass().getSimpleName());
    String name = node.getClass().getSimpleName();
    if (name.equals("StreamPhysicalTableSourceScan")
        || name.equals("StreamPhysicalLegacyTableSourceScan")
        || name.equals("StreamPhysicalSink")
        || name.equals("StreamPhysicalLegacySink")
        || name.equals("PreparedLegacySink")) {
      Object owner = name.endsWith("Scan") ? call(node, "getTable") : node;
      boolean legacy = name.contains("Legacy");
      Object implementation =
          call(owner, name.endsWith("Scan") ? "tableSource" : legacy ? "sink" : "tableSink");
      Map<?, ?> options = Map.of();
      String optionsError = null;
      if (!legacy || name.endsWith("Scan")) {
        Object table =
            legacy
                ? call(owner, "catalogTable")
                : call(call(owner, "contextResolvedTable"), "getResolvedTable");
        try {
          options = (Map<?, ?>) call(table, "getOptions");
        } catch (InvocationTargetException e) {
          // DataStream/collect tables deliberately reject option access.
          optionsError = e.getCause().toString();
        }
      }
      Map<String, Object> boundary = new LinkedHashMap<>();
      boundary.put("operator", name);
      boundary.put("implementation", implementation.getClass().getName());
      if (optionsError != null) boundary.put("options_error", optionsError);
      for (String key : List.of("connector", "format", "value.format", "file.format")) {
        if (options.containsKey(key)) boundary.put(key, options.get(key));
      }
      boundaries.add(boundary);
    }
    for (Object input : (List<?>) call(node, "getInputs"))
      operators(input, names, boundaries, seen);
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

  private static Object call(Object value, String name) throws ReflectiveOperationException {
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
    final Map<String, Long> nativeWork = new java.util.TreeMap<>();
    final Map<String, Long> unattributedWork = new java.util.TreeMap<>();
    final Map<String, Map<String, Long>> workByJob = new java.util.TreeMap<>();
    final Map<String, Map<String, Long>> unmatchedJobs = new java.util.TreeMap<>();
    final List<Object> contracts = new ArrayList<>();
    final Map<String, Map<String, Object>> jobs = new LinkedHashMap<>();
    final Map<String, java.util.concurrent.CompletableFuture<?>> jobResults = new LinkedHashMap<>();
    int translationDepth;
  }
}
