package tech.streamfusion.suite;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.stream.Collectors;

/**
 * Per-invocation native work witnesses for a deliberately bounded set of unchanged upstream ITs.
 */
public final class NativeExecution {
  private static final Map<String, Map<String, String>> CONTRACTS = loadContracts();
  private static final Map<Object, Scope> OPERATORS = new WeakHashMap<>();
  private static final Map<Object, StockBinding> STOCK_OPERATORS = new WeakHashMap<>();
  private static Scope active;

  private NativeExecution() {}

  private static Map<String, Map<String, String>> loadContracts() {
    String flinkLine = System.getProperty("streamfusion.flink-suite.flink-line", "2.2");
    String resource =
        switch (flinkLine) {
          case "2.2" ->
              Boolean.getBoolean("streamfusion.flink-suite.canonical")
                  ? "/canonical-native-execution.tsv"
                  : "/native-execution.tsv";
          case "1.18" ->
              Boolean.getBoolean("streamfusion.flink-suite.native-rocksdb")
                  ? "/native-execution-flink1.18-state.tsv"
                  : "/native-execution-flink1.18.tsv";
          default ->
              throw new IllegalArgumentException(
                  "Unknown native execution contract line: " + flinkLine);
        };
    Map<String, Map<String, String>> contracts = new LinkedHashMap<>();
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(
                NativeExecution.class.getResourceAsStream(resource), StandardCharsets.UTF_8))) {
      for (String line; (line = reader.readLine()) != null; ) {
        if (line.isBlank() || line.startsWith("#")) {
          continue;
        }
        String[] fields = line.split("\t", -1);
        if (fields.length != 3
            || !fields[0].matches("[\\w.$]+#[\\w$]+")
            || !fields[1].matches("\\*|\\w+=\\w+(?:&\\w+=\\w+)*")
            || !(fields[2].matches("\\w+(?:[+|]\\w+)*")
                || fields[2].matches("!.+")
                || fields[2].matches("@restore:\\w+")
                || fields[2].matches("@resume:\\w+(?:&&\\w+(?:[+|]\\w+)*)?")
                || fields[2].equals(ElidedTemporalSort.CONTRACT)
                || fields[2].equals(ElidedIntervalJoin.CONTRACT))
            || fields[2].startsWith("!@") && !fields[2].matches("!@\\w+:.+")) {
          throw new IllegalStateException("Invalid native execution contract: " + line);
        }
        if (fields[2].equals(ElidedTemporalSort.CONTRACT)
            && (!fields[0].equals(ElidedTemporalSort.TEST)
                || !fields[1].equals(ElidedTemporalSort.VARIANT)))
          throw new IllegalStateException(
              "Elided sort proof is restricted to the unchanged PROCTIME fixture");
        if (fields[2].equals(ElidedIntervalJoin.CONTRACT)
            && (!fields[0].equals(ElidedIntervalJoin.TEST)
                || !fields[1].equals(ElidedIntervalJoin.VARIANT)))
          throw new IllegalStateException(
              "Elided interval proof is restricted to the unchanged negative-interval fixture");
        if (fields[2].contains("&&")
            && (!(fields[2].startsWith("!@") || fields[2].startsWith("@resume:"))
                || !fields[2].split("&&", -1)[1].matches("\\w+(?:[+|]\\w+)*")
                || fields[2].split("&&", -1).length != 2))
          throw new IllegalStateException("Invalid stock state-owner contract: " + line);
        Map<String, String> variants =
            contracts.computeIfAbsent(fields[0], key -> new LinkedHashMap<>());
        if (variants.putIfAbsent(fields[1], fields[2]) != null) {
          throw new IllegalStateException("Duplicate native execution contract: " + line);
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException("Cannot read native execution contracts", e);
    }
    if (contracts.isEmpty()) {
      throw new IllegalStateException("No native execution contracts");
    }
    return contracts;
  }

  public static String[] testClasses() {
    return CONTRACTS.keySet().stream()
        .map(key -> key.split("#")[0])
        .distinct()
        .toArray(String[]::new);
  }

  public static String[] testMethods(String className) {
    return CONTRACTS.keySet().stream()
        .filter(key -> key.startsWith(className + "#"))
        .map(key -> key.split("#")[1])
        .toArray(String[]::new);
  }

  public static synchronized Scope begin(String test, Object fixture) {
    if (!CONTRACTS.containsKey(test)) {
      throw new IllegalArgumentException("Unknown native execution contract: " + test);
    }
    if (active != null) {
      throw new AssertionError("Native execution contracts require serial tests in each JVM");
    }
    List<String> variants =
        CONTRACTS.get(test).keySet().stream()
            .filter(selector -> matches(selector, fixture))
            .collect(Collectors.toList());
    if (variants.size() != 1) {
      throw new AssertionError(
          "Expected exactly one native execution contract for " + test + ": " + variants);
    }
    active = new Scope(test, variants.get(0));
    return active;
  }

  public static Scope beginRestore(String fixtureClass, Object fixture) {
    return beginRestore(fixtureClass, fixture, null);
  }

  public static Scope beginRestore(String fixtureClass, Object fixture, Object program) {
    String test = fixtureClass + "#testRestore";
    return CONTRACTS.containsKey(test) ? begin(test, new RestoreFixture(fixture, program)) : null;
  }

  private record RestoreFixture(Object fixture, Object program) {}

  static boolean matches(String selector, Object fixture) {
    if (selector.equals("*")) return true;
    for (String condition : selector.split("&")) {
      String[] parts = condition.split("=", 2);
      Object actual = fixture instanceof RestoreFixture restore ? restore.fixture() : fixture;
      Object value =
          switch (parts[0]) {
            case "program" -> {
              if (!(fixture instanceof RestoreFixture restore) || restore.program() == null)
                throw new AssertionError("Missing canonical restore program");
              Object program = restore.program();
              yield (program instanceof String
                      ? (String) program
                      : String.valueOf(field(program, "id")))
                  .replace('-', '_');
            }
            case "changelog" -> changelogEnabled(actual);
            case "miniBatch" -> miniBatchEnabled(actual);
            default -> field(actual, parts[0]);
          };
      if (!String.valueOf(value).equals(parts[1])) return false;
    }
    return true;
  }

  private static Object field(Object fixture, String name) {
    for (Class<?> type = fixture.getClass(); type != null; type = type.getSuperclass()) {
      try {
        var field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(fixture);
      } catch (NoSuchFieldException ignored) {
        // Pinned fixture parameters may belong to the upstream base class.
      } catch (ReflectiveOperationException e) {
        throw new AssertionError("Cannot read pinned upstream fixture selector " + name, e);
      }
    }
    throw new AssertionError("Cannot read pinned upstream fixture selector " + name);
  }

  private static boolean changelogEnabled(Object fixture) {
    try {
      Object environment = field(fixture, "env");
      Object javaEnvironment = environment.getClass().getMethod("getJavaEnv").invoke(environment);
      return javaEnvironment
          .getClass()
          .getMethod("isChangelogStateBackendEnabled")
          .invoke(javaEnvironment)
          .toString()
          .equals("TRUE");
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("Cannot read pinned upstream changelog configuration", e);
    }
  }

  private static boolean miniBatchEnabled(Object fixture) {
    try {
      Object mode = field(fixture, "miniBatch");
      return (Boolean) mode.getClass().getMethod("on").invoke(mode);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("Cannot read pinned upstream mini-batch configuration", e);
    }
  }

  public static synchronized void opened(Object operator) {
    SqlInventory.opened(operator);
    if (active != null) {
      OPERATORS.putIfAbsent(operator, active);
      String kind = operator.getClass().getSimpleName();
      active.opened.add(kind);
      if (!active.groupImports.isEmpty() && kind.equals("NativeColumnarGroupAggregateOperator")) {
        Long rows = active.groupImports.remove(((Number) field(operator, "handle")).longValue());
        if (rows != null) active.importedRows.merge(kind, rows, Long::sum);
      }
      if (!active.windowImports.isEmpty()
          && (kind.equals("NativeColumnarWindowAggregateOperator")
              || kind.equals("NativeColumnarSessionWindowAggregateOperator")
              || kind.equals("NativeColumnarGlobalWindowAggregateOperator"))) {
        Long rows = active.windowImports.remove(((Number) field(operator, "handle")).longValue());
        if (rows != null) {
          active.importedRows.merge(kind, rows, Long::sum);
          if (kind.equals("NativeColumnarGlobalWindowAggregateOperator")) {
            try {
              boolean finalWindow =
                  (Boolean) operator.getClass().getMethod("isFinalWindow").invoke(operator);
              String role = kind + (finalWindow ? "_Final" : "_Slice");
              active.opened.add(role);
              active.importedRows.merge(role, rows, Long::sum);
            } catch (ReflectiveOperationException failure) {
              throw new AssertionError("Cannot identify imported global window state", failure);
            }
          }
        }
      }
    }
  }

  public static synchronized void restoredWindow(long handle, int rows) {
    if (active != null && handle > 0 && rows > 0)
      active.windowImports.merge(handle, (long) rows, Long::sum);
  }

  public static synchronized void restoredGroup(long handle, int rows) {
    if (active != null && handle > 0 && rows > 0)
      active.groupImports.merge(handle, (long) rows, Long::sum);
  }

  public static synchronized void restored(String operator, int rows) {
    if (active != null && rows > 0) active.importedRows.merge(operator, (long) rows, Long::sum);
  }

  public static synchronized void stockOpened(Object operator) {
    if (active == null || !Boolean.getBoolean("streamfusion.flink-suite.canonical")) return;
    String kind;
    if (operator.getClass().getSimpleName().equals("RowTimeSortOperator")) {
      if (!CONTRACTS.get(active.test).get(active.variant).contains("StockTemporalSort")) return;
      if (field(operator, "dataState") == null
          || field(operator, "lastTriggeringTsState") == null
          || field(operator, "comparator") == null)
        throw new AssertionError("Stock temporal sort state/comparator was not initialized");
      kind = "StockTemporalSort";
    } else if (operator.getClass().getSimpleName().equals("WindowJoinOperator")) {
      if (!CONTRACTS.get(active.test).get(active.variant).contains("StockWindowJoin")) return;
      String kindName = ((Enum<?>) field(operator, "joinType")).name();
      if ((!kindName.equals("SEMI") && !kindName.equals("ANTI"))
          || field(operator, "leftWindowState") == null
          || field(operator, "rightWindowState") == null
          || field(operator, "windowTimerService") == null
          || field(operator, "helper") == null)
        throw new AssertionError("Stock semi/anti window join state/timers were not initialized");
      kind = "StockWindowJoin";
    } else if (operator.getClass().getSimpleName().equals("UnalignedWindowTableFunctionOperator")) {
      if (!field(operator, "windowAssigner")
          .getClass()
          .getSimpleName()
          .equals("SessionWindowAssigner")) return;
      if (field(operator, "windowState") == null || field(operator, "counterState") == null)
        throw new AssertionError("Stock SESSION table-function state was not initialized");
      kind = "StockSessionTableFunction";
    } else {
      Object processor = field(operator, "windowProcessor");
      if (!processor.getClass().getSimpleName().equals("UnsliceSyncStateWindowAggProcessor"))
        return;
      if (field(processor, "windowState") == null)
        throw new AssertionError("Stock unsliced window state was not initialized");
      kind = "StockSessionWindowAggregate";
    }
    STOCK_OPERATORS.put(operator, new StockBinding(active, kind));
  }

  public static synchronized void stockCompleted(Object operator) {
    StockBinding binding = STOCK_OPERATORS.get(operator);
    if (binding != null && binding.scope == active)
      active.stockRows.merge(binding.kind, 1L, Long::sum);
  }

  private static boolean workSatisfied(Scope scope, String contract) {
    return Arrays.stream(contract.split("\\|"))
        .anyMatch(
            route ->
                Arrays.stream(route.split("\\+"))
                    .allMatch(
                        operator ->
                            (operator.startsWith("Stock") ? scope.stockRows : scope.rows)
                                    .getOrDefault(operator, 0L)
                                > 0));
  }

  public static synchronized void fallback(String reason) {
    if (active != null) {
      active.fallbacks.add(reason);
    }
  }

  public static void completed(Object operator, int rows) {
    if (rows <= 0) {
      return;
    }
    SqlInventory.completed(operator, rows);
    synchronized (NativeExecution.class) {
      Scope scope = OPERATORS.get(operator);
      if (scope != null && scope == active) {
        scope.rows.merge(operator.getClass().getSimpleName(), (long) rows, Long::sum);
        String contract = CONTRACTS.get(scope.test).get(scope.variant);
        if (operator
                .getClass()
                .getSimpleName()
                .equals("NativeColumnarGlobalWindowAggregateOperator")
            && (contract.contains("_Slice") || contract.contains("_Final"))) {
          try {
            boolean finalWindow =
                (Boolean) operator.getClass().getMethod("isFinalWindow").invoke(operator);
            scope.rows.merge(
                "NativeColumnarGlobalWindowAggregateOperator" + (finalWindow ? "_Final" : "_Slice"),
                (long) rows,
                Long::sum);
          } catch (ReflectiveOperationException failure) {
            throw new AssertionError("Cannot verify the native global window mode", failure);
          }
        }
      }
    }
  }

  private static synchronized boolean tracked(Object operator) {
    return OPERATORS.containsKey(operator) || SqlInventory.tracked(operator);
  }

  public static int batchRows(Object operator, Object root) throws ReflectiveOperationException {
    if (!tracked(operator)) {
      return 0;
    }
    return (int) root.getClass().getMethod("getRowCount").invoke(root);
  }

  public static int elementRows(Object operator, Object record)
      throws ReflectiveOperationException {
    if (!tracked(operator)) {
      return 0;
    }
    Object batch = record.getClass().getMethod("getValue").invoke(record);
    return (int) batch.getClass().getMethod("rowCount").invoke(batch);
  }

  private record SubmittedGraph(Scope scope, Map<String, Object> graph) {}

  public static synchronized Object submitting(Object[] arguments)
      throws ReflectiveOperationException {
    if (active == null || !CONTRACTS.get(active.test).get(active.variant).startsWith("@elided:"))
      return null;
    for (Object argument : arguments)
      if (argument != null
          && argument
              .getClass()
              .getName()
              .equals("org.apache.flink.streaming.api.graph.StreamGraph"))
        return new SubmittedGraph(active, ElidedTemporalSort.graph(argument));
    return null;
  }

  public static synchronized void submitted(Object token, Object client, Throwable failure)
      throws ReflectiveOperationException {
    if (!(token instanceof SubmittedGraph graph)
        || graph.scope != active
        || failure != null
        || client == null) return;
    active.submittedGraphs.put(ElidedTemporalSort.call(client, "getJobID").toString(), graph.graph);
  }

  public static synchronized void finish(Scope scope) {
    if (scope != active) {
      throw new AssertionError("Native execution scope mismatch");
    }
    active = null;
    OPERATORS.values().removeIf(value -> value == scope);
    STOCK_OPERATORS.values().removeIf(value -> value.scope == scope);
    String counts =
        scope.rows.entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.joining(","));
    Map<String, Long> restored = new TreeMap<>(scope.importedRows);
    restored.keySet().retainAll(scope.opened);
    String restoredCounts =
        restored.entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.joining(","));
    String stockCounts =
        scope.stockRows.entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.joining(","));
    String reportDirectory = System.getProperty("streamfusion.flink-suite.native-reports");
    if (reportDirectory == null) {
      throw new AssertionError("Missing streamfusion.flink-suite.native-reports directory");
    }
    try {
      Path directory = Path.of(reportDirectory);
      Files.createDirectories(directory);
      String recordId = UUID.randomUUID().toString();
      Files.writeString(
          directory.resolve(recordId + ".tsv"),
          scope.test
              + "\t"
              + scope.variant
              + "\t"
              + counts
              + "\t"
              + Base64.getEncoder()
                  .encodeToString(
                      String.join("\n", scope.fallbacks).getBytes(StandardCharsets.UTF_8))
              + (CONTRACTS.get(scope.test).get(scope.variant).startsWith("@elided:")
                  ? "\t" + restoredCounts + "\t" + stockCounts
                  : scope.stockRows.isEmpty()
                      ? (restored.isEmpty() ? "" : "\t" + restoredCounts)
                      : "\t" + restoredCounts + "\t" + stockCounts)
              + (CONTRACTS.get(scope.test).get(scope.variant).startsWith("@elided:")
                  ? "\t"
                      + Base64.getEncoder()
                          .encodeToString(
                              SqlInventory.json(
                                      Map.of(
                                          "submitted_graphs", scope.submittedGraphs,
                                          "native_opened", scope.opened,
                                          "native_imports", scope.importedRows))
                                  .getBytes(StandardCharsets.UTF_8))
                  : "")
              + "\n");
      SqlInventory.contract(recordId, scope.test, scope.variant);
    } catch (IOException e) {
      throw new AssertionError("Cannot write native execution evidence", e);
    }
    String contract = CONTRACTS.get(scope.test).get(scope.variant);
    boolean satisfied;
    if (contract.startsWith("@elided:")) {
      boolean interval = contract.equals(ElidedIntervalJoin.CONTRACT);
      satisfied =
          scope.test.equals(interval ? ElidedIntervalJoin.TEST : ElidedTemporalSort.TEST)
              && scope.variant.equals(
                  interval ? ElidedIntervalJoin.VARIANT : ElidedTemporalSort.VARIANT)
              && !scope.submittedGraphs.isEmpty()
              && scope.submittedGraphs.values().stream()
                  .allMatch(
                      graph ->
                          interval
                              ? ElidedIntervalJoin.valid(graph)
                              : ElidedTemporalSort.valid(graph))
              && (!interval || scope.rows.getOrDefault("NativeCalcOperator", 0L) > 0)
              && scope.stockRows.isEmpty()
              && scope.importedRows.isEmpty()
              && scope.fallbacks.isEmpty()
              && scope.opened.stream().allMatch(name -> name.equals("NativeCalcOperator"))
              && scope.rows.keySet().stream().allMatch(name -> name.equals("NativeCalcOperator"));
    } else if (contract.startsWith("@restore:")) {
      satisfied =
          scope.rows.isEmpty()
              && scope.stockRows.isEmpty()
              && scope.fallbacks.isEmpty()
              && restored.getOrDefault(contract.substring(9), 0L) > 0;
    } else if (contract.startsWith("@resume:")) {
      String[] clauses = contract.substring(8).split("&&", 2);
      String operator = clauses[0];
      satisfied =
          scope.rows.getOrDefault(operator, 0L) > 0
              && restored.getOrDefault(operator, 0L) > 0
              && scope.stockRows.isEmpty()
              && scope.fallbacks.isEmpty()
              && (clauses.length == 1 || workSatisfied(scope, clauses[1]));
    } else if (contract.startsWith("!@")) {
      String[] clauses = contract.split("&&", 2);
      String[] requirement = clauses[0].substring(2).split(":", 2);
      satisfied =
          scope.rows.getOrDefault(requirement[0], 0L) == 0
              && restored.getOrDefault(requirement[0], 0L) == 0
              && scope.fallbacks.contains(requirement[1])
              && (clauses.length == 1 || workSatisfied(scope, clauses[1]));
    } else {
      satisfied =
          contract.startsWith("!")
              ? scope.rows.isEmpty()
                  && restored.isEmpty()
                  && scope.fallbacks.contains(contract.substring(1))
              : workSatisfied(scope, contract);
    }
    if (!satisfied) {
      throw new AssertionError(
          "Execution contract failed for "
              + scope.test
              + " ["
              + scope.variant
              + "]"
              + "; expected "
              + contract
              + ", observed nonempty native input rows "
              + scope.rows
              + "; imported rows in opened native operators "
              + restored
              + "; stock state-owner input rows "
              + scope.stockRows
              + "; planner fallback reasons "
              + scope.fallbacks);
    }
    System.err.println(
        "StreamFusion execution proved "
            + scope.test
            + " ["
            + scope.variant
            + "]: "
            + (contract.startsWith("!")
                ? "fallback: " + contract.substring(1)
                : contract.startsWith("@restore:")
                    ? "canonical import: " + restoredCounts
                    : contract.startsWith("@resume:")
                        ? "canonical import: " + restoredCounts + "; new input: " + counts
                        : contract.startsWith("@elided:")
                            ? "submitted graph without "
                                + contract.substring(8)
                                + "; native input: "
                                + counts
                            : counts));
  }

  public static final class Scope {
    private final String test;
    private final String variant;
    private final Map<String, Long> rows = new TreeMap<>();
    private final Map<String, Long> importedRows = new TreeMap<>();
    private final Map<Long, Long> windowImports = new TreeMap<>();
    private final Map<Long, Long> groupImports = new TreeMap<>();
    private final Map<String, Long> stockRows = new TreeMap<>();
    private final Map<String, Map<String, Object>> submittedGraphs = new LinkedHashMap<>();
    private final Set<String> opened = new LinkedHashSet<>();
    private final Set<String> fallbacks = new LinkedHashSet<>();

    private Scope(String test, String variant) {
      this.test = test;
      this.variant = variant;
    }
  }

  private record StockBinding(Scope scope, String kind) {}
}
