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
  private static Scope active;

  private NativeExecution() {}

  private static Map<String, Map<String, String>> loadContracts() {
    String flinkLine = System.getProperty("streamfusion.flink-suite.flink-line", "2.2");
    String resource =
        switch (flinkLine) {
          case "2.2" -> "/native-execution.tsv";
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
            || !(fields[2].matches("\\w+(?:[+|]\\w+)*") || fields[2].matches("!.+"))) {
          throw new IllegalStateException("Invalid native execution contract: " + line);
        }
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

  static boolean matches(String selector, Object fixture) {
    if (selector.equals("*")) return true;
    for (String condition : selector.split("&")) {
      String[] parts = condition.split("=", 2);
      Object value =
          parts[0].equals("changelog") ? changelogEnabled(fixture) : field(fixture, parts[0]);
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

  public static synchronized void opened(Object operator) {
    SqlInventory.opened(operator);
    if (active != null) {
      OPERATORS.putIfAbsent(operator, active);
    }
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

  public static synchronized void finish(Scope scope) {
    if (scope != active) {
      throw new AssertionError("Native execution scope mismatch");
    }
    active = null;
    OPERATORS.values().removeIf(value -> value == scope);
    String counts =
        scope.rows.entrySet().stream()
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
              + "\n");
      SqlInventory.contract(recordId, scope.test, scope.variant);
    } catch (IOException e) {
      throw new AssertionError("Cannot write native execution evidence", e);
    }
    String contract = CONTRACTS.get(scope.test).get(scope.variant);
    boolean satisfied =
        contract.startsWith("!")
            ? scope.rows.isEmpty() && scope.fallbacks.contains(contract.substring(1))
            : Arrays.stream(contract.split("\\|"))
                .anyMatch(
                    route ->
                        Arrays.stream(route.split("\\+"))
                            .allMatch(operator -> scope.rows.getOrDefault(operator, 0L) > 0));
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
              + "; planner fallback reasons "
              + scope.fallbacks);
    }
    System.err.println(
        "StreamFusion execution proved "
            + scope.test
            + " ["
            + scope.variant
            + "]: "
            + (contract.startsWith("!") ? "fallback: " + contract.substring(1) : counts));
  }

  public static final class Scope {
    private final String test;
    private final String variant;
    private final Map<String, Long> rows = new TreeMap<>();
    private final Set<String> fallbacks = new LinkedHashSet<>();

    private Scope(String test, String variant) {
      this.test = test;
      this.variant = variant;
    }
  }
}
