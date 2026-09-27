package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.table.api.TableEnvironment;

final class SqlAuditHarness {
  enum Kind {
    NATIVE_PARITY,
    EXPLICIT_FALLBACK,
    SCAN_ONLY,
    BATCH_HOST_ONLY,
    HOST_FAILURE,
    NATIVE_FAILURE,
    UNCLASSIFIED,
    VALIDATION_FAILURE
  }

  enum Comparison {
    KINDED_MULTISET,
    ORDERED_CHANGELOG,
    MATERIALIZED,
    KEYED_FIRST_FIELD
  }

  record Spec(
      String id,
      RuntimeExecutionMode mode,
      Map<String, String> settings,
      String sql,
      Set<String> tables,
      Set<String> functions,
      Kind expected,
      String reason,
      NativeFailureParity.Phase failurePhase,
      Comparison comparison,
      List<List<Object>> golden) {
    @Override
    public String toString() {
      return id;
    }
  }

  private static final Pattern PLACEHOLDER =
      Pattern.compile("\\$\\{[^}]*}|\\$[A-Za-z_][A-Za-z0-9_]*|\\{\\{[^}]*}}");
  private static final Set<String> SCANS =
      Set.of(
          "StreamPhysicalDataStreamScan",
          "StreamPhysicalLegacyTableSourceScan",
          "StreamPhysicalTableSourceScan",
          "StreamPhysicalValues",
          "StreamPhysicalSink");

  static List<Map<String, String>> expand(Map<String, List<String>> parameters) {
    List<Map<String, String>> expanded = List.of(Map.of());
    for (var parameter : parameters.entrySet()) {
      if (parameter.getValue().isEmpty()
          || Set.copyOf(parameter.getValue()).size() != parameter.getValue().size()) {
        throw new IllegalArgumentException("empty or duplicate variants: " + parameter.getKey());
      }
      List<Map<String, String>> next = new ArrayList<>();
      for (var previous : expanded) {
        for (String value : parameter.getValue()) {
          Map<String, String> row = new LinkedHashMap<>(previous);
          row.put(parameter.getKey(), value);
          next.add(Map.copyOf(row));
        }
      }
      expanded = next;
    }
    return expanded;
  }

  static String render(String template, Map<String, String> parameters) {
    String sql = template;
    for (var parameter : parameters.entrySet()) {
      sql = sql.replace("${" + parameter.getKey() + "}", parameter.getValue().replace("'", "''"));
    }
    rejectPlaceholders(sql);
    return sql;
  }

  static void preflight(Spec spec, TableEnvironment table) {
    rejectPlaceholders(spec.sql());
    requireRegistered("table", spec.tables(), table.listTables());
    requireRegistered("function", spec.functions(), table.listUserDefinedFunctions());
    for (var setting : spec.settings().entrySet()) {
      assertEquals(
          setting.getValue(),
          table.getConfig().getConfiguration().getString(setting.getKey(), null),
          "fixture configuration changed: " + setting.getKey());
    }
  }

  private static void rejectPlaceholders(String sql) {
    if (PLACEHOLDER.matcher(sql).find())
      throw new IllegalArgumentException("unresolved SQL placeholder: " + sql);
  }

  private static void requireRegistered(String kind, Set<String> required, String[] registered) {
    Set<String> names =
        Arrays.stream(registered)
            .map(s -> s.toLowerCase(Locale.ROOT))
            .collect(java.util.stream.Collectors.toSet());
    for (String name : required) {
      if (!names.contains(name.toLowerCase(Locale.ROOT)))
        throw new IllegalArgumentException("missing fixture " + kind + ": " + name);
    }
  }

  static NativeFailureParity.Comparison execute(Spec spec, Supplier<TableEnvironment> factory) {
    return NativeFailureParity.run(
        () -> {
          TableEnvironment table = factory.get();
          preflight(spec, table);
          return table;
        },
        spec.sql());
  }

  static Kind classify(Spec spec, NativeFailureParity.Comparison runs) {
    if (runs.host().failure() != null) return Kind.HOST_FAILURE;
    if (runs.nativeRun().failure() != null) return Kind.NATIVE_FAILURE;
    var nativeRun = runs.nativeRun();
    if (nativeRun.substitutions() > 0) return Kind.NATIVE_PARITY;
    if (spec.mode() == RuntimeExecutionMode.BATCH) return Kind.BATCH_HOST_ONLY;
    if (!nativeRun.fallbackReasons().isEmpty()) return Kind.EXPLICIT_FALLBACK;
    if (!nativeRun.operatorTypes().isEmpty() && SCANS.containsAll(nativeRun.operatorTypes()))
      return Kind.SCAN_ONLY;
    return Kind.UNCLASSIFIED;
  }

  static void verify(Spec spec, NativeFailureParity.Comparison runs) {
    assertEquals(spec.expected(), classify(spec, runs), spec.id() + ": " + runs);
    if (spec.expected() == Kind.HOST_FAILURE) {
      assertNotNull(runs.nativeRun().failure(), runs.toString());
      assertEquals(
          runs.host().rootCause().getClass(),
          runs.nativeRun().rootCause().getClass(),
          runs.toString());
      for (var run : List.of(runs.host(), runs.nativeRun())) {
        assertEquals(spec.failurePhase(), run.phase(), run.toString());
        assertTrue(
            String.valueOf(run.rootCause().getMessage()).contains(spec.reason()), run.toString());
      }
      return;
    }
    assertNull(runs.host().failure(), runs.toString());
    assertNull(runs.nativeRun().failure(), runs.toString());
    if (spec.expected() == Kind.EXPLICIT_FALLBACK) {
      assertTrue(
          runs.nativeRun().fallbackReasons().stream().anyMatch(r -> r.contains(spec.reason())),
          runs.toString());
    }
    assertEquals(
        result(spec.comparison(), runs.host().rows()),
        result(spec.comparison(), runs.nativeRun().rows()),
        runs.toString());
    if (!spec.golden().isEmpty()) {
      Map<List<Object>, Long> expected = NativeParity.multiset(spec.golden());
      assertEquals(
          expected,
          spec.comparison() == Comparison.KEYED_FIRST_FIELD
              ? keyed(runs.host().rows())
              : materialized(runs.host().rows()),
          "fixture did not implement its declared semantics: " + spec.id());
    }
  }

  private static Object result(Comparison comparison, List<List<Object>> rows) {
    return switch (comparison) {
      case MATERIALIZED -> materialized(rows);
      case KEYED_FIRST_FIELD -> keyed(rows);
      case ORDERED_CHANGELOG -> rows;
      case KINDED_MULTISET -> NativeParity.multiset(rows);
    };
  }

  private static Map<List<Object>, Long> keyed(List<List<Object>> rows) {
    Map<Object, List<Object>> keyed = new HashMap<>();
    for (var row : rows) {
      var fields = new ArrayList<>(row.subList(1, row.size()));
      switch (row.get(0).toString()) {
        case "+I", "+U" -> keyed.put(fields.get(0), fields);
        case "-D", "-U" -> {
          assertEquals(
              fields,
              keyed.remove(fields.get(0)),
              "retraction must remove the current keyed value");
        }
        default -> throw new AssertionError("unknown RowKind: " + row);
      }
    }
    Map<List<Object>, Long> result = new HashMap<>();
    keyed.values().forEach(row -> result.merge(row, 1L, Long::sum));
    return result;
  }

  private static Map<List<Object>, Long> materialized(List<List<Object>> rows) {
    Map<List<Object>, Long> result = new HashMap<>();
    for (var row : rows) {
      long delta =
          switch (row.get(0).toString()) {
            case "+I", "+U" -> 1;
            case "-U", "-D" -> -1;
            default -> throw new AssertionError("unknown RowKind: " + row);
          };
      result.merge(new ArrayList<>(row.subList(1, row.size())), delta, Long::sum);
    }
    result.values().removeIf(count -> count == 0);
    return result;
  }

  static Map<String, Object> record(
      Spec spec, NativeFailureParity.Comparison runs, boolean passed) {
    Map<String, Object> record = new LinkedHashMap<>();
    record.put("id", spec.id());
    record.put("mode", spec.mode().name());
    record.put("configuration", spec.settings());
    record.put("sql", spec.sql());
    record.put("requiredTables", spec.tables());
    record.put("requiredFunctions", spec.functions());
    record.put("comparison", spec.comparison().name());
    record.put("expected", spec.expected().name());
    record.put("observedExecution", classify(spec, runs).name());
    record.put("outcome", passed ? classify(spec, runs).name() : Kind.VALIDATION_FAILURE.name());
    record.put("passed", passed);
    record.put("host", outcome(runs.host()));
    record.put("nativeEnabled", outcome(runs.nativeRun()));
    return record;
  }

  private static Map<String, Object> outcome(NativeFailureParity.Outcome outcome) {
    Map<String, Object> record = new LinkedHashMap<>();
    record.put("success", outcome.failure() == null);
    record.put("phase", outcome.phase().name());
    record.put("rowCount", outcome.rows().size());
    record.put("rowKinds", outcome.rows().stream().map(row -> row.get(0)).distinct().toList());
    record.put("substitutions", outcome.substitutions());
    record.put("operators", outcome.operatorTypes());
    record.put("fallbackReasons", outcome.fallbackReasons());
    if (outcome.failure() != null) {
      record.put("causeClass", outcome.rootCause().getClass().getName());
      record.put("causeMessage", outcome.rootCause().getMessage());
    }
    return record;
  }

  private SqlAuditHarness() {}
}
