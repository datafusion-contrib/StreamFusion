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
import tech.streamfusion.compat.ListCollectors;

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

  static final class Spec {
    private final String id;
    private final RuntimeExecutionMode mode;
    private final Map<String, String> settings;
    private final String sql;
    private final Set<String> tables;
    private final Set<String> functions;
    private final Kind expected;
    private final String reason;
    private final NativeFailureParity.Phase failurePhase;
    private final Comparison comparison;
    private final List<List<Object>> golden;

    Spec(
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
      this.id = id;
      this.mode = mode;
      this.settings = settings;
      this.sql = sql;
      this.tables = tables;
      this.functions = functions;
      this.expected = expected;
      this.reason = reason;
      this.failurePhase = failurePhase;
      this.comparison = comparison;
      this.golden = golden;
    }

    public String id() {
      return id;
    }

    public RuntimeExecutionMode mode() {
      return mode;
    }

    public Map<String, String> settings() {
      return settings;
    }

    public String sql() {
      return sql;
    }

    public Set<String> tables() {
      return tables;
    }

    public Set<String> functions() {
      return functions;
    }

    public Kind expected() {
      return expected;
    }

    public String reason() {
      return reason;
    }

    public NativeFailureParity.Phase failurePhase() {
      return failurePhase;
    }

    public Comparison comparison() {
      return comparison;
    }

    public List<List<Object>> golden() {
      return golden;
    }

    @Override
    public String toString() {
      return id;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      Spec that = (Spec) other;
      return java.util.Objects.equals(id, that.id)
          && java.util.Objects.equals(mode, that.mode)
          && java.util.Objects.equals(settings, that.settings)
          && java.util.Objects.equals(sql, that.sql)
          && java.util.Objects.equals(tables, that.tables)
          && java.util.Objects.equals(functions, that.functions)
          && java.util.Objects.equals(expected, that.expected)
          && java.util.Objects.equals(reason, that.reason)
          && java.util.Objects.equals(failurePhase, that.failurePhase)
          && java.util.Objects.equals(comparison, that.comparison)
          && java.util.Objects.equals(golden, that.golden);
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + java.util.Objects.hashCode(id);
      result = 31 * result + java.util.Objects.hashCode(mode);
      result = 31 * result + java.util.Objects.hashCode(settings);
      result = 31 * result + java.util.Objects.hashCode(sql);
      result = 31 * result + java.util.Objects.hashCode(tables);
      result = 31 * result + java.util.Objects.hashCode(functions);
      result = 31 * result + java.util.Objects.hashCode(expected);
      result = 31 * result + java.util.Objects.hashCode(reason);
      result = 31 * result + java.util.Objects.hashCode(failurePhase);
      result = 31 * result + java.util.Objects.hashCode(comparison);
      result = 31 * result + java.util.Objects.hashCode(golden);
      return result;
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
    switch (comparison) {
      case MATERIALIZED:
        return materialized(rows);
      case KEYED_FIRST_FIELD:
        return keyed(rows);
      case ORDERED_CHANGELOG:
        return rows;
      case KINDED_MULTISET:
        return NativeParity.multiset(rows);
      default:
        throw new IncompatibleClassChangeError();
    }
  }

  private static Map<List<Object>, Long> keyed(List<List<Object>> rows) {
    Map<Object, List<Object>> keyed = new HashMap<>();
    for (var row : rows) {
      var fields = new ArrayList<>(row.subList(1, row.size()));
      switch (row.get(0).toString()) {
        case "+I":
        case "+U":
          keyed.put(fields.get(0), fields);
          break;
        case "-D":
        case "-U":
          {
            {
          assertEquals(
              fields,
              keyed.remove(fields.get(0)),
              "retraction must remove the current keyed value");
        }
            break;
          }
        default:
          throw new AssertionError("unknown RowKind: " + row);
      }
    }
    Map<List<Object>, Long> result = new HashMap<>();
    keyed.values().forEach(row -> result.merge(row, 1L, Long::sum));
    return result;
  }

  static Map<List<Object>, Long> materialized(List<List<Object>> rows) {
    Map<List<Object>, Long> result = new HashMap<>();
    for (var row : rows) {
      long delta;
      switch (row.get(0).toString()) {
        case "+I":
        case "+U":
          delta = 1;
          break;
        case "-U":
        case "-D":
          delta = -1;
          break;
        default:
          throw new AssertionError("unknown RowKind: " + row);
      }
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

  static Map<String, Object> outcome(NativeFailureParity.Outcome outcome) {
    Map<String, Object> record = new LinkedHashMap<>();
    record.put("success", outcome.failure() == null);
    record.put("phase", outcome.phase().name());
    record.put("rowCount", outcome.rows().size());
    record.put("resultTypes", outcome.resultTypes());
    record.put(
        "rowKinds",
        outcome.rows().stream().map(row -> row.get(0)).distinct().collect(ListCollectors.toList()));
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
