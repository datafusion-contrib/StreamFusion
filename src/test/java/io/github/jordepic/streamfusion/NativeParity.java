package io.github.jordepic.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.jordepic.streamfusion.planner.NativePlanner;
import io.github.jordepic.streamfusion.planner.PhysicalPlanScan;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

/**
 * Runs a query twice from a fresh environment each time, once entirely on the host engine and once
 * with native substitution installed, and asserts the results match. It also asserts the native run
 * actually substituted, so a silent fallback cannot masquerade as parity.
 *
 * <p>The environment is supplied by a factory because a table environment executes a query once;
 * each run needs its own with the same sources registered.
 */
final class NativeParity {

  private NativeParity() {}

  static void assertParity(Supplier<TableEnvironment> environment, String sql) throws Exception {
    List<List<Object>> host = collect(environment.get(), sql);

    TableEnvironment nativeEnvironment = environment.get();
    PhysicalPlanScan scan = NativePlanner.install(nativeEnvironment);
    List<List<Object>> nativeRows = collect(nativeEnvironment, sql);

    assertTrue(scan.substitutions() > 0, "query did not route to native; parity check is moot");
    assertEquals(sorted(host), sorted(nativeRows), "native result differs from host");
  }

  /**
   * Asserts the query does <em>not</em> route to native (every operator stays on the host) and still
   * produces the host result — the contract for an unsupported operation: a clean fallback, not a
   * wrong native answer.
   */
  static void assertFallback(Supplier<TableEnvironment> environment, String sql) throws Exception {
    assertFallbackReasonContains(environment, sql, null);
  }

  /**
   * Like {@link #assertFallback}, and additionally asserts that some recorded fallback reason
   * contains {@code expectedReason} (skip the reason check by passing null) — the visibility contract
   * of ticket 29: a query that does not accelerate must be able to say why.
   */
  static void assertFallbackReasonContains(
      Supplier<TableEnvironment> environment, String sql, String expectedReason) throws Exception {
    List<List<Object>> host = collect(environment.get(), sql);

    TableEnvironment nativeEnvironment = environment.get();
    PhysicalPlanScan scan = NativePlanner.install(nativeEnvironment);
    List<List<Object>> nativeRows = collect(nativeEnvironment, sql);

    assertEquals(0, scan.substitutions(), "query unexpectedly routed to native");
    assertEquals(sorted(host), sorted(nativeRows), "fallback result differs from host");
    if (expectedReason != null) {
      assertTrue(
          scan.fallbackReasons().stream().anyMatch(r -> r.contains(expectedReason)),
          "no fallback reason contained \"" + expectedReason + "\"; reasons=" + scan.fallbackReasons());
    }
  }

  private static List<List<Object>> collect(TableEnvironment environment, String sql)
      throws Exception {
    List<List<Object>> rows = new ArrayList<>();
    try (CloseableIterator<Row> iterator = environment.executeSql(sql).collect()) {
      while (iterator.hasNext()) {
        Row row = iterator.next();
        List<Object> fields = new ArrayList<>(row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
          fields.add(row.getField(i));
        }
        rows.add(fields);
      }
    }
    return rows;
  }

  private static List<List<Object>> sorted(List<List<Object>> rows) {
    rows.sort(Comparator.comparing(Object::toString));
    return rows;
  }
}
