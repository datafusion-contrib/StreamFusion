package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.planner.NativePlanner;
import tech.streamfusion.planner.PhysicalPlanScan;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
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

    assertTrue(
        scan.substitutions() > 0,
        "query did not route to native; parity check is moot; reasons=" + scan.fallbackReasons());
    assertEquals(sorted(host), sorted(nativeRows), "native result differs from host");
  }

  /**
   * Like {@link #assertParity} but each row is compared <em>with its RowKind</em>, so the raw
   * changelog must match change for change. This is the only assertion that can see an unsuppressed
   * no-op update: an identical {@code -U}/{@code +U} pair nets to zero in the collapsed changelog
   * and is invisible to a kind-blind row compare — yet with state TTL enabled Flink emits exactly
   * such pairs where it would otherwise suppress. Sound for single-input operators at parallelism 1,
   * whose emission order is deterministic (order-insensitivity still comes from the sort).
   */
  static void assertKindedParity(Supplier<TableEnvironment> environment, String sql)
      throws Exception {
    List<List<Object>> host = collectKinded(environment.get(), sql);

    TableEnvironment nativeEnvironment = environment.get();
    PhysicalPlanScan scan = NativePlanner.install(nativeEnvironment);
    List<List<Object>> nativeRows = collectKinded(nativeEnvironment, sql);

    assertTrue(
        scan.substitutions() > 0,
        "query did not route to native; parity check is moot; reasons=" + scan.fallbackReasons());
    assertEquals(sorted(host), sorted(nativeRows), "kinded changelog differs from host");
  }

  /** {@link #collect} with the row's kind prepended, so ±U/±D changes are distinguishable. */
  private static List<List<Object>> collectKinded(TableEnvironment environment, String sql)
      throws Exception {
    List<List<Object>> rows = new ArrayList<>();
    try (CloseableIterator<Row> iterator = environment.executeSql(sql).collect()) {
      while (iterator.hasNext()) {
        Row row = iterator.next();
        List<Object> fields = new ArrayList<>(row.getArity() + 1);
        fields.add(row.getKind().shortString());
        for (int i = 0; i < row.getArity(); i++) {
          fields.add(row.getField(i));
        }
        rows.add(fields);
      }
    }
    return rows;
  }

  /**
   * Like {@link #assertParity} but for a retracting/updating result: it compares the <em>collapsed</em>
   * changelog (each emitted row folded into a keyed multiset — added on {@code +I}/{@code +U}, removed
   * on {@code -U}/{@code -D}) rather than the raw stream of change rows. A two-input changelog operator
   * (an updating join) emits an interleaving-dependent raw changelog, but the net materialized result
   * is deterministic, so this verifies it end to end where {@link #assertParity} cannot.
   */
  static void assertChangelogParity(Supplier<TableEnvironment> environment, String sql)
      throws Exception {
    Map<List<Object>, Long> host = collapsedChangelog(environment.get(), sql);

    TableEnvironment nativeEnvironment = environment.get();
    PhysicalPlanScan scan = NativePlanner.install(nativeEnvironment);
    Map<List<Object>, Long> nativeRows = collapsedChangelog(nativeEnvironment, sql);

    assertTrue(scan.substitutions() > 0, "query did not route to native; parity check is moot");
    assertEquals(host, nativeRows, "collapsed changelog differs from host");
  }

  /**
   * Folds a query's emitted changelog into the materialized multiset it represents: each row's fields
   * mapped to a net count (incremented for insert/update-after, decremented for delete/update-before),
   * zero-count rows dropped. Order-independent, so it matches regardless of how a changelog interleaves.
   */
  private static Map<List<Object>, Long> collapsedChangelog(TableEnvironment environment, String sql)
      throws Exception {
    Map<List<Object>, Long> counts = new HashMap<>();
    try (CloseableIterator<Row> iterator = environment.executeSql(sql).collect()) {
      while (iterator.hasNext()) {
        Row row = iterator.next();
        List<Object> fields = new ArrayList<>(row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
          fields.add(row.getField(i));
        }
        long delta =
            row.getKind() == RowKind.DELETE || row.getKind() == RowKind.UPDATE_BEFORE ? -1 : 1;
        long next = counts.getOrDefault(fields, 0L) + delta;
        if (next == 0) {
          counts.remove(fields);
        } else {
          counts.put(fields, next);
        }
      }
    }
    return counts;
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
    Map<List<Object>, Long> host = collapsedChangelog(environment.get(), sql);

    TableEnvironment nativeEnvironment = environment.get();
    PhysicalPlanScan scan = NativePlanner.install(nativeEnvironment);
    Map<List<Object>, Long> nativeRows = collapsedChangelog(nativeEnvironment, sql);

    assertEquals(0, scan.substitutions(), "query unexpectedly routed to native");
    assertEquals(host, nativeRows, "fallback result differs from host");
    if (expectedReason != null) {
      assertTrue(
          scan.fallbackReasons().stream().anyMatch(r -> r.contains(expectedReason)),
          "no fallback reason contained \"" + expectedReason + "\"; reasons=" + scan.fallbackReasons());
    }
  }

  /**
   * Asserts the query routes to native (substitutes at least one operator) without comparing results
   * — for verifying an opt-in {@code allowIncompatible} flag enables native execution of a function
   * whose result is intentionally allowed to differ from the host.
   */
  static void assertRoutes(Supplier<TableEnvironment> environment, String sql) throws Exception {
    TableEnvironment nativeEnvironment = environment.get();
    PhysicalPlanScan scan = NativePlanner.install(nativeEnvironment);
    collect(nativeEnvironment, sql);
    assertTrue(scan.substitutions() > 0, "query did not route to native");
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
