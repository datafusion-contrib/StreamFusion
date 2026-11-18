package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.HashSet;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkRandomSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {
    "SELECT RAND(42), RAND_INTEGER(42, 100) FROM inputs",
    "SELECT id, RAND(42), -RAND(42), RAND_INTEGER(42, 100) FROM inputs",
    "SELECT id, RAND(-2147483648), RAND(2147483647), RAND(seed) FROM inputs",
    "SELECT id, RAND_INTEGER(42, bound), RAND_INTEGER(seed, bound) FROM inputs",
    "SELECT id, RAND(CAST(NULL AS INT)), RAND_INTEGER(CAST(NULL AS INT), bound), "
        + "RAND_INTEGER(42, CAST(NULL AS INT)) FROM inputs",
    "SELECT id, CASE WHEN MOD(id, 3) = 0 THEN RAND(42) ELSE RAND(-1) END FROM inputs",
    "SELECT id, RAND(42) FROM inputs WHERE RAND_INTEGER(id, 100) < 50",
    "SELECT id, RAND(42) FROM inputs WHERE RAND_INTEGER(42, 100) < 50"
  })
  void seededStreamsMatchFlinkAcrossBatchesAndCallSites(String sql) throws Exception {
    NativeParity.assertParity(FlinkRandomSqlHarnessTest::environment, sql);
  }

  @Test
  void floatingNegationPreservesSignedZeroAndSpecialValues() throws Exception {
    NativeParity.assertParity(
        () -> {
          var env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          var table = StreamTableEnvironment.create(env);
          table.createTemporaryView(
              "numbers",
              fromData(
                  env,
                  Types.ROW_NAMED(new String[] {"d", "f"}, Types.DOUBLE, Types.FLOAT),
                  Row.of(0.0, 0.0f),
                  Row.of(-0.0, -0.0f),
                  Row.of(Double.NaN, Float.NaN),
                  Row.of(Double.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY),
                  Row.of(null, null)));
          return table;
        },
        "SELECT -d, -f FROM numbers");
  }

  @Test
  void unseededStreamsProducePerRowValuesWithFlinkTypesAndRanges() throws Exception {
    for (boolean nativeRun : new boolean[] {false, true}) {
      TableEnvironment table = environment();
      var scan = nativeRun ? NativePlanner.install(table) : null;
      Set<Double> doubles = new HashSet<>();
      Set<Integer> integers = new HashSet<>();
      int count = 0;
      try (var rows = table.executeSql(
          "SELECT id, RAND(), RAND_INTEGER(100), RAND_INTEGER(bound) FROM inputs").collect()) {
        while (rows.hasNext()) {
          Row row = rows.next();
          double d = (Double) row.getField(1);
          int i = (Integer) row.getField(2);
          assertTrue(d >= 0 && d < 1);
          assertTrue(i >= 0 && i < 100);
          doubles.add(d);
          integers.add(i);
          int id = (Integer) row.getField(0);
          if (id % 11 == 0) {
            assertEquals(null, row.getField(3));
          } else {
            int bounded = (Integer) row.getField(3);
            assertTrue(bounded >= 0 && bounded < bound(id));
          }
          count++;
        }
      }
      assertEquals(5003, count);
      assertTrue(doubles.size() > 4000, "RAND must advance within and across batches");
      assertTrue(integers.size() > 90, "RAND_INTEGER must advance within and across batches");
      if (nativeRun) assertTrue(scan.substitutions() > 0, scan::explainSummary);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
  void invalidBoundsFailOnlyWhenEvaluated(int bound) {
    for (boolean nativeRun : new boolean[] {false, true}) {
      TableEnvironment table = input(Row.of(1, 42, bound));
      var scan = nativeRun ? NativePlanner.install(table) : null;
      Exception failure =
          assertThrows(
              Exception.class,
              () -> {
                try (var rows =
                    table.executeSql("SELECT RAND_INTEGER(seed, bound) FROM inputs").collect()) {
                  while (rows.hasNext()) rows.next();
                }
              });
      StringBuilder messages = new StringBuilder();
      for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
        messages.append(cause.getMessage());
      }
      assertTrue(messages.toString().contains("positive"), messages.toString());
      if (nativeRun) assertTrue(scan.substitutions() > 0, scan::explainSummary);
    }
  }

  @Test
  void nullSeedSuppressesInvalidBoundAndCasePreservesLazyEvaluation() throws Exception {
    NativeParity.assertParity(
        () -> input(Row.of(1, null, 0), Row.of(2, 42, 1)),
        "SELECT id, RAND_INTEGER(seed, bound), CASE WHEN bound > 0 THEN "
            + "RAND_INTEGER(42, bound) ELSE 99 END FROM inputs");
  }

  @Test
  void fallibleConjunctionRetainsFlinkShortCircuiting() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> input(Row.of(1, 42, 0), Row.of(2, 42, 1)),
        "SELECT id FROM inputs WHERE bound > 0 AND RAND_INTEGER(seed, bound) = 0",
        "short-circuit");
  }

  private static int bound(int row) {
    return new int[] {1, 2, 16, 100, 1_073_741_825, Integer.MAX_VALUE}[row % 6];
  }

  private static TableEnvironment environment() {
    Row[] rows = new Row[5003];
    for (int i = 0; i < rows.length; i++) {
      rows[i] = Row.of(i, i % 7 == 0 ? null : i - 2500, i % 11 == 0 ? null : bound(i));
    }
    return input(rows);
  }

  private static TableEnvironment input(Row... rows) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "inputs",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"id", "seed", "bound"}, Types.INT, Types.INT, Types.INT),
            rows));
    return table;
  }
}
