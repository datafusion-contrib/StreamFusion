package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

class FlinkMiniBatchValidationSqlHarnessTest {
  private enum Shape {
    JOIN("SELECT a.k, a.v, b.v FROM src a JOIN src b ON a.k = b.k"),
    LEFT_JOIN("SELECT a.k, a.v, b.v FROM src a LEFT JOIN src b ON a.k = b.k"),
    RIGHT_JOIN("SELECT a.k, a.v, b.v FROM src a RIGHT JOIN src b ON a.k = b.k"),
    FULL_JOIN("SELECT a.k, a.v, b.v FROM src a FULL JOIN src b ON a.k = b.k"),
    UPDATING_JOIN("SELECT a.k, a.v, b.v FROM changes a JOIN changes b ON a.k = b.k"),
    SEMI_JOIN("SELECT a.k, a.v FROM src a WHERE EXISTS (SELECT 1 FROM src b WHERE b.k = a.k)"),
    ANTI_JOIN("SELECT a.k, a.v FROM src a WHERE NOT EXISTS (SELECT 1 FROM src b WHERE b.k = a.k)"),
    GROUP("SELECT k, SUM(v), COUNT(*) FROM src GROUP BY k"),
    TWO_PHASE_GROUP("SELECT k, SUM(v), COUNT(*) FROM src GROUP BY k"),
    ROWTIME_FIRST("SELECT k, v FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY rt) rn FROM src) WHERE rn = 1"),
    ROWTIME_LAST("SELECT k, v FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY rt DESC) rn FROM src) WHERE rn = 1"),
    PROCTIME_FIRST("SELECT k, v FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY pt) rn FROM src) WHERE rn = 1"),
    PROCTIME_LAST("SELECT k, v FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY pt DESC) rn FROM src) WHERE rn = 1"),
    NORMALIZE("SELECT k, v FROM up");

    final String sql;

    Shape(String sql) {
      this.sql = sql;
    }
  }

  @TestFactory
  Stream<DynamicTest> invalidSizesMatchTheSelectedHostOperator() {
    return Stream.of(Shape.values()).flatMap(shape ->
        Stream.of("unset", "0", "-7").map(size -> DynamicTest.dynamicTest(
            shape + "/" + size, () -> {
              try (var metrics = new RecoveryExecutionMetrics()) {
                var comparison = NativeFailureParity.run(
                    () -> environment(metrics, shape, true, size), shape.sql);
                boolean join = shape.name().contains("JOIN");
                boolean rejects = shape != Shape.SEMI_JOIN && shape != Shape.ANTI_JOIN
                    && (!join || tech.streamfusion.compat.FlinkTestCapabilities.MINIBATCH_JOIN);
                if (rejects) {
                  comparison.assertFailure(IllegalArgumentException.class,
                      "table.exec.mini-batch.size", NativeFailureParity.Phase.PLANNING,
                      NativeFailureParity.Route.NATIVE);
                  assertEquals(comparison.host().rootCause().getMessage(),
                      comparison.nativeRun().rootCause().getMessage(), comparison::toString);
                  assertTrue(metrics.nativeOperators().isEmpty(), metrics.nativeOperators()::toString);
                } else {
                  assertMaterializedParity(comparison);
                  assertConsumed(metrics);
                }
              }
            })));
  }

  @TestFactory
  Stream<DynamicTest> disabledMiniBatchDoesNotValidateUnusedSize() {
    return Stream.of(Shape.values()).filter(shape -> shape != Shape.TWO_PHASE_GROUP)
        .map(shape -> DynamicTest.dynamicTest(shape.toString(), () -> {
          try (var metrics = new RecoveryExecutionMetrics()) {
            var comparison = NativeFailureParity.run(
                () -> environment(metrics, shape, false, "0"), shape.sql);
            assertMaterializedParity(comparison);
            assertConsumed(metrics);
          }
        }));
  }

  @TestFactory
  Stream<DynamicTest> positiveSizesKeepNativeExecutionAndDuplicateRows() {
    return Stream.of(Shape.values()).map(shape -> DynamicTest.dynamicTest(shape.toString(), () -> {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var comparison = NativeFailureParity.run(
            () -> environment(metrics, shape, true, "2"), shape.sql);
        // Two-input and bundled changelogs may differ in intermediate updates.
        assertMaterializedParity(comparison);
        if (shape == Shape.JOIN) {
          assertEquals(9, comparison.nativeRun().rows().size(), comparison::toString);
          assertTrue(comparison.nativeRun().plan().contains("NativeColumnarUpdatingJoin"));
        }
        assertConsumed(metrics);
      }
    }));
  }

  private static void assertMaterializedParity(NativeFailureParity.Comparison comparison) {
    assertNull(comparison.host().failure(), comparison::toString);
    assertNull(comparison.nativeRun().failure(), comparison::toString);
    assertEquals(materialize(comparison.host().rows()), materialize(comparison.nativeRun().rows()),
        comparison::toString);
    assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
    assertEquals(NativeFailureParity.Route.NATIVE, comparison.nativeRun().route());
  }

  private static Map<List<Object>, Long> materialize(List<List<Object>> rows) {
    var result = new HashMap<List<Object>, Long>();
    for (var row : rows) {
      long change = row.get(0).equals("+I") || row.get(0).equals("+U") ? 1 : -1;
      result.merge(row.subList(1, row.size()), change, Long::sum);
    }
    result.values().removeIf(count -> count == 0);
    return result;
  }

  private static void assertConsumed(RecoveryExecutionMetrics metrics) {
    assertTrue(metrics.nativeOperators().stream()
        .anyMatch(row -> ((Number) row.get("recordsIn")).longValue() > 0),
        metrics.nativeOperators()::toString);
  }

  private static TableEnvironment environment(
      RecoveryExecutionMetrics metrics, Shape shape, boolean miniBatch, String size) {
    var env = metrics.environment(new Configuration(), 1, 0);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy",
        shape == Shape.TWO_PHASE_GROUP ? "TWO_PHASE" : "ONE_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", Boolean.toString(miniBatch));
    table.getConfig().set("table.exec.mini-batch.allow-latency", "1 h");
    if (!size.equals("unset")) table.getConfig().set("table.exec.mini-batch.size", size);
    var type = Types.ROW_NAMED(new String[] {"k", "v", "rt"},
        Types.INT, Types.LONG, Types.LOCAL_DATE_TIME);
    var rt = LocalDateTime.of(2020, 1, 1, 0, 0);
    table.createTemporaryView("src", table.fromDataStream(
        fromData(env, type, Row.of(1, 10L, rt), Row.of(1, 10L, rt.plusSeconds(1)),
            Row.of(1, 20L, rt.plusSeconds(2))),
        Schema.newBuilder().column("k", DataTypes.INT()).column("v", DataTypes.BIGINT())
            .column("rt", DataTypes.TIMESTAMP(3)).watermark("rt", "rt - INTERVAL '1' SECOND")
            .columnByExpression("pt", "PROCTIME()").build()));
    var changes = fromData(env, Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.LONG),
        Row.ofKind(RowKind.INSERT, 1, 10L), Row.ofKind(RowKind.INSERT, 1, 20L),
        Row.ofKind(RowKind.DELETE, 1, 10L), Row.ofKind(RowKind.INSERT, 1, 30L));
    table.createTemporaryView("changes", table.fromChangelogStream(changes));
    table.createTemporaryView("up", table.fromChangelogStream(
        fromData(env, Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.LONG),
            Row.ofKind(RowKind.INSERT, 1, 10L), Row.ofKind(RowKind.UPDATE_AFTER, 1, 20L),
            Row.ofKind(RowKind.INSERT, 2, 30L)),
        Schema.newBuilder().column("k", DataTypes.INT().notNull()).column("v", DataTypes.BIGINT())
            .primaryKey("k").build(), ChangelogMode.upsert()));
    return table;
  }
}
