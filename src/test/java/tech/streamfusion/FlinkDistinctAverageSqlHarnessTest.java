package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkDistinctAverageSqlHarnessTest {
  @org.junit.jupiter.api.Test
  void decimalOverflowKeepsArrivalOrder() throws Exception {
    Supplier<TableEnvironment> source =
        () -> {
          var env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          var table = StreamTableEnvironment.create(env);
          table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
          BigDecimal large = BigDecimal.TEN.pow(37).multiply(BigDecimal.valueOf(9));
          var rows =
              List.of(
                  Row.of(large),
                  Row.of(large),
                  Row.of(large.subtract(BigDecimal.ONE)),
                  Row.of(large.negate()),
                  Row.of(large.negate().add(BigDecimal.ONE)),
                  Row.of(BigDecimal.valueOf(7)));
          table.createTemporaryView(
              "src",
              table.fromDataStream(
                  fromData(env, rows, Types.ROW_NAMED(new String[] {"d"}, Types.BIG_DEC)),
                  Schema.newBuilder().column("d", DECIMAL(38, 0)).build()));
          return table;
        };
    String sql = "SELECT AVG(DISTINCT d), COUNT(*) FROM src";
    assertTrue(NativePlanner.explain(source.get(), sql).contains("NativeColumnarGroupAggregate"));
    NativeParity.assertOrderedKindedParity(source, sql);
  }

  @org.junit.jupiter.api.Test
  void floatingDistinctAverageRetainsFallback() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> environment(false),
        "SELECT k, AVG(DISTINCT CAST(b AS DOUBLE)) FROM src GROUP BY k",
        "AVG(DISTINCT) requires an integer or DECIMAL");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void exactAveragesPreserveDuplicatesFiltersAndRetractions(boolean retract) throws Exception {
    compare(
        retract,
        "SELECT k, AVG(DISTINCT t), AVG(DISTINCT CAST(t AS SMALLINT)), AVG(DISTINCT CAST(t AS"
            + " INT)), AVG(DISTINCT b), AVG(DISTINCT d), COUNT(*) FROM src GROUP BY k");
    compare(
        retract,
        "SELECT k, AVG(DISTINCT t) FILTER (WHERE keep), AVG(DISTINCT b) FILTER (WHERE NOT keep),"
            + " AVG(DISTINCT d) FILTER (WHERE keep), COUNT(*) FROM src GROUP BY k");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void emptyAndGlobalAveragesKeepResultTypes(boolean retract) throws Exception {
    compare(retract, "SELECT AVG(DISTINCT t), AVG(DISTINCT b), AVG(DISTINCT d) FROM src");
    compare(
        retract, "SELECT AVG(DISTINCT t), AVG(DISTINCT b), AVG(DISTINCT d) FROM src WHERE k < 0");
  }

  private static void compare(boolean retract, String sql) throws Exception {
    Supplier<TableEnvironment> source = () -> environment(retract);
    String plan = NativePlanner.explain(source.get(), sql);
    assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
    NativeParity.assertOrderedKindedParity(source, sql);
  }

  private static TableEnvironment environment(boolean retract) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    List<Row> rows =
        new ArrayList<>(
            List.of(
                row(RowKind.INSERT, 1, -7, Long.MIN_VALUE, "-7.01", true),
                row(RowKind.INSERT, 1, -7, Long.MIN_VALUE, "-7.01", true),
                row(RowKind.INSERT, 1, 4, Long.MAX_VALUE, "4.02", false),
                row(RowKind.INSERT, 1, 9, 3L, "9.04", true),
                Row.of(1, null, null, null, null),
                Row.of(2, null, null, null, true)));
    if (retract)
      rows.addAll(
          List.of(
              row(RowKind.DELETE, 1, -7, Long.MIN_VALUE, "-7.01", true),
              row(RowKind.DELETE, 1, -7, Long.MIN_VALUE, "-7.01", true),
              row(RowKind.UPDATE_BEFORE, 1, 9, 3L, "9.04", true),
              row(RowKind.UPDATE_AFTER, 1, 2, 1L, "1.01", false),
              row(RowKind.DELETE, 1, 4, Long.MAX_VALUE, "4.02", false),
              row(RowKind.DELETE, 1, 2, 1L, "1.01", false),
              Row.ofKind(RowKind.DELETE, 1, null, null, null, null),
              row(RowKind.INSERT, 1, -3, -3L, "-3.00", true)));
    var stream =
        fromData(
            env,
            rows,
            Types.ROW_NAMED(
                new String[] {"k", "t", "b", "d", "keep"},
                Types.INT,
                Types.BYTE,
                Types.LONG,
                Types.BIG_DEC,
                Types.BOOLEAN));
    var schema =
        Schema.newBuilder()
            .column("k", INT())
            .column("t", TINYINT())
            .column("b", BIGINT())
            .column("d", DECIMAL(20, 2))
            .column("keep", BOOLEAN())
            .build();
    table.createTemporaryView(
        "src",
        retract ? table.fromChangelogStream(stream, schema) : table.fromDataStream(stream, schema));
    return table;
  }

  private static Row row(RowKind kind, int key, int tiny, long big, String decimal, boolean keep) {
    return Row.ofKind(kind, key, (byte) tiny, big, new BigDecimal(decimal), keep);
  }
}
