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

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void floatingAveragesPreserveDuplicatesFiltersAndRetractions(boolean retract) throws Exception {
    compare(
        retract,
        "SELECT k, AVG(DISTINCT CAST(b AS DOUBLE)), AVG(DISTINCT CAST(b AS FLOAT)),"
            + " AVG(DISTINCT CAST(t AS FLOAT)) FILTER (WHERE keep), COUNT(*) FROM src GROUP BY k");
    compare(
        retract,
        "SELECT AVG(DISTINCT CAST(b AS DOUBLE)), AVG(DISTINCT CAST(b AS FLOAT)) FROM src");
    compare(
        retract,
        "SELECT AVG(DISTINCT CAST(b AS DOUBLE)), AVG(DISTINCT CAST(b AS FLOAT)) FROM src WHERE k <"
            + " 0");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void floatingMembershipKeepsNanPayloadsAndSignedZeros(boolean retract) throws Exception {
    java.util.function.Supplier<TableEnvironment> setup =
        () -> {
          var env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          var table = StreamTableEnvironment.create(env);
          table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
          long[] bits = {
            Double.doubleToRawLongBits(-0.0), Double.doubleToRawLongBits(0.0),
            Double.doubleToRawLongBits(-0.0), Double.doubleToRawLongBits(3.5),
            0x7ff8000000000001L, 0xfff8000000000002L,
            Double.doubleToRawLongBits(Double.POSITIVE_INFINITY),
            Double.doubleToRawLongBits(Double.NEGATIVE_INFINITY)
          };
          int count = retract ? bits.length * 2 + 1 : bits.length;
          var stream = env.fromSequence(0, count - 1).map(index -> {
            int slot = (int) (index % bits.length);
            boolean deletion = index >= bits.length && index < bits.length * 2;
            double value = Double.longBitsToDouble(bits[slot]);
            return Row.ofKind(deletion ? RowKind.DELETE : RowKind.INSERT,
                slot < 4 ? 1 : 2, (float) value, value);
          }).returns(Types.ROW_NAMED(new String[] {"k", "f", "d"},
              Types.INT, Types.FLOAT, Types.DOUBLE));
          var schema = Schema.newBuilder().column("k", INT()).column("f", FLOAT())
              .column("d", DOUBLE()).build();
          table.createTemporaryView("special", retract
              ? table.fromChangelogStream(stream, schema) : table.fromDataStream(stream, schema));
          return table;
        };
    String sql = "SELECT k, AVG(DISTINCT f), AVG(DISTINCT d), COUNT(DISTINCT f), "
        + "COUNT(DISTINCT d) FROM special GROUP BY k";
    assertTrue(NativePlanner.explain(setup.get(), sql).contains("NativeColumnarGroupAggregate"));
    NativeParity.assertOrderedKindedParity(setup, sql);
  }

  @ParameterizedTest
  @ValueSource(strings = {"hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"})
  void floatingMembershipAndRawSumsSurviveRepeatedRecovery(String backend) {
    var type = ROW(FIELD("k", INT()), FIELD("f", FLOAT()), FIELD("d", DOUBLE()));
    List<Row> input = new ArrayList<>();
    for (int row = 0; row < 32; row++) {
      Double value = row % 7 == 0 ? null : new double[] {-0.0, 0.0, 3.5, -2.25}[row % 4];
      input.add(Row.of(row % 3, value == null ? null : value.floatValue(), value));
    }
    try (var recovery = new PortableSqlRecovery(backend, input,
        org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(type),
        Schema.newBuilder().fromRowDataType(type).build(), false, 8, 24).withExecutionMetrics()) {
      java.util.function.UnaryOperator<TableEnvironment> configure = table -> {
        table.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
        return table;
      };
      var result = NativeFailureParity.runRecovery(
          () -> configure.apply(recovery.uninterrupted()),
          () -> configure.apply(recovery.get()),
          "SELECT k, AVG(DISTINCT f), AVG(DISTINCT d), COUNT(*) FROM recovery_input GROUP BY k");
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      org.junit.jupiter.api.Assertions.assertEquals(result.host().resultTypes(),
          result.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(
          recovery.nativeExecution(), "NativeColumnarGroupAggregate");
    }
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

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT k, AVG(DISTINCT d), COUNT(*) FROM src GROUP BY k",
        "SELECT k, AVG(DISTINCT t), AVG(DISTINCT b), AVG(DISTINCT CAST(d AS DECIMAL(19,2))),"
            + " COUNT(*) FROM src GROUP BY k",
        "SELECT k, AVG(DISTINCT b), COUNT(DISTINCT b), SUM(DISTINCT b), AVG(t), COUNT(DISTINCT t)"
            + " FROM src GROUP BY k",
        "SELECT k, AVG(DISTINCT t) FILTER (WHERE keep), AVG(DISTINCT t) FILTER (WHERE NOT keep),"
            + " AVG(DISTINCT CAST(d AS DECIMAL(19,2))) FILTER (WHERE keep), COUNT(*) FROM src GROUP"
            + " BY k",
        "SELECT AVG(DISTINCT t), AVG(DISTINCT b), AVG(DISTINCT CAST(d AS DECIMAL(19,2))) FROM src",
        "SELECT AVG(DISTINCT t), AVG(DISTINCT b), AVG(DISTINCT CAST(d AS DECIMAL(19,2))) FROM src"
            + " WHERE k < 0"
      })
  void twoPhaseAveragesMergeDistinctViews(String sql) throws Exception {
    Supplier<TableEnvironment> source = () -> twoPhaseEnvironment(false);
    String plan = NativePlanner.explain(source.get(), sql);
    assertTrue(plan.contains("NativeColumnarLocalGroupAggregate"), plan);
    assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
    NativeParity.assertChangelogParity(source, sql);
  }

  @org.junit.jupiter.api.Test
  void twoPhaseRetractingDistinctMergesSignedViews() throws Exception {
    NativeParity.assertChangelogParity(
        () -> twoPhaseEnvironment(true), "SELECT k, AVG(DISTINCT b) FROM src GROUP BY k");
  }

  private static TableEnvironment twoPhaseEnvironment(boolean retract) {
    var table = environment(retract, 6);
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("table.exec.mini-batch.enabled", "true");
    table.getConfig().set("table.exec.mini-batch.allow-latency", "100 ms");
    table.getConfig().set("table.exec.mini-batch.size", "5");
    return table;
  }

  private static void compare(boolean retract, String sql) throws Exception {
    Supplier<TableEnvironment> source = () -> environment(retract);
    String plan = NativePlanner.explain(source.get(), sql);
    assertTrue(plan.contains("NativeColumnarGroupAggregate"), plan);
    NativeParity.assertOrderedKindedParity(source, sql);
  }

  private static TableEnvironment environment(boolean retract) {
    return environment(retract, 1);
  }

  private static TableEnvironment environment(boolean retract, int repetitions) {
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
    if (!retract && repetitions > 1) {
      var seed = new ArrayList<>(rows);
      for (int i = 1; i < repetitions; i++) rows.addAll(seed);
    }
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
