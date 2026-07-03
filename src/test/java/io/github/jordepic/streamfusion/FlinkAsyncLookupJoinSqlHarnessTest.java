package io.github.jordepic.streamfusion;

import java.util.function.Supplier;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

/**
 * Nexmark q13's shape against an <b>async</b> lookup connector: the planner picks the async path, and
 * {@code NativeAsyncLookupJoinOperator} drives Flink's own async lookup runner for every probe row in
 * a batch concurrently, awaiting them before emitting. The join is byte-identical to Flink's async
 * lookup-join runner (it <em>is</em> that runner) while the probe-side Calc/source stay in the native
 * island. Covers the runner-provided shapes too: residual condition, dim-side calc, and pre-filter.
 */
class FlinkAsyncLookupJoinSqlHarnessTest {

  @Test
  void innerAsyncLookupJoinMatchesHost() throws Exception {
    NativeParity.assertParity(
        environment(),
        "SELECT B.auction, B.price, D.val FROM bid AS B"
            + " JOIN dim FOR SYSTEM_TIME AS OF B.p AS D ON MOD(B.auction, 5) = D.k");
  }

  @Test
  void leftAsyncLookupJoinNullPadsMisses() throws Exception {
    // MOD(auction, 7) can be 5 or 6, which the bounded dim (keys 0..4) has no row for — a LEFT join
    // null-pads those, exercising the miss path.
    NativeParity.assertParity(
        environment(),
        "SELECT B.auction, D.val FROM bid AS B"
            + " LEFT JOIN dim FOR SYSTEM_TIME AS OF B.p AS D ON MOD(B.auction, 7) = D.k");
  }

  @Test
  void asyncResidualConditionFiltersMatches() throws Exception {
    // The dim-vs-probe comparison lands as the residual condition, evaluated by the generated async
    // result future after the lookup completes.
    NativeParity.assertParity(
        environment(),
        "SELECT B.auction, D.val FROM bid AS B"
            + " LEFT JOIN dim FOR SYSTEM_TIME AS OF B.p AS D"
            + " ON MOD(B.auction, 5) = D.k AND CHAR_LENGTH(D.val) > B.auction");
  }

  @Test
  void asyncCalcOnTemporalTableProjectsAndFiltersDim() throws Exception {
    // A dim-only conjunct in the ON clause is pushed below the join as a calc on the temporal
    // table, applied to each looked-up row before the join condition.
    NativeParity.assertParity(
        environment(),
        "SELECT B.auction, D.val FROM bid AS B"
            + " LEFT JOIN dim FOR SYSTEM_TIME AS OF B.p AS D"
            + " ON MOD(B.auction, 5) = D.k AND CHAR_LENGTH(D.val) > 5");
  }

  @Test
  void asyncPreFilterConditionGatesLookups() throws Exception {
    // A probe-only conjunct under LEFT becomes the pre-filter: failing rows skip the lookup and
    // null-pad directly.
    NativeParity.assertParity(
        environment(),
        "SELECT B.auction, D.val FROM bid AS B"
            + " LEFT JOIN dim FOR SYSTEM_TIME AS OF B.p AS D"
            + " ON MOD(B.auction, 5) = D.k AND B.price < 400");
  }

  private static Supplier<TableEnvironment> environment() {
    return () -> {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      DataStream<Row> bid =
          env.fromData(
              Types.ROW_NAMED(new String[] {"auction", "price"}, Types.LONG, Types.LONG),
              Row.of(1L, 100L),
              Row.of(2L, 200L),
              Row.of(6L, 300L),
              Row.of(9L, 400L),
              Row.of(5L, 500L),
              Row.of(1L, 600L));
      tEnv.createTemporaryView(
          "bid",
          bid,
          Schema.newBuilder()
              .column("auction", DataTypes.BIGINT())
              .column("price", DataTypes.BIGINT())
              .columnByExpression("p", "PROCTIME()")
              .build());
      tEnv.executeSql(
          "CREATE TABLE dim (k BIGINT, val STRING) WITH ('connector' = 'test-lookup-async')");
      return tEnv;
    };
  }
}
