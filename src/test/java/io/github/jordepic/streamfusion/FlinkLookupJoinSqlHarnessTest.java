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
 * Nexmark q13's shape: a processing-time lookup join against a bounded side input ({@code JOIN dim FOR
 * SYSTEM_TIME AS OF probe.proctime ON mod(auction, n) = dim.k}). The native operator drives Flink's
 * own generated lookup-join runner (which calls the connector's real {@code LookupFunction}), so the
 * join is byte-identical to the host while the probe-side Calc/source stay in the native island.
 * Covers the runner-provided shapes too: residual condition, dim-side calc, pre-filter, and constant
 * lookup keys.
 */
class FlinkLookupJoinSqlHarnessTest {

  @Test
  void innerLookupJoinMatchesHost() throws Exception {
    NativeParity.assertParity(
        environment(),
        "SELECT B.auction, B.price, D.val FROM bid AS B"
            + " JOIN dim FOR SYSTEM_TIME AS OF B.p AS D ON MOD(B.auction, 5) = D.k");
  }

  @Test
  void leftLookupJoinNullPadsMisses() throws Exception {
    // MOD(auction, 7) can be 5 or 6, which the bounded dim (keys 0..4) has no row for — a LEFT join
    // null-pads those, exercising the miss path.
    NativeParity.assertParity(
        environment(),
        "SELECT B.auction, D.val FROM bid AS B"
            + " LEFT JOIN dim FOR SYSTEM_TIME AS OF B.p AS D ON MOD(B.auction, 7) = D.k");
  }

  @Test
  void residualConditionFiltersMatches() throws Exception {
    // The dim-vs-probe comparison can't be a lookup key, so it lands as the residual (non-equi)
    // condition, evaluated by the generated collector after the lookup; the LEFT variant null-pads
    // rows whose match was filtered away.
    NativeParity.assertParity(
        environment(),
        "SELECT B.auction, B.price, D.val FROM bid AS B"
            + " JOIN dim FOR SYSTEM_TIME AS OF B.p AS D"
            + " ON MOD(B.auction, 5) = D.k AND CHAR_LENGTH(D.val) > B.auction");
    NativeParity.assertParity(
        environment(),
        "SELECT B.auction, D.val FROM bid AS B"
            + " LEFT JOIN dim FOR SYSTEM_TIME AS OF B.p AS D"
            + " ON MOD(B.auction, 5) = D.k AND CHAR_LENGTH(D.val) > B.auction");
  }

  @Test
  void calcOnTemporalTableProjectsAndFiltersDim() throws Exception {
    // A dim-only conjunct in the ON clause is pushed below the join as a calc on the temporal
    // table, applied to each looked-up row; under LEFT a filtered-away match null-pads.
    NativeParity.assertParity(
        environment(),
        "SELECT B.auction, D.val FROM bid AS B"
            + " JOIN dim FOR SYSTEM_TIME AS OF B.p AS D"
            + " ON MOD(B.auction, 5) = D.k AND CHAR_LENGTH(D.val) > 5");
    NativeParity.assertParity(
        environment(),
        "SELECT B.auction, D.val FROM bid AS B"
            + " LEFT JOIN dim FOR SYSTEM_TIME AS OF B.p AS D"
            + " ON MOD(B.auction, 5) = D.k AND CHAR_LENGTH(D.val) > 5");
  }

  @Test
  void preFilterConditionGatesLookups() throws Exception {
    // A probe-only conjunct in the ON clause becomes the pre-filter: rows failing it skip the lookup
    // entirely (and null-pad under LEFT).
    NativeParity.assertParity(
        environment(),
        "SELECT B.auction, D.val FROM bid AS B"
            + " LEFT JOIN dim FOR SYSTEM_TIME AS OF B.p AS D"
            + " ON MOD(B.auction, 5) = D.k AND B.price < 400");
  }

  @Test
  void constantLookupKeyJoinsFixedDimRow() throws Exception {
    // Equating the dim key to a literal makes the lookup key a constant, materialized into the key
    // row by the generated fetcher rather than read from the probe.
    NativeParity.assertParity(
        environment(),
        "SELECT B.auction, D.val FROM bid AS B"
            + " JOIN dim FOR SYSTEM_TIME AS OF B.p AS D ON D.k = 3 AND B.price > 150");
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
              Row.of(5L, 500L));
      tEnv.createTemporaryView(
          "bid",
          bid,
          Schema.newBuilder()
              .column("auction", DataTypes.BIGINT())
              .column("price", DataTypes.BIGINT())
              .columnByExpression("p", "PROCTIME()")
              .build());
      tEnv.executeSql("CREATE TABLE dim (k BIGINT, val STRING) WITH ('connector' = 'test-lookup')");
      return tEnv;
    };
  }
}
