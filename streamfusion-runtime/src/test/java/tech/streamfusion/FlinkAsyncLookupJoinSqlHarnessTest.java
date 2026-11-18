package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.compat.FlinkTestCapabilities;
import tech.streamfusion.compat.FlinkTestSources;

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

  @Test
  void neverCompletingConnectorUsesTheConfiguredHostTimeout() {
    String sql =
        "SELECT B.auction, D.val FROM bid AS B"
            + " JOIN dim FOR SYSTEM_TIME AS OF B.p AS D ON MOD(B.auction, 5) = D.k";
    var comparison = NativeFailureParity.run(environment("never"), sql);
    Throwable host = comparison.host().rootCause();
    Throwable nativeError = comparison.nativeRun().rootCause();
    assertInstanceOf(TimeoutException.class, host, comparison.toString());
    assertEquals(host.getClass(), nativeError.getClass(), comparison.toString());
    assertEquals(host.getMessage(), nativeError.getMessage(), comparison.toString());
    assertEquals(NativeFailureParity.Route.NATIVE, comparison.nativeRun().route());
  }

  @Test
  void asynchronousLookupMissRetriesUseFlinksRetryWrapper() {
    String sql =
        "SELECT /*+ LOOKUP('table'='D', 'retry-predicate'='lookup_miss',"
            + " 'retry-strategy'='fixed_delay', 'fixed-delay'='1 ms', 'max-attempts'='2') */"
            + " B.auction, D.val FROM bid AS B"
            + " JOIN dim FOR SYSTEM_TIME AS OF B.p AS D ON MOD(B.auction, 5) = D.k";
    var comparison = NativeFailureParity.run(environment("miss-once"), sql);
    comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(6, comparison.host().rows().size(), comparison.toString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void updatingProbesRetainFlinksChangelogAndKeyedScheduling(boolean keyOrdered) throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        !keyOrdered || FlinkTestCapabilities.KEY_ORDERED_ASYNC_LOOKUP,
        "Flink 1.18 has no key-ordered async lookup mode");
    NativeParity.assertFallbackReasonContains(
        () -> {
          var env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          var table = StreamTableEnvironment.create(env);
          table
              .getConfig()
              .set("table.exec.async-lookup.key-ordered-enabled", Boolean.toString(keyOrdered));
          table.getConfig().set("table.exec.async-lookup.output-mode", "ALLOW_UNORDERED");
          var changes =
              FlinkTestSources.fromData(
                  env,
                  Types.ROW_NAMED(new String[] {"auction", "price"}, Types.LONG, Types.LONG),
                  Row.ofKind(RowKind.INSERT, 1L, 100L),
                  Row.ofKind(RowKind.DELETE, 1L, 100L),
                  Row.ofKind(RowKind.INSERT, 1L, 200L));
          table.createTemporaryView(
              "bid",
              table.fromChangelogStream(
                  changes,
                  Schema.newBuilder()
                      .column("auction", DataTypes.BIGINT())
                      .column("price", DataTypes.BIGINT())
                      .columnByExpression("p", "PROCTIME()")
                      .build()));
          table.executeSql(
              "CREATE TABLE dim (k BIGINT, val STRING)"
                  + " WITH ('connector' = 'test-lookup-async')");
          return table;
        },
        "SELECT B.price, D.val FROM bid AS B"
            + " JOIN dim FOR SYSTEM_TIME AS OF B.p AS D ON B.auction = D.k",
        keyOrdered
            ? "lookup join: key-ordered asynchronous lookup requires Flink's keyed scheduling"
            : "lookup join: updating probes require Flink's changelog-aware lookup operator");
  }

  private static Supplier<TableEnvironment> environment() {
    return environment("normal");
  }

  private static Supplier<TableEnvironment> environment(String mode) {
    return () -> {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      tEnv.getConfig().set("table.exec.async-lookup.buffer-capacity", "2");
      if (mode.equals("never")) {
        tEnv.getConfig().set("table.exec.async-lookup.timeout", "30 ms");
      }
      DataStream<Row> bid =
          FlinkTestSources.fromData(
              env,
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
          "CREATE TABLE dim (k BIGINT, val STRING) WITH ('connector' = 'test-lookup-async',"
              + " 'test-mode' = '"
              + mode
              + "')");
      return tEnv;
    };
  }
}
