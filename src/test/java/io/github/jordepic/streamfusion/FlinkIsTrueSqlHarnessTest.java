package io.github.jordepic.streamfusion;

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
 * {@code IS TRUE} / {@code IS NOT TRUE} / {@code IS FALSE} / {@code IS NOT FALSE} — three-valued
 * predicates over a possibly-null boolean (the form {@code COUNT(*) FILTER (WHERE …)} lowers to). A
 * null operand is neither true nor false; value-compared to the host, with a null-producing operand.
 */
class FlinkIsTrueSqlHarnessTest {

  @Test
  void threeValuedBooleanPredicatesMatchHost() throws Exception {
    NativeParity.assertParity(
        FlinkIsTrueSqlHarnessTest::environment,
        "SELECT k, "
            + "(v > 5) IS TRUE AS t, (v > 5) IS NOT TRUE AS nt, "
            + "(v > 5) IS FALSE AS f, (v > 5) IS NOT FALSE AS nf FROM t");
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.INT),
            Row.of(1L, 9),
            Row.of(2L, 1),
            Row.of(3L, (Integer) null));
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("k", DataTypes.BIGINT())
            .column("v", DataTypes.INT())
            .build());
    return tEnv;
  }
}
