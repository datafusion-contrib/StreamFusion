package io.github.jordepic.streamfusion;

import java.math.BigDecimal;
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
 * A nested aggregate that flows a changelog entirely through native operators: an inner
 * {@code SUM(DECIMAL)} GROUP BY feeds an outer {@code COUNT(*)} grouped by the (decimal) running
 * total. Both halves run native — the inner decimal sum accumulates an i128 at scale and the outer
 * aggregate groups by the decimal key and retracts the old total's count as the inner sum updates —
 * so the native changelog must collapse to the same result as the host.
 */
class FlinkHostChangelogIntoNativeTest {

  @Test
  void nativeAggregateConsumesNativeChangelog() throws Exception {
    NativeParity.assertChangelogParity(
        FlinkHostChangelogIntoNativeTest::environment,
        "SELECT total, COUNT(*) AS n FROM "
            + "(SELECT g, SUM(d) AS total FROM src GROUP BY g) GROUP BY total");
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    tEnv.getConfig().set("table.optimizer.agg-phase-strategy", "ONE_PHASE");
    // Repeated g so the inner SUM updates (emitting -U/+U); the outer COUNT(*) must retract the old
    // total's count each time.
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"g", "d"}, Types.LONG, Types.BIG_DEC),
            Row.of(1L, new BigDecimal("1.00")),
            Row.of(1L, new BigDecimal("2.00")),
            Row.of(2L, new BigDecimal("5.00")),
            Row.of(1L, new BigDecimal("3.00")));
    tEnv.createTemporaryView(
        "src",
        source,
        Schema.newBuilder()
            .column("g", DataTypes.BIGINT())
            .column("d", DataTypes.DECIMAL(10, 2))
            .build());
    return tEnv;
  }
}
