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
 * A host operator that emits a changelog feeding a native one: the row→Arrow transpose at the
 * boundary must carry each row's {@code RowKind}, or the native operator would mistake retractions
 * for inserts. The inner {@code SUM} over a DECIMAL column is declined by the native aggregate (its
 * precision rules stay on the host), so it runs as a host GROUP BY emitting a changelog; the outer
 * {@code COUNT(*)} over that changelog runs natively and must consume the retractions. The collapsed
 * result would differ from the host if the transpose dropped the kind.
 */
class FlinkHostChangelogIntoNativeTest {

  @Test
  void nativeAggregateConsumesHostChangelog() throws Exception {
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
