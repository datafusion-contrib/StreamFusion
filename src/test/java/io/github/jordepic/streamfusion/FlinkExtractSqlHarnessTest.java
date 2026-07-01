package io.github.jordepic.streamfusion;

import java.time.LocalDateTime;
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
 * {@code EXTRACT(unit FROM ts)} / {@code HOUR(ts)} over a plain {@code TIMESTAMP} (Nexmark q14): the
 * integer date/time fields extract natively from the timestamp's wall-clock, byte-identical to Flink.
 */
class FlinkExtractSqlHarnessTest {

  @Test
  void extractFieldsMatchHost() throws Exception {
    NativeParity.assertParity(
        environment(),
        "SELECT k, EXTRACT(YEAR FROM ts) AS y, EXTRACT(MONTH FROM ts) AS mo,"
            + " EXTRACT(DAY FROM ts) AS d, EXTRACT(HOUR FROM ts) AS h,"
            + " EXTRACT(MINUTE FROM ts) AS mi FROM t");
  }

  @Test
  void hourFunctionMatchesHost() throws Exception {
    // q14's shape: HOUR(dateTime) inside a CASE comparison.
    NativeParity.assertParity(
        environment(),
        "SELECT k, CASE WHEN HOUR(ts) >= 8 AND HOUR(ts) <= 18 THEN 'day' ELSE 'other' END AS part"
            + " FROM t");
  }

  private static Supplier<TableEnvironment> environment() {
    return () -> {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      DataStream<Row> source =
          env.fromData(
              Types.ROW_NAMED(new String[] {"k", "ts"}, Types.LONG, Types.LOCAL_DATE_TIME),
              Row.of(1L, LocalDateTime.of(2023, 3, 15, 8, 30, 45)),
              Row.of(2L, LocalDateTime.of(2024, 12, 31, 23, 59, 59)),
              Row.of(3L, LocalDateTime.of(2020, 1, 1, 0, 0, 0)),
              Row.of(4L, LocalDateTime.of(2022, 6, 30, 19, 5, 1)));
      tEnv.createTemporaryView(
          "t",
          source,
          Schema.newBuilder()
              .column("k", DataTypes.BIGINT())
              .column("ts", DataTypes.TIMESTAMP(3))
              .build());
      return tEnv;
    };
  }
}
