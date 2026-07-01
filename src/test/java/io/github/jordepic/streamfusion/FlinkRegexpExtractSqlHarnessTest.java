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
 * Nexmark q21's {@code REGEXP_EXTRACT} shape. By default the function routes through the host's own
 * {@code SqlFunctionUtils.regexpExtract} (a JVM columnar upcall), so the match is byte-identical to
 * Flink for every pattern and q21 accelerates without opting into anything. These run with default
 * settings — no {@code allowIncompatible} — proving the default path is both native and exact.
 */
class FlinkRegexpExtractSqlHarnessTest {

  @Test
  void regexpExtractGroupMatchesHost() throws Exception {
    NativeParity.assertParity(
        environment(),
        "SELECT channel, REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) AS channel_id FROM bid");
  }

  @Test
  void q21ChannelIdMatchesHost() throws Exception {
    // q21 verbatim: REGEXP_EXTRACT in both a CASE arm and the WHERE predicate.
    NativeParity.assertParity(
        environment(),
        "SELECT channel,"
            + " CASE"
            + "   WHEN lower(channel) = 'apple' THEN '0'"
            + "   WHEN lower(channel) = 'google' THEN '1'"
            + "   WHEN lower(channel) = 'facebook' THEN '2'"
            + "   WHEN lower(channel) = 'baidu' THEN '3'"
            + "   ELSE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2)"
            + " END AS channel_id"
            + " FROM bid"
            + " WHERE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) IS NOT NULL"
            + "   OR lower(channel) IN ('apple', 'google', 'facebook', 'baidu')");
  }

  private static Supplier<TableEnvironment> environment() {
    return () -> {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      DataStream<Row> bid =
          env.fromData(
              Types.ROW_NAMED(new String[] {"channel", "url"}, Types.STRING, Types.STRING),
              Row.of("Apple", "https://site/x?channel_id=apple_1&foo=1"),
              Row.of("Google", "https://site/y?a=1&channel_id=goog_2"),
              Row.of("Retail", "channel_id=retail_7&z=9"),
              Row.of("Retail", "https://site/z?no_channel=here"),
              Row.of("Baidu", "https://site/q"),
              Row.of("Vendor", "channel_id="));
      tEnv.createTemporaryView(
          "bid",
          bid,
          Schema.newBuilder()
              .column("channel", DataTypes.STRING())
              .column("url", DataTypes.STRING())
              .build());
      return tEnv;
    };
  }
}
