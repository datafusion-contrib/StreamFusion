package tech.streamfusion;

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
 * {@code SPLIT_INDEX} — Nexmark q22's URL-directory extraction. The native UDF reproduces Flink's
 * {@code splitByWholeSeparatorPreserveAllTokens} (0-based, NULL out of range / negative / empty input
 * / null arg). Value-compared to the host over URLs with leading/empty/short segments.
 */
class FlinkSplitIndexSqlHarnessTest {

  @Test
  void splitIndexMatchesHost() throws Exception {
    NativeParity.assertParity(
        FlinkSplitIndexSqlHarnessTest::environment,
        "SELECT auction, "
            + "SPLIT_INDEX(url, '/', 3) AS dir1, "
            + "SPLIT_INDEX(url, '/', 4) AS dir2, "
            + "SPLIT_INDEX(url, '/', 5) AS dir3 FROM t");
  }

  private static TableEnvironment environment() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    DataStream<Row> source =
        env.fromData(
            Types.ROW_NAMED(new String[] {"auction", "url"}, Types.LONG, Types.STRING),
            Row.of(1L, "https://www.nexmark.com/path/to/item.html"),
            Row.of(2L, "http://x.com/a"),
            Row.of(3L, "noscheme"),
            Row.of(4L, (String) null));
    tEnv.createTemporaryView(
        "t",
        source,
        Schema.newBuilder()
            .column("auction", DataTypes.BIGINT())
            .column("url", DataTypes.STRING())
            .build());
    return tEnv;
  }
}
