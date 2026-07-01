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
 * {@code UPPER}/{@code LOWER} run natively and byte-identically to Flink by default (no {@code
 * allowIncompatible}): they route to the host's own {@code BinaryStringData} case folding via a JVM
 * upcall, so even non-ASCII inputs — where Rust's Unicode default case mapping would diverge from
 * Flink's — match exactly.
 */
class FlinkStringCaseSqlHarnessTest {

  @Test
  void lowerAndUpperMatchHost() throws Exception {
    NativeParity.assertParity(
        environment(),
        "SELECT s, LOWER(s) AS lo, UPPER(s) AS up, LOWER(s) = 'apple' AS is_apple FROM t");
  }

  private static Supplier<TableEnvironment> environment() {
    return () -> {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      DataStream<Row> t =
          env.fromData(
              Types.ROW_NAMED(new String[] {"s"}, Types.STRING),
              Row.of("Apple"),
              Row.of("GOOGLE"),
              Row.of("baiDu"),
              Row.of("Grüße"),
              Row.of("MAÑANA"),
              Row.of("straße"));
      tEnv.createTemporaryView(
          "t", t, Schema.newBuilder().column("s", DataTypes.STRING()).build());
      return tEnv;
    };
  }
}
