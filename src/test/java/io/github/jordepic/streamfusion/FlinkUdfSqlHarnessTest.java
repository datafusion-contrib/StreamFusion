package io.github.jordepic.streamfusion;

import java.util.function.Supplier;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

/**
 * A non-builtin Flink {@link ScalarFunction} (Nexmark q14's {@code count_char}) runs natively via the
 * JVM-upcall path: the native Calc exports the argument columns over the Arrow C Data Interface and the
 * JVM bridge runs the actual {@code eval} over the batch (see {@code NativeUdf}). Because it invokes the
 * same {@code eval} Flink would, the result must be byte-identical.
 */
class FlinkUdfSqlHarnessTest {

  /** Counts occurrences of the (first char of the) second argument in the first — q14's UDF. */
  public static class CountChar extends ScalarFunction {
    public long eval(String s, String c) {
      if (s == null || c == null || c.isEmpty()) {
        return 0L;
      }
      long n = 0;
      for (int i = 0; i < s.length(); i++) {
        if (s.charAt(i) == c.charAt(0)) {
          n++;
        }
      }
      return n;
    }
  }

  @Test
  void udfMatchesHost() throws Exception {
    NativeParity.assertParity(
        environment(), "SELECT k, count_char(s, 'c') AS c_counts FROM t");
  }

  @Test
  void udfWithOtherProjectionsMatchesHost() throws Exception {
    // The UDF alongside a native scalar/arithmetic projection — the whole Calc stays native.
    NativeParity.assertParity(
        environment(),
        "SELECT k, UPPER_LEN(s), count_char(s, 'c') AS c FROM t".replace("UPPER_LEN(s)", "CHAR_LENGTH(s)"));
  }

  private static Supplier<TableEnvironment> environment() {
    return () -> {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
      tEnv.createTemporarySystemFunction("count_char", CountChar.class);
      DataStream<Row> source =
          env.fromData(
              Types.ROW_NAMED(new String[] {"k", "s"}, Types.LONG, Types.STRING),
              Row.of(1L, "abccc"),
              Row.of(2L, "cccc"),
              Row.of(3L, "no match here"),
              Row.of(4L, ""),
              Row.of(5L, (String) null));
      tEnv.createTemporaryView(
          "t",
          source,
          Schema.newBuilder()
              .column("k", DataTypes.BIGINT())
              .column("s", DataTypes.STRING())
              .build());
      return tEnv;
    };
  }
}
