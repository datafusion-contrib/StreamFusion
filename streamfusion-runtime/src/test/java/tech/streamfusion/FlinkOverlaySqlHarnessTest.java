package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkOverlaySqlHarnessTest {
  @Test
  void overlayRetainsUtf16AndLongPositionSemantics() throws Exception {
    parity(
        "SELECT id, OVERLAY(s PLACING t FROM p), OVERLAY(s PLACING t FROM p FOR n),"
            + " OVERLAY(s PLACING 'x' FROM 2 FOR 1) FROM texts");
  }

  @Test
  void overlayMatchesFlinkAtEverySurrogateBoundary() throws Exception {
    NativeParity.assertParity(
        () -> {
          List<Row> rows = new ArrayList<>();
          String[] sources = {
            "",
            "abc",
            "\u4e2da\u00e9",
            "a\ud83d\ude00b",
            "\ud83d\ude00\ud83d\ude42",
            "a\ud83d\ude00\u4e2db",
            "a\u0000b",
            null
          };
          for (String source : sources) {
            for (String replacement : new String[] {"", "X", "\u4e2d", "\ud83d\ude00", null}) {
              for (int start = -1; start <= 8; start++) {
                for (long length : new long[] {-1, 0, 1, 2, 3, 6, 4294967296L, 4294967297L}) {
                  rows.add(Row.of(rows.size(), source, replacement, start, length));
                }
              }
            }
          }
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          StreamTableEnvironment tables = StreamTableEnvironment.create(env);
          tables.createTemporaryView(
              "overlays",
              fromData(
                  env,
                  rows,
                  Types.ROW_NAMED(
                      new String[] {"id", "s", "t", "p", "n"},
                      Types.INT,
                      Types.STRING,
                      Types.STRING,
                      Types.INT,
                      Types.LONG)),
              Schema.newBuilder()
                  .column("id", DataTypes.INT())
                  .column("s", DataTypes.STRING())
                  .column("t", DataTypes.STRING())
                  .column("p", DataTypes.INT())
                  .column("n", DataTypes.BIGINT())
                  .build());
          return tables;
        },
        "SELECT id, OVERLAY(s PLACING t FROM p FOR n), OVERLAY(s PLACING t FROM p) FROM overlays");
  }

  private static void parity(String sql) throws Exception {
    NativeParity.assertParity(StringFunctionTestInputs::text, sql);
  }
}
