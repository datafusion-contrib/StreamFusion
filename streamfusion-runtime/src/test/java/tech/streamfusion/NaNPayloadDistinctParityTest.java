package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class NaNPayloadDistinctParityTest {
  @Test
  void distinctNanPayloads() throws Exception {
    NativeParity.assertChangelogParity(() -> input(false, 0),
        "SELECT COUNT(DISTINCT d), COUNT(DISTINCT f) FROM n");
  }

  @Test
  void distinctNanPayloadsTwoPhase() throws Exception {
    NativeParity.assertChangelogParity(() -> input(false, 1),
        "SELECT COUNT(DISTINCT d), COUNT(DISTINCT f) FROM n");
  }

  @Test
  void canonicalNanControl() throws Exception {
    NativeParity.assertChangelogParity(() -> input(true, 0),
        "SELECT COUNT(DISTINCT d), COUNT(DISTINCT f) FROM n");
  }

  @Test
  void scalarGroupKeyControl() throws Exception {
    NativeParity.assertChangelogParity(() -> input(false, 0),
        "SELECT COUNT(*) FROM n GROUP BY d");
  }

  private static TableEnvironment input(boolean canonical, int batch) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table
        .getConfig()
        .set("table.optimizer.agg-phase-strategy", batch == 0 ? "ONE_PHASE" : "TWO_PHASE");
    if (batch > 0) {
      table.getConfig().set("table.exec.mini-batch.enabled", "true");
      table.getConfig().set("table.exec.mini-batch.allow-latency", "100000 d");
      table.getConfig().set("table.exec.mini-batch.size", Integer.toString(batch));
    }
    // Construct the NaNs after source deserialization, which may otherwise canonicalize their bits.
    table.createTemporaryView(
        "n",
        fromData(env, 1, 2)
            .map(
                id -> {
                  double d = Double.longBitsToDouble(0x7ff8000000000000L + (canonical ? 0 : id));
                  float f = Float.intBitsToFloat(0x7fc00000 + (canonical ? 0 : id));
                  return Row.of(id, d, f, new Double[] {d}, new Float[] {f});
                })
            .returns(
                Types.ROW_NAMED(
                    new String[] {"id", "d", "f", "da", "fa"},
                    Types.INT,
                    Types.DOUBLE,
                    Types.FLOAT,
                    Types.OBJECT_ARRAY(Types.DOUBLE),
                    Types.OBJECT_ARRAY(Types.FLOAT))));
    return table;
  }
}
