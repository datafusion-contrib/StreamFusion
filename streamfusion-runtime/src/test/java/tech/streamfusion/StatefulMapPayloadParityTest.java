package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class StatefulMapPayloadParityTest {
  @Test
  void limitWithMapPayload() throws Exception {
    NativeParity.assertKindedParity(() -> maps(false), "SELECT id, m FROM n LIMIT 1");
  }

  @Test
  void mapProjectionRemainsNative() throws Exception {
    NativeParity.assertParity(() -> maps(false), "SELECT id + 1, m FROM n");
  }

  @Test
  void integerJoinWithMapPayload() throws Exception {
    NativeParity.assertKindedParity(
        () -> maps(false), "SELECT a.id, a.m FROM n a JOIN n b ON a.id = b.id");
  }

  @Test
  void crossJoinWithMapPayload() throws Exception {
    NativeParity.assertKindedParity(() -> maps(false), "SELECT a.m, b.id FROM n a CROSS JOIN n b");
  }

  @Test
  void mapJoinKey() throws Exception {
    NativeParity.assertFallback(
        () -> maps(false), "SELECT a.id, b.id FROM n a JOIN n b ON a.m = b.m");
  }

  @Test
  void topNWithMapPayload() throws Exception {
    NativeParity.assertKindedParity(
        () -> maps(false),
        "SELECT id, m FROM (SELECT id, m, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM n) "
            + "WHERE rn <= 1");
  }

  private static TableEnvironment maps(boolean withNullKey) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    Map<String, Integer> map = new LinkedHashMap<>();
    if (withNullKey) map.put(null, 7);
    map.put("a", 1);
    table.createTemporaryView(
        "n",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "m"}, Types.INT, Types.MAP(Types.STRING, Types.INT)),
            Row.of(1, map)));
    return table;
  }
}
