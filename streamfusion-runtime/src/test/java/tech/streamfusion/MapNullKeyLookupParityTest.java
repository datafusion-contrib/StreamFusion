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

class MapNullKeyLookupParityTest {
  @Test
  void lookupMapWithNullKey() throws Exception {
    NativeParity.assertParity(
        () -> maps(true), "SELECT id, m['a'], m['missing'], m['nullable'] FROM n");
  }

  private static TableEnvironment maps(boolean withNullKey) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    Map<String, Integer> map = new LinkedHashMap<>();
    if (withNullKey) map.put(null, 7);
    map.put("a", 1);
    map.put("nullable", null);
    table.createTemporaryView(
        "n",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "m"}, Types.INT, Types.MAP(Types.STRING, Types.INT)),
            Row.of(1, map),
            Row.of(2, Map.of()),
            Row.of(3, null)));
    return table;
  }
}
