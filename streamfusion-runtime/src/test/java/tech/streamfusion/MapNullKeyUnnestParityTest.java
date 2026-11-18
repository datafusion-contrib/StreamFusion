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

class MapNullKeyUnnestParityTest {
  @Test
  void ordinalityAndNullKeyFilter() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.UNNEST_ORDINALITY,
        "Flink 1.18 cannot plan UNNEST WITH ORDINALITY");
    NativeParity.assertParity(
        () -> maps(true),
        "SELECT id, u.k, u.v, u.pos FROM n CROSS JOIN UNNEST(m) WITH ORDINALITY AS u(k, v, pos)"
            + " WHERE u.k IS NULL");
  }

  @Test
  void outerUnnestRetainsEmptyAndNullMaps() throws Exception {
    NativeParity.assertParity(() -> maps(true),
        "SELECT id, u.k, u.v FROM n LEFT JOIN UNNEST(m) AS u(k, v) ON TRUE");
  }
  @Test
  void unnestMapWithNullKey() throws Exception {
    NativeParity.assertParity(() -> maps(true),
        "SELECT id, u.k, u.v FROM n CROSS JOIN UNNEST(m) AS u(k, v)");
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
