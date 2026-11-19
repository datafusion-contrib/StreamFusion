package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class TopNSignedZeroParityTest {
  @ParameterizedTest
  @ValueSource(strings = {"d DESC, id", "f DESC, id", "d, id DESC", "f, id DESC"})
  void sortDirections(String order) throws Exception {
    String sql = "SELECT id, d, f FROM (SELECT id, d, f, "
        + "ROW_NUMBER() OVER (ORDER BY " + order + ") AS rn FROM n) WHERE rn <= 1";
    Row host = topOne(zeros(), sql);
    TableEnvironment table = zeros();
    var scan = NativePlanner.install(table);
    assertEquals(host, topOne(table, sql));
    assertTrue(scan.substitutions() > 0);
    assertEquals(order.endsWith("id DESC") ? 2 : 1, host.getField(0));
  }

  // Global Top-1 has one logical key. Flink may emit upserts without UPDATE_BEFORE;
  // materialize that key instead of treating each UPDATE_AFTER as an additional row.
  private static Row topOne(TableEnvironment table, String sql) throws Exception {
    Row result = null;
    try (var rows = table.executeSql(sql).collect()) {
      while (rows.hasNext()) {
        Row row = rows.next();
        if (row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER) {
          row.setKind(RowKind.INSERT);
          result = row;
        } else {
          result = null;
        }
      }
    }
    return result;
  }
  @Test
  void signedZeroDoubleTopN() throws Exception {
    NativeParity.assertChangelogParity(TopNSignedZeroParityTest::zeros,
        "SELECT id, d FROM (SELECT id, d, "
            + "ROW_NUMBER() OVER (ORDER BY d, id) AS rn FROM n) WHERE rn <= 1");
  }

  @Test
  void signedZeroFloatTopN() throws Exception {
    NativeParity.assertChangelogParity(TopNSignedZeroParityTest::zeros,
        "SELECT id, f FROM (SELECT id, f, "
            + "ROW_NUMBER() OVER (ORDER BY f, id) AS rn FROM n) WHERE rn <= 1");
  }

  private static TableEnvironment zeros() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.createTemporaryView(
        "n",
        fromData(
            env,
            Types.ROW_NAMED(new String[] {"id", "d", "f"}, Types.INT, Types.DOUBLE, Types.FLOAT),
            Row.of(1, 0.0d, 0.0f),
            Row.of(2, -0.0d, -0.0f)));
    return table;
  }
}
