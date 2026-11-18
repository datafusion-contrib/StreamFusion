package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.UUID;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkUuidSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"5003,false", "0,false", "5003,true"})
  void uuidKeepsCharContractAndGeneratesEachEvaluatedRow(int count, boolean filtered) throws Exception {
    Object expectedSchema = null;
    try (var metrics = new RecoveryExecutionMetrics()) {
      for (boolean nativeEnabled : new boolean[] {false, true}) {
        var env = metrics.environment(new Configuration(), 1, 0);
        var table = input(env, count);
        var scan = nativeEnabled ? NativePlanner.install(table) : null;
        var result = table.executeSql("SELECT id, UUID() AS a, UUID() AS b, "
            + "CASE WHEN MOD(id, 3)=0 THEN UUID() ELSE CAST(NULL AS CHAR(36)) END AS c "
            + "FROM src" + (filtered ? " WHERE id < 0" : ""));
        var schema = result.getResolvedSchema();
        assertEquals("CHAR(36) NOT NULL", schema.getColumnDataTypes().get(1).toString());
        assertEquals("CHAR(36) NOT NULL", schema.getColumnDataTypes().get(2).toString());
        var seen = new HashSet<String>();
        int rows = 0;
        try (var iterator = result.collect()) {
          while (iterator.hasNext()) {
            Row row = iterator.next();
            assertEquals(org.apache.flink.types.RowKind.INSERT, row.getKind());
            for (int column : new int[] {1, 2, 3}) {
              if (column == 3 && ((Integer) row.getField(0)) % 3 != 0) {
                assertNull(row.getField(column));
                continue;
              }
              String value = (String) row.getField(column);
              assertTrue(value.matches("[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"));
              UUID uuid = UUID.fromString(value);
              assertEquals(4, uuid.version());
              assertEquals(2, uuid.variant());
              assertTrue(seen.add(value), "UUID reused across rows, batches or call sites");
            }
            rows++;
          }
        }
        result.getJobClient().orElseThrow().getJobExecutionResult().get();
        assertEquals(filtered ? 0 : count, rows);
        if (!nativeEnabled) expectedSchema = schema;
        else {
          assertEquals(expectedSchema, schema);
          assertTrue(scan.substitutions() > 0, scan::explainSummary);
          assertTrue(scan.fallbackReasons().isEmpty(), scan::explainSummary);
          String job = result.getJobClient().orElseThrow().getJobID().toString();
          assertEquals(count, metrics.recordsIn(job, "NativeCalc("));
        }
      }
    }
  }

  @Test
  void functionNamedUuidRetainsItsOwnImplementation() throws Exception {
    NativeParity.assertFallback(() -> {
      var env = org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      var table = input(env, 4);
      table.createTemporarySystemFunction("UUID", UuidShadow.class);
      return table;
    }, "SELECT UUID() FROM src");
  }

  @Test
  void functionNamedUuidWithAnArgumentUsesTheScalarBridge() throws Exception {
    NativeParity.assertParity(() -> {
      var env = org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      var table = input(env, 4);
      table.createTemporarySystemFunction("UUID", UuidShadow.class);
      return table;
    }, "SELECT UUID(id) FROM src");
  }

  private static StreamTableEnvironment input(
      org.apache.flink.streaming.api.environment.StreamExecutionEnvironment env, int count) {
    var table = StreamTableEnvironment.create(env);
    var rows = new ArrayList<Row>();
    for (int i = 0; i < Math.max(1, count); i++) rows.add(Row.of(i));
    DataStream<Row> source = fromData(env, rows, Types.ROW_NAMED(new String[] {"id"}, Types.INT));
    if (count == 0) source = source.filter(row -> false);
    table.createTemporaryView("src", source);
    return table;
  }

  public static class UuidShadow extends ScalarFunction {
    @Override
    public boolean isDeterministic() { return false; }

    public String eval() { return "custom"; }

    public String eval(Integer value) { return "custom-" + value; }
  }
}
