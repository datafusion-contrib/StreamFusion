package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FlinkJsonReaderScheduleSqlHarnessTest {
  private static final String VALUE =
      "JSON_VALUE(%s,'strict $.a' DEFAULT 'EMPTY' ON EMPTY DEFAULT 'ERROR' ON ERROR)";
  private static final String QUERY =
      "JSON_QUERY(%s,'strict $.a' WITH CONDITIONAL ARRAY WRAPPER EMPTY OBJECT ON ERROR)";
  private final RecoveryExecutionMetrics metrics = new RecoveryExecutionMetrics();

  @AfterAll
  void closeCluster() { metrics.close(); }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void independentReadersRetainRowMajorResults(boolean nullable, boolean binary) throws Exception {
    for (String projection : List.of(
        String.format(VALUE, "a") + "," + String.format(VALUE, "b"),
        String.format(VALUE, "a") + ",JSON_EXISTS(b,'strict $.a' FALSE ON ERROR)",
        "a IS JSON VALUE," + String.format(VALUE, "b"),
        "a IS NOT JSON OBJECT,b IS JSON ARRAY",
        String.format(QUERY, "a") + "," + String.format(QUERY, "b"))) {
      verify(nullable, binary, "SELECT id," + projection + " FROM src", true);
    }
  }

  @Test
  void filtersAndEmptySurvivorsRetainTheCorrectSchedule() throws Exception {
    String projection = String.format(VALUE, "a") + "," + String.format(VALUE, "b");
    verify(true, true, "SELECT id," + projection + " FROM src WHERE id<>1", true);
    verify(true, false, "SELECT id," + projection + " FROM src WHERE id<0", true);
  }

  @Test
  void conditionalReadersAndComputedInputsKeepTheirVerifiedRoute() throws Exception {
    verify(true, false, "SELECT id,CASE WHEN MOD(id,2)=0 THEN " + String.format(VALUE, "a")
        + " ELSE 'skip' END," + String.format(VALUE, "b") + " FROM src", false);
    verify(true, true, "SELECT id," + String.format(VALUE, "CONCAT(a,'')") + ","
        + String.format(VALUE, "b") + " FROM src",
        tech.streamfusion.compat.JsonRuntimeCompat.LEGACY_SQL_JSON);
  }

  private static final String[] LEFT = {
      "{}", "\"" + "x".repeat(16000) + "\"", "{\"a\":null}",
      "{\"a\":{\"z\":2,\"a\":1}}", "{\"a\":[1,2]}", "{\"a\":\"\ud800\"}",
      "{\"a\":true}", "invalid"
  };
  private static final String[] RIGHT = {
      NativeJsonBufferHistoryTest.boundary(3990, "1." + "2".repeat(1000)), "{}",
      "{\"a\":false}", "{\"a\":[{},null]}", "{\"a\":\"text\"}", "{\"a\":\"\udc00\"}",
      "{\"a\":null}", "{\"a\":1,\"bad\":}"
  };

  private void verify(boolean nullable, boolean binary, String sql, boolean pure) throws Exception {
    var type = ROW(FIELD("id", INT().notNull()), FIELD("a", nullable ? STRING() : STRING().notNull()),
        FIELD("b", nullable ? STRING() : STRING().notNull()));
    var rowType = (RowType) type.getLogicalType();
    TreeMap<Integer, List<Object>> expected = null;
    Object expectedSchema = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      var config = new Configuration();
      config.set(org.apache.flink.configuration.JobManagerOptions.JOB_STORE_MAX_CAPACITY, 2);
      var env = metrics.environment(config, 1, 0);
      var table = StreamTableEnvironment.create(env);
      var serializer = new RowDataSerializer(rowType);
      table.createTemporaryView("src", env.fromSequence(0, 5002).map(index -> {
        int id = index.intValue();
        int sample = id < LEFT.length ? id : 2 + id % (LEFT.length - 2);
        RowData row = GenericRowData.of(id,
            nullable && id % 17 == 2 ? null : StringData.fromString(LEFT[sample]),
            nullable && id % 19 == 3 ? null : StringData.fromString(RIGHT[sample]));
        return binary ? serializer.toBinaryRow(row).copy() : row;
      }).returns(InternalTypeInfo.of(rowType)));
      var scan = nativeRun ? NativePlanner.install(table) : null;
      var schema = table.sqlQuery(sql).getResolvedSchema();
      if (nativeRun) assertEquals(expectedSchema, schema); else expectedSchema = schema;
      var result = table.executeSql(sql);
      String jobId = result.getJobClient().orElseThrow().getJobID().toString();
      var actual = new TreeMap<Integer, List<Object>>();
      try (var rows = result.collect()) {
        while (rows.hasNext()) {
          var row = rows.next();
          var fields = new ArrayList<Object>();
          for (int column = 1; column < row.getArity(); column++) fields.add(row.getField(column));
          assertNull(actual.put((Integer) row.getField(0), fields));
        }
      }
      if (nativeRun) {
        assertEquals(expected, actual, sql);
        assertFalse(scan.functionExecutions().isEmpty(), scan.explainSummary());
        assertEquals(pure, scan.functionExecutions().stream().allMatch(s -> s.contains("jvm=[]")),
            scan.explainSummary());
        assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators().stream()
            .filter(row -> jobId.equals(row.get("jobId")))
            .collect(tech.streamfusion.compat.ListCollectors.toList()), "NativeCalc");
      } else expected = actual;
    }
  }
}
