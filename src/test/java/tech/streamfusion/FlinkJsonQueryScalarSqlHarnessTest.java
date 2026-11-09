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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FlinkJsonQueryScalarSqlHarnessTest {
  private final RecoveryExecutionMetrics metrics = new RecoveryExecutionMetrics();

  @AfterAll
  void closeCluster() {
    metrics.close();
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void definiteSelectionsPreserveSerializationAndSourceRepresentations(
      boolean nullable, boolean binary) throws Exception {
    for (String path : List.of("$", "$.a", "$.a[-1]", "$[\"a\"][0]", "$[\"?\"]",
        "$[\"[*,]\"]")) {
      verify(nullable, binary, "SELECT id,JSON_QUERY(s,'" + path + "') FROM src", true);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"WITHOUT ARRAY WRAPPER", "WITH CONDITIONAL ARRAY WRAPPER",
      "WITH UNCONDITIONAL ARRAY WRAPPER"})
  void wrappersAndEmptyErrorPoliciesPreserveStrictAndLaxResults(String wrapper) throws Exception {
    for (String mode : List.of("strict", "lax")) {
      for (String empty : List.of("NULL", "EMPTY ARRAY", "EMPTY OBJECT")) {
        for (String error : List.of("NULL", "EMPTY ARRAY", "EMPTY OBJECT")) {
          verify(true, false, "SELECT id,JSON_QUERY(s,'" + mode + " $.a' " + wrapper + " " + empty
              + " ON EMPTY " + error + " ON ERROR) FROM src", true);
        }
      }
    }
  }

  @Test
  void multipleReadersPreserveSharedParserState() throws Exception {
    verify(true, false,
        "SELECT id,JSON_QUERY(s,'$.a'),JSON_QUERY(s,'$') FROM src", true);
    verify(true, true,
        "SELECT id,JSON_QUERY(s,'$.a'),JSON_EXISTS(s,'$.a') FROM src", false);
  }

  @Test
  void filteredEmptyBatchesAndLiteralPathsDoNotEvaluateRejectedRows() throws Exception {
    verify(true, true, "SELECT id,JSON_QUERY(s,'$.a') FROM src WHERE id<0", true);
    verify(true, false, "SELECT id,JSON_QUERY(s,'  LaX $.a   ') FROM src WHERE MOD(id,2)=0", true);
  }

  @Test
  void consumersBranchingPathsAndErrorPoliciesRetainTheirGeneratedRoutes() throws Exception {
    for (String expression : List.of("JSON_QUERY(s,'$.a[*]')", "JSON_QUERY(s,'$.a[0,1]')",
        "JSON_QUERY(s,'$.a[0:2]')", "JSON_QUERY(s,'$.a' ERROR ON ERROR)",
        "JSON_QUERY(s,'$.a' ERROR ON EMPTY)", "CHAR_LENGTH(JSON_QUERY(s,'$.a'))",
        "JSON_QUERY(CONCAT(s,''),'$.a')")) {
      // The ERROR controls must not select malformed/missing values in this retained-route test.
      String where = expression.contains("ERROR ON")
          ? " WHERE MOD(id," + DOCUMENTS.length + ")=0 AND MOD(id,17)<>0" : "";
      verify(true, false, "SELECT id," + expression + " FROM src" + where, false);
    }
  }

  private static final String[] DOCUMENTS = {
      "{\"a\":{\"z\":1,\"a\":2}}", "{\"a\":[1,2.5000,-0,1e10,1e-8]}",
      "{\"a\":null}", "{}", "{\"a\":true}", "{\"a\":\"/\\\"\\\\\\n\\u0000\"}",
      "{\"a\":[]} trailing", "{\"a\":{}}", "null", "true", "17", "\"text\"", "[1,2]",
      "bad", "{\"a\":[],\"bad\":}", "{\"a\":1,\"a\":{\"n\":2}}",
      "{\"a\":{\"dup\":1,\"d\\u0075p\":null,\"z\":-0.0000}}",
      "{\"a\":{\"\\uE000\":1,\"\\uD83D\\uDE00\":2,\"?\":3,\"\\uD800\":4}}",
      "{\"a\":[\"\\uD800\",\"\\uDC00\",\"\\uD800\\uD800\",\"\\uD83D\\uDE00\"]}",
      "{\"a\":{\"x\":\"\ud800\",\"?\":0,\"\ud800\":1},\"\ud800\":[0],\"?\":[1]}",
      "{\"a\":[0,1e309,1e-999,1.7976931348623159e308,-0.0]}",
      "{\"a\":{\"b\":[null,{\"d\":\"\u4e2d\ud83d\ude00\"}],\"x\":1234567890123456789012345678901234567890}}",
      "{\"a\":[[],{},[true,false]],\"[*,]\":[7]}",
      "{\"a\":{\"z\":[0],\"a\":{},\"z\":[{\"x\":1,\"x\":2}],\"?\":3,\"\ud83d\ude00\":4},"
          + "\"p0\":0,\"p1\":1,\"p2\":2,\"p3\":3,\"p4\":4,\"p5\":5,\"p6\":6,\"p7\":7}",
      "{\"a\":[{},[1,true,null],{\"z\":1,\"a\":-0}],"
          + "\"p0\":0,\"p1\":1,\"p2\":2,\"p3\":3,\"p4\":4,\"p5\":5,\"p6\":6,\"p7\":7}",
      "{\"a\":{\"text\":\"" + "x".repeat(40000) + "\"}}",
      "{\"a\":{\"n\":1e2147483648}}", "{\"a\":{\"n\":" + "1".repeat(1001) + "}}",
      "{\"a\":{\"n\":" + "[".repeat(1001) + "0" + "]".repeat(1001) + "}}"
  };

  private void verify(boolean nullable, boolean binary, String sql, boolean pure)
      throws Exception {
    var type = ROW(FIELD("id", INT().notNull()),
        FIELD("s", nullable ? STRING() : STRING().notNull()));
    var rowType = (RowType) type.getLogicalType();
    TreeMap<Integer, List<Object>> expected = null;
    Object expectedTypes = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      {
        var config = new Configuration();
        config.set(org.apache.flink.configuration.JobManagerOptions.JOB_STORE_MAX_CAPACITY, 2);
        var env = metrics.environment(config, 1, 0);
        var table = StreamTableEnvironment.create(env);
        var serializer = new RowDataSerializer(rowType);
        table.createTemporaryView("src", env.fromSequence(0, 5002).map(index -> {
          int id = index.intValue();
          int document = id % DOCUMENTS.length;
          // Retained job metrics also retain collected results; keep large edge documents once.
          if (id >= DOCUMENTS.length && document >= 25) document = id % 2;
          RowData row = GenericRowData.of(id, nullable && id % 17 == 0 ? null
              : StringData.fromString(DOCUMENTS[document]));
          return binary ? serializer.toBinaryRow(row).copy() : row;
        }).returns(InternalTypeInfo.of(rowType)));
        var scan = nativeRun ? NativePlanner.install(table) : null;
        var types = table.sqlQuery(sql).getResolvedSchema();
        if (nativeRun) assertEquals(expectedTypes, types); else expectedTypes = types;
        var actual = new TreeMap<Integer, List<Object>>();
        var result = table.executeSql(sql);
        String jobId = result.getJobClient().orElseThrow().getJobID().toString();
        try (var output = result.collect()) {
          while (output.hasNext()) {
            var row = output.next();
            var fields = new ArrayList<Object>();
            for (int i = 1; i < row.getArity(); i++) fields.add(row.getField(i));
            assertNull(actual.put((Integer) row.getField(0), fields));
          }
        }
        if (nativeRun) {
          assertEquals(pure, scan.functionExecutions().stream().allMatch(s -> s.contains("jvm=[]")),
              scan.explainSummary());
          assertEquals(expected.keySet(), actual.keySet(), sql);
          for (var entry : expected.entrySet()) {
            var values = actual.get(entry.getKey());
            assertEquals(entry.getValue().size(), values.size());
            for (int i = 0; i < values.size(); i++)
              assertEquals(entry.getValue().get(i), values.get(i), sql + ", row=" + entry.getKey()
                  + ", column=" + i);
          }
          assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
          assertFalse(scan.functionExecutions().isEmpty(), scan.explainSummary());
          assertTrue(scan.functionExecutions().stream().anyMatch(s -> s.contains("JSON_QUERY")),
              scan.explainSummary());
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators().stream()
              .filter(row -> jobId.equals(row.get("jobId")))
              .collect(tech.streamfusion.compat.ListCollectors.toList()), "NativeCalc");
        } else expected = actual;
      }
    }
  }
}
