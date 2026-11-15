package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.compat.JsonRuntimeCompat;
import tech.streamfusion.planner.NativePlanner;

class FlinkJsonArrayScalarSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void scalarArraysKeepNullPoliciesIntegerWidthsEscapingAndInputRepresentation(
      boolean nullable, boolean binary) throws Exception {
    verify(18, 3, nullable, binary,
        "SELECT id,JSON_ARRAY(i,n,t,h,s,c,b),JSON_ARRAY(i,n,t,h,s,c,b NULL ON NULL),"
            + "JSON_ARRAY(i,n,t,h,s,c,b ABSENT ON NULL),JSON_ARRAY(),"
            + "JSON_ARRAY(CAST(NULL AS STRING) NULL ON NULL),"
            + "JSON_ARRAY(CAST(NULL AS BIGINT) ABSENT ON NULL),"
            + "JSON_ARRAY(s,'literal',42,TRUE,CAST(NULL AS STRING) NULL ON NULL) FROM src", true);
  }

  @ParameterizedTest
  @CsvSource({"1,0", "4,2", "18,0", "18,3", "18,18", "19,0", "19,19",
      "38,0", "38,18", "38,38"})
  void decimalArraysPreserveEachReleasedLinesScaleAndExponentRules(int precision, int scale)
      throws Exception {
    for (boolean nullable : new boolean[] {false, true}) {
      verify(precision, scale, nullable, true,
          "SELECT id,JSON_ARRAY(d,e),JSON_ARRAY(d,e NULL ON NULL),"
              + "JSON_ARRAY(d,e ABSENT ON NULL),"
              + "JSON_ARRAY(d,CAST(NULL AS DECIMAL(38,9)) NULL ON NULL) FROM src",
          JsonRuntimeCompat.PRESERVES_DECIMAL_SCALE);
    }
  }

  @Test
  void ordinaryConsumersCaseBranchesAndEmptyFilteredBatchesStayNative() throws Exception {
    verify(18, 3, true, false,
        "SELECT id,JSON_ARRAY(i)='[]',CHAR_LENGTH(JSON_ARRAY(s)),"
            + "CASE WHEN MOD(id,3)=0 THEN JSON_ARRAY(i,n) ELSE JSON_ARRAY(s) END,"
            + "JSON_ARRAY(CASE WHEN MOD(id,2)=0 THEN JSON_ARRAY(n) ELSE '[]' END),"
            + "JSON_ARRAY(COALESCE(s,''),COALESCE(n,0)) FROM src", true);
    verify(18, 3, true, true,
        "SELECT id,JSON_ARRAY(i/(id-id),s) FROM src WHERE id<0", true);
    verify(18, 3, true, false,
        "SELECT id,CASE WHEN id>=0 THEN JSON_ARRAY(i) "
            + "ELSE JSON_ARRAY(i/(id-id)) END FROM src", true);
  }

  @Test
  void nestedRawJsonAndUnverifiedTypesKeepGeneratedSemantics() throws Exception {
    for (String expression : List.of("JSON_ARRAY(JSON_ARRAY(n))",
        "JSON_ARRAY(JSON_OBJECT('n' VALUE n))", "JSON_ARRAY(ARRAY[i,n])",
        "JSON_ARRAY(CAST(n AS DOUBLE))", "JSON_ARRAY(ROW(i,s))")) {
      verify(18, 3, true, true, "SELECT id," + expression + " FROM src", false);
    }
  }

  private static void verify(int precision, int scale, boolean nullable, boolean binary,
      String sql, boolean pure) throws Exception {
    var integer = nullable ? INT() : INT().notNull();
    var bigint = nullable ? BIGINT() : BIGINT().notNull();
    var tinyint = nullable ? TINYINT() : TINYINT().notNull();
    var smallint = nullable ? SMALLINT() : SMALLINT().notNull();
    var string = nullable ? STRING() : STRING().notNull();
    var character = nullable ? CHAR(8) : CHAR(8).notNull();
    var bool = nullable ? BOOLEAN() : BOOLEAN().notNull();
    var decimal = nullable ? DECIMAL(precision, scale) : DECIMAL(precision, scale).notNull();
    var type = ROW(FIELD("id", INT().notNull()), FIELD("i", integer), FIELD("n", bigint),
        FIELD("t", tinyint), FIELD("s", string), FIELD("b", bool), FIELD("d", decimal),
        FIELD("e", decimal), FIELD("h", smallint), FIELD("c", character));
    var rowType = (RowType) type.getLogicalType();
    BigInteger maximum = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
    var coefficients = List.of(maximum.negate(), BigInteger.ONE.negate(), BigInteger.ZERO,
        BigInteger.ONE, maximum);
    StringBuilder controls = new StringBuilder();
    for (char c = 0; c < 160; c++) controls.append(c);
    String[] strings = {"", "plain", "a/b\\c\"d", controls.toString(),
        "\ud83d\ude00\u4e2d\u2028\u2029", "\ud800", "\udc00", "\ud800\ud800", "?",
        "a\ud83d\ude00\u0000".repeat(128), "{\"a\":1}", "[1,2]"};
    TreeMap<Integer, List<Object>> expected = null;
    Object expectedTypes = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        var serializer = new RowDataSerializer(rowType);
        table.createTemporaryView("src", env.fromSequence(0, 5002).map(index -> {
          int k = index.intValue();
          RowData row = GenericRowData.of(k,
              nullable && k % 7 == 0 ? null : k % 2 == 0 ? Integer.MIN_VALUE : Integer.MAX_VALUE,
              nullable && k % 11 == 0 ? null : k % 2 == 0 ? Long.MIN_VALUE : Long.MAX_VALUE,
              nullable && k % 13 == 0 ? null : (byte) k,
              nullable && k % 17 == 0 ? null : StringData.fromString(strings[k % strings.length]),
              nullable && k % 19 == 0 ? null : k % 2 == 0,
              nullable && k % 23 == 0 ? null : DecimalData.fromBigDecimal(new BigDecimal(
                  coefficients.get(k % coefficients.size()), scale), precision, scale),
              nullable && k % 29 == 0 ? null : DecimalData.fromBigDecimal(new BigDecimal(
                  coefficients.get((k / 3) % coefficients.size()), scale), precision, scale),
              nullable && k % 31 == 0 ? null : (short) k,
              nullable && k % 37 == 0 ? null : StringData.fromString(k % 2 == 0 ? " x  "
                  : strings[k % strings.length].substring(0,
                      Math.min(8, strings[k % strings.length].length()))));
          return binary ? serializer.toBinaryRow(row).copy() : row;
        }).returns(InternalTypeInfo.of(rowType)));
        var scan = nativeRun ? NativePlanner.install(table) : null;
        var types = table.sqlQuery(sql).getResolvedSchema();
        if (nativeRun) assertEquals(expectedTypes, types); else expectedTypes = types;
        var actual = new TreeMap<Integer, List<Object>>();
        try (var output = table.executeSql(sql).collect()) {
          while (output.hasNext()) {
            var row = output.next();
            var fields = new ArrayList<Object>();
            for (int i = 1; i < row.getArity(); i++) fields.add(row.getField(i));
            assertNull(actual.put((Integer) row.getField(0), fields));
          }
        }
        assertEquals(sql.contains("WHERE id<0") ? 0 : 5003, actual.size());
        if (nativeRun) {
          assertEquals(expected, actual, sql);
          assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
          assertFalse(scan.functionExecutions().isEmpty(), scan.explainSummary());
          assertEquals(pure, scan.functionExecutions().stream().allMatch(s -> s.contains("jvm=[]")),
              scan.explainSummary());
          assertTrue(scan.functionExecutions().stream().anyMatch(s -> s.contains("JSON_ARRAY")),
              scan.explainSummary());
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        } else expected = actual;
      }
    }
  }
}
