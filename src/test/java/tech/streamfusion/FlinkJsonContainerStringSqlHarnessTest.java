package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.compat.JsonRuntimeCompat;
import tech.streamfusion.planner.NativePlanner;

class FlinkJsonContainerStringSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void finalContainersPreserveValuesSchemaOrderAndNestedNulls(boolean nullable, boolean binary)
      throws Exception {
    verify(nullable, binary,
        "SELECT id,JSON_STRING(a),JSON_STRING(t),JSON_STRING(r),JSON_STRING(aa),"
            + "JSON_STRING(ar) FROM src", true);
  }

  @Test
  void filtersAndEmptyBatchesDoNotSerializeRejectedRows() throws Exception {
    verify(true, true, "SELECT id,JSON_STRING(r),JSON_STRING(aa) FROM src WHERE MOD(id,3)=1", true);
    verify(true, false, "SELECT id,JSON_STRING(a) FROM src WHERE id<0", true);
  }

  @Test
  void decimalArraysUseTheReleasedProvidersRendering() throws Exception {
    verify(true, true, "SELECT id,JSON_STRING(d) FROM src",
        JsonRuntimeCompat.PRESERVES_DECIMAL_SCALE);
  }

  @ParameterizedTest
  @CsvSource({"false", "true"})
  void rowLeavesAndUnicodeNamesUseTheReleasedProvidersOrder(boolean binary) throws Exception {
    verify(true, binary, "SELECT id,JSON_STRING(s) FROM src", true);
  }

  @Test
  void uncheckedNestedGettersRetainGeneratedEvaluation() throws Exception {
    verify(true, true, "SELECT id,JSON_STRING(nn) FROM src", false);
  }

  @ParameterizedTest
  @CsvSource({"false", "true"})
  void serializationPreservesCachesObservedBySiblingProjections(boolean binary) throws Exception {
    verify(true, binary, "SELECT id,JSON_STRING(t),ARRAY_MAX(t) FROM src", false);
    verify(true, binary, "SELECT id,ARRAY_MAX(t),JSON_STRING(t) FROM src", false);
    verify(true, binary, "SELECT id,JSON_STRING(t),t FROM src", false);
  }

  @Test
  void intermediateAndComputedContainersRetainGeneratedEvaluation() throws Exception {
    for (String expression : List.of("CHAR_LENGTH(JSON_STRING(t))", "JSON_STRING(ARRAY[id])",
        "JSON_STRING(ROW(id,t))", "JSON_STRING(CASE WHEN id=0 THEN a ELSE ARRAY[id] END)"))
      verify(true, false, "SELECT id," + expression + " FROM src", false);
  }

  private static void verify(boolean nullable, boolean binary, String sql, boolean pure)
      throws Exception {
    var integers = ARRAY(INT());
    var strings = ARRAY(STRING());
    var row = ROW(FIELD("z", INT()), FIELD("a", STRING()), FIELD("m", ARRAY(BIGINT())));
    var nested = ARRAY(strings);
    var rows = ARRAY(row);
    var decimals = ARRAY(DECIMAL(38, 9));
    var scalarRow = ROW(FIELD("\ud83d\ude00", BOOLEAN()), FIELD("\ue000", TINYINT()),
        FIELD("short", SMALLINT()), FIELD("long", BIGINT()), FIELD("char", CHAR(4)));
    var notNullChildren = ARRAY(INT().notNull());
    var input = ROW(FIELD("id", INT().notNull()),
        FIELD("a", nullable ? integers : integers.notNull()),
        FIELD("t", nullable ? strings : strings.notNull()),
        FIELD("r", nullable ? row : row.notNull()),
        FIELD("aa", nullable ? nested : nested.notNull()),
        FIELD("ar", nullable ? rows : rows.notNull()),
        FIELD("d", nullable ? decimals : decimals.notNull()),
        FIELD("s", scalarRow), FIELD("nn", notNullChildren));
    var type = (RowType) input.getLogicalType();
    var serializer = new RowDataSerializer(type);
    String[] texts = {"", "a/b\\c\"d\u0000\u001f", "\ud83d\ude00\ue000\u4e2d",
        "\ud800", "\udc00", "?", "raw\\uD800", "x\u2028\u2029".repeat(128)};
    TreeMap<Integer, List<Object>> expected = null;
    Object expectedTypes = null;
    for (boolean nativeRun : new boolean[] {false, true}) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var env = metrics.environment(new Configuration(), 1, 0);
        var table = StreamTableEnvironment.create(env);
        table.createTemporaryView("src", env.fromSequence(0, 5002).map(index -> {
          int k = index.intValue();
          var text = StringData.fromString(texts[k % texts.length]);
          var a = new GenericArrayData(k % 3 == 0 ? new Integer[] {}
              : new Integer[] {Integer.MIN_VALUE, null, Integer.MAX_VALUE});
          var t = new GenericArrayData(k % 5 == 0 ? new StringData[] {}
              : new StringData[] {text, null, StringData.fromString("tail"),
                  StringData.fromBytes(new byte[] {(byte) 0xf0, (byte) 0x9f, (byte) 0x98, (byte) 0x80}),
                  StringData.fromBytes(new byte[] {(byte) 0xee, (byte) 0x80, (byte) 0x80})});
          var r = GenericRowData.of(k % 7 == 0 ? null : k, k % 11 == 0 ? null : text,
              k % 13 == 0 ? null : new GenericArrayData(new Long[] {Long.MIN_VALUE, null, Long.MAX_VALUE}));
          r.setRowKind(RowKind.values()[k % RowKind.values().length]);
          var aa = new GenericArrayData(new Object[] {t, null, new GenericArrayData(new StringData[] {})});
          var ar = new GenericArrayData(new Object[] {r, null, GenericRowData.of(null, null, null)});
          var d = new GenericArrayData(new DecimalData[] {
              DecimalData.fromBigDecimal(new BigDecimal("0.000000100"), 38, 9), null,
              DecimalData.fromBigDecimal(new BigDecimal("-12345678901234567890123456789.123456789"), 38, 9),
              DecimalData.fromBigDecimal(BigDecimal.ZERO, 38, 9)});
          RowData value = GenericRowData.of(k,
              nullable && k % 17 == 0 ? null : a, nullable && k % 19 == 0 ? null : t,
              nullable && k % 23 == 0 ? null : r, nullable && k % 29 == 0 ? null : aa,
              nullable && k % 31 == 0 ? null : ar, nullable && k % 37 == 0 ? null : d,
              k % 41 == 0 ? null : GenericRowData.of(k % 2 == 0,
                  k % 3 == 0 ? null : (byte) (k % 2 == 0 ? Byte.MIN_VALUE : Byte.MAX_VALUE),
                  k % 5 == 0 ? null : (short) (k % 2 == 0 ? Short.MIN_VALUE : Short.MAX_VALUE),
                  k % 7 == 0 ? null : k % 2 == 0 ? Long.MIN_VALUE : Long.MAX_VALUE,
                  k % 11 == 0 ? null : StringData.fromString("a   ")),
              new GenericArrayData(new int[] {Integer.MIN_VALUE, 0, Integer.MAX_VALUE}));
          return binary ? serializer.toBinaryRow(value).copy() : value;
        }).returns(InternalTypeInfo.of(type)));
        var scan = nativeRun ? NativePlanner.install(table) : null;
        var resultTypes = table.sqlQuery(sql).getResolvedSchema();
        if (nativeRun) assertEquals(expectedTypes, resultTypes); else expectedTypes = resultTypes;
        var actual = new TreeMap<Integer, List<Object>>();
        try (var output = table.executeSql(sql).collect()) {
          while (output.hasNext()) {
            var record = output.next();
            var values = new ArrayList<Object>();
            for (int field = 1; field < record.getArity(); field++)
              values.add(NativeParity.comparableValue(record.getField(field)));
            assertNull(actual.put((Integer) record.getField(0), values));
          }
        }
        int count = sql.contains("id<0") ? 0 : sql.contains("MOD(id,3)=1") ? 1668 : 5003;
        assertEquals(count, actual.size());
        if (nativeRun) {
          assertEquals(expected.keySet(), actual.keySet(), sql);
          for (int id : expected.keySet()) assertEquals(expected.get(id), actual.get(id), sql + ": " + id);
          assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
          assertFalse(scan.functionExecutions().isEmpty(), scan.explainSummary());
          assertEquals(pure, scan.functionExecutions().stream().allMatch(s -> s.contains("jvm=[]")),
              scan.explainSummary());
          assertTrue(scan.functionExecutions().stream().anyMatch(s -> s.contains("JSON_STRING")),
              scan.explainSummary());
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
        } else expected = actual;
      }
    }
  }
}
