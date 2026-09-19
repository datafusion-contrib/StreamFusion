package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkNestedJsonJvmSqlHarnessTest {
  private static final TypeInformation<Row> RECORD =
      Types.ROW_NAMED(new String[] {"label", "amount"}, Types.STRING, Types.INT);
  private static final TypeInformation<Row> INPUT =
      Types.ROW_NAMED(
          new String[] {"id", "a", "aa", "r", "ar", "s", "p"},
          Types.INT,
          Types.OBJECT_ARRAY(Types.STRING),
          Types.OBJECT_ARRAY(Types.OBJECT_ARRAY(Types.STRING)),
          RECORD,
          Types.OBJECT_ARRAY(RECORD),
          Types.STRING,
          Types.STRING);

  @ParameterizedTest
  @ValueSource(
      strings = {
        "JSON_STRING(a)",
        "JSON_STRING(aa)",
        "JSON_STRING(r)",
        "JSON_STRING(ar)",
        "a, r, JSON_QUERY(s, p)",
        "aa, ar, JSON_QUERY(s, p)",
        "ARRAY[JSON_QUERY(s, p), JSON_STRING(a), CAST(NULL AS STRING)]",
        "ROW(JSON_QUERY(s, p), a, r)",
        "CASE WHEN id = 1 THEN CAST(NULL AS ROW<label STRING, amount INT>) ELSE r END,"
            + " JSON_QUERY(s, p)",
        "JSON_QUERY(s, p), ARRAY[id, CAST(NULL AS INT), id + 1]"
      })
  void nestedInputsAndOutputsMatchTheGeneratedHostCalc(String projection) throws Exception {
    String sql = "SELECT id, " + projection + " FROM inputs";
    String plan = NativePlanner.explain(environment(false), sql);
    assertTrue(plan.contains("NativeCalc") && plan.contains("jsonEvaluation=[JVM]"), plan);
    NativeParity.assertParity(() -> environment(false), sql);
  }

  @Test
  void decimalAndNanoTimestampArraysRetainTheirDeclaredValues() throws Exception {
    NativeParity.assertParity(
        () -> {
          var env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          var table = StreamTableEnvironment.create(env);
          table.createTemporaryView(
              "inputs",
              env.fromData(
                  List.of(
                      Row.of(
                          new java.math.BigDecimal[] {new java.math.BigDecimal("12.345"), null},
                          new java.time.LocalDateTime[] {
                            java.time.LocalDateTime.parse("2025-01-02T03:04:05.123456789"), null
                          }),
                      Row.of(new java.math.BigDecimal[] {}, new java.time.LocalDateTime[] {}),
                      Row.of(null, null)),
                  Types.ROW_NAMED(
                      new String[] {"decimal_values", "time_values"},
                      Types.OBJECT_ARRAY(Types.BIG_DEC),
                      Types.OBJECT_ARRAY(Types.LOCAL_DATE_TIME))),
              Schema.newBuilder()
                  .column("decimal_values", DataTypes.ARRAY(DataTypes.DECIMAL(38, 9)))
                  .column("time_values", DataTypes.ARRAY(DataTypes.TIMESTAMP(9)))
                  .build());
          return table;
        },
        "SELECT JSON_STRING(decimal_values), JSON_STRING(time_values), decimal_values, time_values"
            + " FROM inputs");
  }

  @Test
  void nestedResultsKeepRawChangelogKindsAfterFiltering() throws Exception {
    NativeParity.assertKindedParity(
        () -> environment(true), "SELECT id, a, r, JSON_QUERY(s, p) FROM inputs WHERE id <> 2");
  }

  @ParameterizedTest
  @ValueSource(strings = {"JSON_STRING(m)", "m, JSON_QUERY(s, '$')", "ROW(m), JSON_QUERY(s, '$')"})
  void mapBoundariesRemainExplicitFallback(String projection) throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> {
          var env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          var table = StreamTableEnvironment.create(env);
          table.createTemporaryView(
              "inputs",
              env.fromData(
                  List.of(Row.of(Map.of("a", 1), "{}")),
                  Types.ROW_NAMED(
                      new String[] {"m", "s"}, Types.MAP(Types.STRING, Types.INT), Types.STRING)));
          return table;
        },
        "SELECT " + projection + " FROM inputs",
        "row-fused UDF");
  }

  private static TableEnvironment environment(boolean changelog) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    List<Row> rows =
        new ArrayList<>(
            List.of(
                Row.of(
                    1,
                    new String[] {"first", null, "snowman ☃"},
                    new String[][] {{"a"}, {}, null},
                    Row.of("x", 1),
                    new Row[] {Row.of("y", 2), null, Row.of(null, 3)},
                    "{\"v\":\"one\",\"items\":[{\"n\":1},{\"n\":3}]}",
                    "$.items[*].n"),
                Row.of(
                    2,
                    new String[] {},
                    new String[][] {},
                    null,
                    new Row[] {},
                    "{\"v\":null,\"items\":[]}",
                    "$.items"),
                Row.of(3, null, null, Row.of(null, null), null, "broken", "$.v"),
                Row.of(
                    4,
                    new String[] {"last"},
                    new String[][] {{null}, {"z", "w"}},
                    Row.of("nested", -7),
                    new Row[] {Row.of("z", null)},
                    null,
                    "$.items")));
    if (changelog) {
      Row before = Row.copy(rows.get(0));
      before.setKind(RowKind.UPDATE_BEFORE);
      rows.add(before);
      Row after = Row.copy(rows.get(0));
      after.setKind(RowKind.UPDATE_AFTER);
      after.setField(1, new String[] {"updated", null});
      after.setField(3, Row.of("changed", 9));
      rows.add(after);
      Row deleted = Row.copy(rows.get(3));
      deleted.setKind(RowKind.DELETE);
      rows.add(deleted);
      table.createTemporaryView("inputs", table.fromChangelogStream(env.fromData(rows, INPUT)));
    } else {
      table.createTemporaryView("inputs", env.fromData(rows, INPUT));
    }
    return table;
  }
}
