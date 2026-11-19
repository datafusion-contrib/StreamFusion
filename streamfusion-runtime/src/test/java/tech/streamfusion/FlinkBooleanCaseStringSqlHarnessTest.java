package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkBooleanCaseStringSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"CAST,false", "CAST,true", "TRY_CAST,false", "TRY_CAST,true"})
  void verifiedCaseRewritesKeepValuesTypesAndPureExecution(String operator, boolean legacy)
      throws Exception {
    List<String> expressions = List.of(
        "CASE WHEN b THEN c ELSE NOT c END",
        "CASE WHEN b THEN n<0 WHEN b IS NULL THEN n=1 ELSE NOT b END",
        "CASE WHEN b IS NULL THEN c WHEN b THEN NOT c ELSE b OR c END",
        "CASE WHEN b THEN CASE WHEN c THEN FALSE ELSE TRUE END ELSE c END",
        "CASE WHEN b THEN c ELSE CAST(NULL AS BOOLEAN) END");
    var outputs = new ArrayList<String>();
    for (String expression : expressions)
      for (String target : List.of("STRING", "VARCHAR(1)", "VARCHAR(3)", "CHAR(4)", "CHAR(8)"))
        outputs.add(operator + "(" + expression + " AS " + target + ")");
    for (boolean nullable : new boolean[] {false, true}) {
      var bool = nullable ? BOOLEAN() : BOOLEAN().notNull();
      var integer = nullable ? INT() : INT().notNull();
      var type = ROW(FIELD("id", INT().notNull()), FIELD("b", bool), FIELD("c", bool),
          FIELD("n", integer));
      var rows = new ArrayList<Row>();
      for (int id = 0; id < 5003; id++)
        rows.add(Row.of(id, nullable && id % 7 == 0 ? null : id % 2 == 0,
            nullable && id % 5 == 0 ? null : id % 3 == 0,
            nullable && id % 11 == 0 ? null : id % 17 == 0 ? Integer.MIN_VALUE
                : id % 17 == 1 ? Integer.MAX_VALUE : (id % 2 == 0 ? -id : id)));
      FlinkBooleanStringCastSqlHarnessTest.assertRuntimeCasts(
          "SELECT id," + String.join(",", outputs) + " FROM src", legacy, type, rows);
    }
  }
}
