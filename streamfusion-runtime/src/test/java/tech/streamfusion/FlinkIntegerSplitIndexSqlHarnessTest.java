package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkIntegerSplitIndexSqlHarnessTest {
  @Test
  void allByteCharactersEmptyTokensAndNullsMatchReleasedFlink() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::inputs, "SELECT id, SPLIT_INDEX(s, delimiter, idx) FROM src");
  }

  @Test
  void filtersAndConditionalConsumersRetainTheOverload() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::inputs, "SELECT id FROM src WHERE SPLIT_INDEX(s, delimiter, idx) = ''");
    BuiltinFunctionParity.assertParity(
        this::inputs,
        "SELECT id, CASE WHEN idx < 0 THEN 'skipped' ELSE SPLIT_INDEX(s, delimiter, idx) END FROM"
            + " src");
  }

  @Test
  void emptyFilteredBatchesDoNotEvaluateFailingArguments() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::inputs, "SELECT SPLIT_INDEX(s, delimiter, 1 / (idx - idx)) FROM src WHERE id < 0");
    BuiltinFunctionParity.assertParity(
        this::inputs,
        "SELECT id, CASE WHEN id < 0 THEN SPLIT_INDEX(s, delimiter, 1 / (idx - idx)) ELSE s END"
            + " FROM src");
  }

  @Test
  void nullAndNarrowDelimitersKeepTheirDeclaredConversion() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::inputs,
        "SELECT id, SPLIT_INDEX(s, CAST(NULL AS INT), idx), "
            + "SPLIT_INDEX(s, CAST(delimiter AS TINYINT), idx), "
            + "SPLIT_INDEX(s, CAST(delimiter AS SMALLINT), idx) FROM src");
  }

  private TableEnvironment inputs() {
    List<Row> rows = new ArrayList<>();
    for (int delimiter = 1; delimiter <= 255; delimiter++) {
      String separator = Character.toString((char) delimiter);
      String value = separator + "\ud83d\ude00" + separator + separator + "\u4e2d" + separator;
      for (int index : new int[] {-1, 0, 1, 2, 3, 4, 5, Integer.MAX_VALUE}) {
        rows.add(Row.of(rows.size(), value, delimiter, index));
      }
    }
    for (Integer delimiter :
        new Integer[] {Integer.MIN_VALUE, -1, 0, 256, 65535, Integer.MAX_VALUE, null}) {
      rows.add(Row.of(rows.size(), "a,b", delimiter, 0));
    }
    rows.add(Row.of(rows.size(), "", 44, 0));
    rows.add(Row.of(rows.size(), null, 44, 0));
    rows.add(Row.of(rows.size(), "a,b", 44, null));
    return BuiltinFunctionParity.environment(
        ROW(
            FIELD("id", INT()),
            FIELD("s", STRING()),
            FIELD("delimiter", INT()),
            FIELD("idx", INT())),
        rows);
  }
}
