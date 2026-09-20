package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkBooleanIfSqlHarnessTest {
  @Test
  void nullableConditionsAndBranchesSpanBatches() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::environment,
        "SELECT IF(c,a,b), IF(c, IF(a,b,c), a), IF(c, TRUE, FALSE) FROM src");
  }

  @Test
  void predicatesAndUnselectedDivisionRetainShortCircuiting() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::environment,
        "SELECT IF(n = 0, TRUE, 10 / n > 1) FROM src WHERE IF(c,a,b)");
    BuiltinFunctionParity.assertParity(
        this::environment,
        "SELECT IF(c,a, 10 / n > 1) FROM src WHERE n = 99");
  }

  private TableEnvironment environment() {
    Boolean[] values = {true, false, null};
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 5003; i++) {
      rows.add(Row.of(values[i % 3], values[i / 3 % 3], values[i / 9 % 3], i % 3));
    }
    return BuiltinFunctionParity.environment(
        ROW(FIELD("c", BOOLEAN()), FIELD("a", BOOLEAN()), FIELD("b", BOOLEAN()),
            FIELD("n", INT())), rows);
  }
}
