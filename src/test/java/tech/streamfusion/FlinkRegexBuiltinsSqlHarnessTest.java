package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkRegexBuiltinsSqlHarnessTest {
  @Test
  void javaPatternsTypesNullsAndLiteralReplacementSpanBatches() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::environment,
        "SELECT REGEXP(s,p), REGEXP_REPLACE(s,p,r), REGEXP_COUNT(s,p), "
            + "REGEXP_INSTR(s,p), REGEXP_SUBSTR(s,p) FROM src");
  }

  @Test
  void filterAndComposedConsumersMatchFlink() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::environment,
        "SELECT REGEXP_SUBSTR(s,p), CONCAT(REGEXP_REPLACE(s,p,r), '!'), "
            + "REGEXP_COUNT(s,p) + 1 FROM src WHERE REGEXP(s,p)");
    BuiltinFunctionParity.assertParity(
        this::environment,
        "SELECT REGEXP_REPLACE(s,p,r), REGEXP_INSTR(s,p) FROM src WHERE n = 99");
  }

  private TableEnvironment environment() {
    List<Row> samples = List.of(
        Row.of("abc123abc", "abc", "X", 0), Row.of("ab", "a(?=b)", "$1", 1),
        Row.of("aab", "(a)\\1", "\\$1", 2), Row.of("\u4e2da\ud83d\ude00", "a", "", 3),
        Row.of("abc", "", "X", 4), Row.of("abc", "[", "X", 5),
        Row.of(null, "a", "X", 6), Row.of("abc", null, "X", 7),
        Row.of("abc", "a", null, 8), Row.of("", "z", "X", 9));
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 5003; i++) rows.add(Row.copy(samples.get(i % samples.size())));
    return BuiltinFunctionParity.environment(
        ROW(FIELD("s", STRING()), FIELD("p", STRING()), FIELD("r", STRING()), FIELD("n", INT())), rows);
  }
}
