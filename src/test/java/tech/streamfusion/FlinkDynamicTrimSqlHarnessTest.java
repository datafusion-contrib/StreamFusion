package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkDynamicTrimSqlHarnessTest {
  @org.junit.jupiter.api.BeforeEach
  void requireHostFunction() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("BTRIM");
  }

  @Test
  void perRowSetsPreserveWhitespaceUnicodeAndNullsAcrossBatches() throws Exception {
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT BTRIM(s,LEFT(c,1)), LTRIM(s,LEFT(c,1)), RTRIM(s,LEFT(c,1)) FROM src WHERE c <> ''");
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT BTRIM(s,c), LTRIM(s,c), RTRIM(s,c), BTRIM(s,'ab') FROM src");
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT BTRIM(s,c) FROM src WHERE BTRIM(s,c) = 'XYZ'");
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT LTRIM(s,c), RTRIM(s,c) FROM src WHERE n = 99");
  }

  @Test
  void computedEmptySetRetainsTheReleasedHostFailure() throws Exception {
    NativeFailureParity.run(this::environment, "SELECT BTRIM(s,LEFT(c,1)) FROM src")
        .assertFailure(ArithmeticException.class, "/ by zero", NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @Test
  void unprovenBinaryStringRepresentationsKeepSafeFallback() throws Exception {
    NativeParity.assertFallbackReasonContains(FlinkExtremaBuiltinsSqlHarnessTest::binaryEnvironment,
        "SELECT BTRIM(a,b), LTRIM(a,b), RTRIM(a,b) FROM src", "string representation");
  }

  private TableEnvironment environment() {
    List<Row> samples = List.of(Row.of("aabaXYZbaa", "ab", 0), Row.of(" abaXYZba ", " ab", 1),
        Row.of("\ud83d\ude00XYZ\ud83d\ude00", "\ud83d\ude00", 2), Row.of("abc", "", 3),
        Row.of("", "abc", 4), Row.of("aaa", "a", 5), Row.of(null, "ab", 6),
        Row.of("abc", null, 7), Row.of("\t abc\t", "\t ", 8));
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 5003; i++) rows.add(Row.copy(samples.get(i % samples.size())));
    return BuiltinFunctionParity.environment(
        ROW(FIELD("s", STRING()), FIELD("c", STRING()), FIELD("n", INT())), rows);
  }
}
