package tech.streamfusion;

import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkJsonMemberIdentitySqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"?", "😀", "�", "?x?"})
  void escapedMembersRemainDistinctDuringSelection(String member) throws Exception {
    String path = "$[''" + member + "'']";
    NativeParity.assertParity(
        FlinkJsonMemberIdentitySqlHarnessTest::environment,
        "SELECT id, JSON_VALUE(s, '" + path + "'), JSON_EXISTS(s, 'lax " + path + "'), "
            + "JSON_VALUE(s, 'lax " + path + "' RETURNING INTEGER DEFAULT 42 ON EMPTY) "
            + "FROM inputs");
  }

  private static TableEnvironment environment() {
    return TextTimeFunctionTestInputs.textRows(
        null,
        "{\"\\uD800\":1}",
        "{\"\\uDC00\":2}",
        "{\"?\":3}",
        "{\"?\":4,\"\\uD800\":5}",
        "{\"\\uD800\":6,\"?\":7}",
        "{\"?\":8,\"\\u003f\":9}",
        "{\"\\u003f\":10,\"?\":11}",
        "{\"\\uD83D\\uDE00\":12}",
        "{\"😀\":13}",
        "{\"�\":14,\"\\uD800\":15}",
        "{\"\\uD800x\\uDC00\":16}",
        "{\"?x?\":17,\"\\uD800x\\uDC00\":18}");
  }
}
