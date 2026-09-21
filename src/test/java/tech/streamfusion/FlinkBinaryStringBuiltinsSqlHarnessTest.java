package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkBinaryStringBuiltinsSqlHarnessTest {
  @Test
  void binaryPredicatesAndSelectionPreserveEveryByteAcrossBatches() throws Exception {
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT STARTSWITH(b,p), ENDSWITH(b,q), ELT(n,b,p,q), ELT(2,b,q,p), "
            + "STARTSWITH(ELT(n,b,p,q),p), ENDSWITH(b,X'00FF'), ELT(n,b,X'FF00') FROM src");
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT ELT(n,b,p,q), ELT(1,q,p,b) FROM src WHERE STARTSWITH(b,p) OR ENDSWITH(b,q)");
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT ELT(n,b,p,q), STARTSWITH(b,p) FROM src WHERE n = 99");
  }

  @Test
  void binaryResultCompositionRetainsBytesAndNulls() throws Exception {
    BuiltinFunctionParity.assertParity(this::environment,
        "SELECT TO_BASE64(ELT(n,b,p,q)), "
            + "CASE WHEN STARTSWITH(b,p) THEN ELT(n,b,p,q) ELSE q END FROM src");
  }

  private TableEnvironment environment() {
    byte[] empty = new byte[0];
    byte[] high = {0, (byte) 255, (byte) 128, 0};
    List<Row> samples = List.of(
        Row.of(high, new byte[] {0, (byte) 255}, new byte[] {(byte) 128, 0}, 1),
        Row.of(high, high, empty, 2), Row.of(empty, empty, high, 3),
        Row.of(high, empty, high, 0), Row.of(high, null, empty, -1),
        Row.of(null, high, null, 4), Row.of(high, high, high, null),
        Row.of(new byte[] {1, 2, 3, 4, 5}, high, high, Integer.MAX_VALUE),
        Row.of(empty, null, high, Integer.MIN_VALUE));
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 5003; i++) rows.add(Row.copy(samples.get(i % samples.size())));
    return BuiltinFunctionParity.environment(
        ROW(FIELD("b", BYTES()), FIELD("p", BYTES()), FIELD("q", BYTES()), FIELD("n", INT())), rows);
  }
}
