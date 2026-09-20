package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkBooleanStringCastSqlHarnessTest {
  @Test
  void castsPreserveCaseLengthsPaddingAndNulls() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::environment,
        "SELECT CAST(b AS STRING), CAST(b AS VARCHAR(2)), CAST(b AS VARCHAR(5)), "
            + "CAST(b AS CHAR(2)), CAST(b AS CHAR(8)), TRY_CAST(b AS VARCHAR(1)) FROM src");
  }

  @Test
  void consumersAndEmptyBatchesMatchFlink() throws Exception {
    BuiltinFunctionParity.assertParity(
        this::environment,
        "SELECT CONCAT(CAST(b AS STRING), '!') FROM src WHERE CAST(b AS VARCHAR(1)) = 'T'");
    BuiltinFunctionParity.assertParity(
        this::environment, "SELECT CAST(b AS CHAR(8)) FROM src WHERE n = 99");
  }

  private TableEnvironment environment() {
    return BuiltinFunctionParity.environment(
        ROW(FIELD("b", BOOLEAN()), FIELD("n", INT())),
        List.of(Row.of(true, 0), Row.of(false, 1), Row.of(null, 2)));
  }
}
