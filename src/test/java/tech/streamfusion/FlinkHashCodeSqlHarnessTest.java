package tech.streamfusion;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkHashCodeSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "s",
        "CAST(s AS CHAR(30))",
        "i",
        "n",
        "b",
        "d",
        "CAST(d AS DECIMAL(38,3))",
        "CAST(n AS DECIMAL(38,0))",
        "CAST(i AS DECIMAL(38,18))",
        "CAST(i AS SMALLINT)",
        "CAST(i AS TINYINT)",
        "t",
        "CAST(t AS TIMESTAMP(3))",
        "CAST(t AS TIMESTAMP(6))",
        "CAST(t AS DATE)",
        "CAST(t AS TIME)",
        "CAST(n AS FLOAT)",
        "CAST(n AS DOUBLE)"
      })
  void scalarValuesAndNullsMatchFlink(String expression) throws Exception {
    NativeParity.assertParity(
        FlinkHashCodeSqlHarnessTest::environment,
        "SELECT id, HASH_CODE(" + expression + ") FROM src");
  }

  @Test
  void composesWithBucketArithmeticAndFilters() throws Exception {
    NativeParity.assertParity(
        FlinkHashCodeSqlHarnessTest::environment,
        "SELECT id, MOD(HASH_CODE(s), 1024), HASH_CODE(COALESCE(s, '')) FROM src "
            + "WHERE HASH_CODE(n) < 100 OR n IS NULL");
  }

  @Test
  void sameNamedUserFunctionRetainsItsMeaning() throws Exception {
    NativeParity.assertParity(
        () -> {
          TableEnvironment table = environment();
          table.createTemporarySystemFunction("HASH_CODE", NamedHash.class);
          return table;
        },
        "SELECT HASH_CODE(s) FROM src");
  }

  public static class NamedHash extends ScalarFunction {
    public int eval(String input) {
      return 77;
    }
  }

  private static TableEnvironment environment() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    var beforeEpoch = LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999999999);
    table.createTemporaryView(
        "src",
        env.fromData(
            Types.ROW_NAMED(
                new String[] {"id", "s", "i", "n", "b", "d", "t"},
                Types.INT,
                Types.STRING,
                Types.INT,
                Types.LONG,
                Types.BOOLEAN,
                Types.BIG_DEC,
                Types.LOCAL_DATE_TIME),
            Row.of(
                0,
                "abc",
                Integer.MIN_VALUE,
                Long.MIN_VALUE,
                true,
                new BigDecimal("-99999999999999999999999999999.123456789"),
                beforeEpoch),
            Row.of(
                1,
                "\ud83d\ude00",
                Integer.MAX_VALUE,
                Long.MAX_VALUE,
                false,
                new BigDecimal("99999999999999999999999999999.123456789"),
                LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999999)),
            Row.of(
                2,
                "polygenelubricants",
                -1,
                -1L,
                true,
                new BigDecimal("1.230000000"),
                LocalDateTime.of(1, 1, 1, 0, 0, 0, 1)),
            Row.of(3, "", 0, 0L, false, BigDecimal.ZERO, beforeEpoch),
            Row.of(
                4,
                "\u4e2d\u0000e\u0301",
                127,
                0x100000000L,
                true,
                new BigDecimal("-1.230000000"),
                beforeEpoch),
            Row.of(5, null, null, null, null, null, null)),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("s", DataTypes.STRING())
            .column("i", DataTypes.INT())
            .column("n", DataTypes.BIGINT())
            .column("b", DataTypes.BOOLEAN())
            .column("d", DataTypes.DECIMAL(38, 9))
            .column("t", DataTypes.TIMESTAMP(9))
            .build());
    return table;
  }
}
