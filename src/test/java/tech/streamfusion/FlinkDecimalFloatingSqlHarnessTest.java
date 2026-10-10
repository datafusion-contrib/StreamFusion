package tech.streamfusion;

import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkDecimalFloatingSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"7,2", "18,0", "18,9", "18,18", "38,0", "38,18", "38,37", "38,38"})
  void scalarAndArrayCastsMatchFlinkRounding(int precision, int scale) throws Exception {
    NativeParity.assertParity(
        () -> decimals(precision, scale, false),
        "SELECT id, CAST(d AS FLOAT), CAST(d AS DOUBLE), "
            + "CAST(a AS ARRAY<FLOAT>), CAST(a AS ARRAY<DOUBLE>) FROM src");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void arrayConsumersAndNullabilityKeepTheirResolvedTypes(boolean notNull) throws Exception {
    NativeParity.assertParity(
        () -> decimals(38, 18, notNull),
        "SELECT id, CAST(a AS ARRAY<FLOAT>)[idx], CAST(a AS ARRAY<DOUBLE>)[1] "
            + "FROM src WHERE CAST(a AS ARRAY<FLOAT>)[idx] IS NOT NULL");
  }

  @ParameterizedTest
  @ValueSource(strings = {"POWER(d, 3)", "POWER(2.0, d)", "POWER(d, d)", "SQRT(d)"})
  void decimalPowerUsesFlinksResolvedOverload(String expression) throws Exception {
    NativeParity.assertParity(
        () -> decimals(18, 9, false), "SELECT id, " + expression + " FROM src");
  }

  private static TableEnvironment decimals(int precision, int scale, boolean notNull) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    var decimal = DataTypes.DECIMAL(precision, scale);
    var array = DataTypes.ARRAY(notNull ? decimal.notNull() : decimal);
    BigInteger limit = BigInteger.TEN.pow(precision);
    Random random = new Random(93);
    Row[] rows = new Row[5003];
    for (int i = 0; i < rows.length; i++) {
      BigInteger coefficient;
      switch (i % 19) {
        case 0:
          coefficient = BigInteger.ZERO;
          break;
        case 1:
          coefficient = BigInteger.ONE;
          break;
        case 2:
          coefficient = limit.subtract(BigInteger.ONE);
          break;
        case 3:
          coefficient = new BigInteger("10000000596046447753906250000000000001").mod(limit);
          break;
        default:
          coefficient = new BigInteger(128, random).mod(limit);
          break;
      }
      if (i % 2 == 0) coefficient = coefficient.negate();
      BigDecimal value = new BigDecimal(coefficient, scale);
      BigDecimal[] values =
          notNull
              ? new BigDecimal[] {value, value.negate()}
              : i % 7 == 0
                  ? null
                  : i % 5 == 0 ? new BigDecimal[0] : new BigDecimal[] {value, null, value.negate()};
      rows[i] = Row.of(i, !notNull && i % 11 == 0 ? null : value, values, i % 5 - 1);
    }
    table.createTemporaryView(
        "src",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "d", "a", "idx"},
                Types.INT,
                Types.BIG_DEC,
                Types.OBJECT_ARRAY(Types.BIG_DEC),
                Types.INT),
            rows),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("d", notNull ? decimal.notNull() : decimal)
            .column("a", notNull ? array.notNull() : array)
            .column("idx", DataTypes.INT())
            .build());
    return table;
  }
}
