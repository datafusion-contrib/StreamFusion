package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

class FlinkUdfExactTypesSqlHarnessTest {
  @Test
  void decimalPrecision38AndBinaryValuesMatchHost() throws Exception {
    NativeParity.assertParity(
        () -> environment(32),
        "SELECT id, decimal_identity(d), binary_identity(b), id + 1 FROM src");
  }

  @Test
  void decimalResultsAreConvertedToTheDeclaredType() throws Exception {
    String sql = "SELECT id, decimal_from_text(s) FROM src";
    assertEquals(
        DataTypes.DECIMAL(5, 2),
        environment(32).sqlQuery(sql).getResolvedSchema().getColumnDataTypes().get(1));
    NativeParity.assertParity(() -> environment(32), sql);
  }

  @Test
  void repeatedCallsAndFiltersSpanMultipleBatches() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> environment(5003),
        "SELECT id, decimal_identity(d), decimal_identity(d), binary_identity(b), "
            + "decimal_from_text(s) FROM src "
            + "WHERE MOD(id, 3) <> 0", "shared stateful scalar UDF");
  }

  @Test
  void reusedBinaryResultBuffersAreCopiedBeforeTheNextCall() throws Exception {
    NativeParity.assertParity(() -> environment(5003), "SELECT id, reusable_binary(id) FROM src");
  }

  @Test
  void sharedBinaryBuffersAcrossCallSitesRetainHostEvaluationOrder() throws Exception {
    NativeParity.assertParity(
        () -> environment(5003),
        "SELECT id, reusable_binary(id), reusable_binary(id + 1) FROM src");
  }

  @Test
  void decimalConsumersRetainHostPreConversionNullFlag() throws Exception {
    NativeParity.assertParity(
        () -> environment(32),
        "SELECT id, decimal_from_text(s), decimal_from_text(s) IS NULL FROM src");
    NativeParity.assertParity(
        () -> environment(32),
        "SELECT id FROM src WHERE decimal_from_text(s) IS NULL");
  }

  @Test
  void typedNullArgumentsStillInvokeTheUserFunction() throws Exception {
    NativeParity.assertParity(
        () -> environment(32),
        "SELECT id, binary_identity(CAST(NULL AS BYTES)), "
            + "decimal_identity(CAST(NULL AS DECIMAL(38,9))) FROM src");
  }

  @Test
  void fixedBinaryResultRetainsItsExistingTypeGate() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> environment(32),
        "SELECT id, binary_fixed(b) FROM src",
        "UDF return type not native: BINARY");
  }

  @Test
  void alternateDecimalJavaRepresentationFallsBack() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> environment(32),
        "SELECT id, decimal_internal(s) FROM src",
        "unsupported Java conversion class");
  }

  @FunctionHint(input = @DataTypeHint("DECIMAL(38,9)"), output = @DataTypeHint("DECIMAL(38,9)"))
  public static class DecimalIdentity extends ScalarFunction {
    @Override
    public boolean isDeterministic() {
      return false;
    }

    public BigDecimal eval(BigDecimal value) {
      return value == null ? BigDecimal.ZERO : value;
    }
  }

  @FunctionHint(output = @DataTypeHint("DECIMAL(5,2)"))
  public static class DecimalFromText extends ScalarFunction {
    public BigDecimal eval(String value) {
      return value == null ? null : new BigDecimal(value);
    }
  }

  public static class BinaryIdentity extends ScalarFunction {
    @Override
    public boolean isDeterministic() {
      return false;
    }

    public byte[] eval(byte[] value) {
      return value == null ? new byte[] {0, (byte) 0xff} : value;
    }
  }

  @FunctionHint(output = @DataTypeHint("BINARY(4)"))
  public static class BinaryFixed extends ScalarFunction {
    public byte[] eval(byte[] value) {
      return value == null ? null : Arrays.copyOf(value, 4);
    }
  }

  @FunctionHint(output = @DataTypeHint(value = "DECIMAL(5,2)", bridgedTo = DecimalData.class))
  public static class DecimalInternal extends ScalarFunction {
    public DecimalData eval(String value) {
      return value == null ? null : DecimalData.fromBigDecimal(new BigDecimal(value), 5, 2);
    }
  }

  public static class ReusableBinary extends ScalarFunction {
    private final byte[] buffer = new byte[4];

    public byte[] eval(Integer value) {
      if (value == null || value % 7 == 0) {
        return null;
      }
      for (int i = 0; i < buffer.length; i++) {
        buffer[i] = (byte) (value >>> (i * 8));
      }
      return buffer;
    }
  }

  static TableEnvironment environment(int count) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment table = StreamTableEnvironment.create(env);
    table.createTemporarySystemFunction("decimal_identity", DecimalIdentity.class);
    table.createTemporarySystemFunction("decimal_from_text", DecimalFromText.class);
    table.createTemporarySystemFunction("binary_identity", BinaryIdentity.class);
    table.createTemporarySystemFunction("binary_fixed", BinaryFixed.class);
    table.createTemporarySystemFunction("decimal_internal", DecimalInternal.class);
    table.createTemporarySystemFunction("reusable_binary", ReusableBinary.class);
    String[] decimals = {
      "99999999999999999999999999999.999999999",
      "-99999999999999999999999999999.999999999",
      "1.230000000",
      "-0.000000001",
      "0",
      null
    };
    String[] text = {"999.995", "-999.995", "1.235", "-1.235", "1.2", "0", null, "9.99E+8"};
    byte[][] binary = {
      new byte[0], new byte[] {0, (byte) 0xff, (byte) 0x80}, new byte[] {1, 2, 0, 3}, null
    };
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      String decimal = decimals[i % decimals.length];
      rows.add(
          Row.of(
              i,
              decimal == null ? null : new BigDecimal(decimal),
              binary[i % binary.length],
              text[i % text.length]));
    }
    table.createTemporaryView(
        "src",
        env.fromData(
            rows,
            Types.ROW_NAMED(
                new String[] {"id", "d", "b", "s"},
                Types.INT,
                Types.BIG_DEC,
                Types.PRIMITIVE_ARRAY(Types.BYTE),
                Types.STRING)),
        Schema.newBuilder()
            .column("id", DataTypes.INT().notNull())
            .column("d", DataTypes.DECIMAL(38, 9))
            .column("b", DataTypes.BYTES())
            .column("s", DataTypes.STRING())
            .build());
    return table;
  }
}
