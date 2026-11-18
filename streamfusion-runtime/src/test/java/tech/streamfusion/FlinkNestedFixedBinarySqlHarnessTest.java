package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@DisabledIfSystemProperty(named = "streamfusion.experimental.nestedScalarUdfs", matches = "false")
class FlinkNestedFixedBinarySqlHarnessTest {
  @Test
  void callbackBinaryLengthsRemainValuesRatherThanDeclaredStorageWidths() {
    var runs =
        NativeFailureParity.run(() -> environment(false), "SELECT n, binary_row(n) FROM src");
    assertNull(runs.host().failure(), runs.host().toString());
    assertEquals(11, runs.host().rows().size());
    for (var record : runs.host().rows()) {
      Integer length = (Integer) record.get(1);
      if (length == null) assertNull(record.get(2));
      else {
        Row nested = (Row) record.get(2);
        assertEquals(RowKind.values()[Math.floorMod(length, 4)], nested.getKind());
        assertEquals(
            length < 0 ? null : org.apache.flink.util.StringUtils.byteToHexString(bytes(length)),
            nested.getField(0));
      }
    }
    System.out.println(
        "[nested-fixed-binary-oracle] exact lengths0,1,2,3,4,5,8,17,4097 and NULL preserved;"
            + " host_types="
            + runs.host().resultTypes());
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT n, binary_row(n).payload FROM src",
        "SELECT n, binary_bundle(binary_row(n)) FROM src",
        "SELECT n, binary_bundle(binary_row(n)).items FROM src",
        "SELECT n, binary_bundle(binary_row(n)).copy FROM src",
        "SELECT n, binary_row(n) FROM src WHERE TO_BASE64(binary_row(n).payload) <> ''"
      })
  void nestedResultsComposeWithConsumersWithoutPaddingOrTruncation(String sql) {
    for (boolean reuse : new boolean[] {false, true}) {
      var runs = NativeFailureParity.run(() -> environment(reuse), sql);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    }
  }

  private static TableEnvironment environment(boolean reuse) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    if (reuse) env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    var rows = new ArrayList<Row>();
    for (Integer length : new Integer[] {0, 1, 2, 3, 4, 5, 8, 17, 4097, -1, null})
      rows.add(Row.of(length));
    table.createTemporaryView(
        "src", fromData(env, rows, Types.ROW_NAMED(new String[] {"n"}, Types.INT)));
    table.createTemporarySystemFunction("binary_row", new BinaryRow());
    table.createTemporarySystemFunction("binary_bundle", new BinaryBundle());
    return table;
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, binary_identity(r) FROM src",
        "SELECT id, binary_bundle(r) FROM src",
        "SELECT id, ARRAY[r.payload] FROM src",
        "SELECT r, COUNT(*) FROM src GROUP BY r"
      })
  void typedInputsRetainActualPayloadLengthsAcrossNativeBoundaries(String sql) {
    var runs =
        NativeFailureParity.run(
            () -> {
              var rows = new ArrayList<Row>();
              int id = 0;
              for (Integer n : new Integer[] {0, 1, 2, 3, 4, 5, 8, 4097, -1, null}) {
                Row nested =
                    n == null
                        ? null
                        : Row.ofKind(
                            RowKind.values()[Math.floorMod(n, 4)], n < 0 ? null : bytes(n));
                rows.add(Row.of(id++, nested));
                rows.add(Row.of(id++, nested));
              }
              var table =
                  BuiltinFunctionParity.environment(
                      org.apache.flink.table.api.DataTypes.ROW(
                          org.apache.flink.table.api.DataTypes.FIELD(
                              "id", org.apache.flink.table.api.DataTypes.INT()),
                          org.apache.flink.table.api.DataTypes.FIELD(
                              "r",
                              org.apache.flink.table.api.DataTypes.ROW(
                                  org.apache.flink.table.api.DataTypes.FIELD(
                                      "payload", org.apache.flink.table.api.DataTypes.BINARY(4))))),
                      rows);
              table.getConfig().set("streamfusion.transpose.batchRows", "3");
              table.createTemporarySystemFunction("binary_identity", new BinaryIdentity());
              table.createTemporarySystemFunction("binary_bundle", new BinaryBundle());
              return table;
            },
            sql);
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    assertTrue(
        runs.nativeRun()
            .plan()
            .contains(sql.contains("GROUP BY") ? "NativeColumnarGroupAggregate" : "NativeCalc"),
        runs.nativeRun().plan());
  }

  private static byte[] bytes(int length) {
    byte[] value = new byte[length];
    for (int i = 0; i < length; i++) value[i] = (byte) (i * 73 + 128);
    return value;
  }

  @FunctionHint(input = @DataTypeHint("INT"), output = @DataTypeHint("ROW<payload BINARY(4)>"))
  public static class BinaryRow extends ScalarFunction {
    public Row eval(Integer length) {
      return length == null
          ? null
          : Row.ofKind(
              RowKind.values()[Math.floorMod(length, 4)], length < 0 ? null : bytes(length));
    }
  }

  @FunctionHint(
      input = @DataTypeHint("ROW<payload BINARY(4)>"),
      output = @DataTypeHint("ROW<payload BINARY(4)>"))
  public static class BinaryIdentity extends ScalarFunction {
    public Row eval(Row input) {
      return input;
    }
  }

  @FunctionHint(
      input = @DataTypeHint("ROW<payload BINARY(4)>"),
      output = @DataTypeHint("ROW<items ARRAY<BINARY(4)>, copy BINARY(4)>"))
  public static class BinaryBundle extends ScalarFunction {
    private transient Row output;
    private transient byte[][] items;

    @Override
    public void open(FunctionContext context) {
      output = new Row(2);
      items = new byte[4][];
    }

    public Row eval(Row input) {
      if (input == null) return null;
      byte[] payload = (byte[]) input.getField(0);
      items[0] = payload;
      items[1] = new byte[0];
      items[2] = null;
      items[3] = new byte[] {0, (byte) 255, (byte) 128, 3, 5};
      output.setKind(input.getKind());
      output.setField(0, items);
      output.setField(1, payload);
      return output;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }
}
