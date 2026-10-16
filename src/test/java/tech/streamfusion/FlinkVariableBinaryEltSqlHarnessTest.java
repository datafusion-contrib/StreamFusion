package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.util.ArrayList;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@EnabledIfSystemProperty(named = "streamfusion.experimental.binaryElt", matches = "true")
class FlinkVariableBinaryEltSqlHarnessTest {
  private static final int[] LENGTHS = {0, 1, 2, 3, 4, 5, 8, 17, 4097, -1};

  @BeforeEach
  void requireReleasedHostFunction() {
    tech.streamfusion.compat.FlinkTestCapabilities.requireSqlFunction("ELT");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void declaredBinaryWidthDoesNotChangeSelectedPayloads(boolean reuse) {
    var runs =
        NativeFailureParity.run(() -> environment(reuse), "SELECT id, b, ELT(1,b,c) FROM src");
    assertNull(runs.host().failure(), runs.host().toString());
    assertEquals(30, runs.host().rows().size());
    for (var row : runs.host().rows()) {
      int id = (Integer) row.get(1);
      byte[] value = bytes(LENGTHS[id % LENGTHS.length], id);
      String expected =
          value == null ? null : org.apache.flink.util.StringUtils.byteToHexString(value);
      assertEquals(expected, row.get(2));
      assertEquals(expected, row.get(3));
    }
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT id, ELT(n,b,c), TO_BASE64(ELT(n,b,c)) FROM src",
        "SELECT id, ELT(n,b,c), ELT(2,c,b) FROM src WHERE MOD(id,3) <> 1"
      })
  void dynamicSelectionRetainsNullsBoundsAndFilteredBatches(String sql) {
    for (boolean reuse : new boolean[] {false, true}) {
      var runs = NativeFailureParity.run(() -> environment(reuse), sql);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void selectedRawBytesCanBecomeNativeGroupingKeys(boolean reuse) {
    var runs =
        NativeFailureParity.run(
            () -> environment(reuse), "SELECT ELT(n,b,c), COUNT(*) FROM src GROUP BY ELT(n,b,c)");
    runs.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertTrue(
        runs.nativeRun().plan().contains("NativeColumnarGroupAggregate"), runs.nativeRun().plan());
    assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
  }

  @Test
  void unselectedFailingExpressionStaysUnevaluated() throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> environment(true),
        "SELECT CASE WHEN id < 0 THEN ELT(1, CAST(CAST(1/(id-id) AS STRING) AS BYTES),b) "
            + "ELSE ELT(n,b,c) END FROM src");
  }

  private static TableEnvironment environment(boolean reuse) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    if (reuse) env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", "3");
    var type =
        ROW(FIELD("id", INT()), FIELD("b", BINARY(4)), FIELD("c", BINARY(4)), FIELD("n", INT()));
    var rows = new ArrayList<Row>();
    for (int id = 0; id < 30; id++)
      rows.add(
          Row.of(
              id,
              bytes(LENGTHS[id % LENGTHS.length], id),
              bytes(LENGTHS[(id + 3) % LENGTHS.length], id + 17),
              id % 7 == 0 ? null : id % 5 - 1));
    table.createTemporaryView(
        "src",
        fromData(env, rows, ExternalTypeInfo.<Row>of(type)),
        Schema.newBuilder().fromRowDataType(type).build());
    return table;
  }

  private static byte[] bytes(int length, int seed) {
    if (length < 0) return null;
    byte[] result = new byte[length];
    for (int i = 0; i < length; i++) result[i] = (byte) (seed + i * 131);
    return result;
  }
}
