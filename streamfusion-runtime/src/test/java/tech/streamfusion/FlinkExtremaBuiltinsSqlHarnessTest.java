package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkExtremaBuiltinsSqlHarnessTest {
  @Test
  void binaryBackedStringsRetainFlinkWithoutChangingTheirOrdering() throws Exception {
    NativeParity.assertFallbackReasonContains(
        FlinkExtremaBuiltinsSqlHarnessTest::binaryEnvironment,
        "SELECT GREATEST(a,b), LEAST(a,b) FROM src",
        "string representation");
  }

  static TableEnvironment binaryEnvironment() {
    var env = org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
        .getExecutionEnvironment();
    env.setParallelism(1);
    var table = org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
    var type = ROW(FIELD("a", STRING()), FIELD("b", STRING()));
    var rowType = (org.apache.flink.table.types.logical.RowType) type.getLogicalType();
    List<org.apache.flink.table.data.RowData> rows = List.of(
        org.apache.flink.table.data.GenericRowData.of(
            org.apache.flink.table.data.binary.BinaryStringData.fromBytes(
                "\ud83d\ude00".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
            org.apache.flink.table.data.binary.BinaryStringData.fromBytes(
                "\ue000".getBytes(java.nio.charset.StandardCharsets.UTF_8))));
    table.createTemporaryView("src", tech.streamfusion.compat.FlinkTestSources.fromData(
        env, rows, org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(rowType)),
        org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(type).build());
    return table;
  }

  @ParameterizedTest
  @ValueSource(strings = {"UTC", "Asia/Shanghai", "America/Los_Angeles"})
  void runtimeStringsCoercionsAndNanosecondTimestamps(String zone) throws Exception {
    BuiltinFunctionParity.assertParity(
        () -> environment(zone),
        "SELECT GREATEST(a,b), LEAST(a,b), GREATEST(i,d), LEAST(i,d), "
            + "GREATEST(t,u), LEAST(t,u), GREATEST(lt,lu), LEAST(lt,lu) FROM src");
    BuiltinFunctionParity.assertParity(
        () -> environment(zone),
        "SELECT GREATEST(a,b), LEAST(i,d) FROM src WHERE GREATEST(i,d) > 0");
  }

  private TableEnvironment environment(String zone) {
    var beforeEpoch = LocalDateTime.parse("1969-12-31T23:59:59.999999999");
    var future = LocalDateTime.parse("9999-12-31T23:59:59.999999999");
    return BuiltinFunctionParity.environment(
        ROW(FIELD("a", STRING()), FIELD("b", STRING()), FIELD("i", INT()),
            FIELD("d", DECIMAL(12,2)), FIELD("t", TIMESTAMP(9)), FIELD("u", TIMESTAMP(9)),
            FIELD("lt", TIMESTAMP_LTZ(9)), FIELD("lu", TIMESTAMP_LTZ(9))),
        List.of(
            Row.of("abc", "def", 2, new BigDecimal("1.25"), beforeEpoch, future,
                Instant.ofEpochSecond(-1, 999999999), Instant.ofEpochSecond(1)),
            Row.of("\ud83d\ude00", "\ue000", -2, new BigDecimal("-1.25"), future, beforeEpoch,
                Instant.ofEpochSecond(1), Instant.ofEpochSecond(-1, 999999999)),
            Row.of(null, "z", null, new BigDecimal("0.00"), null, beforeEpoch, null, Instant.EPOCH),
            Row.of("", null, 0, null, beforeEpoch, null, Instant.EPOCH, null)), zone);
  }
}
