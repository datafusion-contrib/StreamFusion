package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkJvmOverRecoveryTest {
  static Stream<Arguments> frames() {
    return Stream.of(false, true)
        .flatMap(
            rocks ->
                Stream.of(
                        "rt,ROWS UNBOUNDED PRECEDING",
                        "rt,RANGE UNBOUNDED PRECEDING",
                        "rt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
                        "rt,RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW",
                        "pt,ROWS UNBOUNDED PRECEDING",
                        "pt,RANGE UNBOUNDED PRECEDING",
                        "pt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW")
                    .map(
                        frame ->
                            Arguments.of(rocks, frame.split(",", 2)[0], frame.split(",", 2)[1])));
  }

  @ParameterizedTest
  @MethodSource("frames")
  void managedFramesAndViewsSurviveTwoFailuresWithPeersAndLateInput(
      boolean rocks, String order, String frame) throws Exception {
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 36; i++)
      rows.add(
          Row.of(
              i,
              i % 7 == 0 ? null : i % 3,
              i % 5 == 0 ? null : (long) (i % 11),
              LocalDateTime.ofInstant(
                  java.time.Instant.ofEpochMilli(i == 12 ? 1000 : 1000 + i / 3 * 400),
                  ZoneOffset.UTC)));
    var schema =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("k", DataTypes.INT())
            .column("v", DataTypes.BIGINT())
            .column("rt", DataTypes.TIMESTAMP(3))
            .watermark("rt", "SOURCE_WATERMARK()")
            .columnByExpression("pt", "PROCTIME()")
            .build();
    var info =
        Types.ROW_NAMED(
            new String[] {"id", "k", "v", "rt"},
            Types.INT,
            Types.INT,
            Types.LONG,
            Types.LOCAL_DATE_TIME);
    String backend = rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap";
    try (var recovery =
        new PortableSqlRecovery(backend, rows, info, schema, false, 16, 28)
            .withExecutionMetrics()
            .withWatermarks(Map.of(12, 1600L, 24, 2400L, 36, 10000L))) {
      String query =
          "SELECT id,k,test_aggregate(v) OVER w,view_aggregate(v) OVER w,COUNT(*) OVER w"
              + " FROM recovery_input WINDOW w AS (PARTITION BY k ORDER BY "
              + order
              + " "
              + frame
              + ")";
      var runs =
          NativeFailureParity.runRecovery(
              () -> register(recovery.uninterrupted()), () -> register(recovery.get()), query);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeJvmOver");
      assertTrue(recovery.counterValue("jvmOverRows") >= rows.size());
      if (order.equals("rt")) {
        assertTrue(recovery.counterValue("jvmOverEventTimers") > 0);
        // Changelog canonicalization puts the kind before the projected fields.
        assertTrue(
            runs.nativeRun().rows().stream()
                .noneMatch(row -> Integer.valueOf(12).equals(row.get(1))),
            runs::toString);
      }
      SharedFlinkCluster.assertNativeMemoryReleased("jvm-over-recovery");
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "STRING",
        "CHAR(4)",
        "BOOLEAN",
        "TINYINT",
        "SMALLINT",
        "INT",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "VARBINARY",
        "BINARY(4)",
        "DECIMAL(18,8)",
        "DECIMAL(38,18)",
        "DATE",
        "TIME",
        "TIMESTAMP(9)",
        "TIMESTAMP_LTZ(9)",
        "DAY_TIME",
        "YEAR_MONTH"
      })
  void scalarKeysValuesAndAccumulatorsSurviveRepeatedRestore(String name) throws Exception {
    var valueType = FlinkTypedTableFunctionSqlHarnessTest.valueType(name);
    var type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()),
            DataTypes.FIELD("k", valueType),
            DataTypes.FIELD("v", valueType),
            DataTypes.FIELD("rt", DataTypes.TIMESTAMP(3)));
    var schema =
        Schema.newBuilder().fromRowDataType(type).watermark("rt", "SOURCE_WATERMARK()").build();
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 36; i++)
      rows.add(
          Row.of(
              i,
              FlinkTypedTableFunctionSqlHarnessTest.value(name, i / 4),
              FlinkTypedTableFunctionSqlHarnessTest.value(name, i / 3),
              LocalDateTime.ofInstant(
                  java.time.Instant.ofEpochMilli(1000 + i / 3 * 400), ZoneOffset.UTC)));
    for (String backend :
        List.of("hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")) {
      try (var recovery =
          new PortableSqlRecovery(
                  backend,
                  rows,
                  org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(type),
                  schema,
                  false,
                  16,
                  28)
              .withExecutionMetrics()
              .withWatermarks(Map.of(12, 1600L, 24, 2400L, 36, 10000L))) {
        java.util.function.UnaryOperator<TableEnvironment> configure =
            table -> {
              table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
              table.getConfig().set("streamfusion.transpose.batchRows", "5");
              table.createTemporarySystemFunction(
                  "remember", new FlinkTypedJvmAggregateSqlHarnessTest.Remember(valueType));
              return table;
            };
        var runs =
            NativeFailureParity.runRecovery(
                () -> configure.apply(recovery.uninterrupted()),
                () -> configure.apply(recovery.get()),
                "SELECT id,k,remember(v) OVER w,COUNT(*) OVER w FROM recovery_input WINDOW w AS"
                    + " (PARTITION BY k ORDER BY rt ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)");
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeJvmOver");
      }
    }
  }

  static TableEnvironment register(TableEnvironment table) {
    table.getConfig().setLocalTimeZone(ZoneOffset.UTC);
    table.getConfig().set("streamfusion.transpose.batchRows", "5");
    table.createTemporarySystemFunction("test_aggregate", new PortableSqlFixtures.Sum());
    table.createTemporarySystemFunction(
        "view_aggregate", new FlinkJvmAggregateSqlHarnessTest.ViewAggregate());
    return table;
  }
}
