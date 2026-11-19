package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkTemporalRowNumberSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"t,FIRST", "t,LAST", "z,FIRST", "z,LAST"})
  void exactRuntimeOrdersPreserveTiesPayloadsAndNullPlacement(String order, String nulls)
      throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    for (int batchRows : new int[] {1, 7}) {
      Supplier<TableEnvironment> setup =
          () -> {
            var env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            var table = StreamTableEnvironment.create(env);
            table.getConfig().setLocalTimeZone(ZoneOffset.ofHoursMinutes(5, 45));
            table.getConfig().set("streamfusion.transpose.batchRows", Integer.toString(batchRows));
            var stream =
                env.fromSequence(0, 47).map(index -> row(index.intValue())).returns(types());
            table.createTemporaryView("src", table.fromDataStream(stream, schema()));
            return table;
          };
      String sql =
          "SELECT id, k, f, d, t, z, ROW_NUMBER() OVER (PARTITION BY k ORDER BY "
              + order
              + " ASC NULLS "
              + nulls
              + ") FROM src";
      assertTrue(NativePlanner.explain(setup.get(), sql).contains("NativeColumnarTopN"));
      NativeParity.assertOrderedKindedParity(setup, sql);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "hashmap,t",
    "hashmap,z",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,t",
    "tech.streamfusion.state.RocksDBNativeStateBackendFactory,z"
  })
  void twoRestoresPreserveFloatingTiesAndTimestampFractions(String backend, String order)
      throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    List<Row> rows =
        IntStream.range(0, 96)
            .mapToObj(FlinkTemporalRowNumberSqlHarnessTest::row)
            .collect(tech.streamfusion.compat.ListCollectors.toList());
    try (var recovery =
        new PortableSqlRecovery(backend, rows, types(), schema(), false, 24, 63)
            .withExecutionMetrics()) {
      var comparison =
          NativeFailureParity.runRecovery(
              recovery::uninterrupted,
              recovery,
              "SELECT id, k, f, d, t, z, ROW_NUMBER() OVER (PARTITION BY k ORDER BY "
                  + order
                  + " ASC NULLS FIRST) FROM recovery_input");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeColumnarTopN");
      SharedFlinkCluster.assertNativeMemoryReleased("floating-temporal-non-time-rank-recovery");
    }
  }

  private static org.apache.flink.api.common.typeinfo.TypeInformation<Row> types() {
    return Types.ROW_NAMED(
        new String[] {"id", "k", "f", "d", "t", "z"},
        Types.INT,
        Types.INT,
        Types.FLOAT,
        Types.DOUBLE,
        Types.LOCAL_DATE_TIME,
        Types.INSTANT);
  }

  private static Schema schema() {
    return Schema.newBuilder()
        .column("id", DataTypes.INT())
        .column("k", DataTypes.INT())
        .column("f", DataTypes.FLOAT())
        .column("d", DataTypes.DOUBLE())
        .column("t", DataTypes.TIMESTAMP(9))
        .column("z", DataTypes.TIMESTAMP_LTZ(9))
        .build();
  }

  private static Row row(int id) {
    int slot = id % 12;
    long[] doubles = {
      0x7ff8000000000001L,
      0xfff8000000000002L,
      0x8000000000000000L,
      0L,
      0xfff0000000000000L,
      0x7ff0000000000000L,
      Double.doubleToRawLongBits(-1),
      Double.doubleToRawLongBits(1)
    };
    int[] floats = {
      0x7fc00001,
      0xffc00002,
      0x80000000,
      0,
      0xff800000,
      0x7f800000,
      Float.floatToRawIntBits(-1),
      Float.floatToRawIntBits(1)
    };
    var time =
        LocalDateTime.of(
            slot % 2 == 0 ? 1969 : 2024,
            12,
            31,
            23,
            59,
            59,
            slot % 3 == 0 ? 999999999 : slot % 3 == 1 ? 999000001 : 999000000);
    boolean missing = slot >= 8;
    return Row.of(
        id,
        id % 5 == 0 ? null : 1,
        missing ? null : Float.intBitsToFloat(floats[slot]),
        missing ? null : Double.longBitsToDouble(doubles[slot]),
        missing ? null : time,
        missing ? null : time.toInstant(ZoneOffset.UTC));
  }
}
