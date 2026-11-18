package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkOverMultisetCountSqlHarnessTest {
  private static final org.apache.flink.table.types.DataType TYPE =
      DataTypes.ROW(
          DataTypes.FIELD("id", DataTypes.BIGINT()),
          DataTypes.FIELD("k", DataTypes.INT()),
          DataTypes.FIELD("v", DataTypes.MULTISET(DataTypes.STRING())),
          DataTypes.FIELD("rt", DataTypes.TIMESTAMP(3)));
  private static final Schema SCHEMA =
      Schema.newBuilder()
          .fromRowDataType(TYPE)
          .watermark("rt", "SOURCE_WATERMARK()")
          .columnByExpression("pt", "PROCTIME()")
          .build();

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> recoveryCases() {
    return java.util.stream.Stream.of(
            "rt,ROWS UNBOUNDED PRECEDING",
            "rt,RANGE UNBOUNDED PRECEDING",
            "rt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW",
            "rt,RANGE BETWEEN INTERVAL '0.3' SECOND PRECEDING AND CURRENT ROW",
            "pt,ROWS UNBOUNDED PRECEDING",
            "pt,RANGE UNBOUNDED PRECEDING",
            "pt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW")
        .flatMap(
            shape ->
                java.util.stream.Stream.of(
                        "hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")
                    .map(
                        backend -> {
                          String[] parts = shape.split(",", 2);
                          return org.junit.jupiter.params.provider.Arguments.of(
                              parts[0], parts[1], backend);
                        }));
  }

  @ParameterizedTest
  @MethodSource("recoveryCases")
  void countsAndOriginalMultisetsSurviveRepeatedRecovery(String order, String frame, String backend)
      throws Exception {
    try (var recovery = recovery(backend).withExecutionMetrics()) {
      var result =
          NativeFailureParity.runRecovery(
              recovery::uninterrupted,
              recovery::get,
              "SELECT id, k, v, COUNT(v) OVER w, COUNT(*) OVER w FROM recovery_input "
                  + "WINDOW w AS (PARTITION BY k ORDER BY "
                  + order
                  + " "
                  + frame
                  + ")");
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
      assertFalse(result.host().rows().isEmpty());
      for (var row : result.host().rows())
        if (Integer.valueOf(2).equals(row.get(2))) assertEquals(0L, row.get(4));
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeOverAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("multiset-over-count-recovery");
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "ROWS UNBOUNDED PRECEDING",
        "RANGE UNBOUNDED PRECEDING",
        "ROWS BETWEEN 3 PRECEDING AND CURRENT ROW"
      })
  void proctimeCountsAndCarriesOriginalBags(String frame) {
    try (var source = recovery("hashmap")) {
      String query =
          "SELECT id, k, v, COUNT(v) OVER (PARTITION BY k ORDER BY pt "
              + frame
              + ") FROM recovery_input";
      var result = NativeFailureParity.run(source::uninterrupted, query);
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
    }
  }

  private static PortableSqlRecovery recovery(String backend) {
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 120; i++) {
      var values = new LinkedHashMap<String, Integer>();
      if (i % 3 != 0) {
        values.put(null, 3);
        values.put("value", 1 + i % 5);
      }
      rows.add(
          Row.of(
              (long) i,
              i % 11 == 0 ? null : i % 4,
              i % 7 == 0 || i % 4 == 2 ? null : values,
              java.time.LocalDateTime.of(1970, 1, 1, 0, 0)
                  .plusNanos((1000L + i / 12 * 100) * 1000000)));
    }
    return new PortableSqlRecovery(
            backend, rows, ExternalTypeInfo.<Row>of(TYPE), SCHEMA, false, 40, 80)
        .withWatermarks(Map.of(24, 1100L, 60, 1400L, 96, 1700L, 120, 9999L));
  }
}
