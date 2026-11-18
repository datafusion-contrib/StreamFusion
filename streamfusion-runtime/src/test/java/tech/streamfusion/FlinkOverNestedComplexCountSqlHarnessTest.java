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

class FlinkOverNestedComplexCountSqlHarnessTest {
  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> cases() {
    return java.util.stream.Stream.of(
            "hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")
        .flatMap(
            backend ->
                java.util.stream.Stream.of(false, true)
                    .flatMap(
                        multiset ->
                            java.util.stream.Stream.of(
                                    "rt,ROWS UNBOUNDED PRECEDING",
                                    "pt,ROWS UNBOUNDED PRECEDING",
                                    "pt,RANGE UNBOUNDED PRECEDING",
                                    "pt,ROWS BETWEEN 3 PRECEDING AND CURRENT ROW")
                                .map(
                                    shape -> {
                                      String[] parts = shape.split(",", 2);
                                      return org.junit.jupiter.params.provider.Arguments.of(
                                          backend, multiset, parts[0], parts[1]);
                                    })));
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.MethodSource("cases")
  void nestedNullableMapAndMultisetKeysSurviveRepeatedRecovery(
      String backend, boolean multisetOnly, String order, String frame) throws Exception {
    var array =
        DataTypes.ARRAY(
            multisetOnly
                ? DataTypes.MULTISET(DataTypes.STRING())
                : DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()));
    var struct = DataTypes.ROW(DataTypes.FIELD("nested", DataTypes.MULTISET(DataTypes.STRING())));
    var type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("a", array),
            DataTypes.FIELD("r", struct),
            DataTypes.FIELD("rt", DataTypes.TIMESTAMP(3)));
    var schema =
        Schema.newBuilder()
            .fromRowDataType(type)
            .watermark("rt", "SOURCE_WATERMARK()")
            .columnByExpression("pt", "PROCTIME()")
            .build();
    var rows = new ArrayList<Row>();
    for (long i = 0; i < 120; i++) {
      var map = new LinkedHashMap<String, Long>();
      map.put(null, i);
      map.put("nil", null);
      var multiset = new LinkedHashMap<String, Integer>();
      multiset.put(null, 2);
      multiset.put("present", 1);
      Object[] values = i % 3 == 0 ? new Map[0] : new Map[] {null, multisetOnly ? multiset : map};
      rows.add(
          Row.of(
              i,
              i % 7 == 0 ? null : values,
              i % 7 == 0 ? null : Row.of(multiset),
              java.time.LocalDateTime.of(1970, 1, 1, 0, 0)
                  .plusNanos((1000 + i / 12 * 100) * 1000000)));
    }
    try (var recovery =
        new PortableSqlRecovery(
                backend, rows, ExternalTypeInfo.<Row>of(type), schema, false, 40, 80)
            .withExecutionMetrics()
            .withWatermarks(Map.of(24, 1100L, 60, 1400L, 96, 1700L, 120, 9999L))) {
      var result =
          NativeFailureParity.runRecovery(
              recovery::uninterrupted,
              recovery::get,
              "SELECT id, a, r, COUNT(a) OVER w, COUNT(r) OVER w FROM recovery_input "
                  + "WINDOW w AS (ORDER BY "
                  + order
                  + " "
                  + frame
                  + ")");
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertFalse(result.host().rows().isEmpty());
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeOverAggregate");
      SharedFlinkCluster.assertNativeMemoryReleased("nested-over-count-recovery");
    }
  }
}
