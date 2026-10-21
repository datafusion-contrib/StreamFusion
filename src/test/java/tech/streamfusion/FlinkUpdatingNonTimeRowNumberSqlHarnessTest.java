package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkUpdatingNonTimeRowNumberSqlHarnessTest {
  static String order(String type) {
    switch (type) {
      case "IEEE":
        return "CASE WHEN v=3 THEN CAST('NaN' AS DOUBLE) WHEN v=5 THEN CAST('-0.0' AS DOUBLE) WHEN"
            + " v=6 THEN CAST('Infinity' AS DOUBLE) ELSE CAST(v AS DOUBLE) END";
      case "BINARY":
        return "CASE WHEN v IS NULL THEN CAST(NULL AS VARBINARY) WHEN MOD(v,3)=0 THEN X'FF' WHEN"
            + " MOD(v,3)=1 THEN X'00' ELSE X'' END";
      case "MONTH":
        return "CAST(v AS INT) * INTERVAL '1' MONTH";
      case "MILLIS":
        return "v * INTERVAL '0.001' SECOND";
      default:
        return "CAST(v AS " + type + ")";
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"BIGINT", "FLOAT", "DOUBLE", "IEEE", "STRING", "BINARY", "MONTH", "MILLIS"})
  void signedInputKeepsEveryRowIdentityAndOriginalInsertKind(String type) throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    for (int batch : new int[] {1, 7}) {
      for (String nulls : new String[] {"FIRST", "LAST"}) {
        try (var metrics = new RecoveryExecutionMetrics()) {
          String query =
              "SELECT k,v,ROW_NUMBER() OVER(PARTITION BY k ORDER BY "
                  + order(type)
                  + " ASC NULLS "
                  + nulls
                  + ") FROM src";
          NativeParity.assertOrderedKindedParity(
              () -> {
                var table = FlinkLegacyUpdatingWindowSqlHarnessTest.environment(metrics);
                table.getConfig().set("streamfusion.transpose.batchRows", Integer.toString(batch));
                return table;
              },
              query);
          RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeColumnarTopN");
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"BIGINT", "FLOAT", "DOUBLE", "IEEE", "BINARY", "MONTH", "MILLIS"})
  void twoRestoresPreserveRetractionsAndSignedArrivalOrder(String type) throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 24; i++) rows.add(Row.of(i % 3, (long) (i % 7)));
    for (int i = 0; i < 24; i++) {
      rows.add(Row.ofKind(RowKind.UPDATE_BEFORE, i % 3, (long) (i % 7)));
      rows.add(Row.ofKind(RowKind.UPDATE_AFTER, i % 3, (long) (i % 5)));
    }
    for (int i = 0; i < 24; i++) rows.add(Row.ofKind(RowKind.DELETE, i % 3, (long) (i % 5)));
    for (String backend :
        new String[] {"hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"}) {
      try (var recovery =
          new PortableSqlRecovery(
                  backend,
                  rows,
                  Types.ROW_NAMED(new String[] {"k", "v"}, Types.INT, Types.LONG),
                  Schema.newBuilder()
                      .column("k", DataTypes.INT())
                      .column("v", DataTypes.BIGINT())
                      .build(),
                  true,
                  24,
                  63)
              .withExecutionMetrics()) {
        var result =
            NativeFailureParity.runRecovery(
                recovery::uninterrupted,
                recovery,
                "SELECT k,v,ROW_NUMBER() OVER(PARTITION BY k ORDER BY "
                    + order(type)
                    + " ASC NULLS FIRST) FROM recovery_input");
        result.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(result.host().rows(), result.nativeRun().rows());
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeColumnarTopN");
      }
    }
  }
}
