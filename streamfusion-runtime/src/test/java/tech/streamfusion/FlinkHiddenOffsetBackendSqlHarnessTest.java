package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.table.api.*;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

class FlinkHiddenOffsetBackendSqlHarnessTest {
  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            rocks ->
                Stream.of(false, true)
                    .flatMap(
                        projected ->
                            Stream.of(false, true)
                                .flatMap(
                                    recovery ->
                                        Stream.of(false, true)
                                            .map(
                                                ties ->
                                                    Arguments.of(
                                                        rocks, projected, recovery, ties)))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void rowKindEqualityFollowsSelectedBackendAcrossRestores(
      boolean rocks, boolean projected, boolean recovering, boolean ties) throws Exception {
    var type =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
            DataTypes.FIELD("k", DataTypes.INT().notNull()),
            DataTypes.FIELD("score", DataTypes.BIGINT()),
            DataTypes.FIELD("p", DataTypes.INT()));
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 32; i++) rows.add(event(i, 0, RowKind.INSERT, ties));
    for (int i = 0; i < 16; i++) {
      rows.add(event(i, 0, RowKind.UPDATE_BEFORE, ties));
      rows.add(event(i, 100, RowKind.UPDATE_AFTER, ties));
    }
    for (int i = 16; i < 24; i++) rows.add(event(i, 0, RowKind.DELETE, ties));
    for (int i = 16; i < 24; i++) rows.add(event(i, 200, RowKind.INSERT, ties));
    try (var recovery =
        new PortableSqlRecovery(
                rocks ? "tech.streamfusion.state.RocksDBNativeStateBackendFactory" : "hashmap",
                rows,
                ExternalTypeInfo.<Row>of(type),
                Schema.newBuilder().fromRowDataType(type).primaryKey("k", "id").build(),
                true,
                24,
                56)
            .withExecutionMetrics()) {
      String query =
          projected
              ? "SELECT id,k,score,p,rn FROM (SELECT *,ROW_NUMBER() OVER (ORDER BY score DESC,id"
                  + " ASC) AS rn FROM recovery_input) WHERE rn>3 AND rn<=10"
              : "SELECT id,k,score,p FROM recovery_input ORDER BY score DESC,id ASC LIMIT 7 OFFSET"
                  + " 3";
      if (ties) query = query.replace("score DESC,id ASC", "score DESC");
      var runs =
          recovering
              ? NativeFailureParity.runRecovery(recovery::uninterrupted, recovery::get, query)
              : NativeFailureParity.run(recovery::uninterrupted, query);
      runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
      if (recovering) recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeColumnarTopN");
      SharedFlinkCluster.assertNativeMemoryReleased("hidden-offset-backend-copies");
    }
  }

  private static Row event(int id, int revision, RowKind kind, boolean ties) {
    return Row.ofKind(
        kind,
        (long) id,
        id % 4,
        (long) (id * 17 % (ties ? 7 : 37) + revision),
        (id + revision) % 13 == 0 ? null : id + revision);
  }
}
