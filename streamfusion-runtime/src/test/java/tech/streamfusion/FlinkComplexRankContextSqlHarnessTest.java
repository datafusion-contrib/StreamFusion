package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.compat.FlinkTestSources;

class FlinkComplexRankContextSqlHarnessTest {
  @ParameterizedTest
  @CsvSource({"false,1,DEFAULT", "true,1,DEFAULT", "false,1,LAST", "true,3,FIRST", "true,3,LAST"})
  void defaultPlacementGlobalRanksAndShuffleKeepTheDeclaredContract(
      boolean partitioned, int parallelism, String nulls) {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    try (var metrics = new RecoveryExecutionMetrics()) {
      var result =
          NativeFailureParity.run(
              () -> {
                var env =
                    metrics.environment(
                        new org.apache.flink.configuration.Configuration(), parallelism, 0);
                var table = StreamTableEnvironment.create(env);
                table.getConfig().set("streamfusion.transpose.batchRows", "7");
                var rows = new java.util.ArrayList<Row>();
                for (int i = 0; i < 60; i++)
                  rows.add(
                      Row.of(
                          i % 11 == 0 ? null : i % 4,
                          i % 7 == 0
                              ? null
                              : new Long[] {i % 3 == 0 ? null : (long) (60 - i) % 9, null},
                          i));
                var input =
                    FlinkTestSources.fromData(
                        env,
                        Types.ROW_NAMED(
                            new String[] {"k", "v", "id"},
                            Types.INT,
                            Types.OBJECT_ARRAY(Types.LONG),
                            Types.INT),
                        rows.toArray(new Row[0]));
                input.setParallelism(1);
                table.createTemporaryView(
                    "src",
                    table.fromDataStream(
                        input,
                        Schema.newBuilder()
                            .column("k", DataTypes.INT())
                            .column("v", DataTypes.ARRAY(DataTypes.BIGINT()))
                            .column("id", DataTypes.INT())
                            .build()));
                return table;
              },
              "SELECT k, v, id, ROW_NUMBER() OVER ("
                  + (partitioned ? "PARTITION BY k " : "")
                  + "ORDER BY v ASC"
                  + (nulls.equals("DEFAULT") ? "" : " NULLS " + nulls)
                  + ") AS rn FROM src");
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
      RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeColumnarTopN");
    }
  }
}
