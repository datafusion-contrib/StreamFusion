package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.compat.FlinkTestSources;

class FlinkNonTimeComplexRankSqlHarnessTest {
  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> cases() {
    return java.util.stream.Stream.of("array", "row", "nested_array", "row_array")
        .flatMap(
            type ->
                java.util.stream.Stream.of("FIRST", "LAST")
                    .flatMap(
                        nulls ->
                            java.util.stream.Stream.of(1, 7)
                                .map(
                                    batch ->
                                        org.junit.jupiter.params.provider.Arguments.of(
                                            type, nulls, batch))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void orderedChangelogPreservesParentsAndChildren(String type, String nulls, int batch) {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    var result =
        NativeFailureParity.run(
            () -> environment(type, batch),
            "SELECT k, v, id, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v ASC NULLS "
                + nulls
                + ") AS rn FROM src");
    result.assertSuccess(NativeFailureParity.Route.NATIVE);
    assertEquals(result.host().resultTypes(), result.nativeRun().resultTypes());
    assertEquals(result.host().rows(), result.nativeRun().rows());
  }

  private static StreamTableEnvironment environment(String type, int batch) {
    var spec = spec(type);
    var info = spec.info;
    var dataType = spec.dataType;
    var payloads = spec.payloads;
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("streamfusion.transpose.batchRows", Integer.toString(batch));
    var rows = new java.util.ArrayList<Row>();
    for (int i = 0; i < payloads.length; i++)
      rows.add(Row.of(i % 5 == 0 ? null : i % 2, payloads[i], i));
    var source =
        FlinkTestSources.fromData(
            env,
            Types.ROW_NAMED(new String[] {"k", "v", "id"}, Types.INT, info, Types.INT),
            rows.toArray(new Row[0]));
    table.createTemporaryView(
        "src",
        table.fromDataStream(
            source,
            Schema.newBuilder()
                .column("k", DataTypes.INT())
                .column("v", dataType)
                .column("id", DataTypes.INT())
                .build()));
    return table;
  }

  static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> recoveryCases() {
    return java.util.stream.Stream.of("array", "row", "nested_array", "row_array")
        .flatMap(
            type ->
                java.util.stream.Stream.of("FIRST", "LAST")
                    .flatMap(
                        nulls ->
                            java.util.stream.Stream.of(
                                    "hashmap",
                                    "tech.streamfusion.state.RocksDBNativeStateBackendFactory")
                                .map(
                                    backend ->
                                        org.junit.jupiter.params.provider.Arguments.of(
                                            type, nulls, backend))));
  }

  @ParameterizedTest
  @MethodSource("recoveryCases")
  void parentFlagAndTieOrderSurviveTwoRestores(String type, String nulls, String backend)
      throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    var spec = spec(type);
    var rows = new java.util.ArrayList<Row>();
    for (int i = 0; i < 96; i++)
      rows.add(
          Row.of(i % 11 == 0 ? null : i % 3, spec.payloads[(95 - i) % spec.payloads.length], i));
    var types = Types.ROW_NAMED(new String[] {"k", "v", "id"}, Types.INT, spec.info, Types.INT);
    var schema =
        Schema.newBuilder()
            .column("k", DataTypes.INT())
            .column("v", spec.dataType)
            .column("id", DataTypes.INT())
            .build();
    try (var recovery =
        new PortableSqlRecovery(backend, rows, types, schema, false, 24, 63)
            .withExecutionMetrics()) {
      java.util.function.UnaryOperator<org.apache.flink.table.api.TableEnvironment> config =
          table -> {
            table.getConfig().set("streamfusion.transpose.batchRows", "7");
            return table;
          };
      var result =
          NativeFailureParity.runRecovery(
              () -> config.apply(recovery.uninterrupted()),
              () -> config.apply(recovery.get()),
              "SELECT k, v, id, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v ASC NULLS "
                  + nulls
                  + ") AS rn FROM recovery_input");
      result.assertSuccess(NativeFailureParity.Route.NATIVE);
      recovery.verifyRepeatedRecovery();
      RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeColumnarTopN");
      SharedFlinkCluster.assertNativeMemoryReleased("non-time-complex-rank-recovery");
    }
  }

  private static final class Spec {
    final org.apache.flink.api.common.typeinfo.TypeInformation<?> info;
    final org.apache.flink.table.types.DataType dataType;
    final Object[] payloads;

    Spec(
        org.apache.flink.api.common.typeinfo.TypeInformation<?> info,
        org.apache.flink.table.types.DataType dataType,
        Object[] payloads) {
      this.info = info;
      this.dataType = dataType;
      this.payloads = payloads;
    }
  }

  private static Spec spec(String type) {
    org.apache.flink.api.common.typeinfo.TypeInformation<?> info;
    org.apache.flink.table.types.DataType dataType;
    Object[] payloads;
    switch (type) {
      case "array":
        info = Types.OBJECT_ARRAY(Types.LONG);
        dataType = DataTypes.ARRAY(DataTypes.BIGINT());
        payloads =
            new Object[] {
              new Long[] {2L},
              new Long[] {1L},
              new Long[] {null},
              new Long[0],
              null,
              new Long[] {1L, null},
              new Long[] {1L, 0L},
              new Long[] {1L},
              new Long[] {-1L},
              new Long[] {Long.MIN_VALUE},
              new Long[] {Long.MAX_VALUE}
            };
        break;
      case "row":
        info = Types.ROW_NAMED(new String[] {"a", "b"}, Types.LONG, Types.STRING);
        dataType =
            DataTypes.ROW(
                DataTypes.FIELD("a", DataTypes.BIGINT()), DataTypes.FIELD("b", DataTypes.STRING()));
        payloads =
            new Object[] {
              Row.of(2L, "b"),
              Row.of(1L, "a"),
              Row.of(null, "a"),
              Row.of(null, null),
              null,
              Row.of(1L, null),
              Row.of(1L, ""),
              Row.of(1L, "a"),
              Row.of(-1L, "é🚀")
            };
        break;
      case "nested_array":
        info = Types.OBJECT_ARRAY(Types.OBJECT_ARRAY(Types.LONG));
        dataType = DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.BIGINT()));
        payloads =
            new Object[] {
              new Long[][] {{2L}},
              new Long[][] {{1L}},
              new Long[][] {null},
              new Long[0][],
              null,
              new Long[][] {{1L, null}},
              new Long[][] {{1L, 0L}},
              new Long[][] {{1L}},
              new Long[][] {{}},
              new Long[][] {{null}}
            };
        break;
      case "row_array":
        info = Types.ROW_NAMED(new String[] {"a", "b"}, Types.OBJECT_ARRAY(Types.LONG), Types.LONG);
        dataType =
            DataTypes.ROW(
                DataTypes.FIELD("a", DataTypes.ARRAY(DataTypes.BIGINT())),
                DataTypes.FIELD("b", DataTypes.BIGINT()));
        payloads =
            new Object[] {
              Row.of(new Long[] {2L}, 1L),
              Row.of(new Long[] {1L}, 1L),
              Row.of(null, 1L),
              Row.of(null, null),
              null,
              Row.of(new Long[] {1L, null}, null),
              Row.of(new Long[] {1L, 0L}, 1L),
              Row.of(new Long[] {1L}, 1L),
              Row.of(new Long[0], -1L)
            };
        break;
      default:
        throw new IllegalArgumentException(type);
    }
    return new Spec(info, dataType, payloads);
  }
}
