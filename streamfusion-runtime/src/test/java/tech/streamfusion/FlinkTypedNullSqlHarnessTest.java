package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativePlanner;

class FlinkTypedNullSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "BOOLEAN",
        "TINYINT",
        "SMALLINT",
        "INT",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "STRING",
        "VARCHAR(3)",
        "CHAR(3)",
        "BYTES",
        "BINARY(3)",
        "DECIMAL(10,2)",
        "DECIMAL(38,18)",
        "DATE",
        "TIME(3)",
        "TIMESTAMP(9)",
        "TIMESTAMP_LTZ(9)",
        "ARRAY<INT>",
        "ARRAY<DECIMAL(38,18) NOT NULL>",
        "MAP<STRING, ARRAY<BIGINT>>",
        "ROW<`odd name` INT NOT NULL, nested ARRAY<MAP<STRING, DECIMAL(10,2)>>>",
        "ARRAY<ROW<t TIMESTAMP(9), z TIMESTAMP_LTZ(9)>>",
        "MULTISET<STRING>"
      })
  void foldedNullProjectionMatchesFlink(String type) throws Exception {
    String sql = "SELECT id + 1, CAST(NULL AS " + type + ") FROM inputs";
    assertTrue(NativePlanner.explain(inputs(), sql).contains("NativeCalc"));
    NativeParity.assertParity(FlinkTypedNullSqlHarnessTest::inputs, sql);
  }

  @Test
  void nullMapSearchFoldsWithoutDisablingOtherCollectionExpressions() throws Exception {
    NativeParity.assertParity(
        FlinkTypedNullSqlHarnessTest::collections,
        "SELECT id + 1, arr[1], m['a'], m[CAST(NULL AS STRING)], "
            + "am['a'], am[CAST(NULL AS STRING)], arr[id + 1], "
            + "m[CASE WHEN id = 0 THEN 'a' ELSE 'missing' END] FROM inputs");
  }

  @Test
  void nullLiteralComposesWithCaseAndDecimalArithmetic() throws Exception {
    NativeParity.assertParity(
        FlinkTypedNullSqlHarnessTest::inputs,
        "SELECT id + 1, CASE WHEN id < 2 THEN CAST(NULL AS DECIMAL(12,2)) "
            + "ELSE CAST(id AS DECIMAL(12,2)) END, "
            + "CAST(NULL AS BIGINT) + id, CAST(NULL AS BOOLEAN) OR id = 1 FROM inputs");
  }

  @Test
  void nestedNullLiteralsSpanBatchesAndPartialFinalBatch() throws Exception {
    NativeParity.assertParity(
        () -> inputs(8201),
        "SELECT id + 1, CAST(NULL AS MAP<STRING, ARRAY<DECIMAL(38,18)>>), "
            + "CAST(NULL AS ROW<t TIMESTAMP(9), z TIMESTAMP_LTZ(9)>) FROM inputs");
  }

  private static TableEnvironment inputs() {
    return inputs(5);
  }

  private static TableEnvironment inputs(int count) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    Row[] rows = new Row[count];
    for (int i = 0; i < count; i++) rows[i] = Row.of(i);
    tables.createTemporaryView(
        "inputs",
        fromData(env, Types.ROW_NAMED(new String[] {"id"}, Types.INT), rows),
        Schema.newBuilder().column("id", DataTypes.INT()).build());
    return tables;
  }

  private static TableEnvironment collections() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tables = StreamTableEnvironment.create(env);
    tables.createTemporaryView(
        "inputs",
        fromData(
            env,
            Types.ROW_NAMED(
                new String[] {"id", "arr", "m", "am"},
                Types.INT,
                Types.OBJECT_ARRAY(Types.INT),
                Types.MAP(Types.STRING, Types.INT),
                Types.MAP(Types.STRING, Types.OBJECT_ARRAY(Types.INT))),
            Row.of(
                0,
                new Integer[] {10, null},
                java.util.Map.of("a", 17),
                java.util.Map.of("a", new Integer[] {1, null})),
            Row.of(1, new Integer[0], java.util.Map.of(), java.util.Map.of()),
            Row.of(2, null, null, null)),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("arr", DataTypes.ARRAY(DataTypes.INT()))
            .column("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
            .column("am", DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT())))
            .build());
    return tables;
  }
}
