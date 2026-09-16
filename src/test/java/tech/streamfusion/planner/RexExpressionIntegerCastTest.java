package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.Native;

class RexExpressionIntegerCastTest {
  private final JavaTypeFactoryImpl types = new JavaTypeFactoryImpl(FlinkTypeSystem.INSTANCE);
  private final RexBuilder rex = new RexBuilder(types);

  @Test
  void nativePairsHaveNoJvmNodesOrUdfRegistrations() {
    for (boolean nullable : new boolean[] {false, true}) {
      for (SqlTypeName source : List.of(SqlTypeName.CHAR, SqlTypeName.VARCHAR)) {
        RexNode cast = cast(source, SqlTypeName.INTEGER, 0, nullable);
        assertNative(RexExpression.encode(cast, TableConfig.getDefault()), 30);
        assertNull(RexExpression.encode(cast), "unknown cast mode must not become default mode");
      }
      for (int length : new int[] {1, 2, 11, Integer.MAX_VALUE}) {
        RexNode cast = cast(SqlTypeName.INTEGER, SqlTypeName.VARCHAR, length, nullable);
        RexExpression encoded = RexExpression.encode(cast, TableConfig.getDefault());
        assertNative(encoded, 31);
        assertEquals(length, encoded.payload()[0]);
      }
    }
  }

  @Test
  void sqlPlannerRemovesBothUpcallsFromTheIngestionPipeline() {
    var table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    table.executeSql("CREATE TABLE inputs (s STRING, v INT) WITH ('connector' = 'datagen')");
    NativePlanner.install(table);
    var config = (CalciteConfig) table.getConfig().getPlannerConfig();
    List<Integer> kinds = new ArrayList<>();
    config
        .getStreamProgram()
        .get()
        .addLast(
            "verify-integer-cast-nodes",
            (root, context) -> {
              collectKinds(root, kinds);
              return root;
            });
    table.explainSql(
        "SELECT CAST(s AS INT), CAST(v AS STRING), "
            + "LPAD(CAST(CAST(SUBSTR(s, 11, 2) AS INT) / 15 * 15 AS VARCHAR), 2, '0') FROM inputs");
    assertTrue(kinds.contains(30), kinds.toString());
    assertTrue(kinds.contains(31), kinds.toString());
    assertFalse(kinds.contains(17), "SQL-planned casts still contain a JVM upcall: " + kinds);
  }

  private static void collectKinds(RelNode node, List<Integer> kinds) {
    if (node instanceof StreamPhysicalNativeCalc || node instanceof StreamPhysicalNativeFilter) {
      try {
        var field = node.getClass().getDeclaredField("kinds");
        field.setAccessible(true);
        for (int kind : (int[]) field.get(node)) kinds.add(kind);
      } catch (ReflectiveOperationException failure) {
        throw new AssertionError(failure);
      }
    }
    node.getInputs().forEach(input -> collectKinds(input, kinds));
  }

  @Test
  void otherPairsKeepHostCastsAndLegacyKeepsFallback() {
    for (SqlTypeName source :
        List.of(
            SqlTypeName.BIGINT,
            SqlTypeName.SMALLINT,
            SqlTypeName.TINYINT,
            SqlTypeName.DOUBLE,
            SqlTypeName.DECIMAL)) {
      RexExpression encoded =
          RexExpression.encode(
              cast(source, SqlTypeName.VARCHAR, 20, true), TableConfig.getDefault());
      assertNotNull(encoded);
      assertEquals(17, encoded.kinds()[0]);
    }
    assertEquals(
        17,
        RexExpression.encode(
                cast(SqlTypeName.INTEGER, SqlTypeName.CHAR, 6, true), TableConfig.getDefault())
            .kinds()[0]);
    TableConfig legacy = TableConfig.getDefault();
    legacy.getConfiguration().setString("table.exec.legacy-cast-behaviour", "ENABLED");
    for (RexNode cast :
        List.of(
            cast(SqlTypeName.VARCHAR, SqlTypeName.INTEGER, 0, true),
            cast(SqlTypeName.INTEGER, SqlTypeName.VARCHAR, 2, true))) {
      assertNull(RexExpression.encode(cast, legacy));
    }
  }

  @Test
  void fallibleParseRequiresRowShortCircuitButFormattingDoesNot() {
    RexNode parsed = cast(SqlTypeName.VARCHAR, SqlTypeName.INTEGER, 0, true);
    RexNode positive =
        rex.makeCall(
            SqlStdOperatorTable.GREATER_THAN, parsed, rex.makeExactLiteral(BigDecimal.ZERO));
    RexNode guard = rex.makeInputRef(types.createSqlType(SqlTypeName.BOOLEAN), 1);
    for (var op : List.of(SqlStdOperatorTable.AND, SqlStdOperatorTable.OR)) {
      assertNull(RexExpression.encode(rex.makeCall(op, guard, positive), TableConfig.getDefault()));
      RexNode formatted = cast(SqlTypeName.INTEGER, SqlTypeName.VARCHAR, 11, true);
      assertNotNull(
          RexExpression.encode(
              rex.makeCall(op, guard, rex.makeCall(SqlStdOperatorTable.IS_NOT_NULL, formatted)),
              TableConfig.getDefault()));
    }
  }

  @Test
  void nestedCastsExecuteWithoutAnyHostBinding() {
    RexNode parsed = cast(SqlTypeName.VARCHAR, SqlTypeName.INTEGER, 0, true);
    RexNode formatted = rex.makeCast(types.createSqlType(SqlTypeName.VARCHAR, 2), parsed);
    RexExpression encoded = RexExpression.encode(formatted, TableConfig.getDefault());
    assertNative(encoded, 31);
    assertTrue(Arrays.stream(encoded.kinds()).anyMatch(kind -> kind == 30));
    assertEquals(
        Arrays.asList("42", "-1", null),
        evaluate(encoded, Arrays.asList("42.9", "-123", null), false));
  }

  @Test
  void coalesceDoesNotHideCastFailures() {
    RexNode parsed = cast(SqlTypeName.VARCHAR, SqlTypeName.INTEGER, 0, true);
    RexNode fallback = rex.makeInputRef(types.createSqlType(SqlTypeName.INTEGER), 1);
    assertNull(
        RexExpression.encode(
            rex.makeCall(SqlStdOperatorTable.COALESCE, fallback, parsed),
            TableConfig.getDefault()));
  }

  @Test
  void releasedFlinkOracleMatchesNativeBatchesWithoutBindingAnyJvmUdf() {
    HostCastFunction oracle = new HostCastFunction(VarCharType.STRING_TYPE, new IntType());
    List<String> candidates =
        new ArrayList<>(
            Arrays.asList(
                null,
                "0",
                "-0",
                " +00042 ",
                "12.9",
                "-12.9",
                ".5",
                ".",
                "+.",
                "-.",
                "1.",
                "2147483647",
                "-2147483648",
                "2147483647.999",
                "-2147483648.999",
                "0".repeat(1024) + "1"));
    Random random = new Random(124);
    for (int i = 0; i < 2000; i++) {
      int value = random.nextInt();
      candidates.add(
          (i % 3 == 0 ? " " : "")
              + (value >= 0 && i % 2 == 0 ? "+" : "")
              + value
              + (i % 4 == 0 ? ".12345678901234567890" : "")
              + (i % 3 == 0 ? " " : ""));
    }
    List<Object> expected = candidates.stream().map(oracle::eval).toList();
    RexExpression encoded =
        RexExpression.encode(
            cast(SqlTypeName.VARCHAR, SqlTypeName.INTEGER, 0, true), TableConfig.getDefault());
    assertNative(encoded, 30);
    assertEquals(expected, evaluate(encoded, candidates, false));
    assertEquals(List.of(), evaluate(encoded, List.of(), false));
    assertEquals(Arrays.asList(null, null), evaluate(encoded, Arrays.asList(null, null), false));
  }

  @Test
  void malformedInputsFailLikeTheReleasedFlinkCast() {
    HostCastFunction oracle = new HostCastFunction(VarCharType.STRING_TYPE, new IntType());
    RexExpression encoded =
        RexExpression.encode(
            cast(SqlTypeName.VARCHAR, SqlTypeName.INTEGER, 0, true), TableConfig.getDefault());
    for (String value :
        List.of(
            "",
            " ",
            "+",
            "-",
            "1e2",
            "1.2.3",
            "1.x",
            "--1",
            "1 2",
            "\t42\n",
            "\r42",
            "\u000042",
            "\u00a042\u00a0",
            "\u300042",
            "\uff11\uff12",
            "2147483648",
            "-2147483649",
            "9".repeat(100),
            "2147483648x")) {
      RuntimeException host = assertThrows(RuntimeException.class, () -> oracle.eval(value));
      Throwable cause = host;
      while (cause.getCause() != null) cause = cause.getCause();
      String message = cause.getMessage();
      String reason = message.substring(message.lastIndexOf("'. ") + 3);
      RuntimeException nativeFailure =
          assertThrows(
              RuntimeException.class, () -> evaluate(encoded, List.of(value), false), value);
      assertTrue(nativeFailure.getMessage().contains(reason), nativeFailure::getMessage);
    }
  }

  @Test
  void formattingAndVarcharTruncationMatchFlink() {
    List<Integer> values =
        new ArrayList<>(Arrays.asList(null, 0, -1, 1, -123, Integer.MIN_VALUE, Integer.MAX_VALUE));
    Random random = new Random(124);
    for (int i = 0; i < 2000; i++) values.add(random.nextInt());
    for (int length : new int[] {1, 2, 10, 11, Integer.MAX_VALUE}) {
      HostCastFunction oracle = new HostCastFunction(new IntType(), new VarCharType(length));
      RexExpression encoded =
          RexExpression.encode(
              cast(SqlTypeName.INTEGER, SqlTypeName.VARCHAR, length, true),
              TableConfig.getDefault());
      assertNative(encoded, 31);
      assertEquals(values.stream().map(oracle::eval).toList(), evaluate(encoded, values, true));
      assertEquals(List.of(), evaluate(encoded, List.of(), true));
    }
  }

  private RexNode cast(SqlTypeName from, SqlTypeName to, int length, boolean nullable) {
    RelDataType source =
        from == SqlTypeName.CHAR || from == SqlTypeName.VARCHAR
            ? types.createSqlType(from, 100)
            : from == SqlTypeName.DECIMAL
                ? types.createSqlType(from, 10, 2)
                : types.createSqlType(from);
    RelDataType target =
        to == SqlTypeName.CHAR || to == SqlTypeName.VARCHAR
            ? types.createSqlType(to, length)
            : types.createSqlType(to);
    return rex.makeCast(
        types.createTypeWithNullability(target, nullable),
        rex.makeInputRef(types.createTypeWithNullability(source, nullable), 0));
  }

  private static void assertNative(RexExpression encoded, int kind) {
    assertNotNull(encoded);
    assertEquals(kind, encoded.kinds()[0]);
    assertFalse(Arrays.stream(encoded.kinds()).anyMatch(value -> value == 17));
    long[] literals = encoded.longs();
    var binding = encoded.udfBinding();
    try {
      assertSame(literals, binding.bind(literals), "a nonempty binding patches the UDF ids");
    } finally {
      binding.unbind();
    }
  }

  private static List<Object> evaluate(RexExpression encoded, List<?> values, boolean integers) {
    // Deliberately do not bind/register any JVM UDF: the encoded tree must execute on its own.
    long calc =
        Native.createCalcExpression(
            encoded.kinds(),
            encoded.payload(),
            encoded.childCounts(),
            encoded.longs(),
            encoded.doubles(),
            encoded.strings(),
            new int[] {0},
            -1,
            new String[] {"cast"});
    try (RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot input =
            VectorSchemaRoot.of(
                integers ? new IntVector("v", allocator) : new VarCharVector("v", allocator));
        ArrowArray inArray = ArrowArray.allocateNew(allocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      input.allocateNew();
      for (int row = 0; row < values.size(); row++) {
        Object value = values.get(row);
        if (value == null) input.getVector(0).setNull(row);
        else if (integers) ((IntVector) input.getVector(0)).setSafe(row, (Integer) value);
        else
          ((VarCharVector) input.getVector(0))
              .setSafe(row, value.toString().getBytes(StandardCharsets.UTF_8));
      }
      input.setRowCount(values.size());
      Data.exportVectorSchemaRoot(allocator, input, null, inArray, inSchema);
      Native.calcExpression(
          calc,
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      try (VectorSchemaRoot output =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, null)) {
        List<Object> rows = new ArrayList<>();
        for (int row = 0; row < output.getRowCount(); row++) {
          Object value = output.getVector(0).getObject(row);
          rows.add(value instanceof Text ? value.toString() : value);
        }
        return rows;
      }
    } finally {
      Native.closeCalcExpression(calc);
    }
  }
}
