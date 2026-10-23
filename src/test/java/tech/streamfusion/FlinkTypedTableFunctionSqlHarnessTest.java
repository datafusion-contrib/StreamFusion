package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkTypedTableFunctionSqlHarnessTest {
  static Stream<Arguments> typesAndZones() {
    var cases = new ArrayList<Arguments>();
    for (String type :
        List.of(
            "CHAR(1)",
            "CHAR(4)",
            "BOOLEAN",
            "TINYINT",
            "SMALLINT",
            "FLOAT",
            "DOUBLE",
            "VARBINARY",
            "BINARY(1)",
            "BINARY(4)",
            "BINARY(128)",
            "DAY_TIME",
            "YEAR_MONTH",
            "DECIMAL(18,8)",
            "DECIMAL(38,18)",
            "DATE",
            "TIME")) {
      cases.add(Arguments.of(type, "UTC"));
    }
    for (int precision : new int[] {0, 3, 6, 9}) {
      for (String zone : List.of("UTC", "Asia/Shanghai", "America/Los_Angeles")) {
        cases.add(Arguments.of("TIMESTAMP(" + precision + ")", zone));
        cases.add(Arguments.of("TIMESTAMP_LTZ(" + precision + ")", zone));
      }
    }
    return cases.stream();
  }

  @ParameterizedTest(name = "{0}, {1}")
  @MethodSource("typesAndZones")
  void typedFanoutRetainsNullsPrecisionAndMutableOutput(String name, String zone) {
    for (String join :
        List.of(
            ", LATERAL TABLE(typed_rows(s.id, s.v)) AS t(v, pos)",
            " LEFT JOIN LATERAL TABLE(typed_rows(s.id, s.v)) AS t(v, pos) ON TRUE",
            ", LATERAL TABLE(typed_rows(s.id, s.v)) AS t(v, pos) WHERE t.pos = 1 AND t.v IS NOT"
                + " NULL")) {
      var comparison =
          NativeFailureParity.run(
              () -> environment(name, zone, 1031, null),
              "SELECT s.id, s.v, t.v, t.pos, t.v FROM src s" + join);
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
      assertTrue(
          comparison.nativeRun().plan().contains("NativeTableFunction"),
          comparison.nativeRun().plan());
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "CHAR(4)",
        "BOOLEAN",
        "TINYINT",
        "SMALLINT",
        "FLOAT",
        "DOUBLE",
        "VARBINARY",
        "BINARY(4)",
        "DAY_TIME",
        "YEAR_MONTH",
        "DECIMAL(18,8)",
        "DECIMAL(38,18)",
        "DATE",
        "TIME",
        "TIMESTAMP(9)",
        "TIMESTAMP_LTZ(9)"
      })
  void allRowKindsAndRepeatedRecoveryRetainTypedRows(String name) throws Exception {
    DataType valueType = valueType(name);
    DataType rowType = rowType(valueType);
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 96; i++) rows.add(Row.of(i, value(name, i)));
    for (int i = 0; i < 32; i++) {
      rows.add(Row.ofKind(RowKind.UPDATE_BEFORE, i, value(name, i)));
      rows.add(Row.ofKind(RowKind.UPDATE_AFTER, i, value(name, i + 2)));
    }
    for (int i = 0; i < 32; i++) rows.add(Row.ofKind(RowKind.DELETE, i, value(name, i + 2)));
    for (String backend :
        List.of("hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory")) {
      try (var recovery =
          new PortableSqlRecovery(
                  backend,
                  rows,
                  ExternalTypeInfo.<Row>of(rowType),
                  Schema.newBuilder().fromRowDataType(rowType).build(),
                  true,
                  32,
                  168)
              .withExecutionMetrics()) {
        var comparison =
            NativeFailureParity.runRecovery(
                () -> register(recovery.uninterrupted(), valueType),
                () -> register(recovery.get(), valueType),
                "SELECT MOD(s.id, 3), COUNT(t.v), COUNT(*) FROM recovery_input s LEFT JOIN LATERAL"
                    + " TABLE(typed_rows(s.id, s.v)) AS t(v, pos) ON TRUE GROUP BY MOD(s.id, 3)");
        comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeTableFunction");
        RecoveryExecutionMetrics.assertConsumed(
            recovery.nativeExecution(), "NativeColumnarGroupAggregate");
        SharedFlinkCluster.assertNativeMemoryReleased("typed-table-function-recovery");
      }
    }
  }

  @Test
  void explicitlyDisabledTableFunctionsRetainCompleteQueryFallback() throws Exception {
    NativeParity.assertFallbackReasonContains(
        () -> {
          var table = environment("DECIMAL(38,18)", "UTC", 32, null);
          table.getConfig().set("streamfusion.operator.tableFunction.enabled", "false");
          return table;
        },
        "SELECT t.v FROM src s, LATERAL TABLE(typed_rows(s.id, s.v)) AS t(v, pos)",
        "tableFunction");
  }

  @Test
  void releasedTimePrecisionMismatchRemainsAHostFailure() {
    var comparison =
        NativeFailureParity.run(
            () -> {
              var table = environment("TIME", "UTC", 32, null);
              table.createTemporarySystemFunction(
                  "time_three_rows", new TypedRows(DataTypes.TIME(3)));
              return table;
            },
            "SELECT t.v FROM src s, LATERAL TABLE(time_three_rows(s.id, s.v)) AS t(v, pos)");
    assertNotNull(comparison.host().failure());
    assertNotNull(comparison.nativeRun().failure());
    assertEquals(
        comparison.host().rootCause().getClass(), comparison.nativeRun().rootCause().getClass());
    assertEquals(
        comparison.host().rootCause().getMessage(),
        comparison.nativeRun().rootCause().getMessage());
    assertTrue(comparison.host().rootCause().getMessage().contains("TIME(3)"));
  }

  @Test
  void decimalResultConversionRetainsReleasedRoundingAndOverflow() {
    for (DataType output : List.of(DataTypes.DECIMAL(5, 2), DataTypes.DECIMAL(38, 38))) {
      var comparison =
          NativeFailureParity.run(
              () -> {
                var table = environment("DECIMAL(38,18)", "UTC", 1031, null);
                table.createTemporarySystemFunction(
                    "converted_rows", new TypedRows(valueType("DECIMAL(38,18)"), output));
                return table;
              },
              "SELECT s.id, t.v, t.pos FROM src s, "
                  + "LATERAL TABLE(converted_rows(s.id, s.v)) AS t(v, pos)");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
    }
  }

  @Test
  void emptyInputAndFilteredInvocationsDoNotCallTheFunction() {
    for (int rows : new int[] {0, 32}) {
      var comparison =
          NativeFailureParity.run(
              () -> environment("DECIMAL(38,18)", "UTC", rows, null),
              "SELECT t.v FROM (SELECT * FROM src WHERE id < 0) s, "
                  + "LATERAL TABLE(typed_rows(s.id, s.v)) AS t(v, pos)");
      comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
      assertTrue(comparison.nativeRun().rows().isEmpty());
    }
  }

  @Test
  void annotatedCharArgumentsRetainCastWidthsAndLeftPadding() {
    for (String join :
        List.of(
            ", LATERAL TABLE(character_rows(CAST(s.v AS CHAR(4)))) AS t(v)",
            " LEFT JOIN LATERAL TABLE(character_rows(CAST(s.v AS CHAR(4)))) AS t(v) ON TRUE")) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var comparison =
            NativeFailureParity.run(
                () -> {
                  var table = environment("CHAR(4)", "UTC", 1031, metrics);
                  table.createTemporarySystemFunction("character_rows", new CharacterRows());
                  return table;
                },
                "SELECT s.id, s.v, t.v, CHAR_LENGTH(t.v) FROM src s" + join);
        comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(comparison.host().resultTypes(), comparison.nativeRun().resultTypes());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
      }
    }
  }

  @Test
  void fixedBinaryArgumentsAndResultsKeepActualBytes() {
    for (String join :
        List.of(
            ", LATERAL TABLE(binary_rows(s.v)) AS t(v)",
            " LEFT JOIN LATERAL TABLE(binary_rows(s.v)) AS t(v) ON TRUE")) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var comparison =
            NativeFailureParity.run(
                () -> {
                  var table = environment("BINARY(4)", "UTC", 1031, metrics);
                  table.createTemporarySystemFunction("binary_rows", new BinaryRows());
                  return table;
                },
                "SELECT s.id, s.v, t.v, t.v FROM src s" + join);
        comparison.assertSuccess(NativeFailureParity.Route.NATIVE);
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeTableFunction");
      }
    }
  }

  @Test
  void fixedBinaryRetainsFallbackWithoutTheVariableByteTransport() {
    String nested = "streamfusion.experimental.nestedScalarUdfs";
    String binary = "streamfusion.experimental.binaryElt";
    String previousNested = System.getProperty(nested);
    String previousBinary = System.getProperty(binary);
    try {
      System.setProperty(nested, "false");
      System.setProperty(binary, "false");
      var comparison =
          NativeFailureParity.run(
              () -> environment("BINARY(4)", "UTC", 32, null),
              "SELECT t.v FROM src s, LATERAL TABLE(typed_rows(s.id, s.v)) AS t(v, pos)");
      comparison.assertSuccess(NativeFailureParity.Route.FALLBACK);
    } finally {
      if (previousNested == null) System.clearProperty(nested);
      else System.setProperty(nested, previousNested);
      if (previousBinary == null) System.clearProperty(binary);
      else System.setProperty(binary, previousBinary);
    }
  }

  @FunctionHint(input = @DataTypeHint("BINARY(4)"), output = @DataTypeHint("BINARY(4)"))
  public static class BinaryRows extends TableFunction<byte[]> {
    public void eval(byte[] value) {
      if (value == null) return;
      byte[] reused = value.clone();
      collect(reused);
      java.util.Arrays.fill(reused, (byte) 0x7f);
      collect(reused);
      java.util.Arrays.fill(reused, (byte) 0);
    }
  }

  @FunctionHint(input = @DataTypeHint("CHAR(4)"), output = @DataTypeHint("CHAR(4)"))
  public static class CharacterRows extends TableFunction<String> {
    public void eval(String value) {
      if (value != null) collect(value);
    }
  }

  static StreamTableEnvironment environment(
      String name, String zone, int rows, RecoveryExecutionMetrics metrics) {
    var env =
        metrics == null
            ? StreamExecutionEnvironment.getExecutionEnvironment()
            : metrics.environment(new Configuration(), 1, 0);
    env.setParallelism(1);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneId.of(zone));
    table.getConfig().set("streamfusion.transpose.batchRows", "127");
    DataType type = rowType(valueType(name));
    Object[] values =
        java.util.stream.IntStream.range(0, 8).mapToObj(i -> value(name, i)).toArray();
    var input =
        env.fromSequence(0, Math.max(0, rows - 1))
            .filter(i -> i < rows)
            .map(i -> Row.of(i.intValue(), values[(int) ((i / 4) % values.length)]))
            .returns(ExternalTypeInfo.<Row>of(type));
    table.createTemporaryView(
        "src", table.fromDataStream(input, Schema.newBuilder().fromRowDataType(type).build()));
    register(table, valueType(name));
    return table;
  }

  private static TableEnvironment register(TableEnvironment table, DataType type) {
    table.createTemporarySystemFunction("typed_rows", new TypedRows(type));
    return table;
  }

  private static DataType rowType(DataType type) {
    return DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("v", type));
  }

  static DataType valueType(String name) {
    if (name.startsWith("BINARY("))
      return DataTypes.BINARY(Integer.parseInt(name.substring(7, name.length() - 1)));
    if (name.startsWith("CHAR("))
      return DataTypes.CHAR(Integer.parseInt(name.substring(5, name.length() - 1)));
    if (name.startsWith("TIMESTAMP_LTZ("))
      return DataTypes.TIMESTAMP_LTZ(Integer.parseInt(name.substring(14, name.length() - 1)));
    if (name.startsWith("TIMESTAMP("))
      return DataTypes.TIMESTAMP(Integer.parseInt(name.substring(10, name.length() - 1)));
    switch (name) {
      case "STRING":
        return DataTypes.STRING();
      case "FLOAT":
        return DataTypes.FLOAT();
      case "DOUBLE":
        return DataTypes.DOUBLE();
      case "VARBINARY":
        return DataTypes.BYTES();
      case "DAY_TIME":
        return DataTypes.INTERVAL(DataTypes.SECOND(3));
      case "YEAR_MONTH":
        return DataTypes.INTERVAL(DataTypes.MONTH());
      case "BOOLEAN":
        return DataTypes.BOOLEAN();
      case "TINYINT":
        return DataTypes.TINYINT();
      case "SMALLINT":
        return DataTypes.SMALLINT();
      case "DECIMAL(18,8)":
        return DataTypes.DECIMAL(18, 8);
      case "DECIMAL(38,18)":
        return DataTypes.DECIMAL(38, 18);
      case "DATE":
        return DataTypes.DATE();
      case "TIME":
        return DataTypes.TIME(0);
      default:
        throw new IllegalArgumentException("Unknown type " + name);
    }
  }

  static Object value(String name, int index) {
    int slot = index % 8;
    if (slot == 7) return null;
    if (name.startsWith("BINARY(")) return value("VARBINARY", index);
    if (name.startsWith("CHAR("))
      return new String[] {"", "a", "a ", "abcd", "abcde", "  ", "z"}[slot];
    switch (name) {
      case "STRING":
        return new String[] {"", "é🙂", "a", "a ", "\ud800", "\udc00", "x\ud800y"}[slot];
      case "FLOAT":
        return new float[] {
              Float.NEGATIVE_INFINITY,
              -0.0f,
              0.0f,
              Float.MIN_VALUE,
              Float.MAX_VALUE,
              Float.POSITIVE_INFINITY,
              Float.intBitsToFloat(0x7fc00001)
            }
            [slot];
      case "DOUBLE":
        return new double[] {
              Double.NEGATIVE_INFINITY,
              -0.0d,
              0.0d,
              Double.MIN_VALUE,
              Double.MAX_VALUE,
              Double.POSITIVE_INFINITY,
              Double.longBitsToDouble(0x7ff8000000000001L)
            }
            [slot];
      case "VARBINARY":
        return new byte[][] {
              new byte[0],
              {0},
              {(byte) 0xff},
              {0, (byte) 0xff, 1},
              new byte[4097],
              {1, 0, 2},
              {(byte) 0x80, 0, 127}
            }
            [slot];
      case "DAY_TIME":
        return Duration.ofMillis(new long[] {-86_400_001, -1234, -1, 0, 1, 1234, 86_400_001}[slot]);
      case "YEAR_MONTH":
        return Period.ofMonths(new int[] {-12001, -12, -1, 0, 1, 12, 12001}[slot]);
      case "BOOLEAN":
        return slot % 2 == 0;
      case "TINYINT":
        return new byte[] {Byte.MIN_VALUE, -1, 0, 1, Byte.MAX_VALUE, 2, -2}[slot];
      case "SMALLINT":
        return new short[] {Short.MIN_VALUE, -1, 0, 1, Short.MAX_VALUE, 2, -2}[slot];
      case "DECIMAL(18,8)":
        return new BigDecimal(
            new String[] {
                  "-9999999999.99999999",
                  "0.00000001",
                  "-0.00000001",
                  "0.00000000",
                  "9999999999.99999999",
                  "1.23456789",
                  "-1.23456789"
                }
                [slot]);
      case "DECIMAL(38,18)":
        return new BigDecimal(
            new String[] {
                  "-99999999999999999999.999999999999999999", "0.000000000000000001",
                  "-0.000000000000000001", "0.000000000000000000",
                  "99999999999999999999.999999999999999999", "1.234567890123456789",
                  "-1.234567890123456789"
                }
                [slot]);
      case "DATE":
        return LocalDate.ofEpochDay(new int[] {-719162, -1, 0, 1, 2932896, 20000, -20000}[slot]);
      case "TIME":
        return LocalTime.ofNanoOfDay(
            new long[] {
                  0,
                  1_000_000,
                  999_000_000,
                  43_200_123_000_000L,
                  86_399_999_000_000L,
                  1_234_000_000,
                  2_345_000_000L
                }
                [slot]);
      default:
        String[] timestamps = {
          "0001-01-01T00:00:00.123456789",
          "1969-12-31T23:59:59.999999999",
          "1970-01-01T00:00:00.000000001",
          "2026-03-08T02:30:00.123456789",
          "2026-11-01T01:30:00.987654321",
          "9999-12-31T23:59:59.999999999",
          "2026-03-08T03:30:00.999999999"
        };
        if (name.startsWith("TIMESTAMP_LTZ")) {
          timestamps =
              new String[] {
                "0001-01-01T00:00:00.123456789",
                "1969-12-31T23:59:59.999999999",
                "2026-03-08T09:59:59.999999999",
                "2026-03-08T10:00:00.000000001",
                "2026-11-01T08:30:00.123456789",
                "2026-11-01T09:30:00.123456789",
                "9999-12-31T23:59:59.999999999"
              };
        }
        var timestamp = LocalDateTime.parse(timestamps[slot]);
        int precision = Integer.parseInt(name.substring(name.indexOf('(') + 1, name.length() - 1));
        int unit = (int) Math.pow(10, 9 - precision);
        timestamp = timestamp.withNano(timestamp.getNano() / unit * unit);
        return name.startsWith("TIMESTAMP_LTZ") ? timestamp.toInstant(ZoneOffset.UTC) : timestamp;
    }
  }

  public static class TypedRows extends TableFunction<Row> {
    private final DataType type;
    private final DataType outputType;
    private transient Row result;

    public TypedRows(DataType type) {
      this(type, type);
    }

    public TypedRows(DataType type, DataType outputType) {
      this.type = type;
      this.outputType = outputType;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory factory) {
      var inference =
          TypeInference.newBuilder()
              .outputTypeStrategy(
                  TypeStrategies.explicit(
                      DataTypes.ROW(
                          DataTypes.FIELD("v", outputType),
                          DataTypes.FIELD("pos", DataTypes.INT()))));
      switch (type.getLogicalType().getTypeRoot()) {
        case INTERVAL_DAY_TIME:
        case INTERVAL_YEAR_MONTH:
          return inference
              .inputTypeStrategy(
                  InputTypeStrategies.sequence(
                      InputTypeStrategies.explicit(DataTypes.INT()), InputTypeStrategies.ANY))
              .build();
        default:
          return inference.typedArguments(DataTypes.INT(), type).build();
      }
    }

    @Override
    public void open(FunctionContext context) {
      if (result != null) throw new IllegalStateException("typed table function opened twice");
      result = new Row(2);
    }

    public void eval(Integer mode, Object value) {
      if (result == null) throw new IllegalStateException("typed table function is not open");
      if (mode == null || mode % 4 == 0) return;
      for (int position = 0; position < 2; position++) {
        result.setField(0, position == 0 && mode % 4 == 1 ? null : value);
        result.setField(1, position);
        collect(result);
      }
      result.setField(0, null);
      result.setField(1, -1);
    }

    @Override
    public void close() {
      if (result == null) throw new IllegalStateException("typed table function closed twice");
      result = null;
    }
  }
}
