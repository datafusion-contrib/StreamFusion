package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkArrayScalarUdfSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "int",
        "bigint",
        "decimal",
        "string",
        "boolean",
        "tinyint",
        "smallint",
        "date",
        "time",
        "bytes",
        "nested",
        "float",
        "double",
        "timestamp",
        "ltz"
      })
  void nullableArraysAndTheirConsumersKeepTypesValuesAndNativeWork(String type) {
    for (String sql :
        new String[] {
          "SELECT id, array_identity(p) FROM src",
          "SELECT id, CARDINALITY(array_identity(p)) FROM src",
          "SELECT id, array_identity(array_identity(p)) FROM src",
          "SELECT id, CASE WHEN MOD(id,2)=0 THEN array_identity(p) ELSE p END FROM src",
          "SELECT id, array_identity(p) FROM src WHERE array_identity(p) IS NOT NULL"
        }) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs = NativeFailureParity.run(() -> environment(type, 1031, metrics), sql);
        if (type.equals("bytes")
            && org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.18")
            && sql.contains("CASE WHEN")) {
          runs.assertFailure(
              org.codehaus.commons.compiler.CompileException.class,
              "'{' expected instead of '['",
              NativeFailureParity.Phase.INITIALIZATION,
              NativeFailureParity.Route.FALLBACK);
          continue;
        }
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
      }
    }
  }

  @Test
  void scalarResultsFromArraysAndArrayResultsFromScalarsCompose() {
    for (String sql :
        new String[] {
          "SELECT id, array_size(p) FROM src",
          "SELECT id, array_result(id) FROM src",
          "SELECT id, CARDINALITY(array_result(id)) FROM src",
          "SELECT id, array_size(array_result(id)) FROM src"
        }) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs = NativeFailureParity.run(() -> environment("int", 1031, metrics), sql);
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
      }
    }
  }

  @Test
  void mutableResultsSharedCallsAndRetainedArgumentsKeepReleasedOwnership() throws Exception {
    for (String sql :
        new String[] {
          "SELECT id, array_reuse(p), array_reuse(p) FROM src",
          "SELECT array_reuse(p), array_reuse(p) FROM src WHERE array_reuse(p) IS NOT NULL",
          "SELECT id, array_previous(p) FROM src"
        }) {
      NativeParity.assertOrderedKindedParity(() -> environment("int", 1031, null), sql);
    }
  }

  @Test
  void unselectedBranchesAndAllFilteredBatchesDoNotInvokeFailingArrays() {
    for (int count : new int[] {0, 1031}) {
      for (String sql :
          new String[] {
            "SELECT array_identity(p) FROM src",
            "SELECT CASE WHEN id>=0 THEN p ELSE array_failure(p) END FROM src",
            "SELECT array_failure(p) FROM src WHERE id<0"
          }) {
        NativeFailureParity.run(() -> environment("int", count, null), sql)
            .assertSuccess(NativeFailureParity.Route.NATIVE);
      }
    }
  }

  @Test
  void callbackFailureKeepsItsJavaCause() {
    NativeFailureParity.run(() -> environment("int", 32, null), "SELECT array_failure(p) FROM src")
        .assertFailure(
            IllegalStateException.class,
            "array sentinel",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @ValueSource(strings = {"int", "string", "nested"})
  void repeatedRecoveryRetainsArraysAndNullableElements(String type) {
    DataType rowType = rowType(type);
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 48; i++) rows.add(Row.of(i, value(type, i)));
    for (String backend :
        new String[] {"hashmap", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"}) {
      try (var recovery =
          new PortableSqlRecovery(
                  backend,
                  rows,
                  ExternalTypeInfo.<Row>of(rowType),
                  Schema.newBuilder().fromRowDataType(rowType).build(),
                  false,
                  12,
                  32)
              .withExecutionMetrics()) {
        var runs =
            NativeFailureParity.runRecovery(
                () -> register(recovery.uninterrupted(), type),
                () -> register(recovery.get(), type),
                "SELECT id,array_identity(p),CARDINALITY(array_identity(p)) FROM recovery_input");
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeCalc");
      }
    }
  }

  @Test
  void customArrayInferenceKeepsExplicitFallback() {
    var runs =
        NativeFailureParity.run(
            () -> {
              var table = environment("int", 32, null);
              table.createTemporarySystemFunction("array_custom", new CustomArray());
              return table;
            },
            "SELECT array_custom(p) FROM src");
    runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
    assertTrue(
        runs.nativeRun().fallbackReasons().stream()
            .anyMatch(reason -> reason.contains("custom type inference")),
        runs::toString);
  }

  @Test
  void primitiveAndRowArrayResultsKeepExplicitFallback() {
    for (boolean primitive : new boolean[] {true, false}) {
      var runs =
          NativeFailureParity.run(
              () -> {
                var table = environment("int", 32, null);
                table.createTemporarySystemFunction(
                    "unverified_array", primitive ? new PrimitiveResult() : new RowResult());
                return table;
              },
              "SELECT unverified_array(id) FROM src");
      runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
    }
  }

  public static class PrimitiveResult extends ScalarFunction {
    public int[] eval(Integer value) {
      return new int[] {value};
    }
  }

  @FunctionHint(input = @DataTypeHint("INT"), output = @DataTypeHint("ARRAY<ROW<n INT>>"))
  public static class RowResult extends ScalarFunction {
    public Row[] eval(Integer value) {
      return new Row[] {Row.of(value)};
    }
  }

  static TableEnvironment environment(String type, int count, RecoveryExecutionMetrics metrics) {
    var env =
        metrics == null
            ? StreamExecutionEnvironment.getExecutionEnvironment()
            : metrics.environment(new Configuration(), 1, 0);
    env.setParallelism(1);
    env.getConfig().enableObjectReuse();
    var table = StreamTableEnvironment.create(env);
    DataType rowType = rowType(type);
    table.createTemporaryView(
        "src",
        table.fromDataStream(
            env.fromSequence(0, Math.max(0, count - 1))
                .filter(i -> i < count)
                .map(i -> Row.of(i.intValue(), value(type, i.intValue())))
                .returns(ExternalTypeInfo.<Row>of(rowType)),
            Schema.newBuilder().fromRowDataType(rowType).build()));
    return register(table, type);
  }

  private static TableEnvironment register(TableEnvironment table, String type) {
    table.getConfig().set("streamfusion.transpose.batchRows", "5");
    ScalarFunction identity;
    switch (type) {
      case "float":
        identity = new FloatIdentity();
        break;
      case "double":
        identity = new DoubleIdentity();
        break;
      case "timestamp":
        identity = new LocalDateTimeIdentity();
        break;
      case "ltz":
        identity = new InstantIdentity();
        break;
      case "bigint":
        identity = new LongIdentity();
        break;
      case "decimal":
        identity = new DecimalIdentity();
        break;
      case "string":
        identity = new StringIdentity();
        break;
      case "boolean":
        identity = new BooleanIdentity();
        break;
      case "tinyint":
        identity = new ByteIdentity();
        break;
      case "smallint":
        identity = new ShortIdentity();
        break;
      case "date":
        identity = new DateIdentity();
        break;
      case "time":
        identity = new TimeIdentity();
        break;
      case "bytes":
        identity = new BytesIdentity();
        break;
      case "nested":
        identity = new NestedIdentity();
        break;
      default:
        identity = new IntIdentity();
        break;
    }
    table.createTemporarySystemFunction("array_identity", identity);
    table.createTemporarySystemFunction("array_size", new ArraySize());
    table.createTemporarySystemFunction("array_result", new ArrayResult());
    table.createTemporarySystemFunction("array_reuse", new Reuse());
    table.createTemporarySystemFunction("array_previous", new Previous());
    table.createTemporarySystemFunction("array_failure", new Failure());
    return table;
  }

  static DataType rowType(String type) {
    DataType element;
    switch (type) {
      case "float":
        element = DataTypes.FLOAT();
        break;
      case "double":
        element = DataTypes.DOUBLE();
        break;
      case "timestamp":
        element = DataTypes.TIMESTAMP(9);
        break;
      case "ltz":
        element = DataTypes.TIMESTAMP_LTZ(9);
        break;
      case "bigint":
        element = DataTypes.BIGINT();
        break;
      case "decimal":
        element = DataTypes.DECIMAL(38, 18);
        break;
      case "string":
        element = DataTypes.STRING();
        break;
      case "boolean":
        element = DataTypes.BOOLEAN();
        break;
      case "tinyint":
        element = DataTypes.TINYINT();
        break;
      case "smallint":
        element = DataTypes.SMALLINT();
        break;
      case "date":
        element = DataTypes.DATE();
        break;
      case "time":
        element = DataTypes.TIME(0);
        break;
      case "bytes":
        element = DataTypes.BYTES();
        break;
      case "nested":
        element = DataTypes.ARRAY(DataTypes.INT());
        break;
      default:
        element = DataTypes.INT();
        break;
    }
    return DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("p", DataTypes.ARRAY(element)));
  }

  static Object value(String type, int id) {
    if (id % 8 == 7) return null;
    int size = id % 8 == 0 ? 0 : 3;
    Object[] result;
    switch (type) {
      case "float":
        result = new Float[size];
        break;
      case "double":
        result = new Double[size];
        break;
      case "timestamp":
        result = new LocalDateTime[size];
        break;
      case "ltz":
        result = new Instant[size];
        break;
      case "bigint":
        result = new Long[size];
        break;
      case "decimal":
        result = new BigDecimal[size];
        break;
      case "string":
        result = new String[size];
        break;
      case "boolean":
        result = new Boolean[size];
        break;
      case "tinyint":
        result = new Byte[size];
        break;
      case "smallint":
        result = new Short[size];
        break;
      case "date":
        result = new LocalDate[size];
        break;
      case "time":
        result = new LocalTime[size];
        break;
      case "bytes":
        result = new byte[size][];
        break;
      case "nested":
        result = new Integer[size][];
        break;
      default:
        result = new Integer[size];
        break;
    }
    for (int i = 0; i < size; i++) {
      if (i == 1) continue;
      switch (type) {
        case "float":
          result[i] = i == 0 ? Float.NaN : -0.0f;
          break;
        case "double":
          result[i] = i == 0 ? Double.POSITIVE_INFINITY : -0.0d;
          break;
        case "timestamp":
          result[i] =
              LocalDateTime.parse(
                  i == 0 ? "0001-01-01T00:00:00.123456789" : "1969-12-31T23:59:59.999999999");
          break;
        case "ltz":
          result[i] =
              Instant.parse(
                  i == 0 ? "9999-12-31T23:59:59.123456789Z" : "1969-12-31T23:59:59.999999999Z");
          break;
        case "bigint":
          result[i] = i == 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
          break;
        case "decimal":
          result[i] = new BigDecimal("-99999999999999999999.123456789012345678");
          break;
        case "string":
          result[i] = i == 0 ? "x\ud800" : "z".repeat(4097);
          break;
        case "boolean":
          result[i] = i == 0;
          break;
        case "tinyint":
          result[i] = i == 0 ? Byte.MIN_VALUE : Byte.MAX_VALUE;
          break;
        case "smallint":
          result[i] = i == 0 ? Short.MIN_VALUE : Short.MAX_VALUE;
          break;
        case "date":
          result[i] = LocalDate.ofEpochDay(i == 0 ? -1 : 19782);
          break;
        case "time":
          result[i] = LocalTime.ofNanoOfDay(i == 0 ? 0 : 86_399_999_000_000L);
          break;
        case "bytes":
          result[i] = i == 0 ? new byte[0] : new byte[] {0, -1, 1};
          break;
        case "nested":
          result[i] = i == 0 ? new Integer[0] : new Integer[] {id, null};
          break;
        default:
          result[i] = i == 0 ? Integer.MIN_VALUE : id;
          break;
      }
    }
    return result;
  }

  public static class IntIdentity extends ScalarFunction {
    public Integer[] eval(Integer[] value) {
      return value;
    }
  }

  public static class FloatIdentity extends ScalarFunction {
    public Float[] eval(Float[] value) {
      return value;
    }
  }

  public static class DoubleIdentity extends ScalarFunction {
    public Double[] eval(Double[] value) {
      return value;
    }
  }

  public static class LocalDateTimeIdentity extends ScalarFunction {
    public LocalDateTime[] eval(LocalDateTime[] value) {
      return value;
    }
  }

  public static class InstantIdentity extends ScalarFunction {
    public Instant[] eval(Instant[] value) {
      return value;
    }
  }

  public static class LongIdentity extends ScalarFunction {
    public Long[] eval(Long[] value) {
      return value;
    }
  }

  @FunctionHint(
      input = @DataTypeHint("ARRAY<DECIMAL(38,18)>"),
      output = @DataTypeHint("ARRAY<DECIMAL(38,18)>"))
  public static class DecimalIdentity extends ScalarFunction {
    public BigDecimal[] eval(BigDecimal[] value) {
      return value;
    }
  }

  public static class StringIdentity extends ScalarFunction {
    public String[] eval(String[] value) {
      return value;
    }
  }

  public static class BooleanIdentity extends ScalarFunction {
    public Boolean[] eval(Boolean[] value) {
      return value;
    }
  }

  public static class ByteIdentity extends ScalarFunction {
    public Byte[] eval(Byte[] value) {
      return value;
    }
  }

  public static class ShortIdentity extends ScalarFunction {
    public Short[] eval(Short[] value) {
      return value;
    }
  }

  public static class DateIdentity extends ScalarFunction {
    public LocalDate[] eval(LocalDate[] value) {
      return value;
    }
  }

  @FunctionHint(input = @DataTypeHint("ARRAY<TIME(0)>"), output = @DataTypeHint("ARRAY<TIME(0)>"))
  public static class TimeIdentity extends ScalarFunction {
    public LocalTime[] eval(LocalTime[] value) {
      return value;
    }
  }

  public static class BytesIdentity extends ScalarFunction {
    public byte[][] eval(byte[][] value) {
      return value;
    }
  }

  public static class NestedIdentity extends ScalarFunction {
    public Integer[][] eval(Integer[][] value) {
      return value;
    }
  }

  public static class ArraySize extends ScalarFunction {
    public Integer eval(Integer[] value) {
      return value == null ? null : value.length;
    }
  }

  public static class ArrayResult extends ScalarFunction {
    public Integer[] eval(Integer value) {
      return value == null || value % 8 == 7 ? null : new Integer[] {value, null};
    }
  }

  public static class Reuse extends FlinkDateTimeUdfSqlHarnessTest.Lifecycle {
    private final Integer[] result = new Integer[2];

    public Integer[] eval(Integer[] value) {
      checkOpen();
      result[0] = ++calls;
      result[1] = value == null ? null : value.length;
      return result;
    }
  }

  public static class Previous extends ScalarFunction {
    private transient Integer[] previous;

    public Integer[] eval(Integer[] value) {
      var result = previous;
      previous = value;
      return result;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  public static class Failure extends ScalarFunction {
    public Integer[] eval(Integer[] value) {
      throw new IllegalStateException("array sentinel");
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  public static class CustomArray extends IntIdentity {
    @Override
    public org.apache.flink.table.types.inference.TypeInference getTypeInference(
        org.apache.flink.table.catalog.DataTypeFactory factory) {
      return super.getTypeInference(factory);
    }
  }
}
