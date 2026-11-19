package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkDateTimeUdfSqlHarnessTest {
  @ParameterizedTest
  @ValueSource(strings = {"DATE", "TIME"})
  void argumentsResultsAndConsumersRetainReleasedConversions(String type) {
    for (String expression :
        new String[] {
          "identity_value(v)",
          "observe_value(v)",
          "result_value(id)",
          "observe_value(result_value(id))",
          "identity_value(v) IS NULL",
          "CAST(identity_value(v) AS STRING)"
        }) {
      try (var metrics = new RecoveryExecutionMetrics()) {
        var runs =
            NativeFailureParity.run(
                () -> environment(type, 1031, metrics), "SELECT id," + expression + " FROM src");
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        assertEquals(1031, runs.nativeRun().rows().size());
        RecoveryExecutionMetrics.assertConsumed(metrics.nativeOperators(), "NativeCalc");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"DATE", "TIME"})
  void selectedBranchesEmptyInputAndFilteredBatchesSkipFailures(String type) {
    for (int count : new int[] {0, 1031}) {
      for (String sql :
          new String[] {
            "SELECT identity_value(v) FROM src",
            "SELECT CASE WHEN id >= 0 THEN identity_value(v) ELSE fail_value(id) END FROM src",
            "SELECT fail_value(id) FROM src WHERE id < 0"
          }) {
        var runs = NativeFailureParity.run(() -> environment(type, count, null), sql);
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"DATE", "TIME"})
  void callbackFailuresKeepTheirOriginalCause(String type) {
    NativeFailureParity.run(() -> environment(type, 32, null), "SELECT fail_value(id) FROM src")
        .assertFailure(
            IllegalStateException.class,
            "date-time sentinel",
            NativeFailureParity.Phase.ROW_EVALUATION,
            NativeFailureParity.Route.NATIVE);
  }

  @ParameterizedTest
  @ValueSource(strings = {"DATE", "TIME"})
  void sharedCallsKeepOneLifecycleAndRowMajorState(String type) throws Exception {
    NativeParity.assertOrderedKindedParity(
        () -> environment(type, 1031, null),
        "SELECT identity_value(v), identity_value(v) FROM src WHERE identity_value(v) IS NOT NULL");
  }

  @ParameterizedTest
  @ValueSource(strings = {"DATE", "TIME"})
  void repeatedRecoveryRestoresTypedBoundaries(String type) {
    var rowType =
        org.apache.flink.table.api.DataTypes.ROW(
            org.apache.flink.table.api.DataTypes.FIELD(
                "id", org.apache.flink.table.api.DataTypes.INT()),
            org.apache.flink.table.api.DataTypes.FIELD(
                "v", FlinkTypedTableFunctionSqlHarnessTest.valueType(type)));
    var rows = new ArrayList<Row>();
    for (int i = 0; i < 48; i++)
      rows.add(Row.of(i, FlinkTypedTableFunctionSqlHarnessTest.value(type, i)));
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
                "SELECT id, v, observe_value(v), result_value(id) FROM recovery_input");
        runs.assertSuccess(NativeFailureParity.Route.NATIVE);
        assertEquals(runs.host().resultTypes(), runs.nativeRun().resultTypes());
        recovery.verifyRepeatedRecovery();
        RecoveryExecutionMetrics.assertConsumed(recovery.nativeExecution(), "NativeCalc");
      }
    }
  }

  @Test
  void alternateDateResultConversionKeepsFallback() {
    var runs =
        NativeFailureParity.run(
            () -> {
              var table = environment("DATE", 32, null);
              table.createTemporarySystemFunction("legacy_date", new LegacyDate());
              return table;
            },
            "SELECT legacy_date(id) FROM src");
    runs.assertSuccess(NativeFailureParity.Route.FALLBACK);
    assertTrue(
        runs.nativeRun().fallbackReasons().stream()
            .anyMatch(reason -> reason.contains("unsupported Java conversion class")),
        runs::toString);
  }

  static TableEnvironment environment(String type, int rows, RecoveryExecutionMetrics metrics) {
    return register(
        FlinkTypedTableFunctionSqlHarnessTest.environment(type, "UTC", rows, metrics), type);
  }

  private static TableEnvironment register(TableEnvironment table, String type) {
    boolean date = type.equals("DATE");
    table.getConfig().set("streamfusion.transpose.batchRows", "5");
    table.createTemporarySystemFunction(
        "identity_value", date ? new DateIdentity() : new TimeIdentity());
    table.createTemporarySystemFunction("observe_value", date ? new DatePart() : new TimePart());
    table.createTemporarySystemFunction("result_value", date ? new DateResult() : new TimeResult());
    table.createTemporarySystemFunction("fail_value", date ? new DateFailure() : new TimeFailure());
    return table;
  }

  public static class Lifecycle extends ScalarFunction {
    private transient boolean opened;
    protected transient int calls;

    @Override
    public void open(FunctionContext context) {
      if (opened) throw new IllegalStateException("opened twice");
      opened = true;
      calls = 0;
    }

    protected void checkOpen() {
      if (!opened) throw new IllegalStateException("function is not open");
    }

    @Override
    public void close() {
      checkOpen();
      opened = false;
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  public static class DateIdentity extends Lifecycle {
    public LocalDate eval(LocalDate value) {
      checkOpen();
      return value == null ? null : value.plusDays(calls++ % 3);
    }
  }

  @FunctionHint(input = @DataTypeHint("TIME(0)"), output = @DataTypeHint("TIME(0)"))
  public static class TimeIdentity extends Lifecycle {
    public LocalTime eval(LocalTime value) {
      checkOpen();
      return value == null ? null : value.plusNanos((calls++ % 3) * 1_000_000L);
    }
  }

  public static class DatePart extends ScalarFunction {
    public Integer eval(LocalDate value) {
      return value == null ? null : value.getDayOfMonth();
    }
  }

  @FunctionHint(input = @DataTypeHint("TIME(0)"), output = @DataTypeHint("INT"))
  public static class TimePart extends ScalarFunction {
    public Integer eval(LocalTime value) {
      return value == null ? null : value.getNano();
    }
  }

  public static class DateResult extends ScalarFunction {
    public LocalDate eval(Integer value) {
      return value == null || value % 8 == 7
          ? null
          : value % 8 == 5 ? LocalDate.of(2024, 2, 29)
              : (LocalDate) FlinkTypedTableFunctionSqlHarnessTest.value("DATE", value);
    }
  }

  @FunctionHint(input = @DataTypeHint("INT"), output = @DataTypeHint("TIME(0)"))
  public static class TimeResult extends ScalarFunction {
    public LocalTime eval(Integer value) {
      return value == null || value % 8 == 7
          ? null
          : (LocalTime) FlinkTypedTableFunctionSqlHarnessTest.value("TIME", value);
    }
  }

  public static class DateFailure extends ScalarFunction {
    public LocalDate eval(Integer value) {
      throw new IllegalStateException("date-time sentinel");
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  @FunctionHint(input = @DataTypeHint("INT"), output = @DataTypeHint("TIME(0)"))
  public static class TimeFailure extends ScalarFunction {
    public LocalTime eval(Integer value) {
      throw new IllegalStateException("date-time sentinel");
    }

    @Override
    public boolean isDeterministic() {
      return false;
    }
  }

  public static class LegacyDate extends ScalarFunction {
    public java.sql.Date eval(Integer value) {
      return java.sql.Date.valueOf("2024-02-29");
    }
  }
}
