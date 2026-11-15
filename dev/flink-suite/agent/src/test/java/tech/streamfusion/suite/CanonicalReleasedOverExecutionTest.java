package tech.streamfusion.suite;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Proxy;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@EnabledIfSystemProperty(named = "streamfusion.flink-suite.canonical", matches = "true")
class CanonicalReleasedOverExecutionTest {
  @org.junit.jupiter.api.io.TempDir java.nio.file.Path reports;

  @ParameterizedTest
  @ValueSource(
      strings = {"complete", "missingState", "missingInput", "missingReason", "nativeOwner"})
  void releasedUdfProgramsRequireTheStockStateOwner(String mode) throws Exception {
    System.setProperty("streamfusion.flink-suite.native-reports", reports.toString());
    var scope =
        NativeExecution.beginRestore(
            "org.apache.flink.table.planner.plan.nodes.exec.stream.OverAggregateRestoreTest",
            new Object(),
            "over-aggregate-bounded-partitioned-rows");
    var function =
        new org.apache.flink.table.runtime.operators.over.RowTimeRangeBoundedPrecedingFunction<>(
            null,
            new org.apache.flink.table.types.logical.LogicalType[0],
            new org.apache.flink.table.types.logical.LogicalType[0],
            1,
            0);
    if (!mode.equals("missingState")) {
      for (String name : new String[] {"accState", "inputState", "function"}) {
        var field = function.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(
            function,
            Proxy.newProxyInstance(
                field.getType().getClassLoader(),
                new Class<?>[] {field.getType()},
                (proxy, method, arguments) -> null));
      }
    }
    var owner = new KeyedProcessOperator(function);
    if (mode.equals("missingState"))
      assertThrows(AssertionError.class, () -> NativeExecution.stockOpened(owner));
    else {
      NativeExecution.stockOpened(owner);
      if (!mode.equals("missingInput")) NativeExecution.stockCompleted(owner);
    }
    var calc = new NativeCalcOperator();
    NativeExecution.opened(calc);
    NativeExecution.completed(calc, 3);
    if (!mode.equals("missingReason"))
      NativeExecution.fallback("over aggregate: released state layout is not bound");
    if (mode.equals("nativeOwner")) {
      var nativeOwner = new NativeOverAggregateOperator();
      NativeExecution.opened(nativeOwner);
      NativeExecution.completed(nativeOwner, 2);
    }
    if (mode.equals("complete")) assertDoesNotThrow(() -> NativeExecution.finish(scope));
    else assertThrows(AssertionError.class, () -> NativeExecution.finish(scope));
  }

  private static final class KeyedProcessOperator {
    final Object userFunction;

    KeyedProcessOperator(Object function) {
      userFunction = function;
    }
  }

  private static final class NativeCalcOperator {}

  private static final class NativeOverAggregateOperator {}
}
