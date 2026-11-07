package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.GroupAggregateStockTestFixture.field;

import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import tech.streamfusion.state.StockGroupTypedStateContract;

class CanonicalGroupTypedTemporalAdmissionTest {
  static Stream<LogicalType> types() {
    return Stream.of(new DateType(), new TimeType(0), new TimeType(3), new BooleanType());
  }

  static Stream<Arguments> invalidScalars() {
    return types()
        .flatMap(type -> Stream.of(0, 3, 4, 7, 22).map(kind -> Arguments.of(type, kind, false)));
  }

  @ParameterizedTest(name = "invalid:{0},kind={1}")
  @MethodSource("invalidScalars")
  void doesNotAdmitOtherAggregateLayouts(LogicalType type, int kind, boolean retract) {
    assertThrows(IllegalArgumentException.class, () -> contract(type, kind, retract));
  }

  static Stream<Arguments> retractingScalars() {
    return types().flatMap(type -> Stream.of(10, 11).map(kind -> Arguments.of(type, kind, true)));
  }

  @ParameterizedTest(name = "retract:{0},kind={1}")
  @MethodSource("retractingScalars")
  void doesNotReplaceRetractingStateWithAScalar(LogicalType type, int kind, boolean retract) {
    assertThrows(IllegalArgumentException.class, () -> contract(type, kind, retract));
  }

  private static StockGroupTypedStateContract contract(
      LogicalType type, int kind, boolean retract) {
    var key = RowType.of(new BigIntType());
    var accumulator = kind == 4 || retract ? RowType.of(type, new BigIntType()) : RowType.of(type);
    return new StockGroupTypedStateContract(
        new RowDataSerializer(key),
        key,
        accumulator,
        new int[] {kind},
        new int[] {0},
        new int[] {kind == 4 ? 1 : -1},
        retract ? 1 : -1,
        false,
        List.of());
  }

  static Stream<String> sqlTypes() {
    return Stream.of("DATE", "TIME(0)", "TIME(3)", "BOOLEAN");
  }

  @ParameterizedTest
  @MethodSource("sqlTypes")
  void releasedRetractingPlansDoNotAcquireTheAppendOnlyContract(String type) throws Exception {
    var method =
        TypedGroupStockFixture.class.getDeclaredMethod(
            "graph", String.class, boolean.class, boolean.class, boolean.class);
    method.setAccessible(true);
    var graph = (StreamGraph) method.invoke(null, type, false, true, true);
    int owners = 0;
    for (var node : graph.getStreamNodes()) {
      if (!(node.getOperatorFactory() instanceof SimpleOperatorFactory)) continue;
      var operator = ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator();
      if (operator instanceof NativeColumnarGroupAggregateOperator) {
        owners++;
        assertFalse(field(operator, "stockStateContract") instanceof StockGroupTypedStateContract);
      } else if (operator instanceof KeyedProcessOperator
          && ((KeyedProcessOperator<?, ?, ?>) operator).getUserFunction()
              instanceof GroupAggFunction) owners++;
    }
    assertTrue(owners > 0, "The actual released plan must retain its aggregate state owner");
  }
}
