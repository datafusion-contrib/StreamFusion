package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.GroupAggregateStockTestFixture.field;

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

class CanonicalGroupStringExtremaAdmissionTest {
  static Stream<Integer> otherKinds() {
    return Stream.of(0, 3, 4, 7, 22);
  }

  @ParameterizedTest
  @MethodSource("otherKinds")
  void varcharDoesNotAdmitOtherAggregateLayouts(int kind) {
    assertThrows(
        IllegalArgumentException.class, () -> contract(new VarCharType(4096), kind, false));
  }

  static Stream<Arguments> rejectedExtrema() {
    return Stream.of(10, 11)
        .flatMap(kind -> Stream.of(false, true).map(retract -> Arguments.of(kind, retract)));
  }

  @ParameterizedTest
  @MethodSource("rejectedExtrema")
  void scalarCharAndRetractingVarcharRemainOutsideTheContract(int kind, boolean retract) {
    assertThrows(
        IllegalArgumentException.class,
        () -> contract(retract ? new VarCharType(4096) : new CharType(4), kind, retract));
  }

  private static StockGroupTypedStateContract contract(
      LogicalType type, int kind, boolean retract) {
    var key = RowType.of(new BigIntType());
    return new StockGroupTypedStateContract(
        new RowDataSerializer(key),
        key,
        kind == 4 || retract ? RowType.of(type, new BigIntType()) : RowType.of(type),
        new int[] {kind},
        new int[] {0},
        new int[] {kind == 4 ? 1 : -1},
        retract ? 1 : -1,
        false,
        List.of());
  }

  static Stream<String> types() {
    return Stream.of("STRING", "VARCHAR(4096)", "CHAR(4)")
        .filter(
            type ->
                !type.startsWith("CHAR") || tech.streamfusion.compat.CanonicalTestCompat.flink22());
  }

  @ParameterizedTest
  @MethodSource("types")
  void actualRetractingPlansNeverReceiveTheFlatAppendContract(String type) throws Exception {
    var method =
        TypedGroupStockFixture.class.getDeclaredMethod(
            "graph", String.class, boolean.class, boolean.class, boolean.class);
    method.setAccessible(true);
    var graph = (StreamGraph) method.invoke(null, type, false, true, true);
    int owners = 0;
    for (var node : graph.getStreamNodes()) {
      if (!(node.getOperatorFactory() instanceof SimpleOperatorFactory)) continue;
      var owner = ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator();
      if (owner instanceof NativeColumnarGroupAggregateOperator) {
        owners++;
        assertFalse(field(owner, "stockStateContract") instanceof StockGroupTypedStateContract);
      } else if (owner instanceof KeyedProcessOperator
          && ((KeyedProcessOperator<?, ?, ?>) owner).getUserFunction() instanceof GroupAggFunction)
        owners++;
    }
    assertTrue(owners > 0);
  }
}
