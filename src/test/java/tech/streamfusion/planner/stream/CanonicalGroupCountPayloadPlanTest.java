package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.GroupAggregateStockTestFixture.field;

import java.util.stream.Stream;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.operator.CanonicalOverValueFixture;
import tech.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import tech.streamfusion.state.StockGroupTypedStateContract;

class CanonicalGroupCountPayloadPlanTest {
  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(
        Stream.concat(CanonicalOverValueFixture.countTypes(), Stream.of("DECIMAL(38,20)"))
            .flatMap(
                type ->
                    Stream.of("eager", "mini", "global")
                        .flatMap(
                            mode ->
                                Stream.of(false, true)
                                    .flatMap(
                                        compiled ->
                                            Stream.of(false, true)
                                                .map(
                                                    retract ->
                                                        Arguments.of(
                                                            type, mode, compiled, retract))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void validityOnlyCountArgumentsBindTheReleasedScalarAccumulator(
      String type, String mode, boolean compiled, boolean retract) throws Exception {
    var graph =
        MiniBatchGroupStockFixture.graph(
            type,
            mode.equals("global"),
            true,
            false,
            retract,
            compiled,
            true,
            !mode.equals("eager"));
    var owners =
        graph.getStreamNodes().stream()
            .filter(node -> node.getOperatorFactory() instanceof SimpleOperatorFactory)
            .map(node -> ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator())
            .filter(NativeColumnarGroupAggregateOperator.class::isInstance)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var contract =
        assertInstanceOf(
            StockGroupTypedStateContract.class, field(owners.get(0), "stockStateContract"));
    assertTrue(((java.util.List<?>) field(contract, "views")).isEmpty());
    assertEquals(mode.equals("global"), field(contract, "globalMerge"));
    assertEquals(!mode.equals("eager"), field(owners.get(0), "miniBatch"));
  }
}
