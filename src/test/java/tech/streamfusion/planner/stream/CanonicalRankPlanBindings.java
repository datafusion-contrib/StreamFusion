package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import tech.streamfusion.operator.AbstractNativeStatefulOperator;
import tech.streamfusion.operator.NativeColumnarGroupAggregateOperator;
import tech.streamfusion.operator.NativeColumnarTopNOperator;
import tech.streamfusion.state.StockStateContract;

final class CanonicalRankPlanBindings {
  private CanonicalRankPlanBindings() {}

  static void verifyAggregateAndRank(StreamGraph graph, Map<?, ?> registrations) throws Exception {
    var owners =
        graph.getStreamNodes().stream()
            .filter(
                node ->
                    node.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator()
                            instanceof AbstractNativeStatefulOperator)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(2, owners.size(), graph.getStreamingPlanAsJSON());
    var hashes = new StreamGraphHasherV2().traverseStreamGraphAndGenerateHashes(graph);
    var expected = new java.util.HashSet<String>();
    var classes = new java.util.HashSet<Class<?>>();
    var contractField = AbstractNativeStatefulOperator.class.getDeclaredField("stockStateContract");
    contractField.setAccessible(true);
    for (var node : owners) {
      Object operator = ((SimpleOperatorFactory<?>) node.getOperatorFactory()).getOperator();
      classes.add(operator.getClass());
      String id = new OperatorID(hashes.get(node.getId())).toString();
      assertTrue(expected.add(id), "Each state owner must have a distinct identity");
      var registration = registrations.get(id);
      assertNotNull(registration, id);
      var registeredClass = registration.getClass().getDeclaredField("operatorClass");
      registeredClass.setAccessible(true);
      assertEquals(operator.getClass().getSimpleName(), registeredClass.get(registration));
      var registeredContract = registration.getClass().getDeclaredField("contract");
      registeredContract.setAccessible(true);
      var actual = assertInstanceOf(StockStateContract.class, registeredContract.get(registration));
      var contract = assertInstanceOf(StockStateContract.class, contractField.get(operator));
      assertEquals(contract.getClass(), actual.getClass());
      assertEquals(contract.stateDescriptors(), actual.stateDescriptors());
      assertEquals(contract.keySerializer(), actual.keySerializer());
    }
    assertEquals(
        java.util.Set.of(
            NativeColumnarGroupAggregateOperator.class, NativeColumnarTopNOperator.class),
        classes);
    assertEquals(
        expected,
        registrations.keySet(),
        "Every state owner must have exactly one matching binding");
  }
}
