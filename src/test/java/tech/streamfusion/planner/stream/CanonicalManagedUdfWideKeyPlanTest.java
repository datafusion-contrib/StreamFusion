package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.GroupAggregateStockTestFixture.field;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import tech.streamfusion.operator.*;

class CanonicalManagedUdfWideKeyPlanTest {
  static Stream<Arguments> cases() {
    return Arrays.stream(ManagedUdfKeyShape.values())
        .flatMap(
            shape ->
                Stream.of(0, 1, 2)
                    .flatMap(
                        mode ->
                            Stream.of(false, true)
                                .flatMap(
                                    compiled ->
                                        Stream.of("DISABLED", "ALWAYS")
                                            .map(
                                                uid -> Arguments.of(shape, mode, compiled, uid)))));
  }

  @ParameterizedTest(name = "{0}:mode={1},compiled={2},uid={3}")
  @MethodSource("cases")
  void stockKeyTypesAndIdentitiesSurviveAutomaticLowering(
      ManagedUdfKeyShape shape, int mode, boolean compiled, String uid) throws Exception {
    var stock =
        CanonicalManagedUdfGroupPlanTest.graph(
            false, compiled, false, uid, mode > 0, mode == 2, shape.keys);
    var actual =
        CanonicalManagedUdfGroupPlanTest.graph(
            true, compiled, false, uid, mode > 0, mode == 2, shape.keys);
    var original =
        stock.getStreamNodes().stream()
            .filter(
                n -> {
                  if (!(n.getOperatorFactory() instanceof SimpleOperatorFactory)) return false;
                  var op = ((SimpleOperatorFactory<?>) n.getOperatorFactory()).getOperator();
                  return mode > 0
                      ? op.getClass()
                          == org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator
                              .class
                      : op instanceof KeyedProcessOperator
                          && ((KeyedProcessOperator<?, ?, ?>) op).getUserFunction()
                              instanceof GroupAggFunction;
                })
            .findFirst()
            .orElseThrow();
    var owners =
        actual.getStreamNodes().stream()
            .filter(
                n ->
                    n.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) n.getOperatorFactory())
                                .getOperator()
                                .getClass()
                            == NativeColumnarJvmGroupOperator.class)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(1, owners.size(), actual.getStreamingPlanAsJSON());
    var owner = owners.get(0);
    var op = ((SimpleOperatorFactory<?>) owner.getOperatorFactory()).getOperator();
    assertEquals(
        ((RowDataKeySelector) original.getStatePartitioners()[0]).getProducedType(),
        ((RowDataKeySelector) field(op, "keySelector")).getProducedType());
    // Independent planning assigns different node numbers; full jobs reuse the same portable plan.
    assertEquals(uid.equals("DISABLED"), owner.getTransformationUID() == null);
    if (owner.getTransformationUID() != null)
      assertTrue(
          owner
              .getTransformationUID()
              .endsWith(mode == 2 ? "_global-group-aggregate" : "_group-aggregate"));
    assertEquals(257, tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(owner));
    assertEquals(2, owner.getParallelism());
    long locals =
        actual.getStreamNodes().stream()
            .filter(
                n ->
                    n.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) n.getOperatorFactory())
                                .getOperator()
                                .getClass()
                            == NativeColumnarLocalJvmGroupOperator.class)
            .count();
    assertEquals(mode == 2 ? 1 : 0, locals);
    assertTrue(
        actual.getStreamNodes().stream()
            .anyMatch(n -> n.getOperatorName().contains("RowDataToArrow")));
    assertTrue(
        actual.getStreamNodes().stream()
            .anyMatch(n -> n.getOperatorName().contains("ArrowToRowData")));
  }
}
