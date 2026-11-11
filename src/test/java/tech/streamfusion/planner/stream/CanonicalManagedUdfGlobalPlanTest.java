package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.stream.GroupAggregateStockTestFixture.field;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.runtime.operators.aggregate.*;
import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import tech.streamfusion.operator.*;

class CanonicalManagedUdfGlobalPlanTest {
  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            compiled ->
                Stream.of(false, true)
                    .flatMap(
                        append ->
                            Stream.of("DISABLED", "ALWAYS")
                                .flatMap(
                                    uid ->
                                        Stream.of(0, 16)
                                            .map(
                                                cache ->
                                                    Arguments.of(compiled, append, uid, cache)))));
  }

  @ParameterizedTest(name = "compiled={0},append={1},uid={2},cache={3}")
  @MethodSource("cases")
  void actualLocalAndGlobalHandlersKeepStockStateAndPrivatePartialTransport(
      boolean compiled, boolean append, String uid, int cache) throws Exception {
    String previous = System.getProperty("streamfusion.experimental.jvmAccumulatorCacheEntries");
    System.setProperty(
        "streamfusion.experimental.jvmAccumulatorCacheEntries", Integer.toString(cache));
    try {
      var stock = CanonicalManagedUdfGroupPlanTest.graph(false, compiled, append, uid, true, true);
      var nativeGraph =
          CanonicalManagedUdfGroupPlanTest.graph(true, compiled, append, uid, true, true);
      var locals = new ArrayList<Object>();
      var globals = new ArrayList<Object>();
      for (var n : nativeGraph.getStreamNodes()) {
        if (!(n.getOperatorFactory() instanceof SimpleOperatorFactory)) continue;
        var op = ((SimpleOperatorFactory<?>) n.getOperatorFactory()).getOperator();
        if (op.getClass() == NativeColumnarLocalJvmGroupOperator.class) {
          locals.add(op);
          assertTrue(
              n.getStatePartitioners() == null
                  || Arrays.stream(n.getStatePartitioners()).allMatch(Objects::isNull));
          assertTrue(n.getOperatorName().startsWith("NativeColumnarLocalJvmGroup"));
          assertEquals(0, field(op, "cacheEntries"));
          assertEquals(
              MiniBatchLocalGroupAggFunction.class, field(op, "miniBatchFunction").getClass());
        } else if (op.getClass() == NativeColumnarJvmGroupOperator.class) {
          globals.add(op);
          assertNotNull(n.getStatePartitioners()[0]);
          assertEquals(257, tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(n));
          assertEquals(2, n.getParallelism());
          assertEquals(cache, field(op, "cacheEntries"));
          assertEquals(uid.equals("DISABLED"), n.getTransformationUID() == null);
          assertEquals(
              MiniBatchGlobalGroupAggFunction.class, field(op, "miniBatchFunction").getClass());
        }
      }
      assertEquals(1, locals.size(), nativeGraph.getStreamingPlanAsJSON());
      assertEquals(1, globals.size(), nativeGraph.getStreamingPlanAsJSON());
      var original =
          stock.getStreamNodes().stream()
              .filter(n -> n.getOperatorFactory() instanceof SimpleOperatorFactory)
              .map(n -> ((SimpleOperatorFactory<?>) n.getOperatorFactory()).getOperator())
              .filter(
                  op ->
                      op
                          instanceof
                          org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator)
              .findFirst()
              .orElseThrow();
      assertArrayEquals(
          (LogicalType[]) field(field(original, "function"), "accTypes"),
          (LogicalType[]) field(field(globals.get(0), "miniBatchFunction"), "accTypes"));
      var output = (JvmGroupPartialTransport) field(locals.get(0), "outputPartials");
      var input = (JvmGroupPartialTransport) field(globals.get(0), "inputPartials");
      assertEquals(output.type(), input.type());
      assertEquals(
          List.of(LogicalTypeRoot.BIGINT, LogicalTypeRoot.VARBINARY),
          output.type().getChildren().stream()
              .map(LogicalType::getTypeRoot)
              .collect(java.util.stream.Collectors.toList()));
      assertNotNull(field(output, "serializer"));
      assertTrue(
          nativeGraph.getStreamNodes().stream()
              .anyMatch(n -> n.getOperatorName().contains("RowDataToArrow")));
      assertTrue(
          nativeGraph.getStreamNodes().stream()
              .anyMatch(n -> n.getOperatorName().contains("ArrowToRowData")));
    } finally {
      if (previous == null)
        System.clearProperty("streamfusion.experimental.jvmAccumulatorCacheEntries");
      else System.setProperty("streamfusion.experimental.jvmAccumulatorCacheEntries", previous);
    }
  }
}
