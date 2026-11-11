package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.GroupAggregateStockTestFixture.field;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.*;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.data.*;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import tech.streamfusion.operator.NativeColumnarJvmGroupOperator;

class CanonicalManagedUdfKeyPrecisionTest {
  static Stream<LogicalType> types() {
    return Stream.concat(
        IntStream.range(20, 38).mapToObj(p -> (LogicalType) new DecimalType(p, 2)),
        IntStream.range(4, 9)
            .boxed()
            .flatMap(p -> Stream.of(new TimestampType(p), new LocalZonedTimestampType(p))));
  }

  static Stream<Arguments> plans() {
    return types()
        .flatMap(
            type ->
                Stream.of(0, 1, 2)
                    .flatMap(
                        mode ->
                            Stream.of(false, true)
                                .map(compiled -> Arguments.of(type, mode, compiled))));
  }

  @ParameterizedTest(name = "{0}:mode={1},compiled={2}")
  @MethodSource("plans")
  void intermediatePrecisionsUseTheReleasedKeySelector(LogicalType type, int mode, boolean compiled)
      throws Exception {
    var graph =
        CanonicalManagedUdfGroupPlanTest.graph(
            true, compiled, false, "ALWAYS", mode > 0, mode == 2, RowType.of(type));
    var owners =
        graph.getStreamNodes().stream()
            .filter(
                n ->
                    n.getOperatorFactory() instanceof SimpleOperatorFactory
                        && ((SimpleOperatorFactory<?>) n.getOperatorFactory())
                                .getOperator()
                                .getClass()
                            == NativeColumnarJvmGroupOperator.class)
            .collect(Collectors.toList());
    assertEquals(1, owners.size(), graph.getStreamingPlanAsJSON());
    var op = ((SimpleOperatorFactory<?>) owners.get(0).getOperatorFactory()).getOperator();
    var selector = (RowDataKeySelector) field(op, "keySelector");
    assertEquals(type, selector.getProducedType().toRowFieldTypes()[0]);
    assertNotNull(owners.get(0).getTransformationUID());
    assertEquals(257, tech.streamfusion.compat.FlinkPlanCompat.maximumParallelism(owners.get(0)));
  }

  static Stream<Arguments> switches() {
    return types()
        .flatMap(
            type ->
                Stream.of(0, 1, 2)
                    .flatMap(
                        mode ->
                            Stream.of(false, true)
                                .flatMap(
                                    rocks ->
                                        Stream.of(false, true)
                                            .map(
                                                first -> Arguments.of(type, mode, rocks, first)))));
  }

  @ParameterizedTest(name = "{0}:mode={1},rocks={2},nativeFirst={3}")
  @MethodSource("switches")
  void everyAdmittedPrecisionPreservesCanonicalKeyAndViewBytes(
      LogicalType type, int mode, boolean rocks, boolean first) throws Exception {
    OperatorSubtaskState saved = null, referenceSaved = null;
    for (int stage = 0; stage < 3; stage++) {
      boolean nativeRun = stage % 2 == 0 ? first : !first;
      try (var reference = fixture(false, type, mode, rocks);
          var receiver = fixture(nativeRun, type, mode, rocks)) {
        reference.open(referenceSaved, 1000 + stage * 100);
        receiver.open(saved, 1000 + stage * 100);
        if (saved != null) {
          assertEquals(reference.rawState(), receiver.rawState());
          try (var immediate = fixture(!nativeRun, type, mode, rocks)) {
            immediate.open(receiver.canonical(), 1000 + stage * 100);
            assertEquals(reference.rawState(), immediate.rawState(), "immediate re-export");
          }
        }
        var rows =
            CanonicalManagedUdfGroupStateTest.seeds(stage).stream()
                .map(
                    original -> {
                      var row = new GenericRowData(original.getRowKind(), 2);
                      row.setField(0, original.isNullAt(0) ? null : key(type, original.getLong(0)));
                      row.setField(1, original.isNullAt(1) ? null : original.getLong(1));
                      return (RowData) row;
                    })
                .collect(Collectors.toList());
        assertEquals(reference.feed(rows, false), receiver.feed(rows, stage % 2 == 0));
        assertEquals(reference.rawState(), receiver.rawState(), "all stock canonical KV bytes");
        saved = receiver.canonical();
        referenceSaved = reference.canonical();
      }
    }
  }

  @ParameterizedTest(name = "routing:{0}")
  @MethodSource("types")
  void intermediateArrowUnitsKeepStockBinaryKeysAndHashRouting(LogicalType type) throws Exception {
    var rows =
        IntStream.range(0, 65)
            .mapToObj(i -> (RowData) GenericRowData.of(i == 0 ? null : key(type, i)))
            .collect(Collectors.toList());
    CanonicalManagedUdfWideKeyRoutingTest.verify(RowType.of(type), rows);
  }

  private static Object key(LogicalType type, long id) {
    int precision = LogicalTypeChecks.getPrecision(type);
    if (type.is(LogicalTypeRoot.DECIMAL)) {
      var value = BigDecimal.TEN.pow(precision - 3).add(BigDecimal.valueOf(id, 2));
      return DecimalData.fromBigDecimal(id % 2 == 0 ? value.negate() : value, precision, 2);
    }
    long unitsPerMillisecond = (long) Math.pow(10, precision - 3);
    int nanosPerUnit = (int) Math.pow(10, 9 - precision);
    return TimestampData.fromEpochMillis(
        id / unitsPerMillisecond - 1, Math.toIntExact(id % unitsPerMillisecond) * nanosPerUnit);
  }

  private static ManagedUdfGroupStockFixture fixture(
      boolean nativeRun, LogicalType type, int mode, boolean rocks) throws Exception {
    return new ManagedUdfGroupStockFixture(
        nativeRun, mode > 0, rocks, 0, 0, mode == 2, false, RowType.of(type));
  }
}
