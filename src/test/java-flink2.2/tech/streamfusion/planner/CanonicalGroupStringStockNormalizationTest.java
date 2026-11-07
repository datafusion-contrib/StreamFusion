package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.CanonicalGroupArithmeticInitializationTest.snapshot;
import static tech.streamfusion.planner.CanonicalGroupStringRepresentationTest.*;
import static tech.streamfusion.planner.GroupAggregateStockTestFixture.field;

import java.util.stream.Stream;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CanonicalGroupStringStockNormalizationTest {
  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            rocks ->
                Stream.of("STRING", "VARCHAR(4096)", "CHAR(4)")
                    .map(type -> Arguments.of(type, rocks)));
  }

  @ParameterizedTest(name = "stock normalization:{0},rocks={1}")
  @MethodSource("cases")
  void releasedStockCanonicalStateDropsTheJavaCacheAndUsesBinaryComparison(
      String type, boolean rocks) throws Exception {
    String first = "\uD800\uDC00", second = "\uE000";
    if (type.equals("CHAR(4)")) {
      first += "   ";
      second += "   ";
    }
    try (var original = new TypedGroupStockFixture(type, true, rocks, true);
        var restored = new TypedGroupStockFixture(type, true, rocks, true)) {
      original.harness.open();
      var input = row(first, "java", 0);
      stockStep(original, input);
      assertEquals(!rocks, hasJavaCache(original), "actual live stock accumulator representation");
      var saved = snapshot(original.harness);
      restored.harness.initializeState(saved);
      restored.harness.open();
      restored.backend().setCurrentKey(restored.selector.getKey(input));
      assertFalse(hasJavaCache(restored), "stock canonical restore produces binary-backed strings");
      var uninterrupted = stockStep(original, row(second, "java", 1));
      var continued = stockStep(restored, row(second, "java", 1));
      if (rocks) assertEquals(uninterrupted, continued);
      else
        assertNotEquals(
            uninterrupted,
            continued,
            "released stock heap execution itself changes comparison path after canonical restore");
    }
  }

  @SuppressWarnings("unchecked")
  private static boolean hasJavaCache(TypedGroupStockFixture fixture) throws Exception {
    var state = (ValueState<RowData>) field(fixture.function, "accState");
    var scalars = (int[]) field(fixture.contract, "scalarFields");
    return ((BinaryStringData) state.value().getString(scalars[2])).getJavaObject() != null;
  }
}
