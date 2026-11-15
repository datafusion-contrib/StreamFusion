package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalTestMatrixTest {
  @Test
  void representativesRetainEverySingleOptionAndPairWithoutInventingInvalidCases() {
    var all = new ArrayList<Arguments>();
    for (String type : List.of("BIGINT", "FLOAT", "DOUBLE", "DECIMAL"))
      for (boolean keyless : new boolean[] {false, true})
        for (boolean stock : new boolean[] {false, true})
          for (boolean nativeStore : new boolean[] {false, true})
            for (int offset : new int[] {0, 2})
              for (int clock : new int[] {0, 1, 2, 3})
                if (!keyless || offset == 0)
                  all.add(Arguments.of(type, keyless, stock, nativeStore, offset, clock));
    var selected = CanonicalTestMatrix.representatives(all);
    assertEquals(pairs(all), pairs(selected));
    assertTrue(selected.size() < all.size() / 3);
    assertTrue(all.containsAll(selected));
    assertEquals(selected, CanonicalTestMatrix.representatives(all));
  }

  @Test
  void emptyAndNullableOptionsHaveDefinedCoverage() {
    assertTrue(CanonicalTestMatrix.representatives(List.of()).isEmpty());
    assertEquals(
        1, CanonicalTestMatrix.representatives(List.of(Arguments.of(), Arguments.of())).size());
    var all = List.of(Arguments.of(null, false), Arguments.of("value", true));
    assertEquals(pairs(all), pairs(CanonicalTestMatrix.representatives(all)));
    assertThrows(
        IllegalArgumentException.class,
        () -> CanonicalTestMatrix.representatives(List.of(Arguments.of(1), Arguments.of(1, 2))));
  }

  @Test
  void defaultSelectionKeepsEveryInvocationInItsOriginalOrder() {
    var all = List.of(Arguments.of(1), Arguments.of(2), Arguments.of(1));
    assertEquals(all, CanonicalTestMatrix.select(all.stream(), false).collect(Collectors.toList()));
  }

  @Test
  void materializingRepresentativesClosesTheProviderStream() {
    var closed = new java.util.concurrent.atomic.AtomicBoolean();
    var source = java.util.stream.Stream.of(Arguments.of(1)).onClose(() -> closed.set(true));
    try (var selected = CanonicalTestMatrix.select(source, true)) {
      assertTrue(closed.get());
      assertEquals(1, selected.count());
    }
  }

  private static Set<List<Object>> pairs(List<Arguments> entries) {
    var result = new HashSet<List<Object>>();
    for (var entry : entries) {
      var values = entry.get();
      for (int first = 0; first < values.length; first++)
        for (int second = first; second < values.length; second++)
          result.add(Arrays.asList(first, values[first], second, values[second]));
    }
    return result;
  }
}
