package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class TpchResultOrderingTest {
  @Test
  void acceptsOnlyPermutationsWithinSqlOrderingTiesAndPreservesMultiplicity() {
    var keys = List.<List<Object>>of(List.of(1), List.of(1), List.of(2));
    var expected = TpchBenchmark.canonicalRows(List.of("a,1", "b,1", "c,2"), keys);
    assertEquals(expected, TpchBenchmark.canonicalRows(List.of("b,1", "a,1", "c,2"), keys));
    assertNotEquals(
        expected,
        TpchBenchmark.canonicalRows(
            List.of("c,2", "a,1", "b,1"), List.of(List.of(2), List.of(1), List.of(1))));
    assertNotEquals(expected, TpchBenchmark.canonicalRows(List.of("a,1", "a,1", "c,2"), keys));
    assertArrayEquals(
        new int[] {1},
        TpchBenchmark.orderFields(
            "SELECT n,v FROM t ORDER BY\n-- v DESC\n`v` DESC", List.of("n", "v")));
    assertArrayEquals(
        new int[] {1, 0},
        TpchBenchmark.orderFields(
            "SELECT n,v FROM t ORDER BY `v` DESC,n LIMIT 3", List.of("n", "v")));
    assertEquals(
        List.of("a", "b"),
        TpchBenchmark.canonicalRows(
            List.of("b", "a"),
            Arrays.asList(Arrays.asList((Object) null), Arrays.asList((Object) null))));
  }
}
