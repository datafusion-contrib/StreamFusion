package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class NativeParityTest {

  @Test
  void comparesArraysInsideMapsByContent() {
    assertComparableEquals(
        Map.of("a", new Integer[] {1, null, 2}),
        Map.of("a", new Integer[] {1, null, 2}));
    assertComparableNotEquals(
        Map.of("a", new int[] {1, 2}), Map.of("a", new int[] {2, 1}));
    assertComparableNotEquals(
        Map.of("a", new int[] {1, 2}), Map.of("a", new int[] {1, 2, 2}));
    assertComparableNotEquals(
        Map.of("a", new Integer[] {1, null}), Map.of("a", new Integer[] {1, 2}));
  }

  @Test
  void comparesMapKeysRecursivelyAndIgnoresEntryOrder() {
    Map<Object, Object> left = new LinkedHashMap<>();
    left.put(new int[] {1, 2}, Row.of(Map.of("a", new byte[] {0, -1})));
    left.put("other", 3);
    Map<Object, Object> right = new LinkedHashMap<>();
    right.put("other", 3);
    right.put(new int[] {1, 2}, Row.of(Map.of("a", new byte[] {0, -1})));
    assertComparableEquals(left, right);
    right.remove("other");
    assertComparableNotEquals(left, right);
    assertComparableNotEquals(Map.of(new int[] {1}, 2), Map.of(new int[] {2}, 2));
  }

  @Test
  void retainsNullMapKeysAndValues() {
    Map<Object, Object> left = new HashMap<>();
    left.put(null, new Integer[] {null, 1});
    left.put("null", null);
    Map<Object, Object> right = new HashMap<>();
    right.put("null", null);
    right.put(null, new Integer[] {null, 1});
    assertComparableEquals(left, right);
    right.remove("null");
    assertComparableNotEquals(left, right);
  }

  @Test
  void comparesRowsContainingMapsAndArraysWithoutChangingTheInput() {
    Map<String, Object> map = Map.of("a", new Object[] {Row.of(new int[] {1, 2}), null});
    Row left = Row.ofKind(RowKind.UPDATE_AFTER, map, null);
    Row right = Row.ofKind(
        RowKind.UPDATE_AFTER, Map.of("a", new Object[] {Row.of(new int[] {1, 2}), null}), null);
    assertComparableEquals(left, right);
    assertSame(map, left.getField(0));
    right.setKind(RowKind.DELETE);
    assertComparableNotEquals(left, right);
    assertComparableNotEquals(left, Row.ofKind(RowKind.UPDATE_AFTER, map));
  }

  @Test
  void comparesNamedRowsAndRetainsFieldNames() {
    Row left = Row.withNames(RowKind.UPDATE_BEFORE);
    left.setField("payload", Map.of("a", new int[] {1, 2}));
    Row right = Row.withNames(RowKind.UPDATE_BEFORE);
    right.setField("payload", Map.of("a", new int[] {1, 2}));
    assertComparableEquals(left, right);
    right.setField("payload", Map.of("a", new int[] {1, 3}));
    assertComparableNotEquals(left, right);
    Row renamed = Row.withNames(RowKind.UPDATE_BEFORE);
    renamed.setField("different", Map.of("a", new int[] {1, 2}));
    assertComparableNotEquals(left, renamed);
  }

  @Test
  void preservesBinaryContentsAndScalarTypes() {
    assertComparableEquals(Row.of(Map.of("a", new byte[] {0, -1, 1})),
        Row.of(Map.of("a", new byte[] {0, -1, 1})));
    assertComparableNotEquals(Row.of(Map.of("a", new byte[] {0, -1, 1})),
        Row.of(Map.of("a", new byte[] {0, -1, 2})));
    assertComparableNotEquals(Map.of("a", new Integer[] {1}), Map.of("a", new Long[] {1L}));
    assertComparableEquals(null, null);
    assertComparableNotEquals(null, Map.of());
  }

  @Test
  void normalizesListFixturesAndPreviouslyNormalizedValues() {
    Object fixture = List.of(Row.of(Map.of("a", new Integer[] {1, null, 2})));
    Object collected = new Object[] {Row.of(Map.of("a", new Integer[] {1, null, 2}))};
    assertComparableEquals(fixture, collected);
    Object normalized = NativeParity.comparableValue(collected);
    assertEquals(normalized, NativeParity.comparableValue(normalized));
    assertComparableNotEquals(fixture, List.of(Row.of(Map.of("a", new Integer[] {1, 2, null}))));
  }

  @Test
  void unorderedRowsIgnoreMapIterationOrderButRetainDuplicateRows() {
    Map<String, Integer> first = new LinkedHashMap<>();
    first.put("Aa", 1);
    first.put("BB", 9);
    Map<String, Integer> second = new LinkedHashMap<>();
    second.put("Aa", 2);
    second.put("BB", 0);
    Map<String, Integer> reversedFirst = new LinkedHashMap<>();
    reversedFirst.put("BB", 9);
    reversedFirst.put("Aa", 1);
    Map<String, Integer> reversedSecond = new LinkedHashMap<>();
    reversedSecond.put("BB", 0);
    reversedSecond.put("Aa", 2);
    List<List<Object>> expected = List.of(List.of(first), List.of(second), List.of(first));
    List<List<Object>> actual =
        List.of(List.of(reversedSecond), List.of(reversedFirst), List.of(reversedFirst));
    assertEquals(NativeParity.multiset(expected), NativeParity.multiset(actual));
    assertNotEquals(NativeParity.multiset(expected), NativeParity.multiset(actual.subList(0, 2)));
  }

  @Test
  void mapNormalizationDoesNotOverwriteEqualContentArrayKeys() {
    Map<Object, Object> left = new LinkedHashMap<>();
    left.put(new int[] {1}, "first");
    left.put(new int[] {1}, "second");
    left.put(new int[] {1}, "second");
    Map<Object, Object> right = new LinkedHashMap<>();
    right.put(new int[] {1}, "second");
    right.put(new int[] {1}, "first");
    right.put(new int[] {1}, "second");
    assertComparableEquals(left, right);
    assertComparableNotEquals(left, Map.of(new int[] {1}, "second"));
    right.remove(right.keySet().iterator().next());
    assertComparableNotEquals(left, right);
  }

  private static void assertComparableEquals(Object left, Object right) {
    Object normalizedLeft = NativeParity.comparableValue(left);
    Object normalizedRight = NativeParity.comparableValue(right);
    assertEquals(normalizedLeft, normalizedRight);
    if (normalizedLeft != null) {
      assertEquals(normalizedLeft.hashCode(), normalizedRight.hashCode());
    }
  }

  private static void assertComparableNotEquals(Object left, Object right) {
    assertNotEquals(NativeParity.comparableValue(left), NativeParity.comparableValue(right));
  }
}
