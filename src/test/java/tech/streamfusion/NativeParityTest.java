package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.HashMap;
import java.util.LinkedHashMap;
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
