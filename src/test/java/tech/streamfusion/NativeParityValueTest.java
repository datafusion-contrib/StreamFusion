package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class NativeParityValueTest {
  @Test
  void nestedMapsCompareArrayContentsRegardlessOfEntryOrder() {
    Map<String, Object> left = new LinkedHashMap<>();
    left.put("values", new String[] {"one", null});
    left.put("row", Row.of(new byte[] {1, 2}, null));
    left.put("absent", null);
    Map<String, Object> right = new LinkedHashMap<>();
    right.put("absent", null);
    right.put("row", Row.of(new byte[] {1, 2}, null));
    right.put("values", new String[] {"one", null});
    Object expected = NativeParity.comparableValue(left);
    assertEquals(expected, NativeParity.comparableValue(right));
    assertEquals(expected.toString(), NativeParity.comparableValue(right).toString());
    right.put("values", new String[] {"different", null});
    assertNotEquals(expected, NativeParity.comparableValue(right));
    right.put("values", new String[] {"one", null});
    right.put("row", Row.ofKind(RowKind.DELETE, new byte[] {1, 2}, null));
    assertNotEquals(expected, NativeParity.comparableValue(right));
  }
}
