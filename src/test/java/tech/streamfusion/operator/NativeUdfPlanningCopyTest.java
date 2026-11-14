package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class NativeUdfPlanningCopyTest {
  @Test
  void remappedWholeCalcBindingsRetainIndependentInputCoordinates() {
    var binding = new NativeUdf.Binding(new NativeUdf.Descriptor[0], new int[0], new int[] {2, 0});
    var left = binding.planningCopy(new int[] {1, -1, 0});
    var right = binding.planningCopy(new int[] {0, -1, 1});
    assertArrayEquals(new int[] {2, 0}, binding.rowInputColumns());
    assertArrayEquals(new int[] {0, 1}, left.rowInputColumns());
    assertArrayEquals(new int[] {1, 0}, right.rowInputColumns());
    assertNotSame(binding, binding.planningCopy(null));
    assertEquals(-1, left.rowCalcId());
    assertEquals(-1, right.rowCalcId());
  }
}
