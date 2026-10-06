package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class SortedOutputTest {
  @Test
  void comparesExactRowsAcrossDifferentSpillsAndPreservesDuplicateCounts() throws Exception {
    try (SortedOutput expected = new SortedOutput(2);
        SortedOutput actual = new SortedOutput(3)) {
      for (String row : new String[] {"b", "a", "b", "", "é\n😀"}) expected.add(row);
      for (String row : new String[] {"é\n😀", "", "b", "b", "a"}) actual.add(row);
      expected.assertSame(actual, "parity");
    }
    try (SortedOutput expected = new SortedOutput(1);
        SortedOutput actual = new SortedOutput(1)) {
      expected.add("a");
      expected.add("b");
      actual.add("a");
      actual.add("a");
      assertThrows(AssertionError.class, () -> expected.assertSame(actual, "duplicate mismatch"));
    }
  }
}
