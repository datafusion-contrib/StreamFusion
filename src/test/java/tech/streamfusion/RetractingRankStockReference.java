package tech.streamfusion;

import java.util.Map;

/**
 * Released stock results for the fixed hidden OFFSET input, including retained row-kind effects.
 */
final class RetractingRankStockReference {
  private RetractingRankStockReference() {}

  static Map<String, Long> table(int inputRows, String backend) {
    if (!backend.equals("hashmap") && !backend.equals("rocksdb"))
      throw new IllegalArgumentException("Unknown reference backend: " + backend);
    switch (inputRows) {
      case 5:
        return Map.of("2|10|0", 1L);
      case 10:
        return backend.equals("hashmap")
            ? Map.of("2|10|0", 1L, "2|30|0", 1L, "2|40|0", 1L, "3|20|0", 1L)
            : Map.of("2|10|0", 1L, "2|30|0", 1L, "3|20|0", 1L);
      case Integer.MAX_VALUE:
        return Map.of(
            "2|10|0", 1L,
            "2|30|0", 1L,
            "2|40|0", backend.equals("hashmap") ? 2L : 1L,
            "2|50|0", 1L,
            "3|20|0", 1L);
      default:
        throw new IllegalArgumentException("No released reference for input cut: " + inputRows);
    }
  }
}
