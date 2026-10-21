package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import org.junit.jupiter.api.Test;

class FlinkMapRemovalOrderContractTest {
  @Test
  void removalFixturesRetainTheReleasedJdkBucketAndTreeOrder() throws Exception {
    try (var input = getClass().getResourceAsStream("/java-map-removal-order.json")) {
      for (var fixture : new com.fasterxml.jackson.databind.ObjectMapper().readTree(input)) {
        var map = new HashMap<Key, Integer>();
        for (var step : fixture.get("steps")) {
          int spread = step.get("hash").asInt();
          var key = new Key(step.get("key").asLong(), spread ^ (spread >>> 16));
          if (step.get("op").asText().equals("put")) map.put(key, 1);
          else assertNotNull(map.remove(key));
          var actual = new ArrayList<Long>();
          map.keySet().forEach(value -> actual.add(value.value));
          var expected = new ArrayList<Long>();
          step.get("order").forEach(value -> expected.add(value.asLong()));
          assertEquals(expected, actual, fixture.get("name") + ": " + step);
        }
      }
    }
  }

  private static final class Key implements Comparable<Key> {
    private final long value;
    private final int hash;

    Key(long value, int hash) {
      this.value = value;
      this.hash = hash;
    }

    public int hashCode() {
      return hash;
    }

    public boolean equals(Object other) {
      return other instanceof Key && ((Key) other).value == value;
    }

    public int compareTo(Key other) {
      return Long.compare(value, other.value);
    }
  }
}
