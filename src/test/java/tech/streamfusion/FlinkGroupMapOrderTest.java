package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.junit.jupiter.api.Test;

class FlinkGroupMapOrderTest {
  @Test
  void nativeGroupOrderFixturesMatchReleasedBinaryRowKeys() throws Exception {
    try (var input = getClass().getResourceAsStream("/group-map-order.json")) {
      var fixtures = new ObjectMapper().readTree(input);
      for (var fixture : fixtures) {
        var map = new HashMap<BinaryRowData, String>();
        var hashes = new HashMap<Integer, String>();
        int index = 0;
        for (var value : fixture.get("values")) {
          if (fixture.has("clear_after") && index == fixture.get("clear_after").asInt())
            map.clear();
          var row = new BinaryRowData(1);
          var writer = new BinaryRowWriter(row);
          if (value.isNull()) writer.setNullAt(0);
          else writer.writeLong(0, Long.parseLong(value.asText()));
          writer.complete();
          int hash = row.hashCode();
          hash ^= hash >>> 16;
          assertEquals(fixture.get("hashes").get(index++).asInt(), hash);
          String scalar = value.isNull() ? null : value.asText();
          // Equal-hash binary keys use JVM identity to break tree ties, a separate contract.
          if (hashes.containsKey(hash)) assertEquals(hashes.get(hash), scalar);
          hashes.put(hash, scalar);
          map.put(row, scalar);
        }
        var expected = new ArrayList<String>();
        fixture.get("order").forEach(v -> expected.add(v.isNull() ? null : v.asText()));
        assertEquals(expected, new ArrayList<>(map.values()), fixture.get("name").asText());
      }
    }
  }
}
