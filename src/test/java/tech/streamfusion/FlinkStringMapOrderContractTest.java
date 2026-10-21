package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.dataview.NullAwareMapSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.StringDataSerializer;
import org.junit.jupiter.api.Test;

class FlinkStringMapOrderContractTest {
  @Test
  void stringAndCompositeKeysMatchReleasedMapCopiesAndRemovals() throws Exception {
    try (var input = getClass().getResourceAsStream("/string-map-order.json")) {
      for (var fixture : new ObjectMapper().readTree(input)) {
        var actual = evaluate(fixture);
        assertEquals(fixture.get("hashes"), actual.get("hashes"), fixture.toString());
        assertEquals(fixture.get("order"), actual.get("order"), fixture.toString());
      }
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  static ObjectNode evaluate(JsonNode fixture) {
    var result = new ObjectMapper().createObjectNode();
    var hashes = result.putArray("hashes");
    var output = result.putArray("order");
    boolean composite = !fixture.get("separator").isNull();
    TypeSerializer keySerializer =
        composite ? new BinaryRowDataSerializer(2) : StringDataSerializer.INSTANCE;
    var serializer =
        new NullAwareMapSerializer<Object, Long>(keySerializer, LongSerializer.INSTANCE);
    Map<Object, Long> map = new HashMap<>();
    int position = 0;
    for (var operation : fixture.get("operations")) {
      if (position++ == fixture.get("copy_after").asInt()) map = serializer.copy(map);
      var value = operation.get("value");
      Object key = value.isNull() ? null : StringData.fromString(value.asText());
      if (composite) {
        var row = new BinaryRowData(2);
        var writer = new BinaryRowWriter(row);
        if (key == null) writer.setNullAt(0);
        else writer.writeString(0, (StringData) key);
        writer.writeString(1, StringData.fromString(fixture.get("separator").asText()));
        writer.complete();
        key = row;
      }
      int hash = key == null ? 0 : key.hashCode();
      hashes.add(hash ^ (hash >>> 16));
      if (operation.get("remove").asBoolean()) map.remove(key);
      else map.put(key, 1L);
    }
    for (Object key : map.keySet()) {
      StringData value = composite ? ((BinaryRowData) key).getString(0) : (StringData) key;
      if (key == null || composite && ((BinaryRowData) key).isNullAt(0)) output.addNull();
      else output.add(value.toString());
    }
    return result;
  }
}
