package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.table.dataview.NullAwareMapSerializer;
import org.junit.jupiter.api.Test;

class FlinkFloatingMapOrderContractTest {
  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void numericOrderingFixturesMatchReleasedMapSerializers() throws Exception {
    try (var input = getClass().getResourceAsStream("/floating-map-order.json")) {
      for (var fixture : new com.fasterxml.jackson.databind.ObjectMapper().readTree(input)) {
        boolean single = fixture.get("type").asText().equals("FLOAT");
        TypeSerializer keySerializer =
            single ? FloatSerializer.INSTANCE : DoubleSerializer.INSTANCE;
        var serializer =
            new NullAwareMapSerializer<Number, Long>(keySerializer, LongSerializer.INSTANCE);
        Map<Number, Long> map = new HashMap<>();
        var keys = fixture.get("keys");
        for (int position = 0; position <= keys.size(); position++) {
          if (position == fixture.get("copy_after").asInt()) map = serializer.copy(map);
          if (position < keys.size()) {
            var key = keys.get(position);
            Number value = null;
            if (!key.isNull()) {
              long bits = Long.parseUnsignedLong(key.asText(), 16);
              if (single) value = Float.intBitsToFloat((int) bits);
              else value = Double.longBitsToDouble(bits);
            }
            map.put(value, 1L);
          }
        }
        var actual = new ArrayList<String>();
        for (Number value : map.keySet()) {
          actual.add(
              value == null
                  ? null
                  : single
                      ? Integer.toUnsignedString(Float.floatToRawIntBits(value.floatValue()), 16)
                      : Long.toUnsignedString(Double.doubleToRawLongBits(value.doubleValue()), 16));
        }
        var expected = new ArrayList<String>();
        fixture.get("order").forEach(bits -> expected.add(bits.isNull() ? null : bits.asText()));
        assertEquals(
            expected, actual, fixture.get("type") + " copy_after=" + fixture.get("copy_after"));
      }
    }
  }
}
