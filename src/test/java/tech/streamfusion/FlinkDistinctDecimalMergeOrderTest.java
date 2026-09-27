package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.util.Map;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.runtime.typeutils.DecimalDataSerializer;
import org.junit.jupiter.api.Test;

class FlinkDistinctDecimalMergeOrderTest {
  @Test
  void copyingOrSerializingLocalMembershipChangesOverflowOrder() throws Exception {
    BigDecimal large = BigDecimal.TEN.pow(37).multiply(BigDecimal.valueOf(9));
    var view = new MapView<DecimalData, Long>();
    for (BigDecimal value :
        new BigDecimal[] {large, large.subtract(BigDecimal.valueOf(3)), large.negate()}) {
      view.put(DecimalData.fromBigDecimal(value, 38, 0), 1L);
    }
    var serializer = new MapSerializer<>(new DecimalDataSerializer(38, 0), LongSerializer.INSTANCE);
    Map<DecimalData, Long> original = view.getMap();
    Map<DecimalData, Long> copied = serializer.copy(original);
    var output = new DataOutputSerializer(128);
    serializer.serialize(original, output);
    Map<DecimalData, Long> restored =
        serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));
    assertEquals(original, copied);
    assertEquals(original, restored);
    assertEquals(large.negate(), foldSum(original));
    assertEquals(large.subtract(BigDecimal.valueOf(3)), foldSum(copied));
    assertEquals(foldSum(copied), foldSum(restored));
    assertNull(foldAverageSum(original));
    assertEquals(large.subtract(BigDecimal.valueOf(3)), foldAverageSum(copied));
    assertEquals(foldAverageSum(copied), foldAverageSum(restored));
  }

  @Test
  void bufferingSingletonViewsBeforeMergingChangesOverflowOrder() throws Exception {
    BigDecimal large = BigDecimal.TEN.pow(37).multiply(BigDecimal.valueOf(9));
    var arrivals = new java.util.LinkedHashMap<DecimalData, Long>();
    var buffer = new MapView<DecimalData, Long>();
    for (BigDecimal value :
        new BigDecimal[] {large, large.negate(), large.subtract(BigDecimal.valueOf(3))}) {
      DecimalData decimal = DecimalData.fromBigDecimal(value, 38, 0);
      arrivals.put(decimal, 1L);
      var local = new MapView<DecimalData, Long>();
      local.put(decimal, 1L);
      for (var entry : local.entries()) buffer.put(entry.getKey(), entry.getValue());
    }
    assertEquals(arrivals, buffer.getMap());
    assertEquals(large.subtract(BigDecimal.valueOf(3)), foldSum(arrivals));
    assertEquals(large.negate(), foldSum(buffer.getMap()));
    assertEquals(large.subtract(BigDecimal.valueOf(3)), foldAverageSum(arrivals));
    assertNull(foldAverageSum(buffer.getMap()));
  }

  private static BigDecimal foldSum(Map<DecimalData, Long> values) {
    DecimalData sum = null;
    for (DecimalData value : values.keySet()) {
      sum = sum == null ? value : DecimalDataUtils.add(sum, value, 38, 0);
    }
    return sum == null ? null : sum.toBigDecimal();
  }

  private static BigDecimal foldAverageSum(Map<DecimalData, Long> values) {
    DecimalData sum = DecimalData.fromBigDecimal(BigDecimal.ZERO, 38, 0);
    for (DecimalData value : values.keySet()) {
      if (sum != null) sum = DecimalDataUtils.add(sum, value, 38, 0);
    }
    return sum == null ? null : sum.toBigDecimal();
  }
}
