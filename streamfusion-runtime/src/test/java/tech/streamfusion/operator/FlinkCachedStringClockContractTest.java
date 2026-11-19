package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.functions.aggregate.FirstValueWithRetractAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.MinWithRetractAggFunction;
import org.apache.flink.table.runtime.typeutils.StringDataSerializer;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkCachedStringClockContractTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void implicitClockGroupsCanChangeFirstValueForMalformedCachedAliases(boolean sameClock)
      throws Exception {
    var function = new FirstValueWithRetractAggFunction<StringData>(new VarCharType());
    var accumulator = function.createAccumulator();
    var first = BinaryStringData.fromString("\ud800");
    var second = BinaryStringData.fromString("\ud801");
    var binary = BinaryStringData.fromBytes("?".getBytes(StandardCharsets.UTF_8));
    StringData[] values = {first, second, BinaryStringData.fromString("tail"), binary};
    for (int i = 0; i < values.length; i++) {
      long order = sameClock ? 0 : i;
      var orders = accumulator.valueToOrderMap.get(values[i]);
      if (orders == null) orders = new ArrayList<>();
      orders.add(order);
      accumulator.valueToOrderMap.put(values[i], orders);
      function.accumulate(accumulator, values[i], order);
    }
    function.retract(accumulator, first);
    function.retract(accumulator, binary);
    var result = (BinaryStringData) function.getValue(accumulator);
    assertEquals(sameClock ? "tail" : "\ud801", result.getJavaObject());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void serializedDuplicateKeysMakeRestoredMinDependOnMapIteration(boolean reverse)
      throws Exception {
    var entries = new LinkedHashMap<StringData, Long>();
    var first = BinaryStringData.fromString("\ud800");
    var second = BinaryStringData.fromString("\ud801");
    entries.put(reverse ? second : first, reverse ? 2L : 1L);
    entries.put(reverse ? first : second, reverse ? 1L : 2L);
    entries.put(BinaryStringData.fromString("tail"), 1L);
    var serializer = new MapSerializer<>(StringDataSerializer.INSTANCE, LongSerializer.INSTANCE);
    var output = new DataOutputSerializer(128);
    serializer.serialize(entries, output);
    var function = new MinWithRetractAggFunction<StringData>(new VarCharType());
    var accumulator = function.createAccumulator();
    accumulator.min = BinaryStringData.fromBytes(new byte[] {'?'});
    accumulator.mapSize = 3L;
    accumulator.map.setMap(
        serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer())));
    function.retract(accumulator, BinaryStringData.fromBytes(new byte[] {'?'}));
    assertEquals(reverse ? "tail" : "?", function.getValue(accumulator).toString());
  }
}
