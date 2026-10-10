package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.flink.table.api.JsonValueOnEmptyOrError;
import org.apache.flink.table.runtime.functions.SqlJsonUtils;
import org.junit.jupiter.api.Test;
import tech.streamfusion.compat.ListCollectors;

class NativeJsonDoubleTest {
  @Test
  void nativeDoubleBitsMatchTheHostProvider() {
    List<String> numbers =
        new ArrayList<>(
            List.of(
                "-0.0",
                "-0e2147483647",
                "1e309",
                "-1e-400",
                "5e-324",
                "2.4703282292062327e-324",
                "1.7976931348623157e308",
                "1.7976931348623159e308",
                "1.00000000000000011102230246251565404236316680908203125",
                "1e+" + "0".repeat(800) + "1"));
    Random random = new Random(721903);
    for (int i = 0; i < 2000; i++) {
      numbers.add(
          random.nextLong()
              + "."
              + (random.nextInt(900000) + 100000)
              + "e"
              + (random.nextInt(800) - 400));
    }
    List<String> documents =
        numbers.stream().map(number -> "{\"n\":" + number + "}").collect(ListCollectors.toList());
    List<String> actual =
        NativeJsonBufferHistoryTest.nativeRows(
            documents,
            150,
            new String[] {
              "strict $.n", "NULL", null, "NULL", null, NativeJsonBufferHistoryTest.unicodeVersion()
            },
            127);
    for (int i = 0; i < numbers.size(); i++) {
      Object value =
          SqlJsonUtils.jsonValue(
              documents.get(i),
              "$.n",
              JsonValueOnEmptyOrError.NULL,
              null,
              JsonValueOnEmptyOrError.NULL,
              null);
      Double expected = value == null ? null : ((Number) value).doubleValue();
      if (expected == null) {
        assertEquals(null, actual.get(i), numbers.get(i));
      } else {
        assertEquals(
            Double.doubleToRawLongBits(expected),
            Double.doubleToRawLongBits(Double.parseDouble(actual.get(i))),
            numbers.get(i));
      }
    }
  }
}
