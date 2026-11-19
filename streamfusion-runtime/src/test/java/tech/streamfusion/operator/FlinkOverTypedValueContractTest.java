package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.functions.aggregate.FirstValueAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.LastValueAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.MaxWithRetractAggFunction;
import org.apache.flink.table.runtime.functions.aggregate.MinWithRetractAggFunction;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class FlinkOverTypedValueContractTest {
  @ParameterizedTest
  @CsvSource({"12,2", "38,0", "38,38"})
  void releasedDecimalValueFunctionsPreserveNullsDuplicatesAndInputScale(int precision, int scale)
      throws Exception {
    var first = DecimalData.fromBigDecimal(BigDecimal.valueOf(250, scale), precision, scale);
    var min = DecimalData.fromBigDecimal(BigDecimal.valueOf(-100, scale), precision, scale);
    var last = DecimalData.fromBigDecimal(BigDecimal.valueOf(125, scale), precision, scale);
    check(
        new DecimalType(precision, scale),
        Arrays.asList(null, first, min, min, null, last),
        min,
        first,
        first,
        last);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void releasedTemporalValueFunctionsUseMillisecondAndNanosecondOrdering(boolean ltz)
      throws Exception {
    var first = TimestampData.fromEpochMillis(0, 2);
    var min = TimestampData.fromEpochMillis(-62135596800000L, 1);
    var last = TimestampData.fromEpochMillis(253402300799999L, 999999);
    check(
        ltz ? new LocalZonedTimestampType(9) : new TimestampType(9),
        Arrays.asList(null, first, min, min, null, last),
        min,
        last,
        first,
        last);
  }

  private static <T extends Comparable<T>> void check(
      LogicalType type,
      List<T> values,
      T expectedMin,
      T expectedMax,
      T expectedFirst,
      T expectedLast)
      throws Exception {
    var min = new MinWithRetractAggFunction<T>(type);
    var max = new MaxWithRetractAggFunction<T>(type);
    var first = new FirstValueAggFunction<T>(type);
    var last = new LastValueAggFunction<T>(type);
    var minAcc = min.createAccumulator();
    var maxAcc = max.createAccumulator();
    var firstAcc = first.createAccumulator();
    var lastAcc = last.createAccumulator();
    assertNull(min.getValue(minAcc));
    assertNull(max.getValue(maxAcc));
    assertNull(first.getValue(firstAcc));
    assertNull(last.getValue(lastAcc));
    for (T value : values) {
      min.accumulate(minAcc, value);
      max.accumulate(maxAcc, value);
      first.accumulate(firstAcc, value);
      last.accumulate(lastAcc, value);
    }
    assertEquals(expectedMin, min.getValue(minAcc));
    assertEquals(expectedMax, max.getValue(maxAcc));
    assertEquals(expectedFirst, first.getValue(firstAcc));
    assertEquals(expectedLast, last.getValue(lastAcc));
    min.retract(minAcc, expectedMin);
    assertEquals(expectedMin, min.getValue(minAcc));
    for (T value : values) max.retract(maxAcc, value);
    assertNull(max.getValue(maxAcc));
  }
}
