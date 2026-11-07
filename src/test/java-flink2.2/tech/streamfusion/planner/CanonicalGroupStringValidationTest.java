package tech.streamfusion.planner;

import java.util.stream.Stream;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.params.provider.Arguments;

class CanonicalGroupStringValidationTest extends CanonicalGroupTypedValidationTest {
  static Stream<Arguments> cases() {
    return CanonicalGroupTypedValidationTest.cases().filter(test -> (boolean) test.get()[0]);
  }

  @Override
  TypedGroupStockFixture fixture(boolean append, boolean rocks) throws Exception {
    return new TypedGroupStockFixture("STRING", true, rocks, true);
  }

  @Override
  RowData seed(String key) {
    return org.apache.flink.table.data.GenericRowData.of(
        org.apache.flink.table.data.StringData.fromString(key),
        org.apache.flink.table.data.TimestampData.fromEpochMillis(0),
        org.apache.flink.table.data.StringData.fromString("\uD800\uDC00"),
        org.apache.flink.table.data.StringData.fromString("member"),
        true);
  }
}
