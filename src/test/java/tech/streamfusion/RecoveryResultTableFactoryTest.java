package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.apache.flink.table.api.DataTypes;
import org.junit.jupiter.api.Test;

class RecoveryResultTableFactoryTest {
  @Test
  void cancellationBeforeInitializationNeedsNoCollectorResources() {
    var sink = new RecoveryResultTableFactory.CaptureFunction(
        DataTypes.ROW(DataTypes.FIELD("value", DataTypes.BIGINT())));
    assertDoesNotThrow(sink::close);
    assertDoesNotThrow(sink::close);
  }
}
