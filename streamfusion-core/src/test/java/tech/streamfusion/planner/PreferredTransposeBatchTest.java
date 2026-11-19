package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

class PreferredTransposeBatchTest {
  @Test
  void preferredRowsApplyOnlyWhenNoUserLimitExists() {
    String key = "streamfusion.transpose.batchRows";
    String previous = System.getProperty(key);
    try {
      System.clearProperty(key);
      Configuration config = new Configuration();
      assertEquals(4096, NativeConfig.transposeBatchRows(config, 4096));
      assertEquals(1024, NativeConfig.transposeBatchRows(config));
      System.setProperty(key, "7");
      assertEquals(7, NativeConfig.transposeBatchRows(config, 4096));
      config.setString(key, "5");
      assertEquals(5, NativeConfig.transposeBatchRows(config, 4096));
      config.setString(key, "0");
      assertThrows(
          IllegalArgumentException.class, () -> NativeConfig.transposeBatchRows(config, 4096));
    } finally {
      if (previous == null) System.clearProperty(key);
      else System.setProperty(key, previous);
    }
  }
}
