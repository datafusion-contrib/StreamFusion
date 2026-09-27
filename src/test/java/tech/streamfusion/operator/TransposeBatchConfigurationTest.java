package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.planner.NativeConfig;

class TransposeBatchConfigurationTest {
  private static final String KEY = "streamfusion.transpose.batchRows";

  @ParameterizedTest
  @ValueSource(ints = {1, 5, 64})
  void serializedBatchLimitControlsPhysicalEmission(int limit) throws Exception {
    Configuration config = new Configuration();
    config.setString(KEY, Integer.toString(limit));
    var operator =
        new RowDataToArrowOperator(
            RowType.of(new BigIntType()), NativeConfig.transposeBatchRows(config), false, null);
    var copy = InstantiationUtil.clone(operator, getClass().getClassLoader());
    try (var harness = new OneInputStreamOperatorTestHarness<RowData, ArrowBatch>(copy)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      for (long i = 0; i < 13; i++)
        harness.processElement(new StreamRecord<>(GenericRowData.of(i)));
      harness.processWatermark(new Watermark(1));
      List<Integer> sizes = new ArrayList<>();
      List<Long> values = new ArrayList<>();
      while (!harness.getOutput().isEmpty()) {
        Object event = harness.getOutput().poll();
        if (event instanceof StreamRecord<?> record
            && record.getValue() instanceof ArrowBatch batch) {
          try (var root = batch.root()) {
            sizes.add(root.getRowCount());
            var vector = (org.apache.arrow.vector.BigIntVector) root.getVector(0);
            for (int i = 0; i < root.getRowCount(); i++) values.add(vector.get(i));
          }
        }
      }
      assertEquals(
          limit == 1
              ? java.util.Collections.nCopies(13, 1)
              : limit == 5 ? List.of(5, 5, 3) : List.of(13),
          sizes);
      assertEquals(java.util.stream.LongStream.range(0, 13).boxed().toList(), values);
    }
  }

  @Test
  void jobConfigurationOverridesPropertyAndRejectsNonpositiveLimits() {
    String previous = System.getProperty(KEY);
    try {
      System.setProperty(KEY, "7");
      Configuration config = new Configuration();
      assertEquals(7, NativeConfig.transposeBatchRows(config));
      config.setString(KEY, "5");
      assertEquals(5, NativeConfig.transposeBatchRows(config));
      config.setString(KEY, "0");
      assertThrows(IllegalArgumentException.class, () -> NativeConfig.transposeBatchRows(config));
      config.setString(KEY, "-1");
      assertThrows(IllegalArgumentException.class, () -> NativeConfig.transposeBatchRows(config));
      System.clearProperty(KEY);
      assertEquals(1024, NativeConfig.transposeBatchRows(new Configuration()));
    } finally {
      if (previous == null) System.clearProperty(KEY);
      else System.setProperty(KEY, previous);
    }
  }
}
