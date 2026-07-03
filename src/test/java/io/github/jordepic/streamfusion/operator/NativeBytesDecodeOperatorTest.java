package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

/**
 * The bytes-decode operator's flush triggers beyond batch size: the processing-time flush (a stream
 * running below the batch size waits at most the interval) and the pre-barrier flush (buffered bytes
 * are records the source's checkpoint already covers — unflushed they would be lost on restore).
 */
class NativeBytesDecodeOperatorTest {

  private static final RowType OUTPUT =
      RowType.of(new LogicalType[] {new BigIntType()}, new String[] {"id"});
  private static final int BATCH_SIZE = 100;
  private static final long FLUSH_INTERVAL = 50;

  @Test
  void timerFlushesPartialBatch() throws Exception {
    try (OneInputStreamOperatorTestHarness<byte[], ArrowBatch> harness = harness()) {
      harness.setProcessingTime(0);
      harness.processElement(record("{\"id\": 1}"));
      harness.processElement(record("{\"id\": 2}"));
      assertTrue(harness.getOutput().isEmpty(), "below batch size: nothing flushed yet");

      harness.setProcessingTime(FLUSH_INTERVAL);
      assertEquals(List.of(1L, 2L), collectIds(harness));

      // The timer re-arms per batch: a later record starts a new clock and flushes on it.
      harness.processElement(record("{\"id\": 3}"));
      harness.setProcessingTime(2 * FLUSH_INTERVAL + 1);
      assertEquals(List.of(3L), collectIds(harness));
    }
  }

  @Test
  void preBarrierFlushesPartialBatch() throws Exception {
    try (OneInputStreamOperatorTestHarness<byte[], ArrowBatch> harness = harness()) {
      harness.processElement(record("{\"id\": 7}"));
      // What the checkpoint barrier drives in a task, right before the operator's snapshot.
      harness.prepareSnapshotPreBarrier(1L);
      assertEquals(List.of(7L), collectIds(harness));
    }
  }

  private static OneInputStreamOperatorTestHarness<byte[], ArrowBatch> harness() throws Exception {
    OneInputStreamOperatorTestHarness<byte[], ArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(
            new NativeBytesDecodeOperator(
                OUTPUT, BATCH_SIZE, 0, "", "", 0, null, null, null, false, FLUSH_INTERVAL),
            BytePrimitiveArraySerializer.INSTANCE);
    harness.setup(new ArrowBatchSerializer());
    harness.open();
    return harness;
  }

  private static StreamRecord<byte[]> record(String json) {
    return new StreamRecord<>(json.getBytes(StandardCharsets.UTF_8));
  }

  private static List<Long> collectIds(
      OneInputStreamOperatorTestHarness<byte[], ArrowBatch> harness) {
    List<Long> ids = new ArrayList<>();
    while (!harness.getOutput().isEmpty()) {
      Object event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (VectorSchemaRoot root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (RowData row : RowDataArrowConverter.read(root, OUTPUT)) {
            ids.add(row.getLong(0));
          }
        }
      }
    }
    return ids;
  }
}
