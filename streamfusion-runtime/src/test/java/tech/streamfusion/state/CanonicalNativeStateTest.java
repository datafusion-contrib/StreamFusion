package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class CanonicalNativeStateTest {
  static final String OPERATOR = "chunk-copy-test";

  static final class Operator extends AbstractStreamOperator<Integer>
      implements OneInputStreamOperator<Integer, Integer> {
    public void processElement(StreamRecord<Integer> record) {}

    CheckpointableKeyedStateBackend<?> backend() {
      return (CheckpointableKeyedStateBackend<?>) getKeyedStateBackend();
    }
  }

  static KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> harness(Operator op)
      throws Exception {
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(op, value -> value, Types.INT, 128, 1, 0);
    harness.setStateBackend(new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
    harness.setCheckpointStorage(
        new org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage(32 * 1024 * 1024));
    return harness;
  }

  static byte[] partition(int keyGroup, int bytes) {
    byte[] partition = new byte[Integer.BYTES + bytes];
    ByteBuffer.wrap(partition).putInt(keyGroup);
    for (int i = Integer.BYTES; i < partition.length; i++)
      partition[i] = (byte) (i * 31 + keyGroup);
    return partition;
  }

  @Test
  void chunkBoundariesRemainIndependentAndRecoverAfterReplacement() throws Exception {
    int chunk = CanonicalNativeState.CHUNK_BYTES;
    int[] sizes = {0, 1, chunk - 1, chunk, chunk + 1, chunk * 2 + 13};
    byte[][] partitions =
        IntStream.range(0, sizes.length)
            .mapToObj(i -> partition(i, sizes[i]))
            .toArray(byte[][]::new);
    List<byte[]> expected =
        Arrays.stream(partitions)
            .map(p -> Arrays.copyOfRange(p, Integer.BYTES, p.length))
            .collect(java.util.stream.Collectors.toList());
    OperatorSubtaskState snapshot;
    var writer = new Operator();
    try (var harness = harness(writer)) {
      harness.open();
      CanonicalNativeState.write(writer.backend(), partitions, OPERATOR, 12345);
      for (byte[] partition : partitions) Arrays.fill(partition, (byte) 0);
      snapshot = harness.snapshot(1, 1);
    }
    var reader = new Operator();
    try (var harness = harness(reader)) {
      harness.initializeState(snapshot);
      harness.open();
      var restored = CanonicalNativeState.readAndClear(reader.backend(), OPERATOR);
      assertEquals(12345, restored.timerDeadline);
      assertEquals(expected.size(), restored.partitions.size());
      for (int i = 0; i < expected.size(); i++)
        assertArrayEquals(expected.get(i), restored.partitions.get(i));
      assertTrue(
          CanonicalNativeState.readAndClear(reader.backend(), OPERATOR).partitions.isEmpty());
      CanonicalNativeState.write(
          reader.backend(), new byte[][] {partition(0, chunk * 2 + 1)}, OPERATOR, 10);
      CanonicalNativeState.write(reader.backend(), new byte[][] {partition(0, 3)}, OPERATOR, 20);
      var replaced = CanonicalNativeState.readAndClear(reader.backend(), OPERATOR);
      assertEquals(20, replaced.timerDeadline);
      assertArrayEquals(Arrays.copyOfRange(partition(0, 3), 4, 7), replaced.partitions.get(0));
    }
  }
}
