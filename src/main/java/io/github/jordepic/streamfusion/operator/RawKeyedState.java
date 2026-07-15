package io.github.jordepic.streamfusion.operator;

import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;

/** Raw keyed-state I/O shared by native operators whose hot state stays in Rust. */
final class RawKeyedState {

  private static final int TIMER_FRAME_MAGIC = 0x5354_4654; // "STFT"
  private static final int TIMER_FRAME_BYTES = Integer.BYTES + Long.BYTES;

  private RawKeyedState() {}

  /** Reads every raw keyed-state partition Flink assigned to this subtask after restore/rescale. */
  static List<byte[]> restore(StateInitializationContext context) throws Exception {
    List<byte[]> snapshots = new ArrayList<>();
    for (KeyGroupStatePartitionStreamProvider provider : context.getRawKeyedStateInputs()) {
      try (InputStream in = provider.getStream()) {
        snapshots.add(readPartition(in));
      }
    }
    return snapshots;
  }

  /** Restores native payloads and the latest task cleanup deadline copied into each key group. */
  static TimedRestore restoreWithTimer(StateInitializationContext context) throws Exception {
    List<byte[]> snapshots = new ArrayList<>();
    long deadline = Long.MIN_VALUE;
    for (KeyGroupStatePartitionStreamProvider provider : context.getRawKeyedStateInputs()) {
      try (InputStream in = provider.getStream()) {
        byte[] partition = readPartition(in);
        if (partition.length >= TIMER_FRAME_BYTES
            && ByteBuffer.wrap(partition).getInt() == TIMER_FRAME_MAGIC) {
          ByteBuffer frame = ByteBuffer.wrap(partition);
          frame.getInt();
          deadline = Math.max(deadline, frame.getLong());
          byte[] payload = new byte[frame.remaining()];
          frame.get(payload);
          snapshots.add(payload);
        } else {
          snapshots.add(partition);
        }
      }
    }
    return new TimedRestore(snapshots, deadline);
  }

  /** Writes each non-empty native key group to Flink's corresponding raw keyed-state partition. */
  static void snapshot(
      StateSnapshotContext context, int[] keyGroups, IntFunction<byte[]> snapshotForKeyGroup)
      throws Exception {
    if (keyGroups.length == 0) {
      return;
    }
    KeyedStateCheckpointOutputStream out = context.getRawKeyedOperatorStateOutput();
    for (int keyGroup : keyGroups) {
      if (!out.getKeyGroupList().contains(keyGroup)) {
        throw new IllegalStateException(
            "native state for key group " + keyGroup + " is outside this subtask's Flink range");
      }
      out.startNewKeyGroup(keyGroup);
      byte[] payload = snapshotForKeyGroup.apply(keyGroup);
      writeLength(out, payload.length);
      out.write(payload);
    }
    out.close();
  }

  /** Writes key-group payloads framed by native code as a big-endian id followed by state bytes. */
  static void snapshotPartitions(StateSnapshotContext context, byte[][] partitions)
      throws Exception {
    if (partitions.length == 0) {
      return;
    }
    KeyedStateCheckpointOutputStream out = context.getRawKeyedOperatorStateOutput();
    for (byte[] partition : partitions) {
      if (partition.length < Integer.BYTES) {
        throw new IllegalStateException("native keyed-state partition has no key-group id");
      }
      int keyGroup = ByteBuffer.wrap(partition).getInt();
      if (!out.getKeyGroupList().contains(keyGroup)) {
        throw new IllegalStateException(
            "native state for key group " + keyGroup + " is outside this subtask's Flink range");
      }
      out.startNewKeyGroup(keyGroup);
      int payloadLength = partition.length - Integer.BYTES;
      writeLength(out, payloadLength);
      out.write(partition, Integer.BYTES, payloadLength);
    }
    out.close();
  }

  /** Writes one-pass native partitions plus a cleanup deadline into each rescale-safe payload. */
  static void snapshotPartitionsWithTimer(
      StateSnapshotContext context, byte[][] partitions, long deadline) throws Exception {
    if (partitions.length == 0) {
      return;
    }
    KeyedStateCheckpointOutputStream out = context.getRawKeyedOperatorStateOutput();
    for (byte[] partition : partitions) {
      if (partition.length < Integer.BYTES) {
        throw new IllegalStateException("native keyed-state partition has no key-group id");
      }
      int keyGroup = ByteBuffer.wrap(partition).getInt();
      if (!out.getKeyGroupList().contains(keyGroup)) {
        throw new IllegalStateException(
            "native state for key group " + keyGroup + " is outside this subtask's Flink range");
      }
      out.startNewKeyGroup(keyGroup);
      int payloadLength = partition.length - Integer.BYTES;
      writeLength(out, TIMER_FRAME_BYTES + payloadLength);
      ByteBuffer timerFrame = ByteBuffer.allocate(TIMER_FRAME_BYTES);
      timerFrame.putInt(TIMER_FRAME_MAGIC);
      timerFrame.putLong(deadline);
      out.write(timerFrame.array());
      out.write(partition, Integer.BYTES, payloadLength);
    }
    out.close();
  }

  /** Writes a cleanup deadline into every native key-group payload, keeping it rescale-safe. */
  static void snapshotWithTimer(
      StateSnapshotContext context,
      int[] keyGroups,
      long deadline,
      IntFunction<byte[]> snapshotForKeyGroup)
      throws Exception {
    snapshot(
        context,
        keyGroups,
        keyGroup -> {
          byte[] payload = snapshotForKeyGroup.apply(keyGroup);
          ByteBuffer frame = ByteBuffer.allocate(TIMER_FRAME_BYTES + payload.length);
          frame.putInt(TIMER_FRAME_MAGIC);
          frame.putLong(deadline);
          frame.put(payload);
          return frame.array();
        });
  }

  static final class TimedRestore {

    private final List<byte[]> snapshots;
    private final long deadline;

    private TimedRestore(List<byte[]> snapshots, long deadline) {
      this.snapshots = snapshots;
      this.deadline = deadline;
    }

    List<byte[]> snapshots() {
      return snapshots;
    }

    long deadline() {
      return deadline;
    }
  }

  private static byte[] readPartition(InputStream in) throws Exception {
    DataInputStream data = new DataInputStream(in);
    int length = data.readInt();
    if (length < 0) {
      throw new IllegalStateException("native raw keyed-state payload has a negative length");
    }
    byte[] payload = data.readNBytes(length);
    if (payload.length != length) {
      throw new IllegalStateException(
          "native raw keyed-state payload ended before its declared length");
    }
    return payload;
  }

  private static void writeLength(KeyedStateCheckpointOutputStream out, int length) throws Exception {
    out.write(length >>> 24);
    out.write(length >>> 16);
    out.write(length >>> 8);
    out.write(length);
  }
}
