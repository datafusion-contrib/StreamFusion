package tech.streamfusion.operator;

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

  static final int TIMER_FRAME_MAGIC = 0x5354_4654; // "STFT"
  static final int TIMER_FRAME_BYTES = Integer.BYTES + Long.BYTES;

  /**
   * Every key-group payload is prefixed with a versioned header (Flink's own versioned-snapshot
   * discipline, {@code TypeSerializerSnapshot#writeVersionedSnapshot}), so a payload written by a
   * release with a different native snapshot layout fails restore with the writer's version named
   * instead of misparsing garbage: magic (8 bytes), the state-format version (4), and a reserved
   * length-prefixed operator-config fingerprint (4 + n, empty at version 1 — room for
   * https://github.com/datafusion-contrib/StreamFusion/issues/22 to bind a payload to the operator
   * configuration that wrote it).
   *
   * <p>Pre-header payloads carry no stamp, so restore detects the header by magic and treats
   * anything else as version 0 (the legacy layout, still readable). The magic bytes are
   * {@code 53 46 53 81 52 4B 53 90} ("SFS"+0x81, "RKS"+0x90), chosen so no version-0 payload can
   * begin with them: every legacy payload starts with either the "STFT" timer frame (differs in
   * byte 1), an Arrow IPC stream (first byte 0xFF), a little-endian u32 section length whose
   * fourth byte is at most 0x7F (lengths never exceed Integer.MAX_VALUE, and byte 3 here is 0x81),
   * or a little-endian i64 that is a watermark or an arrival counter — read that way the magic is
   * a large negative number, never a counter (they start at zero and grow) and not a watermark
   * Flink emits (Long.MIN_VALUE, Long.MAX_VALUE, or a real event time).
   */
  static final long STATE_MAGIC = 0x53465381_524B5390L;

  static final int STATE_FORMAT_VERSION = 1;
  private static final int STATE_HEADER_BYTES = Long.BYTES + Integer.BYTES + Integer.BYTES;

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
      writeLength(out, STATE_HEADER_BYTES + payload.length);
      writeHeader(out);
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
      writeLength(out, STATE_HEADER_BYTES + payloadLength);
      writeHeader(out);
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
      writeLength(out, STATE_HEADER_BYTES + TIMER_FRAME_BYTES + payloadLength);
      writeHeader(out);
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
    return stripHeader(payload);
  }

  /** Peels the versioned header off a payload; a payload without one restores as version 0. */
  private static byte[] stripHeader(byte[] payload) {
    if (payload.length < STATE_HEADER_BYTES || ByteBuffer.wrap(payload).getLong() != STATE_MAGIC) {
      return payload;
    }
    ByteBuffer header = ByteBuffer.wrap(payload);
    header.getLong();
    int version = header.getInt();
    if (version <= 0 || version > STATE_FORMAT_VERSION) {
      throw new IllegalStateException(
          "native raw keyed-state snapshot was written as state-format version "
              + version
              + ", but this StreamFusion build reads versions up to "
              + STATE_FORMAT_VERSION
              + ": restore with the StreamFusion release that wrote the snapshot, or drain the job"
              + " and start fresh (docs/coverage-and-fallbacks.md, state backend section)");
    }
    int fingerprintLength = header.getInt();
    if (fingerprintLength < 0 || fingerprintLength > header.remaining()) {
      throw new IllegalStateException(
          "native raw keyed-state header declares fingerprint length "
              + fingerprintLength
              + " with only "
              + header.remaining()
              + " bytes remaining");
    }
    header.position(header.position() + fingerprintLength);
    byte[] inner = new byte[header.remaining()];
    header.get(inner);
    return inner;
  }

  private static void writeHeader(KeyedStateCheckpointOutputStream out) throws Exception {
    ByteBuffer header = ByteBuffer.allocate(STATE_HEADER_BYTES);
    header.putLong(STATE_MAGIC);
    header.putInt(STATE_FORMAT_VERSION);
    header.putInt(0);
    out.write(header.array());
  }

  private static void writeLength(KeyedStateCheckpointOutputStream out, int length) throws Exception {
    out.write(length >>> 24);
    out.write(length >>> 16);
    out.write(length >>> 8);
    out.write(length);
  }
}
