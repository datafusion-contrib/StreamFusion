package tech.streamfusion.state;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32C;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;

/** Backend-independent representation of native state inside a Flink canonical savepoint. */
public final class CanonicalNativeState {

  static final String STATE_NAME = "__streamfusion_canonical_native_state_v1";
  static final int FORMAT_VERSION = 1;
  static final int CHUNK_BYTES = 4 * 1024 * 1024;

  private static final int MAGIC = 0x53464353; // SFCS
  private static final ValueStateDescriptor<byte[]> HEADER_DESCRIPTOR = descriptor(STATE_NAME);

  private CanonicalNativeState() {}

  public static void write(
      CheckpointableKeyedStateBackend<?> backend,
      byte[][] partitions,
      String operatorId,
      long timerDeadline)
      throws Exception {
    Object previousKey = backend.getCurrentKey();
    try {
      for (int keyGroup : backend.getKeyGroupRange()) {
        ValueState<byte[]> headerState = stateForKeyGroup(backend, keyGroup, HEADER_DESCRIPTOR);
        byte[] previous = headerState.value();
        if (previous != null) {
          Header old = decodeHeader(previous, operatorId);
          for (int chunk = 0; chunk < old.chunks; chunk++) {
            stateForKeyGroup(backend, keyGroup, chunkDescriptor(chunk)).clear();
          }
        }
        headerState.clear();
      }
      for (byte[] partition : partitions) {
        if (partition.length < Integer.BYTES) {
          throw new IllegalStateException("canonical native partition has no key-group id");
        }
        int keyGroup = ByteBuffer.wrap(partition).getInt();
        requireOwned(backend.getKeyGroupRange(), keyGroup);
        byte[] payload = Arrays.copyOfRange(partition, Integer.BYTES, partition.length);
        int chunks = (payload.length + CHUNK_BYTES - 1) / CHUNK_BYTES;
        stateForKeyGroup(backend, keyGroup, HEADER_DESCRIPTOR)
            .update(header(operatorId, timerDeadline, chunks, payload));
        for (int chunk = 0; chunk < chunks; chunk++) {
          int start = chunk * CHUNK_BYTES;
          stateForKeyGroup(backend, keyGroup, chunkDescriptor(chunk))
              .update(
                  Arrays.copyOfRange(
                      payload, start, Math.min(payload.length, start + CHUNK_BYTES)));
        }
      }
    } finally {
      restoreKey(backend, previousKey);
    }
  }

  public static Restore readAndClear(
      CheckpointableKeyedStateBackend<?> backend, String operatorId)
      throws Exception {
    Object previousKey = backend.getCurrentKey();
    List<byte[]> partitions = new ArrayList<>();
    long timerDeadline = Long.MIN_VALUE;
    try {
      for (int keyGroup : backend.getKeyGroupRange()) {
        ValueState<byte[]> headerState = stateForKeyGroup(backend, keyGroup, HEADER_DESCRIPTOR);
        byte[] header = headerState.value();
        if (header == null) {
          continue;
        }
        Header decoded = decodeHeader(header, operatorId);
        byte[] partition = new byte[decoded.payloadBytes];
        int offset = 0;
        for (int chunk = 0; chunk < decoded.chunks; chunk++) {
          ValueState<byte[]> chunkState =
              stateForKeyGroup(backend, keyGroup, chunkDescriptor(chunk));
          byte[] bytes = chunkState.value();
          if (bytes == null || offset + bytes.length > partition.length) {
            throw new IllegalStateException(
                "canonical native state for key group "
                    + keyGroup
                    + " has missing or invalid chunk "
                    + chunk
                    + " (chunk bytes="
                    + (bytes == null ? "missing" : bytes.length)
                    + ", payload bytes="
                    + partition.length
                    + ", offset="
                    + offset
                    + ")");
          }
          System.arraycopy(bytes, 0, partition, offset, bytes.length);
          offset += bytes.length;
          chunkState.clear();
        }
        if (offset != partition.length
            || checksum(partition, 0, decoded.payloadBytes) != decoded.checksum) {
          throw new IllegalStateException(
              "canonical native state checksum mismatch for key group " + keyGroup);
        }
        partitions.add(partition);
        timerDeadline = Math.max(timerDeadline, decoded.timerDeadline);
        headerState.clear();
      }
    } finally {
      restoreKey(backend, previousKey);
    }
    return new Restore(partitions, timerDeadline);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static ValueState<byte[]> stateForKeyGroup(
      CheckpointableKeyedStateBackend<?> backend,
      int keyGroup,
      ValueStateDescriptor<byte[]> descriptor)
      throws Exception {
    CheckpointableKeyedStateBackend raw = backend;
    raw.setCurrentKeyAndKeyGroup(keyGroup, keyGroup);
    return (ValueState<byte[]>)
        raw.getPartitionedState(
            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);
  }

  private static ValueStateDescriptor<byte[]> chunkDescriptor(int chunk) {
    return descriptor(STATE_NAME + "_chunk_" + chunk);
  }

  private static ValueStateDescriptor<byte[]> descriptor(String name) {
    return new ValueStateDescriptor<>(name, BytePrimitiveArraySerializer.INSTANCE);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void restoreKey(CheckpointableKeyedStateBackend<?> backend, Object previousKey) {
    if (previousKey != null) {
      ((CheckpointableKeyedStateBackend) backend).setCurrentKey(previousKey);
    }
  }

  private static byte[] header(String operatorId, long timerDeadline, int chunks, byte[] payload) {
    byte[] operator = operatorId.getBytes(StandardCharsets.UTF_8);
    ByteBuffer header = ByteBuffer.allocate(4 + 4 + 4 + operator.length + 8 + 4 + 4 + 4);
    header.putInt(MAGIC).putInt(FORMAT_VERSION).putInt(operator.length).put(operator);
    header.putLong(timerDeadline).putInt(chunks).putInt(payload.length);
    header.putInt(checksum(payload, 0, payload.length));
    return header.array();
  }

  private static Header decodeHeader(byte[] bytes, String expectedOperator) {
    ByteBuffer in = ByteBuffer.wrap(bytes);
    if (in.remaining() < 32 || in.getInt() != MAGIC) {
      throw new IllegalStateException("invalid StreamFusion canonical state header");
    }
    int version = in.getInt();
    if (version != FORMAT_VERSION) {
      throw new IllegalStateException(
          "unsupported StreamFusion canonical state version "
              + version
              + "; this build reads "
              + FORMAT_VERSION);
    }
    int operatorBytes = in.getInt();
    if (operatorBytes < 0 || operatorBytes > in.remaining() - 20) {
      throw new IllegalStateException("invalid operator identifier in canonical state header");
    }
    byte[] operator = new byte[operatorBytes];
    in.get(operator);
    String actualOperator = new String(operator, StandardCharsets.UTF_8);
    if (!expectedOperator.equals(actualOperator)) {
      throw new IllegalStateException(
          "canonical state belongs to " + actualOperator + ", not " + expectedOperator);
    }
    long deadline = in.getLong();
    int chunks = in.getInt();
    int payloadBytes = in.getInt();
    int checksum = in.getInt();
    if (chunks < 0
        || payloadBytes < 0
        || chunks != (payloadBytes + CHUNK_BYTES - 1) / CHUNK_BYTES) {
      throw new IllegalStateException("invalid chunk metadata in canonical state header");
    }
    return new Header(deadline, chunks, payloadBytes, checksum);
  }

  private static int checksum(byte[] bytes, int offset, int length) {
    CRC32C checksum = new CRC32C();
    checksum.update(bytes, offset, length);
    return (int) checksum.getValue();
  }

  private static void requireOwned(KeyGroupRange range, int keyGroup) {
    if (!range.contains(keyGroup)) {
      throw new IllegalStateException(
          "canonical native state for key group " + keyGroup + " is outside " + range);
    }
  }

  public static final class Restore {
    public final List<byte[]> partitions;
    public final long timerDeadline;

    public Restore(List<byte[]> partitions, long timerDeadline) {
      this.partitions = partitions;
      this.timerDeadline = timerDeadline;
    }
  }

  private static final class Header {
    final long timerDeadline;
    final int chunks;
    final int payloadBytes;
    final int checksum;

    Header(long timerDeadline, int chunks, int payloadBytes, int checksum) {
      this.timerDeadline = timerDeadline;
      this.chunks = chunks;
      this.payloadBytes = payloadBytes;
      this.checksum = checksum;
    }
  }
}
