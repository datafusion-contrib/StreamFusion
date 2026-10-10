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
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;

/** Backend-independent representation of native state inside a Flink canonical savepoint. */
public final class CanonicalNativeState {
  static final String STATE_NAME = "__streamfusion_canonical_native_state_v1";
  static final String ASYNC_HEADER_STATE_NAME = "__streamfusion_canonical_native_state_v2_header";
  static final String ASYNC_PAYLOAD_STATE_NAME = "__streamfusion_canonical_native_state_v2_payload";
  static final int FORMAT_VERSION = 5;
  private static final int ASYNC_FORMAT_VERSION = 6;
  static final int CHUNK_BYTES = 4 * 1024 * 1024;

  private static final int MAGIC = 0x53464353; // SFCS
  private static final ValueStateDescriptor<byte[]> HEADER_DESCRIPTOR = descriptor(STATE_NAME);
  private static final ValueStateDescriptor<byte[]> ASYNC_HEADER_DESCRIPTOR =
      descriptor(ASYNC_HEADER_STATE_NAME);
  private static final ValueStateDescriptor<AsyncNativeStateSnapshot> ASYNC_PAYLOAD_DESCRIPTOR =
      new ValueStateDescriptor<>(ASYNC_PAYLOAD_STATE_NAME, new AsyncNativeStateSerializer());

  private CanonicalNativeState() {}

  public static boolean captureAndWriteAsyncTopN(
      CheckpointableKeyedStateBackend<?> backend,
      long rankerHandle,
      int maxParallelism,
      String operatorId,
      long timerDeadline)
      throws Exception {
    AsyncNativeStateSnapshot.TopNSnapshotToken snapshot =
        AsyncNativeStateSnapshot.captureTopN(rankerHandle, maxParallelism);
    if (snapshot == null) {
      return false;
    }
    try {
      writeAsyncTopN(backend, snapshot, operatorId, timerDeadline);
      return true;
    } catch (Throwable failure) {
      snapshot.close();
      throw failure;
    }
  }

  /**
   * Installs immutable Top-N partitions in heap keyed state without encoding them. Heap backend
   * snapshot preparation captures these immutable values synchronously; their serializer performs
   * the native IPC work later on Flink's async checkpoint thread.
   */
  public static void writeAsyncTopN(
      CheckpointableKeyedStateBackend<?> backend,
      AsyncNativeStateSnapshot.TopNSnapshotToken snapshot,
      String operatorId,
      long timerDeadline)
      throws Exception {
    SavedKeyContext previous = saveKeyContext(backend);
    boolean retained = false;
    try {
      for (int keyGroup : backend.getKeyGroupRange()) {
        stateForKeyGroup(backend, keyGroup, ASYNC_HEADER_DESCRIPTOR).clear();
        stateForKeyGroup(backend, keyGroup, ASYNC_PAYLOAD_DESCRIPTOR).clear();
      }
      for (int keyGroup : snapshot.keyGroups()) {
        requireOwned(backend.getKeyGroupRange(), keyGroup);
        stateForKeyGroup(backend, keyGroup, ASYNC_HEADER_DESCRIPTOR)
            .update(asyncHeader(operatorId, timerDeadline));
        stateForKeyGroup(backend, keyGroup, ASYNC_PAYLOAD_DESCRIPTOR)
            .update(snapshot.partition(keyGroup));
        retained = true;
      }
    } finally {
      restoreKey(backend, previous);
      if (!retained) {
        snapshot.close();
      }
    }
  }

  public static void write(
      CheckpointableKeyedStateBackend<?> backend,
      byte[][] partitions,
      String operatorId,
      long timerDeadline)
      throws Exception {
    SavedKeyContext previous = saveKeyContext(backend);
    try {
      for (int keyGroup : backend.getKeyGroupRange()) {
        ValueState<byte[]> headerState = stateForKeyGroup(backend, keyGroup, HEADER_DESCRIPTOR);
        byte[] previousHeader = headerState.value();
        if (previousHeader != null) {
          Header old = decodeHeader(previousHeader, operatorId);
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
      restoreKey(backend, previous);
    }
  }

  public static Restore readAndClear(
      CheckpointableKeyedStateBackend<?> backend, String operatorId)
      throws Exception {
    SavedKeyContext previous = saveKeyContext(backend);
    List<byte[]> partitions = new ArrayList<>();
    long timerDeadline = Long.MIN_VALUE;
    boolean sawAsync = false;
    try {
      for (int keyGroup : backend.getKeyGroupRange()) {
        ValueState<byte[]> headerState =
            stateForKeyGroup(backend, keyGroup, ASYNC_HEADER_DESCRIPTOR);
        byte[] header = headerState.value();
        ValueState<AsyncNativeStateSnapshot> payloadState =
            stateForKeyGroup(backend, keyGroup, ASYNC_PAYLOAD_DESCRIPTOR);
        AsyncNativeStateSnapshot payload = payloadState.value();
        if ((header == null) != (payload == null)) {
          throw new IllegalStateException(
              "canonical native async state for key group " + keyGroup + " is incomplete");
        }
        if (header != null) {
          long deadline = decodeAsyncHeader(header, operatorId);
          byte[] partition = payload.materialize();
          partitions.add(partition);
          sawAsync = true;
          timerDeadline = Math.max(timerDeadline, deadline);
          headerState.clear();
          payloadState.clear();
        }
      }
      for (int keyGroup : backend.getKeyGroupRange()) {
        ValueState<byte[]> headerState = stateForKeyGroup(backend, keyGroup, HEADER_DESCRIPTOR);
        byte[] header = headerState.value();
        if (header == null) {
          continue;
        }
        if (sawAsync) {
          throw new IllegalStateException("restore contains both v1 and v2 canonical native state");
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
      restoreKey(backend, previous);
    }
    return new Restore(partitions, timerDeadline);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static <T> ValueState<T> stateForKeyGroup(
      CheckpointableKeyedStateBackend<?> backend,
      int keyGroup,
      ValueStateDescriptor<T> descriptor)
      throws Exception {
    CheckpointableKeyedStateBackend raw = backend;
    tech.streamfusion.compat.StateCompat.setCurrentKeyAndGroup(raw, keyGroup, keyGroup);
    return (ValueState<T>)
        raw.getPartitionedState(
            VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);
  }

  private static ValueStateDescriptor<byte[]> chunkDescriptor(int chunk) {
    return descriptor(STATE_NAME + "_chunk_" + chunk);
  }

  private static ValueStateDescriptor<byte[]> descriptor(String name) {
    return new ValueStateDescriptor<>(name, BytePrimitiveArraySerializer.INSTANCE);
  }

  @SuppressWarnings("null")
  private static SavedKeyContext saveKeyContext(CheckpointableKeyedStateBackend<?> backend) {
    if (backend instanceof AbstractKeyedStateBackend) {
      AbstractKeyedStateBackend<?> keyed = (AbstractKeyedStateBackend<?>) backend;
      return new SavedKeyContext(keyed.getCurrentKey(), keyed.getCurrentKeyGroupIndex());
    }
    if (backend instanceof RocksDBNativeKeyedStateBackend) {
      RocksDBNativeKeyedStateBackend<?> keyed = (RocksDBNativeKeyedStateBackend<?>) backend;
      return new SavedKeyContext(keyed.getCurrentKey(), keyed.getCurrentKeyGroupIndex());
    }
    throw new IllegalStateException(
        "unsupported keyed backend for canonical native state: " + backend.getClass().getName());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void restoreKey(
      CheckpointableKeyedStateBackend<?> backend, SavedKeyContext previous) {
    if (previous.key == null) {
      clearCurrentKey(backend);
      return;
    }
    tech.streamfusion.compat.StateCompat.setCurrentKeyAndGroup(
        (CheckpointableKeyedStateBackend) backend, previous.key, previous.keyGroup);
  }

  private static void clearCurrentKey(CheckpointableKeyedStateBackend<?> backend) {
    if (backend instanceof AbstractKeyedStateBackend) {
      clearKeyContext((AbstractKeyedStateBackend<?>) backend);
      return;
    }
    if (backend instanceof RocksDBNativeKeyedStateBackend) {
      ((RocksDBNativeKeyedStateBackend<?>) backend).clearCurrentKey();
      return;
    }
    throw new IllegalStateException(
        "unsupported keyed backend for canonical native state: " + backend.getClass().getName());
  }

  /**
   * Returns the keyed context to its unset shape: no key, and the group index parked on the first
   * owned key group so no synthetic group from the canonical iteration survives. Flink validates
   * the index against the owned range, which makes its pristine default unrepresentable here.
   */
  @SuppressWarnings("null")
  static void clearKeyContext(AbstractKeyedStateBackend<?> keyed) {
    keyed.getKeyContext().setCurrentKey(null);
    KeyGroupRange range = keyed.getKeyGroupRange();
    if (range.getNumberOfKeyGroups() > 0) {
      keyed.getKeyContext().setCurrentKeyGroupIndex(range.getStartKeyGroup());
    }
  }

  private static final class SavedKeyContext {
    private final Object key;
    private final int keyGroup;

    private SavedKeyContext(Object key, int keyGroup) {
      this.key = key;
      this.keyGroup = keyGroup;
    }

    public Object key() {
      return key;
    }

    public int keyGroup() {
      return keyGroup;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      SavedKeyContext that = (SavedKeyContext) other;
      return java.util.Objects.equals(key, that.key) && keyGroup == that.keyGroup;
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + java.util.Objects.hashCode(key);
      result = 31 * result + keyGroup;
      return result;
    }

    @Override
    public String toString() {
      return "SavedKeyContext[key=" + key + ", keyGroup=" + keyGroup + "]";
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

  private static byte[] asyncHeader(String operatorId, long timerDeadline) {
    byte[] operator = operatorId.getBytes(StandardCharsets.UTF_8);
    ByteBuffer header = ByteBuffer.allocate(4 + 4 + 4 + operator.length + 8);
    header.putInt(MAGIC).putInt(ASYNC_FORMAT_VERSION).putInt(operator.length).put(operator).putLong(timerDeadline);
    return header.array();
  }

  private static long decodeAsyncHeader(byte[] bytes, String expectedOperator) {
    ByteBuffer in = ByteBuffer.wrap(bytes);
    if (in.remaining() < 20 || in.getInt() != MAGIC) {
      throw new IllegalStateException("invalid StreamFusion asynchronous canonical state header");
    }
    int version = in.getInt();
    if (version != ASYNC_FORMAT_VERSION) {
      throw new IllegalStateException(
          "unsupported StreamFusion asynchronous canonical state version "
              + version
              + "; this build reads "
              + ASYNC_FORMAT_VERSION
              + " (docs/backends/canonical-state.md)");
    }
    int operatorBytes = in.getInt();
    if (operatorBytes < 0 || operatorBytes != in.remaining() - 8) {
      throw new IllegalStateException(
          "invalid operator identifier in async canonical state header");
    }
    byte[] operator = new byte[operatorBytes];
    in.get(operator);
    String actualOperator = new String(operator, StandardCharsets.UTF_8);
    if (!expectedOperator.equals(actualOperator)) {
      throw new IllegalStateException(
          "canonical state belongs to " + actualOperator + ", not " + expectedOperator);
    }
    return in.getLong();
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
              + FORMAT_VERSION + " (docs/backends/canonical-state.md)");
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
