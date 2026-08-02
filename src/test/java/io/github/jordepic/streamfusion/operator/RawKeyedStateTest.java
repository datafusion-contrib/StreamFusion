package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RunnableFuture;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.junit.jupiter.api.Test;

/**
 * The raw keyed-state framing: every payload snapshots behind the versioned header, a pre-header
 * payload still restores (version 0), and a payload from a newer state format fails with the
 * writer's version named instead of misparsing.
 */
class RawKeyedStateTest {

  private static final KeyGroupRange RANGE = new KeyGroupRange(0, 3);

  @Test
  void payloadsRoundTripThroughTheVersionedHeader() throws Exception {
    byte[][] partitions = {partition(0, 7, 8, 9), partition(2, 4, 5)};

    List<byte[]> restored =
        RawKeyedState.restore(restoreContext(snapshot(partitions, null)));

    assertEquals(2, restored.size());
    assertArrayEquals(new byte[] {7, 8, 9}, restored.get(0));
    assertArrayEquals(new byte[] {4, 5}, restored.get(1));
  }

  @Test
  void timerDeadlineRoundTripsInsideTheVersionedHeader() throws Exception {
    byte[][] partitions = {partition(1, 6)};

    RawKeyedState.TimedRestore restored =
        RawKeyedState.restoreWithTimer(restoreContext(snapshot(partitions, 1234L)));

    assertEquals(1234L, restored.deadline());
    assertEquals(1, restored.snapshots().size());
    assertArrayEquals(new byte[] {6}, restored.snapshots().get(0));
  }

  @Test
  void preHeaderPayloadRestoresAsVersionZero() throws Exception {
    byte[] legacy = new byte[] {42, 43, 44};

    List<byte[]> restored = RawKeyedState.restore(restoreContext(framed(legacy)));

    assertEquals(1, restored.size());
    assertArrayEquals(legacy, restored.get(0));
  }

  @Test
  void preHeaderTimerFrameRestoresAsVersionZero() throws Exception {
    ByteBuffer legacy = ByteBuffer.allocate(RawKeyedState.TIMER_FRAME_BYTES + 2);
    legacy.putInt(RawKeyedState.TIMER_FRAME_MAGIC);
    legacy.putLong(555L);
    legacy.put(new byte[] {1, 2});

    RawKeyedState.TimedRestore restored =
        RawKeyedState.restoreWithTimer(restoreContext(framed(legacy.array())));

    assertEquals(555L, restored.deadline());
    assertArrayEquals(new byte[] {1, 2}, restored.snapshots().get(0));
  }

  @Test
  void newerStateFormatVersionFailsNamingBothVersions() {
    int newerVersion = RawKeyedState.STATE_FORMAT_VERSION + 41;
    ByteBuffer payload = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + Integer.BYTES + 1);
    payload.putLong(RawKeyedState.STATE_MAGIC);
    payload.putInt(newerVersion);
    payload.putInt(0);
    payload.put((byte) 9);

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () -> RawKeyedState.restore(restoreContext(framed(payload.array()))));

    assertTrue(failure.getMessage().contains("version " + newerVersion), failure.getMessage());
    assertTrue(
        failure.getMessage().contains(String.valueOf(RawKeyedState.STATE_FORMAT_VERSION)),
        failure.getMessage());
  }

  /** A native-framed partition: the big-endian key-group id followed by the payload bytes. */
  private static byte[] partition(int keyGroup, int... payload) {
    ByteBuffer partition = ByteBuffer.allocate(Integer.BYTES + payload.length);
    partition.putInt(keyGroup);
    for (int b : payload) {
      partition.put((byte) b);
    }
    return partition.array();
  }

  /** Snapshots through the real raw keyed-state stream and slices it back per key group. */
  private static List<KeyGroupStatePartitionStreamProvider> snapshot(
      byte[][] partitions, Long deadline) throws Exception {
    StateSnapshotContextSynchronousImpl context =
        new StateSnapshotContextSynchronousImpl(
            1L, 1L, new MemCheckpointStreamFactory(4 << 20), RANGE, new CloseableRegistry());
    if (deadline == null) {
      RawKeyedState.snapshotPartitions(context, partitions);
    } else {
      RawKeyedState.snapshotPartitionsWithTimer(context, partitions, deadline);
    }
    RunnableFuture<SnapshotResult<KeyedStateHandle>> future = context.getKeyedStateStreamFuture();
    future.run();
    KeyGroupsStateHandle handle = (KeyGroupsStateHandle) future.get().getJobManagerOwnedSnapshot();

    List<KeyGroupStatePartitionStreamProvider> providers = new ArrayList<>();
    for (Tuple2<Integer, Long> groupOffset : handle.getGroupRangeOffsets()) {
      if (groupOffset.f1 == KeyedStateCheckpointOutputStream.NO_OFFSET_SET) {
        continue;
      }
      FSDataInputStream stream = handle.openInputStream();
      stream.seek(groupOffset.f1);
      providers.add(new KeyGroupStatePartitionStreamProvider(stream, groupOffset.f0));
    }
    return providers;
  }

  /** One raw key-group stream holding the payload behind the length framing, as Flink stores it. */
  private static List<KeyGroupStatePartitionStreamProvider> framed(byte[] payload)
      throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream data = new DataOutputStream(bytes);
    data.writeInt(payload.length);
    data.write(payload);
    return List.of(
        new KeyGroupStatePartitionStreamProvider(
            new ByteArrayInputStream(bytes.toByteArray()), 0));
  }

  private static StateInitializationContext restoreContext(
      List<KeyGroupStatePartitionStreamProvider> providers) {
    // The context refuses raw keyed reads without a keyed-state store; none of its state methods
    // are exercised here, so an unusable stand-in satisfies the guard.
    KeyedStateStore unusedStore =
        (KeyedStateStore)
            Proxy.newProxyInstance(
                RawKeyedStateTest.class.getClassLoader(),
                new Class<?>[] {KeyedStateStore.class},
                (proxy, method, args) -> {
                  throw new UnsupportedOperationException(method.getName());
                });
    return new StateInitializationContextImpl(1L, null, unusedStore, providers, null);
  }
}
