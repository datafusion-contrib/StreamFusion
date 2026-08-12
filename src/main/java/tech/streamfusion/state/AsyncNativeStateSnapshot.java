package tech.streamfusion.state;

import tech.streamfusion.Native;
import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Immutable keyed-state value whose native bytes are materialized by Flink's asynchronous heap
 * checkpoint serializer. A restored value already owns its byte array; a live value retains a
 * shallow native Top-N snapshot until every heap snapshot referencing it has been released.
 */
final class AsyncNativeStateSnapshot {

  private final TopNSnapshotToken token;
  private final int keyGroup;
  private final byte[] restoredBytes;

  private AsyncNativeStateSnapshot(TopNSnapshotToken token, int keyGroup, byte[] restoredBytes) {
    this.token = token;
    this.keyGroup = keyGroup;
    this.restoredBytes = restoredBytes;
  }

  static TopNSnapshotToken captureTopN(long rankerHandle, int maxParallelism) {
    long handle = Native.captureTopNRankerSnapshot(rankerHandle, maxParallelism);
    return handle == 0 ? null : new TopNSnapshotToken(handle);
  }

  static AsyncNativeStateSnapshot restored(byte[] bytes) {
    return new AsyncNativeStateSnapshot(null, -1, bytes);
  }

  byte[] materialize() {
    return restoredBytes != null ? restoredBytes : token.encode(keyGroup);
  }

  static final class TopNSnapshotToken implements AutoCloseable {
    private static final Cleaner CLEANER = Cleaner.create();

    private final NativeHandle nativeHandle;
    private final Cleaner.Cleanable cleanable;
    private final int[] keyGroups;
    private final AtomicInteger remainingPartitions;

    private TopNSnapshotToken(long handle) {
      nativeHandle = new NativeHandle(handle);
      cleanable = CLEANER.register(this, nativeHandle);
      keyGroups = Native.topNRankerSnapshotKeyGroups(handle);
      remainingPartitions = new AtomicInteger(keyGroups.length);
    }

    int[] keyGroups() {
      return keyGroups;
    }

    AsyncNativeStateSnapshot partition(int keyGroup) {
      return new AsyncNativeStateSnapshot(this, keyGroup, null);
    }

    byte[] encode(int keyGroup) {
      try {
        return Native.encodeTopNRankerSnapshotPartition(nativeHandle.requireOpen(), keyGroup);
      } finally {
        if (remainingPartitions.decrementAndGet() == 0) {
          close();
        }
      }
    }

    @Override
    public void close() {
      cleanable.clean();
    }
  }

  private static final class NativeHandle implements Runnable {
    private final AtomicLong handle;

    private NativeHandle(long handle) {
      this.handle = new AtomicLong(handle);
    }

    long requireOpen() {
      long current = handle.get();
      if (current == 0) {
        throw new IllegalStateException("native Top-N snapshot is already closed");
      }
      return current;
    }

    @Override
    public void run() {
      long current = handle.getAndSet(0);
      if (current != 0) {
        Native.closeTopNRankerSnapshot(current);
      }
    }
  }
}
