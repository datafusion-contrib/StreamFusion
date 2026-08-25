package tech.streamfusion.state;

import tech.streamfusion.Native;

/**
 * The slot-scoped native RocksDB memory pool (block cache plus write-buffer manager), leased
 * through Flink's shared-resource machinery so its lifetime is refcounted across every operator in
 * the slot. The C++ objects live in StreamFusion's own RocksDB library — they cannot be shared with
 * the delegate backend's frocksdbjni instances — but they are sized by the same Flink options and
 * formulas, so the slot's native state memory obeys one Flink-configured bound.
 */
public final class NativeRocksSharedResources implements AutoCloseable {

  private final long nativeHandle;

  NativeRocksSharedResources(long totalBytes, double writeBufferRatio) {
    this.nativeHandle = Native.createRocksDBSharedResources(totalBytes, writeBufferRatio);
  }

  long nativeHandle() {
    return nativeHandle;
  }

  @Override
  public void close() {
    Native.releaseRocksDBSharedResources(nativeHandle);
  }
}
