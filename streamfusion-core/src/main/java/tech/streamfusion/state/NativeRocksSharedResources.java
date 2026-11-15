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

  private long nativeHandle;
  private int leases;
  private boolean closed;

  NativeRocksSharedResources(long totalBytes, double writeBufferRatio) {
    this.nativeHandle = Native.createRocksDBSharedResources(totalBytes, writeBufferRatio);
  }

  synchronized long nativeHandle() {
    if (closed) {
      throw new IllegalStateException("native RocksDB memory pool is closed");
    }
    return nativeHandle;
  }

  synchronized Lease retain() {
    long handle = nativeHandle();
    leases++;
    return new Lease(this, handle);
  }

  @Override
  public synchronized void close() {
    closed = true;
    releaseIfUnused();
  }

  private synchronized void releaseLease() {
    leases--;
    releaseIfUnused();
  }

  private void releaseIfUnused() {
    if (closed && leases == 0 && nativeHandle != 0) {
      Native.releaseRocksDBSharedResources(nativeHandle);
      nativeHandle = 0;
    }
  }

  static final class Lease implements AutoCloseable {
    private NativeRocksSharedResources owner;
    private final long handle;

    private Lease(NativeRocksSharedResources owner, long handle) {
      this.owner = owner;
      this.handle = handle;
    }

    synchronized long nativeHandle() {
      if (owner == null) {
        throw new IllegalStateException("native RocksDB memory lease is closed");
      }
      return handle;
    }

    @Override
    public void close() {
      NativeRocksSharedResources resource;
      synchronized (this) {
        resource = owner;
        owner = null;
      }
      if (resource != null) {
        resource.releaseLease();
      }
    }
  }
}
