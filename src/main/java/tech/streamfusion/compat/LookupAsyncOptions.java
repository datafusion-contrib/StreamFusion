package tech.streamfusion.compat;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;

public final class LookupAsyncOptions {
  private final int asyncBufferCapacity;
  private final long asyncTimeout;
  private final boolean keyOrdered;
  private final AsyncDataStream.OutputMode asyncOutputMode;

  public LookupAsyncOptions(
      int asyncBufferCapacity,
      long asyncTimeout,
      boolean keyOrdered,
      AsyncDataStream.OutputMode asyncOutputMode) {
    this.asyncBufferCapacity = asyncBufferCapacity;
    this.asyncTimeout = asyncTimeout;
    this.keyOrdered = keyOrdered;
    this.asyncOutputMode = asyncOutputMode;
  }

  public int asyncBufferCapacity() {
    return asyncBufferCapacity;
  }

  public long asyncTimeout() {
    return asyncTimeout;
  }

  public boolean keyOrdered() {
    return keyOrdered;
  }

  public AsyncDataStream.OutputMode asyncOutputMode() {
    return asyncOutputMode;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;
    LookupAsyncOptions that = (LookupAsyncOptions) other;
    return asyncBufferCapacity == that.asyncBufferCapacity
        && asyncTimeout == that.asyncTimeout
        && keyOrdered == that.keyOrdered
        && java.util.Objects.equals(asyncOutputMode, that.asyncOutputMode);
  }

  @Override
  public int hashCode() {
    int result = 0;
    result = 31 * result + asyncBufferCapacity;
    result = 31 * result + Long.hashCode(asyncTimeout);
    result = 31 * result + Boolean.hashCode(keyOrdered);
    result = 31 * result + java.util.Objects.hashCode(asyncOutputMode);
    return result;
  }

  @Override
  public String toString() {
    return "LookupAsyncOptions[asyncBufferCapacity="
        + asyncBufferCapacity
        + ", asyncTimeout="
        + asyncTimeout
        + ", keyOrdered="
        + keyOrdered
        + ", asyncOutputMode="
        + asyncOutputMode
        + "]";
  }
}
