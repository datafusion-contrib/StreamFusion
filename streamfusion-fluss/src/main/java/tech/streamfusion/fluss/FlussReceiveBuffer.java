package tech.streamfusion.fluss;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ForeignAllocation;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;

/** Accounts and retains a complete RPC allocation until its last Arrow buffer is released. */
final class FlussReceiveBuffer extends ForeignAllocation {
  private final ByteBuf buffer;
  private final AtomicBoolean released = new AtomicBoolean();

  private FlussReceiveBuffer(ByteBuf buffer) {
    super(buffer.capacity(), buffer.memoryAddress());
    this.buffer = buffer.retain();
  }

  static ArrowBuf borrow(ByteBuf buffer, BufferAllocator allocator) {
    if (buffer == null || !buffer.isDirect() || !buffer.hasMemoryAddress()) return null;
    FlussReceiveBuffer allocation = new FlussReceiveBuffer(buffer);
    try {
      return allocator.wrapForeignAllocation(allocation);
    } catch (Throwable failure) {
      allocation.release0();
      throw failure;
    }
  }

  @Override
  protected void release0() {
    if (released.compareAndSet(false, true)) buffer.release();
  }
}
