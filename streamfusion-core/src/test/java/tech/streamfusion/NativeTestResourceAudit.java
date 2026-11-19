package tech.streamfusion;

import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Native accounting stays with core; the cluster fixture depends only on the test contract. */
public final class NativeTestResourceAudit implements TestResourceAudit {
  @Override
  public void initialize(Configuration configuration) {
    TaskOffHeapMemory.initialize(configuration);
  }

  /**
   * Asserts the native side is quiescent after every test, turning the whole suite into a standing
   * leak audit: every native handle created over JNI must have been closed (a live handle is Rust
   * memory the JVM's tooling cannot see), and the shared Arrow FFI allocator must hold zero bytes
   * (a nonzero balance is an unclosed {@code VectorSchemaRoot} or a dropped C Data release
   * callback, in either transfer direction — imports are registered as foreign allocations). The
   * task pool must also release every reservation, including native state outside Arrow.
   *
   * <p>Task cleanup can trail the job result by a moment (source fetcher threads close
   * asynchronously), so the check polls briefly before failing; when clean it costs one JNI call
   * per loaded library.
   */
  @Override
  public void assertReleased(String scenario) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    String handles = NativeExtensionLoader.liveNativeHandles();
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    long reserved = TaskOffHeapMemory.reservedBytes();
    while ((!handles.isEmpty() || allocated != 0 || reserved != 0)
        && System.nanoTime() < deadline) {
      // Records a failed job dropped in flight are freed by the ArrowBatch cleaner backstop, which
      // only runs once a GC notices they are unreachable — nudge it rather than waiting one out.
      System.gc();
      Thread.sleep(20);
      handles = NativeExtensionLoader.liveNativeHandles();
      allocated = NativeAllocator.SHARED.getAllocatedMemory();
      reserved = TaskOffHeapMemory.reservedBytes();
    }
    if (!handles.isEmpty() || allocated != 0 || reserved != 0) {
      throw new AssertionError(
          "native memory outstanding after "
              + scenario
              + ": live native handles ["
              + handles
              + "], Arrow allocator holds "
              + allocated
              + " bytes, task pool reserves "
              + reserved
              + " bytes\n"
              + NativeAllocator.SHARED.toVerboseString());
    }
  }
}
