package tech.streamfusion.operator;

import java.util.Arrays;
import org.apache.flink.core.fs.AutoCloseableRegistry;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

/** Task-thread ownership of native resources, including partially initialized operators. */
public final class NativeResources implements AutoCloseable {
  private final AutoCloseableRegistry resources = new AutoCloseableRegistry();

  /** Registers an acquired resource; the most recently acquired resource is released first. */
  public <T extends AutoCloseable> T own(T resource) throws Exception {
    if (resource != null) resources.registerCloseable(resource);
    return resource;
  }

  @Override
  public void close() throws Exception {
    resources.close();
  }

  /**
   * Attempts every cleanup in dependency order, retaining later failures as suppressed exceptions.
   */
  public static void closeAll(AutoCloseable... resources) throws Exception {
    IOUtils.closeAll(Arrays.asList(resources), Throwable.class);
  }

  public static void closeAllUnchecked(AutoCloseable... resources) {
    try {
      closeAll(resources);
    } catch (Exception failure) {
      ExceptionUtils.rethrow(failure);
    }
  }
}
