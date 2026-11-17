package tech.streamfusion;

import java.util.concurrent.TimeUnit;

/** Child JVMs must stop before their parent's temporary archives can be removed. */
final class CanonicalWorkerProcess implements AutoCloseable {
  private final Process process;
  private final Thread shutdown;

  CanonicalWorkerProcess(Process process) {
    this.process = process;
    shutdown = new Thread(this::stop, "stop-canonical-worker-" + process.pid());
    try {
      Runtime.getRuntime().addShutdownHook(shutdown);
    } catch (IllegalStateException closing) {
      stop();
      throw closing;
    }
  }

  private void stop() {
    if (!process.isAlive()) return;
    process.destroyForcibly();
    try {
      process.waitFor(10, TimeUnit.SECONDS);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void close() {
    try {
      Runtime.getRuntime().removeShutdownHook(shutdown);
    } catch (IllegalStateException closing) {
      // A shutdown already in progress also owns the child through the registered hook.
    } finally {
      stop();
    }
  }
}
