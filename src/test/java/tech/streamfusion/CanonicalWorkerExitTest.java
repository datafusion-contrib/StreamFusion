package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.example.canonical.CanonicalJobWorker;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CanonicalWorkerExitTest {
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void completedJobsExitAfterCleanupWithTheirOriginalOutcome(boolean fail) throws Exception {
    Path log = directory.resolve("worker.log");
    var process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                Worker.class.getName(),
                directory.toString(),
                Boolean.toString(fail))
            .redirectErrorStream(true)
            .redirectOutput(log.toFile())
            .start();
    try {
      assertTrue(process.waitFor(20, TimeUnit.SECONDS), () -> "worker did not exit: " + log);
      assertEquals(fail ? 1 : 0, process.exitValue(), () -> read(log));
      assertEquals("closed", Files.readString(directory.resolve("cleanup")));
      assertEquals(fail, Files.readString(log).contains("original job failure"));
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        process.waitFor(10, TimeUnit.SECONDS);
      }
    }
  }

  private static String read(Path file) {
    try {
      return Files.readString(file);
    } catch (Exception failure) {
      return failure.toString();
    }
  }

  public static final class Worker {
    public static void main(String[] arguments) {
      CanonicalJobWorker.runAndExit(
          () -> {
            var waiting = new CountDownLatch(1);
            var thread =
                new Thread(
                    () -> {
                      try {
                        waiting.await();
                      } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                      }
                    },
                    "non-daemon-runtime-thread");
            thread.setDaemon(false);
            thread.start();
            try {
              if (Boolean.parseBoolean(arguments[1]))
                throw new AssertionError("original job failure");
            } finally {
              Files.writeString(Path.of(arguments[0]).resolve("cleanup"), "closed");
            }
          });
    }
  }
}
