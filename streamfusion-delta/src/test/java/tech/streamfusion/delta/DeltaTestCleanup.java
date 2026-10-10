package tech.streamfusion.delta;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Comparator;
import tech.streamfusion.compat.ListCollectors;

/** Test-only cleanup after released Hadoop checksum writers finish their background work. */
final class DeltaTestCleanup {
  static void deleteDirectory(Path directory) throws IOException, InterruptedException {
    long deadline = System.nanoTime() + 1_000_000_000L;
    while (true) {
      try (var files = Files.walk(directory)) {
        for (Path file : files.sorted(Comparator.reverseOrder()).collect(ListCollectors.toList()))
          Files.deleteIfExists(file);
        return;
      } catch (IOException | UncheckedIOException failure) {
        IOException cause = failure instanceof UncheckedIOException
            ? ((UncheckedIOException) failure).getCause() : (IOException) failure;
        if (!(cause instanceof NoSuchFileException) && !(cause instanceof DirectoryNotEmptyException)) {
          throw cause;
        }
        if (Files.notExists(directory)) return;
        if (System.nanoTime() >= deadline) throw cause;
        Thread.sleep(10);
      }
    }
  }

  private DeltaTestCleanup() {}
}
