package tech.streamfusion;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/** Optional aggregate timings for CI diagnostics; disabled during ordinary test execution. */
public final class CiFixtureTimings {
  private static final String DIRECTORY = System.getProperty("sf.fixtureTimingsDirectory");
  private static final boolean ENABLED = DIRECTORY != null;
  private static final ConcurrentHashMap<String, Measurement> MEASUREMENTS =
      new ConcurrentHashMap<>();

  static {
    if (ENABLED) {
      Runtime.getRuntime()
          .addShutdownHook(new Thread(CiFixtureTimings::write, "ci-fixture-timings"));
    }
  }

  private CiFixtureTimings() {}

  private static void write() {
    var lines = new ArrayList<String>();
    MEASUREMENTS.forEach(
        (name, value) ->
            lines.add(
                "[ci-fixture-timing] "
                    + name
                    + " calls="
                    + value.calls.sum()
                    + " nanoseconds="
                    + value.nanoseconds.sum()));
    lines.sort(String::compareTo);
    try {
      Path directory = Path.of(DIRECTORY);
      Files.createDirectories(directory);
      Files.write(directory.resolve("fixture-" + ProcessHandle.current().pid() + ".txt"), lines);
    } catch (IOException failure) {
      throw new UncheckedIOException(failure);
    }
  }

  public static long start() {
    return ENABLED ? System.nanoTime() : 0;
  }

  public static void record(String name, long started) {
    if (!ENABLED) return;
    Measurement value = MEASUREMENTS.computeIfAbsent(name, ignored -> new Measurement());
    value.calls.increment();
    value.nanoseconds.add(System.nanoTime() - started);
  }

  public static <T> T measure(String name, Operation<T> operation) throws Exception {
    long started = start();
    try {
      return operation.run();
    } finally {
      record(name, started);
    }
  }

  @FunctionalInterface
  public interface Operation<T> {
    T run() throws Exception;
  }

  private static final class Measurement {
    final LongAdder calls = new LongAdder();
    final LongAdder nanoseconds = new LongAdder();
  }
}
