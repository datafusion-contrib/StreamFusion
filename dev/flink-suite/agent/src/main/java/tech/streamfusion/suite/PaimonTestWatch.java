package tech.streamfusion.suite;

import java.io.PrintStream;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Retains the active test and task stacks before a stalled upstream job reaches CI's timeout. */
public final class PaimonTestWatch {
  private static PrintStream diagnostics;
  private static final ScheduledExecutorService TIMER =
      Executors.newSingleThreadScheduledExecutor(
          task -> {
            Thread thread = new Thread(task, "streamfusion-paimon-test-watch");
            thread.setDaemon(true);
            return thread;
          });

  private final String test;
  private final long started = System.nanoTime();
  private final ScheduledFuture<?> diagnostic;
  private boolean finished;

  public static void initialize(PrintStream output) {
    // Capture stderr in premain, before Surefire redirects test output into unfinished XML reports.
    diagnostics = output;
  }

  private PaimonTestWatch(String test, Object fixture) {
    this.test = test;
    diagnostics.println(
        "StreamFusion upstream test started "
            + test
            + " at "
            + Instant.now()
            + "; randomized table defaults "
            + tableDefaults(fixture));
    long delay = Long.getLong("streamfusion.flink-suite.diagnostic-delay-seconds", 120L);
    if (delay <= 0) {
      throw new IllegalArgumentException("The upstream diagnostic delay must be positive");
    }
    diagnostic = TIMER.schedule(this::dumpThreads, delay, TimeUnit.SECONDS);
  }

  private static String tableDefaults(Object fixture) {
    for (Class<?> type = fixture.getClass(); type != null; type = type.getSuperclass()) {
      try {
        var field = type.getDeclaredField("tableDefaultProperties");
        field.setAccessible(true);
        return String.valueOf(field.get(fixture));
      } catch (NoSuchFieldException ignored) {
        // Most Paimon fixtures do not randomize table defaults.
      } catch (IllegalAccessException failure) {
        return "unavailable: " + failure;
      }
    }
    return "none";
  }

  public static PaimonTestWatch start(String test, Object fixture) {
    return new PaimonTestWatch(test, fixture);
  }

  public static void clusterFailure(String handler, Throwable failure) {
    diagnostics.println(
        "StreamFusion upstream cluster failed in " + handler + " at " + Instant.now());
    failure.printStackTrace(diagnostics);
  }

  private synchronized void dumpThreads() {
    if (finished) {
      return;
    }
    StringBuilder dump =
        new StringBuilder("StreamFusion upstream test still running ")
            .append(test)
            .append(" after ")
            .append(elapsedSeconds())
            .append(" seconds\n");
    Thread.getAllStackTraces()
        .forEach(
            (thread, stack) -> {
              dump.append('"')
                  .append(thread.getName())
                  .append("\" ")
                  .append(thread.getState())
                  .append('\n');
              for (StackTraceElement frame : stack) {
                dump.append("    at ").append(frame).append('\n');
              }
            });
    diagnostics.print(dump);
  }

  public synchronized void finish(Throwable failure) {
    finished = true;
    diagnostic.cancel(false);
    diagnostics.println(
        "StreamFusion upstream test finished "
            + test
            + " after "
            + elapsedSeconds()
            + " seconds: "
            + (failure == null ? "passed" : failure.getClass().getName()));
    if (failure != null) {
      failure.printStackTrace(diagnostics);
    }
  }

  private long elapsedSeconds() {
    return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started);
  }
}
