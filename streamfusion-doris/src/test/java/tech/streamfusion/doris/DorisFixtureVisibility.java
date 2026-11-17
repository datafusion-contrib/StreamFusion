package tech.streamfusion.doris;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Wait for externally committed rows; the released helper still checks their exact values. */
public final class DorisFixtureVisibility {
  private DorisFixtureVisibility() {}

  public static <T> List<T> await(Supplier<List<T>> query, int expectedRows) {
    return await(query, expectedRows, 30000);
  }

  static <T> List<T> await(Supplier<List<T>> query, int expectedRows, long timeoutMillis) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    List<T> actual = query.get();
    while (actual.size() < expectedRows && System.nanoTime() < deadline) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new AssertionError("Interrupted while waiting for Doris row visibility", interrupted);
      }
      actual = query.get();
    }
    return actual;
  }
}
