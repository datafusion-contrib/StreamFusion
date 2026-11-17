package tech.streamfusion.doris;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DorisFixtureVisibilityTest {
  @Test
  void waitsForCommittedRowsThenReturnsTheirUnmodifiedValues() {
    var reads = new AtomicInteger();
    var rows = DorisFixtureVisibility.await(
        () -> reads.incrementAndGet() < 3 ? List.of() : List.of("wrong-value"), 1, 1000);
    assertEquals(3, reads.get());
    assertEquals(List.of("wrong-value"), rows, "value mismatches must still reach the upstream assertion");
  }

  @Test
  void missingRowsAndQueryFailuresCannotBecomeSuccessfulResults() {
    assertEquals(List.of(), DorisFixtureVisibility.await(List::of, 3, 10));
    var reads = new AtomicInteger();
    var error = new IllegalStateException("query failed");
    assertSame(error, assertThrows(IllegalStateException.class,
        () -> DorisFixtureVisibility.await(() -> {
          reads.incrementAndGet();
          throw error;
        }, 3, 1000)));
    assertEquals(1, reads.get());
  }
}
