package tech.streamfusion.planner.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class FixturePlanCacheTest {
  @Test
  void everyCallerReceivesIndependentNestedObjectsIncludingTheFirst() throws Exception {
    var cache = new FixturePlanCache();
    var builds = new AtomicInteger();
    var original = new ArrayList<>(List.of(new ArrayList<>(List.of("unopened"))));
    FixturePlanCache.Factory<ArrayList<ArrayList<String>>> factory =
        () -> {
          builds.incrementAndGet();
          return original;
        };
    var first = cache.get(List.of("plan", 1), factory, true);
    first.get(0).add("opened");
    original.get(0).add("builder-mutated");
    var second = cache.get(List.of("plan", 1), factory, true);
    assertEquals(List.of(List.of("unopened")), second);
    assertNotSame(first, second);
    assertNotSame(first.get(0), second.get(0));
    assertEquals(1, builds.get());
  }

  @Test
  void distinctConfigurationAndEvictionRebuildDefinitions() throws Exception {
    var cache = new FixturePlanCache(2, 1024);
    var builds = new AtomicInteger();
    FixturePlanCache.Factory<ArrayList<String>> factory =
        () -> {
          builds.incrementAndGet();
          return new ArrayList<>(List.of("definition"));
        };
    cache.get(List.of("stock", "BIGINT"), factory, true);
    cache.get(List.of("native", "BIGINT"), factory, true);
    cache.get(List.of("stock", "BIGINT"), factory, true);
    cache.get(List.of("stock", "STRING"), factory, true);
    cache.get(List.of("native", "BIGINT"), factory, true);
    assertEquals(4, builds.get());
    cache.get(List.of("native", "BIGINT"), factory, false);
    cache.get(List.of("native", "BIGINT"), factory, false);
    assertEquals(6, builds.get());
  }

  @Test
  void oversizedDefinitionsAreNotRetainedAndFailuresArePropagated() throws Exception {
    var cache = new FixturePlanCache(2, 1);
    var builds = new AtomicInteger();
    FixturePlanCache.Factory<ArrayList<String>> factory =
        () -> {
          builds.incrementAndGet();
          return new ArrayList<>(List.of("too-large"));
        };
    cache.get(List.of("plan"), factory, true);
    cache.get(List.of("plan"), factory, true);
    assertEquals(2, builds.get());
    assertThrows(
        IllegalStateException.class,
        () ->
            cache.get(
                List.of("failure"),
                () -> {
                  throw new IllegalStateException("unchanged");
                },
                true));
  }
}
