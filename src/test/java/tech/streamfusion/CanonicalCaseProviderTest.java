package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

class CanonicalCaseProviderTest {
  @Test
  void selectionUsesConcreteProvidersAndRetainsCriticalRecoveryInteractions() throws Exception {
    var method =
        Parent.class.getDeclaredMethod(
            "handoff", String.class, boolean.class, boolean.class, String.class);
    var all = CanonicalCaseProvider.selected(Control.class, method, "full");
    var selected = CanonicalCaseProvider.selected(Control.class, method, "representative");
    assertEquals(28, all.size());
    assertTrue(selected.size() < all.size());
    assertEquals(interactions(all), interactions(selected));
    assertTrue(rows(all).containsAll(rows(selected)));
    var reversed = new ArrayList<>(all);
    Collections.reverse(reversed);
    assertEquals(
        rows(selected), rows(CanonicalCaseProvider.representatives(reversed, new int[] {0, 2, 3})));
    assertEquals(
        rows(selected),
        rows(CanonicalCaseProvider.selected(Control.class, method, "representative")));
    assertThrows(
        IllegalArgumentException.class,
        () -> CanonicalCaseProvider.selected(Control.class, method, "typo"));
    assertThrows(
        IllegalArgumentException.class,
        () -> CanonicalTestMatrix.representatives(all, new int[] {0, 0, 3}));
  }

  @Test
  void realJunitExecutesSelectedInvocationsWithoutManufacturingPassesOrSkips() throws Exception {
    String previous = System.getProperty("sf.canonical.coverage");
    try {
      for (String policy : List.of("full", "representative")) {
        System.setProperty("sf.canonical.coverage", policy);
        Control.executed.set(0);
        var summary = new SummaryGeneratingListener();
        var request =
            LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(Control.class))
                .configurationParameter("junit.jupiter.extensions.autodetection.enabled", "false")
                .build();
        LauncherFactory.create().execute(request, summary);
        int expected =
            policy.equals("full")
                ? 28
                : CanonicalCaseProvider.representatives(
                        Control.cases().collect(Collectors.toList()), new int[] {0, 2, 3})
                    .size();
        assertEquals(expected, Control.executed.get());
        assertEquals(expected, summary.getSummary().getTestsSucceededCount());
        assertEquals(0, summary.getSummary().getTestsSkippedCount());
        assertEquals(0, summary.getSummary().getTestsAbortedCount());
        assertEquals(
            0,
            summary.getSummary().getTotalFailureCount(),
            summary.getSummary().getFailures().toString());
      }
    } finally {
      if (previous == null) System.clearProperty("sf.canonical.coverage");
      else System.setProperty("sf.canonical.coverage", previous);
    }
  }

  static class Parent {
    static Stream<Arguments> cases() {
      throw new AssertionError("The concrete fixture must override this provider");
    }

    @ParameterizedTest
    @CanonicalCases(
        value = "cases",
        recoveryAxes = {0, 2, 3})
    void handoff(String mode, boolean ordinary, boolean nativeFirst, String backend) {
      Control.executed.incrementAndGet();
    }
  }

  static class Control extends Parent {
    static final AtomicInteger executed = new AtomicInteger();

    static Stream<Arguments> cases() {
      return Stream.of("continue", "checkpoint", "no-input", "rescale")
          .flatMap(
              mode ->
                  Stream.of(false, true)
                      .flatMap(
                          ordinary ->
                              Stream.of(false, true)
                                  .filter(first -> !mode.equals("checkpoint") || first)
                                  .flatMap(
                                      first ->
                                          Stream.of("hashmap", "rocksdb")
                                              .map(
                                                  backend ->
                                                      Arguments.of(
                                                          mode, ordinary, first, backend)))));
    }
  }

  private static List<List<Object>> rows(List<Arguments> entries) {
    return entries.stream().map(entry -> Arrays.asList(entry.get())).collect(Collectors.toList());
  }

  private static Set<List<Object>> interactions(List<Arguments> entries) {
    Set<List<Object>> interactions = new HashSet<>();
    for (var entry : entries) {
      var row = entry.get();
      for (int first = 0; first < row.length; first++)
        for (int second = first; second < row.length; second++)
          interactions.add(Arrays.asList(first, row[first], second, row[second]));
      interactions.add(Arrays.asList(0, row[0], 2, row[2], 3, row[3]));
    }
    return interactions;
  }
}
