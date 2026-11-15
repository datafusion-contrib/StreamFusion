package tech.streamfusion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

/** Optional local iteration matrix; the default and CI retain every invocation. */
public final class CanonicalTestMatrix {
  private CanonicalTestMatrix() {}

  public static Stream<Arguments> select(Stream<Arguments> cases) {
    return select(cases, Boolean.getBoolean("streamfusion.test.canonicalSmoke"));
  }

  static Stream<Arguments> select(Stream<Arguments> cases, boolean smoke) {
    if (!smoke) return cases;
    try (cases) {
      return representatives(cases.collect(Collectors.toList())).stream();
    }
  }

  static List<Arguments> representatives(List<Arguments> cases) {
    var coverage = new ArrayList<Set<List<Object>>>();
    var remaining = new HashSet<List<Object>>();
    int width = cases.isEmpty() ? 0 : cases.get(0).get().length;
    for (var entry : cases) {
      var values = entry.get();
      if (values.length != width) throw new IllegalArgumentException("Mixed matrix widths");
      var pairs = new HashSet<List<Object>>();
      for (int first = 0; first < width; first++)
        for (int second = first; second < width; second++)
          pairs.add(Arrays.asList(first, values[first], second, values[second]));
      coverage.add(pairs);
      remaining.addAll(pairs);
    }
    if (width == 0 && !cases.isEmpty()) return List.of(cases.get(0));
    var selected = new ArrayList<Arguments>();
    while (!remaining.isEmpty()) {
      int best = -1, score = 0;
      for (int index = 0; index < coverage.size(); index++) {
        int candidate = 0;
        for (var pair : coverage.get(index)) if (remaining.contains(pair)) candidate++;
        if (candidate > score) {
          best = index;
          score = candidate;
        }
      }
      selected.add(cases.get(best));
      remaining.removeAll(coverage.get(best));
    }
    return selected;
  }
}
