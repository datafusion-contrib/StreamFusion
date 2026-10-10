package tech.streamfusion.compat;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/** Java 11 collector with the null and mutability contract of Stream.toList(). */
public final class ListCollectors {
  private ListCollectors() {}

  public static <T> Collector<T, ?, List<T>> toList() {
    return Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList);
  }
}
