package tech.streamfusion;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

public final class CanonicalCaseProvider implements ArgumentsProvider {
  @Override
  public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
    var method = context.getRequiredTestMethod();
    return selected(
        context.getRequiredTestClass(), method, System.getProperty("sf.canonical.coverage", "full"))
        .stream();
  }

  static List<Arguments> selected(Class<?> type, Method test, String coverage) throws Exception {
    if (!coverage.equals("full") && !coverage.equals("representative"))
      throw new IllegalArgumentException("Unknown canonical coverage: " + coverage);
    var cases = all(type, test);
    if (coverage.equals("full")) return cases;
    return representatives(cases, test.getAnnotation(CanonicalCases.class).recoveryAxes());
  }

  static List<Arguments> representatives(List<Arguments> cases, int[] recoveryAxes) {
    var ordered = new ArrayList<>(cases);
    ordered.sort(
        (left, right) -> {
          for (int index = 0; index < left.get().length; index++) {
            int compared = token(left.get()[index]).compareTo(token(right.get()[index]));
            if (compared != 0) return compared;
          }
          return 0;
        });
    return CanonicalTestMatrix.representatives(ordered, recoveryAxes);
  }

  private static String token(Object value) {
    if (value == null) return "";
    if (value instanceof Enum<?>)
      return value.getClass().getName() + ":" + ((Enum<?>) value).name();
    if (value instanceof String || value instanceof Number || value instanceof Boolean)
      return value.getClass().getName() + ":" + value;
    throw new IllegalArgumentException(
        "Canonical matrix arguments need stable scalar identities: " + value.getClass());
  }

  static List<Arguments> all(Class<?> type, Method test) throws Exception {
    var source = test.getAnnotation(CanonicalCases.class);
    if (source == null) throw new IllegalArgumentException("Missing canonical provider: " + test);
    var result = new ArrayList<Arguments>();
    for (String name : source.value()) {
      var provider = provider(type, name);
      if (!Modifier.isStatic(provider.getModifiers()) || provider.getParameterCount() != 0)
        throw new IllegalArgumentException("Canonical providers must be static and parameterless");
      provider.setAccessible(true);
      try (var stream = (Stream<?>) provider.invoke(null)) {
        for (Object entry : stream.collect(Collectors.toList())) {
          if (entry instanceof Arguments) result.add((Arguments) entry);
          else if (test.getParameterCount() == 1) result.add(Arguments.of(entry));
          else if (entry instanceof Object[]) result.add(Arguments.of((Object[]) entry));
          else throw new IllegalArgumentException("Unsupported canonical arguments: " + provider);
        }
      }
    }
    if (result.isEmpty()) throw new IllegalArgumentException("Empty canonical provider: " + test);
    for (var entry : result)
      if (entry.get().length != test.getParameterCount())
        throw new IllegalArgumentException("Canonical argument width differs from test: " + test);
    return result;
  }

  private static Method provider(Class<?> type, String name) throws NoSuchMethodException {
    for (Class<?> current = type; current != null; current = current.getSuperclass()) {
      try {
        return current.getDeclaredMethod(name);
      } catch (NoSuchMethodException ignored) {
        // An inherited test can use a provider overridden by the concrete fixture.
      }
    }
    throw new NoSuchMethodException(type.getName() + "#" + name);
  }
}
