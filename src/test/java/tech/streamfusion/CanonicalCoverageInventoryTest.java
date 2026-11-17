package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Inventories real providers without starting a cluster or a worker JVM. */
@EnabledIfSystemProperty(named = "sf.canonical.inventory.output", matches = ".+")
class CanonicalCoverageInventoryTest {
  @Test
  void inventoryEveryCompiledCanonicalFixture() throws Exception {
    Path classes = Path.of(getClass().getProtectionDomain().getCodeSource().getLocation().toURI());
    var inventory = new TreeMap<String, Object>();
    int fullTotal = 0, representativeTotal = 0;
    try (var paths = Files.list(classes.resolve("tech/streamfusion"))) {
      for (Path path : paths.sorted().collect(Collectors.toList())) {
        String name = path.getFileName().toString();
        if (!name.startsWith("Canonical") || !name.endsWith("JobTest.class") || name.contains("$"))
          continue;
        String className = "tech.streamfusion." + name.substring(0, name.length() - 6);
        Class<?> type = Class.forName(className, false, getClass().getClassLoader());
        var methods = new TreeMap<String, Object>();
        int fullCount = 0, representativeCount = 0;
        if (!Modifier.isAbstract(type.getModifiers())) {
          for (Method test : testMethods(type).values()) {
            Map<String, Object> selection;
            var source = test.getAnnotation(CanonicalCases.class);
            if (source != null) {
              var all = CanonicalCaseProvider.all(type, test);
              var selected = CanonicalCaseProvider.representatives(all, source.recoveryAxes());
              var rows = all.stream().map(a -> Arrays.asList(a.get())).collect(Collectors.toList());
              var indices =
                  selected.stream()
                      .map(a -> rows.indexOf(Arrays.asList(a.get())))
                      .collect(Collectors.toList());
              assertFalse(indices.isEmpty(), className + "#" + test.getName());
              assertEquals(indices.size(), indices.stream().distinct().count());
              selection = new LinkedHashMap<>();
              selection.put("kind", "parameterized");
              selection.put("full", all.size());
              selection.put("representative", selected.size());
              selection.put("full_arguments", rows);
              selection.put("selected_indices", indices);
              selection.put("recovery_axes", source.recoveryAxes());
            } else if (test.isAnnotationPresent(ParameterizedTest.class)) {
              assertNull(
                  test.getAnnotation(MethodSource.class), "Unmanaged canonical provider: " + test);
              var values = test.getAnnotation(ValueSource.class);
              var csv = test.getAnnotation(CsvSource.class);
              int count;
              if (values != null) {
                count = values.strings().length;
                assertTrue(count > 0, "Inventory must support this ValueSource: " + test);
              } else {
                assertNotNull(csv, "Inventory must support this parameter source: " + test);
                count = csv.value().length;
                assertTrue(count > 0, "Inventory must support this CsvSource: " + test);
              }
              selection = Map.of("kind", "parameterized", "full", count, "representative", count);
            } else {
              selection = Map.of("kind", "test", "full", 1, "representative", 1);
            }
            assertNull(
                methods.put(test.getName(), selection), "Overloaded canonical test: " + test);
            fullCount += ((Number) selection.get("full")).intValue();
            representativeCount += ((Number) selection.get("representative")).intValue();
          }
          assertTrue(fullCount > 0, "No inventoried tests: " + className);
        }
        inventory.put(
            className,
            Map.of(
                "abstract", Modifier.isAbstract(type.getModifiers()),
                "full", fullCount,
                "representative", representativeCount,
                "methods", methods));
        fullTotal += fullCount;
        representativeTotal += representativeCount;
      }
    }
    assertTrue(inventory.size() > 100, "The canonical inventory must not be empty or partial");
    assertTrue(representativeTotal < fullTotal, "Representative coverage must reduce the matrix");
    var result =
        Map.of(
            "schema_version",
            1,
            "line",
            System.getProperty("sf.canonical.inventory.line"),
            "jdk",
            Runtime.version().feature(),
            "full_cases",
            fullTotal,
            "representative_cases",
            representativeTotal,
            "classes",
            inventory);
    Path output = Path.of(System.getProperty("sf.canonical.inventory.output"));
    Files.createDirectories(output.toAbsolutePath().getParent());
    new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(output.toFile(), result);
    System.out.println(
        "Canonical coverage: "
            + representativeTotal
            + " representative / "
            + fullTotal
            + " full cases");
  }

  private static Map<String, Method> testMethods(Class<?> type) {
    var methods = new TreeMap<String, Method>();
    for (Class<?> current = type; current != null; current = current.getSuperclass()) {
      for (Method method : current.getDeclaredMethods()) {
        if (method.isAnnotationPresent(Test.class)
            || method.isAnnotationPresent(ParameterizedTest.class))
          methods.putIfAbsent(
              method.getName() + Arrays.toString(method.getParameterTypes()), method);
      }
    }
    return methods;
  }
}
