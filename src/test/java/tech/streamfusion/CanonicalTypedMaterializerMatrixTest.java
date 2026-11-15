package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;
import tech.streamfusion.compat.MaterializerStateCompat;

class CanonicalTypedMaterializerMatrixTest {
  @Test
  void shapeSuitesRetainEveryOriginalConfigurationExactlyOnce() throws Exception {
    Assumptions.assumeFalse(Boolean.getBoolean("streamfusion.test.canonicalSmoke"));
    Assumptions.assumeFalse(Boolean.getBoolean("streamfusion.test.groupArithmeticJobSmoke"));
    var suites =
        Map.ofEntries(
            Map.entry(CanonicalTypedMaterializerTinyIntJobTest.class, "scalar-tinyint"),
            Map.entry(CanonicalTypedMaterializerIntJobTest.class, "scalar-int"),
            Map.entry(CanonicalTypedMaterializerDoubleJobTest.class, "scalar-double"),
            Map.entry(CanonicalTypedMaterializerDecimalJobTest.class, "scalar-wide-decimal"),
            Map.entry(CanonicalTypedMaterializerTimestampJobTest.class, "scalar-timestamp-nanos"),
            Map.entry(CanonicalTypedMaterializerBinaryJobTest.class, "scalar-varbinary"),
            Map.entry(CanonicalTypedMaterializerArrayJobTest.class, "array"),
            Map.entry(CanonicalTypedMaterializerRowPayloadJobTest.class, "row"),
            Map.entry(CanonicalTypedMaterializerMapPayloadJobTest.class, "map"),
            Map.entry(CanonicalTypedMaterializerRowKeyJobTest.class, "row-key"),
            Map.entry(CanonicalTypedMaterializerNestedRowKeyJobTest.class, "nested-row-key"));
    assertTrue(Modifier.isAbstract(CanonicalTypedMaterializerJobTest.class.getModifiers()));
    assertEquals(
        CanonicalMaterializerTypeFixture.shapes().collect(Collectors.toSet()),
        new HashSet<>(suites.values()));
    Set<List<Object>> actual = new HashSet<>();
    int strategies = MaterializerStateCompat.supportsV2() ? 5 : 1;
    for (var suite : suites.entrySet())
      for (boolean inputKey : new boolean[] {false, true}) {
        var type =
            inputKey
                ? Class.forName(suite.getKey().getName().replace("JobTest", "InputKeyJobTest"))
                : suite.getKey();
        assertEquals(CanonicalTypedMaterializerJobTest.class, type.getSuperclass());
        var method = type.getDeclaredMethod("cases");
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Stream<Arguments> cases = (Stream<Arguments>) method.invoke(null);
        int count = 0;
        try (cases) {
          for (var entry : cases.collect(Collectors.toList())) {
            var values = entry.get();
            assertEquals(8, values.length);
            assertEquals(suite.getValue(), values[0]);
            assertEquals(inputKey, values[2]);
            assertTrue(actual.add(Arrays.asList(values)), "Duplicate configuration across suites");
            count++;
          }
        }
        assertEquals(strategies * 56, count, type.getSimpleName());
      }
    Set<List<Object>> expected = new HashSet<>();
    var variants =
        MaterializerStateCompat.supportsV2()
            ? List.of("LEGACY", "VALUE", "MAP", "ADAPTIVE_VALUE", "ADAPTIVE_MAP")
            : List.of("LEGACY");
    for (String shape : suites.values())
      for (String strategy : variants)
        for (boolean inputKey : new boolean[] {false, true})
          for (String uid : List.of("DISABLED", "ALWAYS"))
            for (String mode : List.of("continue", "no-input", "checkpoint", "rescale"))
              for (boolean ordinary : new boolean[] {false, true})
                for (boolean first : new boolean[] {false, true})
                  for (String backend : List.of("hashmap", "rocksdb"))
                    if (!mode.equals("checkpoint") || first)
                      expected.add(
                          List.of(shape, strategy, inputKey, uid, mode, ordinary, first, backend));
    assertEquals(expected, actual);
    assertEquals(strategies * 1232, actual.size());
    Set<List<Object>> ordinary = new HashSet<>();
    for (var suite :
        List.of(
            CanonicalSinkMaterializerWithInputKeyJobTest.class,
            CanonicalSinkMaterializerWithoutInputKeyJobTest.class)) {
      var provider = suite.getDeclaredMethod("cases");
      provider.setAccessible(true);
      @SuppressWarnings("unchecked")
      Stream<Arguments> cases = (Stream<Arguments>) provider.invoke(null);
      try (cases) {
        for (var entry : cases.collect(Collectors.toList()))
          assertTrue(ordinary.add(Arrays.asList(entry.get())));
      }
    }
    assertEquals(
        expected.stream()
            .filter(row -> row.get(0).equals("scalar-tinyint"))
            .map(row -> row.subList(1, row.size()))
            .collect(Collectors.toSet()),
        ordinary);
  }
}
