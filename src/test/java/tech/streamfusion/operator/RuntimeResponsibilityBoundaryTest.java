package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.aggregate.stream.JvmAccumulatorCache;
import tech.streamfusion.operator.aggregate.stream.JvmMiniBatchGroup;
import tech.streamfusion.operator.batch.BoundedArrowOutput;
import tech.streamfusion.operator.join.stream.FlinkWindowJoinMetrics;
import tech.streamfusion.operator.stream.MiniBatchBoundary;
import tech.streamfusion.operator.stream.MiniBatchMetrics;
import tech.streamfusion.operator.window.stream.FlinkWindowMetrics;
import tech.streamfusion.operator.window.stream.WindowTimeDomain;

class RuntimeResponsibilityBoundaryTest {
  private static final List<Class<?>> HELPERS =
      List.of(
          JvmAccumulatorCache.class,
          JvmMiniBatchGroup.class,
          BoundedArrowOutput.class,
          FlinkWindowJoinMetrics.class,
          MiniBatchBoundary.class,
          MiniBatchMetrics.class,
          FlinkWindowMetrics.class,
          WindowTimeDomain.class);

  @Test
  void deployedSerializationIdentitiesAndConfigurationFieldsRemainStable() throws Exception {
    try (var input =
        getClass().getResourceAsStream("/runtime-compatibility/operator-descriptors.tsv")) {
      assertNotNull(input);
      var reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
      int checked = 0;
      for (String line : reader.lines().collect(Collectors.toList())) {
        if (line.startsWith("#") || line.isEmpty()) continue;
        String[] expected = line.split("\t", -1);
        Class<?> type = Class.forName(expected[0], false, getClass().getClassLoader());
        ObjectStreamClass descriptor = ObjectStreamClass.lookup(type);
        assertNotNull(descriptor, expected[0]);
        assertEquals(Long.parseLong(expected[1]), descriptor.getSerialVersionUID(), expected[0]);
        String fields =
            Arrays.stream(descriptor.getFields())
                .map(
                    field ->
                        field.getName()
                            + ":"
                            + (field.getTypeString() == null
                                ? field.getTypeCode()
                                : field.getTypeString()))
                .collect(Collectors.joining(","));
        assertEquals(expected[2], fields, expected[0]);
        for (var field : type.getDeclaredFields()) {
          if (HELPERS.contains(field.getType())) {
            assertTrue(Modifier.isPrivate(field.getModifiers()), field.toString());
            assertTrue(Modifier.isTransient(field.getModifiers()), field.toString());
          }
        }
        checked++;
      }
      assertEquals(73, checked, "All pre-refactor operator descriptors must be checked");
    }
  }

  @Test
  void runtimeHelpersAreNotDurableConfigurationOrPlannerImplementations() throws Exception {
    for (Class<?> helper : HELPERS) {
      assertFalse(Serializable.class.isAssignableFrom(helper), helper.getName());
      String resource = "/" + helper.getName().replace('.', '/') + ".class";
      try (var input = helper.getResourceAsStream(resource)) {
        assertNotNull(input, resource);
        String constants = new String(input.readAllBytes(), StandardCharsets.ISO_8859_1);
        assertFalse(constants.contains("tech/streamfusion/planner/"), helper.getName());
        if (helper == BoundedArrowOutput.class) {
          assertFalse(constants.contains("tech/streamfusion/operator/stream/"));
          assertFalse(constants.contains("tech/streamfusion/operator/window/"));
        } else {
          assertFalse(constants.contains("tech/streamfusion/operator/batch/"), helper.getName());
        }
      }
    }
  }
}
