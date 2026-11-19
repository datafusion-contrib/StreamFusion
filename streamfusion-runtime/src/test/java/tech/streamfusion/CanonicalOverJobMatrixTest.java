package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;
import tech.streamfusion.compat.FlinkPlanCompat;

class CanonicalOverJobMatrixTest {
  @Test
  void clockAndFrameSuitesRetainEveryOriginalExtremaAndTypedKeyConfiguration() throws Exception {
    Assumptions.assumeFalse(Boolean.getBoolean("streamfusion.test.canonicalSmoke"));
    Assumptions.assumeFalse(Boolean.getBoolean("streamfusion.test.groupArithmeticJobSmoke"));
    Set<List<Object>> extrema =
        collect(
            CanonicalUnboundedProcessingOverExtremaJobTest.class,
            CanonicalUnboundedEventRowsOverExtremaJobTest.class,
            CanonicalUnboundedEventRangeOverExtremaJobTest.class);
    Set<List<Object>> expectedExtrema = new HashSet<>();
    for (String clock : List.of("proc", "event"))
      for (String frame : List.of("rows", "range"))
        for (String type :
            List.of(
                "BIGINT",
                "TINYINT",
                "FLOAT",
                "DOUBLE",
                "DECIMAL(18,2)",
                "DECIMAL(38,20)",
                "STRING"))
          for (int version = 1;
              version <= (clock.equals("proc") ? 1 : FlinkPlanCompat.unboundedOverVersion());
              version++)
            expectedExtrema.addAll(handoffs(clock + ":" + frame + ":" + type + ":" + version));
    assertEquals(expectedExtrema, extrema);
    assertEquals(392 * (1 + FlinkPlanCompat.unboundedOverVersion()), extrema.size());
    Set<List<Object>> keys =
        collect(
            CanonicalTypedKeyEventIntKeyOverJobTest.class,
                CanonicalTypedKeyEventStringKeyOverJobTest.class,
            CanonicalTypedKeyEventTinyIntValueOverJobTest.class,
                CanonicalTypedKeyEventSmallIntValueOverJobTest.class,
            CanonicalTypedKeyEventIntValueOverJobTest.class,
                CanonicalTypedKeyEventDecimalValueOverJobTest.class,
            CanonicalTypedKeyEventWideDecimalValueOverJobTest.class,
                CanonicalTypedKeyEventHighScaleDecimalValueOverJobTest.class);
    Set<List<Object>> expectedKeys = new HashSet<>();
    for (String frame : List.of("rows", "range"))
      for (String types :
          List.of(
              "INT:BIGINT",
              "STRING:BIGINT",
              "BIGINT:TINYINT",
              "BIGINT:SMALLINT",
              "BIGINT:INT",
              "BIGINT:DECIMAL(18,2)",
              "BIGINT:DECIMAL(38,2)",
              "BIGINT:DECIMAL(38,20)"))
        for (int version = 1; version <= FlinkPlanCompat.unboundedOverVersion(); version++)
          expectedKeys.addAll(handoffs(frame + ":" + types + ":" + version));
    assertEquals(expectedKeys, keys);
    assertEquals(448 * FlinkPlanCompat.unboundedOverVersion(), keys.size());
    var payloads =
        collect(
            CanonicalOverCountRowsPayloadJobTest.class,
            CanonicalOverCountRangePayloadJobTest.class);
    Set<List<Object>> expectedPayloads = new HashSet<>();
    for (String clock : List.of("proc", "event"))
      for (String frame : List.of("rows", "range"))
        for (String shape : List.of("scalar-varbinary", "array", "row", "map"))
          for (int version = 1;
              version <= (clock.equals("proc") ? 1 : FlinkPlanCompat.unboundedOverVersion());
              version++)
            expectedPayloads.addAll(handoffs(clock + ":" + frame + ":" + shape + ":" + version));
    assertEquals(expectedPayloads, payloads);
    assertEquals(224 * (1 + FlinkPlanCompat.unboundedOverVersion()), payloads.size());
  }

  private static Set<List<Object>> handoffs(String kind) {
    Set<List<Object>> result = new HashSet<>();
    for (String mode : List.of("continue", "no-input", "checkpoint", "rescale"))
      for (boolean ordinary : new boolean[] {false, true})
        for (boolean first : new boolean[] {false, true})
          for (String backend : List.of("hashmap", "rocksdb"))
            if (!mode.equals("checkpoint") || first)
              result.add(List.of(kind, mode, ordinary, first, backend));
    return result;
  }

  private static Set<List<Object>> collect(Class<?>... suites) throws Exception {
    Set<List<Object>> result = new HashSet<>();
    for (var suite : suites)
      for (String provider : List.of("continuedCases", "recoveryCases")) {
        var method = suite.getDeclaredMethod(provider);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Stream<Arguments> cases = (Stream<Arguments>) method.invoke(null);
        try (cases) {
          for (var entry : cases.collect(Collectors.toList())) {
            var values = Arrays.asList(entry.get());
            assertEquals(provider.equals("continuedCases"), values.get(1).equals("continue"));
            assertTrue(result.add(values), "Duplicate configuration across clock/frame suites");
          }
        }
      }
    return result;
  }
}
