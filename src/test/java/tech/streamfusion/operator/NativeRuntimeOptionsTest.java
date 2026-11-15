package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.compat.FlinkStreamOperator;
import tech.streamfusion.configuration.NativeRuntimeOptions;
import tech.streamfusion.planner.NativeConfig;

class NativeRuntimeOptionsTest {
  @Test
  void serializedOperatorsRetainIndependentJobSettingsWhenWorkerDefaultsDiffer() throws Exception {
    Configuration first = new Configuration();
    first.set(NativeRuntimeOptions.COALESCE_ROWS, 1);
    first.set(NativeRuntimeOptions.COALESCE_LATENCY_MS, 0L);
    first.set(NativeRuntimeOptions.DIRECT_TEMPORAL_CALC, false);
    first.set(NativeRuntimeOptions.NESTED_SCALAR_UDFS, false);
    first.set(NativeRuntimeOptions.BINARY_ELT, false);
    Configuration second = new Configuration();
    second.set(NativeRuntimeOptions.COALESCE_ROWS, 73);
    second.set(NativeRuntimeOptions.NESTED_SCALAR_UDFS, true);
    second.set(NativeRuntimeOptions.BINARY_ELT, true);
    Probe firstOperator;
    Probe secondOperator;
    try (var ignored = NativeConfig.usePlannerConfig(first)) {
      firstOperator = new Probe();
    }
    try (var ignored = NativeConfig.usePlannerConfig(second)) {
      secondOperator = new Probe();
    }
    first.set(NativeRuntimeOptions.COALESCE_ROWS, 999);

    Map<String, String> worker =
        Map.of(
            NativeRuntimeOptions.COALESCE_ROWS.key(), "2048",
            NativeRuntimeOptions.DIRECT_TEMPORAL_CALC.key(), "true",
            NativeRuntimeOptions.NESTED_SCALAR_UDFS.key(), "true",
            NativeRuntimeOptions.BINARY_ELT.key(), "true");
    withProperties(
        worker,
        () -> {
          NativeRuntimeOptions a = roundTrip(firstOperator).options();
          NativeRuntimeOptions b = roundTrip(secondOperator).options();
          assertEquals(1, a.get(NativeRuntimeOptions.COALESCE_ROWS));
          assertEquals(73, b.get(NativeRuntimeOptions.COALESCE_ROWS));
          assertEquals(0L, a.get(NativeRuntimeOptions.COALESCE_LATENCY_MS));
          assertFalse(a.get(NativeRuntimeOptions.DIRECT_TEMPORAL_CALC));
          RowType binary = RowType.of(new BinaryType(4));
          assertTrue(
              a.arrowSchema(binary).getFields().get(0).getType()
                  instanceof ArrowType.FixedSizeBinary);
          assertTrue(
              b.arrowSchema(binary).getFields().get(0).getType() instanceof ArrowType.Binary);
          assertTrue(tech.streamfusion.arrow.ArrowConversion.nestedScalarUdfsEnabled());
          try (var ignored = a.use()) {
            assertFalse(tech.streamfusion.arrow.ArrowConversion.nestedScalarUdfsEnabled());
            try (var nested = b.use()) {
              assertTrue(tech.streamfusion.arrow.ArrowConversion.nestedScalarUdfsEnabled());
            }
            assertFalse(tech.streamfusion.arrow.ArrowConversion.nestedScalarUdfsEnabled());
          }
          assertTrue(tech.streamfusion.arrow.ArrowConversion.nestedScalarUdfsEnabled());
        });
  }

  @Test
  void legacySerializedOperatorRetainsItsWorkerDefaultBehavior() throws Exception {
    withProperties(
        Map.of(NativeRuntimeOptions.COALESCE_ROWS.key(), "37"),
        () -> {
          try (var resource =
              getClass()
                  .getResourceAsStream(
                      "/runtime-compatibility/row-to-arrow-pre-runtime-options.bin")) {
            assertNotNull(resource);
            try (var input = new ObjectInputStream(resource)) {
              Object operator = input.readObject();
              assertTrue(operator instanceof RowDataToArrowOperator);
              var accessor = FlinkStreamOperator.class.getDeclaredMethod("runtimeOptions");
              accessor.setAccessible(true);
              var options = (NativeRuntimeOptions) accessor.invoke(operator);
              assertEquals(37, options.get(NativeRuntimeOptions.COALESCE_ROWS));
            }
          }
        });
  }

  @Test
  void runtimeOptionsAreValidatedWhilePlanning() {
    Configuration config = new Configuration();
    config.setString(NativeRuntimeOptions.COALESCE_ROWS.key(), "invalid");
    try (var ignored = NativeConfig.usePlannerConfig(config)) {
      assertThrows(IllegalArgumentException.class, Probe::new);
    }
  }

  private static Probe roundTrip(Probe value) throws Exception {
    var bytes = new ByteArrayOutputStream();
    try (var output = new ObjectOutputStream(bytes)) {
      output.writeObject(value);
    }
    try (var input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (Probe) input.readObject();
    }
  }

  private static void withProperties(
      Map<String, String> values, org.apache.flink.util.function.ThrowingRunnable<Exception> check)
      throws Exception {
    Map<String, String> previous = new LinkedHashMap<>();
    values.forEach(
        (key, value) -> {
          previous.put(key, System.getProperty(key));
          System.setProperty(key, value);
        });
    try {
      check.run();
    } finally {
      previous.forEach(
          (key, value) -> {
            if (value == null) System.clearProperty(key);
            else System.setProperty(key, value);
          });
    }
  }

  private static final class Probe extends FlinkStreamOperator<Object> {
    private static final long serialVersionUID = 1L;

    NativeRuntimeOptions options() {
      return runtimeOptions();
    }
  }
}
