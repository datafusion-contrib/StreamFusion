package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.BoundsChecking;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullCheckingForGet;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ArrowRuntimeTest {
  private static final String UNSAFE = "arrow.enable_unsafe_memory_access";
  private static final String LEGACY = "drill.enable_unsafe_memory_access";
  private static final String NULLS = "arrow.enable_null_check_for_get";
  private static final String UNSAFE_ENV = "ARROW_ENABLE_UNSAFE_MEMORY_ACCESS";
  private static final String NULLS_ENV = "ARROW_ENABLE_NULL_CHECK_FOR_GET";

  @ParameterizedTest
  @ValueSource(
      strings = {
        "tech.streamfusion.arrow.ArrowRuntime",
        "tech.streamfusion.arrow.ArrowConversion",
        "tech.streamfusion.operator.NativeAllocator",
        "tech.streamfusion.NativeExtensionLoader",
        "tech.streamfusion.Native"
      })
  void initializesBeforeEachIndependentEntryPoint(String entry) throws Exception {
    String output = probe(entry, "none", Map.of(), Map.of(), false, false);
    assertTrue(output.contains(UNSAFE + "=true"), output);
    assertTrue(output.contains(NULLS + "=false"), output);
  }

  @Test
  void preservesBothExplicitSafetyChecks() throws Exception {
    probe(Map.of(UNSAFE, "false", NULLS, "true"), Map.of(), true, true);
  }

  @Test
  void defaultsEachUnspecifiedPropertyIndependently() throws Exception {
    probe(Map.of(UNSAFE, "false"), Map.of(), true, false);
    probe(Map.of(NULLS, "true"), Map.of(), false, true);
  }

  @Test
  void preservesEnvironmentSettingsWithoutAddingOverrides() throws Exception {
    String output = probe(Map.of(), Map.of(UNSAFE_ENV, "false", NULLS_ENV, "true"), true, true);
    assertTrue(output.contains(UNSAFE + "=null"), output);
    assertTrue(output.contains(NULLS + "=null"), output);
  }

  @Test
  void preservesLegacyConfigurationAndItsPrecedenceOverEnvironment() throws Exception {
    String output = probe(Map.of(LEGACY, "false"), Map.of(UNSAFE_ENV, "true"), true, false);
    assertTrue(output.contains(UNSAFE + "=null"), output);
    assertTrue(output.contains(LEGACY + "=false"), output);
  }

  @Test
  void explicitPropertiesTakePrecedenceOverEnvironmentAndLegacy() throws Exception {
    probe(
        Map.of(UNSAFE, "true", LEGACY, "false", NULLS, "false"),
        Map.of(UNSAFE_ENV, "false", NULLS_ENV, "true"),
        false,
        false);
  }

  @Test
  void retainsArrowsExactStringInterpretation() throws Exception {
    probe(Map.of(UNSAFE, "TRUE", NULLS, "FALSE"), Map.of(), true, true);
  }

  @ParameterizedTest
  @ValueSource(strings = {"both", "bounds", "nulls"})
  void reportsChecksThatWereInitializedBeforeTheDefaults(String early) throws Exception {
    String output =
        probe(
            "tech.streamfusion.arrow.ArrowRuntime",
            early,
            Map.of(),
            Map.of(),
            !early.equals("nulls"),
            !early.equals("bounds"));
    assertTrue(output.contains("initialized before the current configuration took effect"), output);
  }

  private static String probe(
      Map<String, String> properties,
      Map<String, String> environment,
      boolean bounds,
      boolean nulls)
      throws Exception {
    String output =
        probe(
            "tech.streamfusion.arrow.ArrowRuntime", "none", properties, environment, bounds, nulls);
    assertFalse(
        output.contains("initialized before the current configuration took effect"), output);
    for (var property : properties.entrySet()) {
      assertTrue(output.contains(property.getKey() + "=" + property.getValue()), output);
    }
    return output;
  }

  private static String probe(
      String entry,
      String early,
      Map<String, String> properties,
      Map<String, String> environment,
      boolean bounds,
      boolean nulls)
      throws Exception {
    List<String> command =
        new ArrayList<>(
            List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "-Djava.library.path=" + System.getProperty("java.library.path"),
                "-Dlog4j.configurationFile="
                    + ArrowRuntimeTest.class
                        .getResource("/arrow-runtime-log4j2.xml")
                        .toExternalForm(),
                "-Dstreamfusion.native.development=true"));
    properties.forEach((key, value) -> command.add("-D" + key + "=" + value));
    command.addAll(
        List.of(
            "-cp",
            System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
            Probe.class.getName(),
            entry,
            early,
            Boolean.toString(bounds),
            Boolean.toString(nulls)));
    ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
    for (String key :
        List.of(UNSAFE_ENV, NULLS_ENV, "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS")) {
      builder.environment().remove(key);
    }
    builder.environment().putAll(environment);
    Path log = Files.createTempFile("arrow-runtime-", ".log");
    try {
      Process process = builder.redirectOutput(log.toFile()).start();
      try {
        assertTrue(process.waitFor(60, TimeUnit.SECONDS), "Arrow probe timed out: " + entry);
        String output = Files.readString(log);
        assertEquals(0, process.exitValue(), output);
        assertEquals(
            1L,
            output.lines().filter(line -> line.contains("Arrow Java access checks:")).count(),
            output);
        return output;
      } finally {
        process.destroyForcibly();
      }
    } finally {
      Files.deleteIfExists(log);
    }
  }

  /** A fresh JVM keeps Surefire flags and previously initialized Arrow classes out of the test. */
  public static final class Probe {
    public static void main(String[] args) throws Exception {
      if (args[1].equals("both") || args[1].equals("bounds")) {
        assertTrue(BoundsChecking.BOUNDS_CHECKING_ENABLED);
      }
      if (args[1].equals("both") || args[1].equals("nulls")) {
        assertTrue(NullCheckingForGet.NULL_CHECKING_ENABLED);
      }
      Class.forName(args[0]);
      boolean bounds = Boolean.parseBoolean(args[2]);
      boolean nulls = Boolean.parseBoolean(args[3]);
      assertEquals(bounds, BoundsChecking.BOUNDS_CHECKING_ENABLED);
      assertEquals(nulls, NullCheckingForGet.NULL_CHECKING_ENABLED);
      for (String key : List.of(UNSAFE, LEGACY, NULLS)) {
        System.out.println(key + "=" + System.getProperty(key));
      }
      ArrowRuntime.initialize();
      ArrowRuntime.initialize();
      assertEquals(bounds, BoundsChecking.BOUNDS_CHECKING_ENABLED);
      assertEquals(nulls, NullCheckingForGet.NULL_CHECKING_ENABLED);
      try (var allocator = new RootAllocator(1 << 20);
          var vector = new IntVector("v", allocator)) {
        vector.allocateNew(2);
        vector.setSafe(0, 7);
        vector.setSafe(1, 123456789);
        vector.setNull(1);
        vector.setValueCount(2);
        assertEquals(7, vector.get(0));
        assertTrue(vector.isNull(1));
        assertNull(vector.getObject(1));
        if (nulls) assertThrows(IllegalStateException.class, () -> vector.get(1));
        else assertEquals(123456789, vector.get(1));
        vector.setSafe(10000, 99);
        assertEquals(99, vector.get(10000));
      }
      roundTripRows();
    }

    private static void roundTripRows() {
      RowType type =
          RowType.of(
              new IntType(),
              new BigIntType(),
              new DecimalType(20, 3),
              new VarCharType(),
              new TimestampType(9));
      GenericRowData[] input = {
        GenericRowData.of(
            Integer.MIN_VALUE,
            Long.MAX_VALUE,
            DecimalData.fromBigDecimal(new BigDecimal("12345678901234567.890"), 20, 3),
            StringData.fromString("\ud83d\ude00\u4e2d"),
            TimestampData.fromEpochMillis(-1, 999999)),
        new GenericRowData(5),
        GenericRowData.of(0, null, null, StringData.fromString(""), null)
      };
      try (var allocator = new RootAllocator(1 << 20);
          var root = VectorSchemaRoot.create(ArrowConversion.toArrowSchema(type), allocator)) {
        var writer = ArrowConversion.createRowDataArrowWriter(root, type, 1);
        for (int batch = 0; batch < 2; batch++) {
          for (var row : input) writer.write(row);
          writer.finish();
          assertEquals(input.length, root.getRowCount());
          var reader = ArrowConversion.createArrowReader(root, type);
          for (int row = 0; row < input.length; row++) {
            for (int field = 0; field < type.getFieldCount(); field++) {
              var getter = RowData.createFieldGetter(type.getTypeAt(field), field);
              assertEquals(input[row].getField(field), getter.getFieldOrNull(reader.read(row)));
            }
          }
          writer.reset();
        }
      }
    }
  }
}
