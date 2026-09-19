package tech.streamfusion.loader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.planner.loader.PlannerModule;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies planner installation from the classpath, without an application-side hook. */
class StreamFusionPlannerLoaderTest {
  @TempDir Path directory;

  @Test
  void acceptsARenamedMatchingExtensionAndRejectsAnUnmarkedLegacyExtension() throws Exception {
    Path renamed = directory.resolve("renamed.jar");
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    String line = loaderLine();
    manifest
        .getMainAttributes()
        .putValue(
            "StreamFusion-Module",
            "streamfusion-json" + (line.equals("2.2") ? "" : "-flink" + line));
    manifest.getMainAttributes().putValue("StreamFusion-Flink-Line", line);
    try (var ignored = new JarOutputStream(Files.newOutputStream(renamed), manifest)) {}
    Path legacy = directory.resolve("01-streamfusion-paimon.jar");
    try (var ignored = new JarOutputStream(Files.newOutputStream(legacy))) {}
    String original = System.getProperty("java.class.path");
    var constructor = PlannerModule.class.getDeclaredConstructor();
    constructor.setAccessible(true);

    try {
      System.setProperty("java.class.path", original + java.io.File.pathSeparator + renamed);
      PlannerModule.main(new String[0]);
      var module = constructor.newInstance();
      try (var classLoader = module.getSubmoduleClassLoader()) {
        assertTrue(List.of(classLoader.getURLs()).contains(renamed.toUri().toURL()));
      }

      System.setProperty("java.class.path", original + java.io.File.pathSeparator + legacy);
      var startupFailure =
          assertThrows(
              org.apache.flink.table.api.TableException.class,
              () -> PlannerModule.main(new String[0]));
      assertTrue(startupFailure.getMessage().contains("missing marker"));
      var failure = assertThrows(InvocationTargetException.class, constructor::newInstance);
      assertTrue(failure.getCause().getMessage().contains("missing marker"));
    } finally {
      System.setProperty("java.class.path", original);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"streamfusion-core", "streamfusion-json", "streamfusion-paimon"})
  void rejectsMixedInstalledPayloadsBeforeCreatingThePlanner(String module) throws Exception {
    // A renamed JAR must retain its identity, including modules loaded by Flink's host classloader.
    Path jar = directory.resolve("renamed.jar");
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().putValue("StreamFusion-Module", module);
    String line = loaderLine();
    String otherLine = line.equals("2.2") ? "1.18" : "2.2";
    manifest.getMainAttributes().putValue("StreamFusion-Flink-Line", otherLine);
    try (var ignored = new JarOutputStream(Files.newOutputStream(jar), manifest)) {}
    String original = System.getProperty("java.class.path");
    var constructor = PlannerModule.class.getDeclaredConstructor();
    constructor.setAccessible(true);

    try {
      System.setProperty("java.class.path", original + java.io.File.pathSeparator + jar);
      var startupFailure =
          assertThrows(
              org.apache.flink.table.api.TableException.class,
              () -> PlannerModule.main(new String[0]));
      assertTrue(startupFailure.getMessage().contains("loader targets Flink " + line));
      assertTrue(startupFailure.getMessage().contains("targets Flink " + otherLine));
      var failure = assertThrows(InvocationTargetException.class, constructor::newInstance);

      assertTrue(failure.getCause().getMessage().contains("loader targets Flink " + line));
      assertTrue(failure.getCause().getMessage().contains("targets Flink " + otherLine));
    } finally {
      System.setProperty("java.class.path", original);
    }
  }

  @Test
  void corePayloadDoesNotContainOptionalConnectorIntegrations() {
    ClassLoader plannerClassLoader = PlannerModule.getInstance().getSubmoduleClassLoader();

    assertMissing(plannerClassLoader, "tech.streamfusion.planner.KafkaTables");
    assertMissing(plannerClassLoader, "tech.streamfusion.kafka.NativeKafka");
    assertMissing(plannerClassLoader, "tech.streamfusion.planner.ParquetSourceMatcher");
    assertMissing(plannerClassLoader, "tech.streamfusion.parquet.NativeParquet");
  }

  @Test
  void installsTheNativePlannerStageWithoutApplicationCode() throws Exception {
    assertNotNull(
        PlannerModule.class.getResource("/streamfusion-planner.jar"),
        "the loader artifact must embed the StreamFusion runtime payload");

    TableEnvironment tableEnvironment =
        TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    String sql = "SELECT c0 * 2 AS doubled FROM (VALUES (3), (4), (5)) AS t(c0)";

    String explain = tableEnvironment.explainSql(sql);
    assertTrue(explain.contains("NativeCalc"), "StreamFusion was not installed:\n" + explain);
    assertEquals(List.of(6, 8, 10), collectInts(tableEnvironment.executeSql(sql)));
  }

  @Test
  void installsNativeStatefulPlannerWithConfiguredHostBackend() throws Exception {
    org.apache.flink.configuration.Configuration configuration =
        new org.apache.flink.configuration.Configuration();
    configuration.set(
        org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND,
        "tech.streamfusion.state.RocksDBNativeStateBackendFactory");
    TableEnvironment table =
        TableEnvironment.create(
            EnvironmentSettings.newInstance()
                .inStreamingMode()
                .withConfiguration(configuration)
                .build());
    String sql = "SELECT c0, SUM(c1) FROM (VALUES (1, 2), (1, 3), (2, 4)) AS t(c0, c1) GROUP BY c0";
    Class<?> planner =
        Class.forName(
            "tech.streamfusion.planner.NativePlanner",
            true,
            PlannerModule.getInstance().getSubmoduleClassLoader());
    String explain =
        (String)
            planner
                .getMethod("explain", TableEnvironment.class, String.class)
                .invoke(null, table, sql);
    assertTrue(explain.contains("NativeColumnarGroupAggregate"), explain);
  }

  private static String loaderLine() throws Exception {
    Properties properties = new Properties();
    try (var input = PlannerModule.class.getResourceAsStream("streamfusion-loader.properties")) {
      assertNotNull(input);
      properties.load(input);
    }
    return properties.getProperty("flink.line");
  }

  private static List<Integer> collectInts(TableResult result) throws Exception {
    List<Integer> values = new ArrayList<>();
    try (CloseableIterator<Row> rows = result.collect()) {
      while (rows.hasNext()) {
        values.add((Integer) rows.next().getField(0));
      }
    }
    values.sort(null);
    return values;
  }

  private static void assertMissing(ClassLoader classLoader, String className) {
    assertThrows(
        ClassNotFoundException.class,
        () -> Class.forName(className, false, classLoader),
        "the core payload must not carry optional integration " + className);
  }
}
