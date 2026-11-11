package tech.streamfusion.loader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
  void installsBatchPlannerFromThePackagedPayload() throws Exception {
    TableEnvironment table = TableEnvironment.create(EnvironmentSettings.inBatchMode());
    table.getConfig().set("parallelism.default", "1");
    table.getConfig().set("taskmanager.memory.task.off-heap.size", "128m");
    table.executeSql(
        "CREATE TABLE batch_input (id BIGINT) WITH ('connector'='datagen',"
            + " 'number-of-rows'='3', 'fields.id.kind'='sequence',"
            + " 'fields.id.start'='3', 'fields.id.end'='5')");
    String sql = "SELECT CAST(id * 2 AS INT), SHA2(CAST(id AS STRING), 256) FROM batch_input";
    String plan = table.explainSql(sql);
    assertTrue(plan.contains("NativeCalc"), plan);
    assertEquals(List.of(6, 8, 10), collectInts(table.executeSql(sql)));
  }

  @Test
  void sortsBlockingArrowInputFromThePackagedPayload() throws Exception {
    assertPackagedSort(0, -1);
  }

  @Test
  void limitsBlockingArrowInputFromThePackagedPayload() throws Exception {
    assertPackagedSort(1023, 1025);
  }

  @Test
  void aggregatesBlockingPartialStatesFromThePackagedPayload() throws Exception {
    TableEnvironment table = packagedAggregateEnvironment();
    for (String predicate : List.of("", " AND id < 0")) {
      String sql =
          "SELECT SUM(id),COUNT(*),AVG(id) FROM batch_aggregate_input"
              + " WHERE SHA2(CAST(id AS STRING),256) <> ''"
              + predicate;
      String plan = table.explainSql(sql);
      assertTrue(plan.contains("NativeAggregate") && plan.contains("NativeExchange"), plan);
      try (CloseableIterator<Row> rows = table.executeSql(sql).collect()) {
        assertTrue(rows.hasNext());
        Row result = rows.next();
        assertEquals(
            predicate.isEmpty() ? Row.of(2_102_275L, 2051L, 1025L) : Row.of(null, 0L, null),
            result);
        assertFalse(rows.hasNext());
      }
    }
  }

  @Test
  void aggregatesTypedPartialStatesFromThePackagedPayload() throws Exception {
    TableEnvironment table = packagedAggregateEnvironment();
    for (String predicate : List.of("", " AND id < 0")) {
      String sql =
          "SELECT MIN(MOD(id,2)=0),MAX(MOD(id,2)=0),"
              + "MIN(CAST(id AS DECIMAL(38,18))),MAX(CAST(id AS DECIMAL(38,18))),COUNT(*)"
              + " FROM batch_aggregate_input WHERE SHA2(CAST(id AS STRING),256) <> ''"
              + predicate;
      String plan = table.explainSql(sql);
      assertTrue(plan.contains("NativeAggregate") && plan.contains("NativeExchange"), plan);
      try (CloseableIterator<Row> rows = table.executeSql(sql).collect()) {
        assertTrue(rows.hasNext());
        Row expected =
            predicate.isEmpty()
                ? Row.of(
                    false,
                    true,
                    new java.math.BigDecimal("0.000000000000000000"),
                    new java.math.BigDecimal("2050.000000000000000000"),
                    2051L)
                : Row.of(null, null, null, null, 0L);
        assertEquals(expected, rows.next());
        assertFalse(rows.hasNext());
      }
    }
  }

  @Test
  void groupsBlockingArrowPartitionsFromThePackagedPayload() throws Exception {
    TableEnvironment table = packagedAggregateEnvironment();
    table.getConfig().set("execution.batch.adaptive.auto-parallelism.enabled", "false");
    table.getConfig().set("table.exec.resource.default-parallelism", "2");
    String sql = "SELECT MOD(id,3),SUM(id),COUNT(*) FROM batch_aggregate_input GROUP BY MOD(id,3)";
    String plan = table.explainSql(sql);
    assertTrue(plan.contains("NativeAggregate") && plan.contains("NativeExchange"), plan);
    List<String> actual = new java.util.ArrayList<>();
    try (CloseableIterator<Row> rows = table.executeSql(sql).collect()) {
      while (rows.hasNext()) actual.add(rows.next().toString());
    }
    actual.sort(String::compareTo);
    assertEquals(
        List.of(
            Row.of(0L, 700758L, 684L).toString(),
            Row.of(1L, 701442L, 684L).toString(),
            Row.of(2L, 700075L, 683L).toString()),
        actual);
  }

  @Test
  void joinsBlockingArrowInputsFromThePackagedPayload() throws Exception {
    TableEnvironment table = packagedAggregateEnvironment();
    table.getConfig().set("execution.batch.adaptive.auto-parallelism.enabled", "false");
    table.getConfig().set("table.exec.resource.default-parallelism", "2");
    table.getConfig().set("table.optimizer.join.broadcast-threshold", "-1");
    String sql =
        "SELECT COUNT(*),COUNT(b.id),SUM(b.id) FROM batch_aggregate_input a"
            + " LEFT JOIN batch_aggregate_input b ON a.id=b.id AND b.id<1031";
    String plan = table.explainSql(sql);
    assertTrue(plan.contains("NativeJoin") && plan.contains("NativeExchange"), plan);
    try (CloseableIterator<Row> rows = table.executeSql(sql).collect()) {
      assertEquals(Row.of(2051L, 1031L, 530965L), rows.next());
      assertFalse(rows.hasNext());
    }
  }

  private static TableEnvironment packagedAggregateEnvironment() throws Exception {
    TableEnvironment table = TableEnvironment.create(EnvironmentSettings.inBatchMode());
    table.getConfig().set("parallelism.default", "2");
    table.getConfig().set("pipeline.operator-chaining.enabled", "false");
    table.getConfig().set("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    table.getConfig().set("taskmanager.memory.task.off-heap.size", "128m");
    table.executeSql(
        "CREATE TABLE batch_aggregate_input(id BIGINT) WITH ('connector'='datagen',"
            + " 'number-of-rows'='2051', 'fields.id.kind'='sequence',"
            + " 'fields.id.start'='0', 'fields.id.end'='2050')");
    return table;
  }

  private static void assertPackagedSort(long offset, long fetch) throws Exception {
    TableEnvironment table = TableEnvironment.create(EnvironmentSettings.inBatchMode());
    table.getConfig().set("parallelism.default", "2");
    table.getConfig().set("pipeline.operator-chaining.enabled", "false");
    table.getConfig().set("table.optimizer.range-sort.enabled", "false");
    table.getConfig().set("taskmanager.memory.task.off-heap.size", "128m");
    table.executeSql(
        "CREATE TABLE batch_sort_input (id BIGINT) WITH ('connector'='datagen',"
            + " 'number-of-rows'='2051', 'fields.id.kind'='sequence',"
            + " 'fields.id.start'='0', 'fields.id.end'='2050')");
    String sql =
        "SELECT id FROM batch_sort_input"
            + (fetch < 0 ? "" : " WHERE SHA2(CAST(id AS STRING), 256) <> ''")
            + " ORDER BY id DESC";
    if (fetch >= 0) sql += " OFFSET " + offset + " ROWS FETCH NEXT " + fetch + " ROWS ONLY";
    String plan = table.explainSql(sql);
    assertTrue(plan.contains("NativeSort"), plan);
    assertTrue(plan.contains("NativeExchange"), plan);
    try (CloseableIterator<Row> rows = table.executeSql(sql).collect()) {
      long expected = 2050 - offset;
      while (rows.hasNext()) {
        assertEquals(expected--, rows.next().getField(0));
      }
      assertEquals(fetch < 0 ? -1 : 2050 - offset - fetch, expected);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"rocksdb", "tech.streamfusion.state.RocksDBNativeStateBackendFactory"})
  void installsNativeStatefulPlannerWithConfiguredHostBackend(String backend) throws Exception {
    assertEquals(
        Class.forName("org.rocksdb.CompressionType"),
        Class.forName(
            "org.rocksdb.CompressionType",
            false,
            PlannerModule.getInstance().getSubmoduleClassLoader()));
    org.apache.flink.configuration.Configuration configuration =
        new org.apache.flink.configuration.Configuration();
    configuration.set(org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND, backend);
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
