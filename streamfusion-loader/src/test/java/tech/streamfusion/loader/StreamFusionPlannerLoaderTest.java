package tech.streamfusion.loader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.planner.loader.PlannerModule;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

/** Verifies planner installation from the classpath, without an application-side hook. */
class StreamFusionPlannerLoaderTest {

  @Test
  void corePayloadDoesNotContainOptionalConnectorIntegrations() {
    ClassLoader plannerClassLoader = PlannerModule.getInstance().getSubmoduleClassLoader();

    assertMissing(plannerClassLoader, "tech.streamfusion.planner.KafkaTables");
    assertMissing(plannerClassLoader, "tech.streamfusion.kafka.NativeKafka");
    assertMissing(plannerClassLoader, "tech.streamfusion.planner.FlussTables");
    assertMissing(plannerClassLoader, "tech.streamfusion.fluss.NativeFluss");
    assertMissing(plannerClassLoader, "tech.streamfusion.planner.ParquetSourceMatcher");
    assertMissing(plannerClassLoader, "tech.streamfusion.parquet.NativeParquet");
  }

  @Test
  void installsTheNativePlannerStageWithoutApplicationCode() throws Exception {
    assertNotNull(
        PlannerModule.class.getResource("/streamfusion-planner.jar"),
        "the loader artifact must embed the StreamFusion runtime payload");

    TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    String sql = "SELECT c0 * 2 AS doubled FROM (VALUES (3), (4), (5)) AS t(c0)";

    String explain = tableEnvironment.explainSql(sql);
    assertTrue(explain.contains("NativeCalc"), "StreamFusion was not installed:\n" + explain);
    assertEquals(List.of(6, 8, 10), collectInts(tableEnvironment.executeSql(sql)));
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
