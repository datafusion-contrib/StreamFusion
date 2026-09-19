package tech.streamfusion.suite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PaimonCatalogFixtureTest {
  @Test
  void currentHostKeepsTheOriginalFixture() {
    String key = "streamfusion.flink-suite.flink-line";
    String previous = System.getProperty(key);
    try {
      System.setProperty(key, "2.2");
      assertNull(StreamFusionSuiteAgent.BuildLegacyPaimonCatalog.enter(getClass(), null));
    } finally {
      if (previous == null) System.clearProperty(key);
      else System.setProperty(key, previous);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void preservesTheUpstreamFixtureMetadata(boolean keyed) {
    var columns =
        List.of(
            Column.physical("k", DataTypes.INT().notNull()),
            Column.physical("values", DataTypes.ARRAY(DataTypes.STRING())));
    var options = Map.of("path", "fixture-table", "bucket", "2");
    var table =
        (ResolvedCatalogTable)
            PaimonCatalogFixture.create(
                getClass().getClassLoader(),
                options,
                columns,
                List.of("k"),
                keyed ? List.of("k") : List.of());
    assertEquals(columns, table.getResolvedSchema().getColumns());
    assertEquals(List.of(), table.getResolvedSchema().getWatermarkSpecs());
    assertEquals(options, table.getOptions());
    assertEquals(List.of("k"), table.getPartitionKeys());
    assertEquals("a comment", table.getComment());
    assertEquals(
        Schema.newBuilder().fromResolvedSchema(table.getResolvedSchema()).build(),
        table.getUnresolvedSchema());
    if (keyed) {
      var primaryKey = table.getResolvedSchema().getPrimaryKey().orElseThrow();
      assertEquals("pk", primaryKey.getName());
      assertEquals(List.of("k"), primaryKey.getColumns());
    } else {
      assertTrue(table.getResolvedSchema().getPrimaryKey().isEmpty());
    }
  }
}
