package io.github.jordepic.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

class FlinkSqlHarnessTest {

  @Test
  void runsFlinkSqlProjectionEndToEnd() throws Exception {
    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

    TableResult result =
        tEnv.executeSql("SELECT c0 * 2 AS doubled FROM (VALUES (3), (4), (5)) AS t(c0)");

    List<Integer> doubled = new ArrayList<>();
    try (CloseableIterator<Row> rows = result.collect()) {
      while (rows.hasNext()) {
        doubled.add((Integer) rows.next().getField(0));
      }
    }

    doubled.sort(null);
    assertEquals(List.of(6, 8, 10), doubled);
  }
}
