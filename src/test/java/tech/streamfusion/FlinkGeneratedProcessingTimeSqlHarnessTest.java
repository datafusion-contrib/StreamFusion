package tech.streamfusion;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import tech.streamfusion.planner.NativePlanner;

class FlinkGeneratedProcessingTimeSqlHarnessTest {
  @TestFactory
  Stream<DynamicTest> generatedClockSelectionsAndFiltersExecuteNatively() {
    return Stream.of("UTC", "Asia/Shanghai", "America/Los_Angeles")
        .flatMap(
            zone ->
                Stream.of(3, 6, 9)
                    .flatMap(
                        precision ->
                            Stream.of(false, true)
                                .flatMap(
                                    filter ->
                                        Stream.of(false, true)
                                            .map(
                                                forceGenerated ->
                                                    DynamicTest.dynamicTest(
                                                        zone
                                                            + "/"
                                                            + precision
                                                            + "/filter="
                                                            + filter
                                                            + "/generated="
                                                            + forceGenerated,
                                                        () ->
                                                            compare(
                                                                zone,
                                                                precision,
                                                                filter,
                                                                forceGenerated))))));
  }

  private static void compare(String zone, int precision, boolean filter, boolean forceGenerated)
      throws Exception {
    try (var metrics = new RecoveryExecutionMetrics()) {
      int count = 2051;
      List<String> stockTypes = null;
      for (boolean nativeEnabled : new boolean[] {false, true}) {
        var env = metrics.environment(new Configuration(), 1, 0);
        var table = environment(env, zone, precision, count);
        table.getConfig().set("streamfusion.native.enabled", Boolean.toString(nativeEnabled));
        var scan = NativePlanner.install(table);
        String clock = precision == 3 ? "pt" : "CAST(pt AS TIMESTAMP_LTZ(" + precision + "))";
        String caseSelection = "CASE WHEN ts IS NOT NULL THEN ts ELSE " + clock + " END";
        String selection = precision == 3 ? caseSelection : "COALESCE(ts, " + clock + ")";
        String sql =
            "SELECT id, "
                + selection
                + ", CASE WHEN ts IS NOT NULL THEN ts "
                + "ELSE "
                + clock
                + " END"
                + (forceGenerated ? ", 100 / (id + 1), 100 / (id + 2)" : "")
                + " FROM src"
                + (filter ? " WHERE " + selection + " > TO_TIMESTAMP_LTZ(0,3)" : "");
        String plan = table.explainSql(sql);
        if (nativeEnabled) assertTrue(plan.contains("NativeCalc"), plan + scan.explainSummary());
        long before = System.currentTimeMillis();
        var result = table.executeSql(sql);
        String jobId = result.getJobClient().orElseThrow().getJobID().toString();
        var rows = new ArrayList<Row>();
        try (var iterator = result.collect()) {
          while (iterator.hasNext()) rows.add(iterator.next());
        }
        long after = System.currentTimeMillis();
        assertEquals(filter ? count / 2 : count, rows.size());
        int previousId = -1;
        for (Row row : rows) {
          assertEquals(RowKind.INSERT, row.getKind());
          int id = (Integer) row.getField(0);
          assertTrue(id > previousId);
          previousId = id;
          for (int field = 1; field <= 2; field++) {
            Instant timestamp = (Instant) row.getField(field);
            if (id % 2 == 0) assertEquals(fixed(precision), timestamp);
            else
              assertTrue(
                  timestamp.toEpochMilli() >= before && timestamp.toEpochMilli() <= after,
                  timestamp + " outside task execution time");
          }
          if (forceGenerated) {
            assertEquals(100 / (id + 1), row.getField(3));
            assertEquals(100 / (id + 2), row.getField(4));
          }
        }
        var actualTypes =
            result.getResolvedSchema().getColumnDataTypes().stream()
                .map(Object::toString)
                .collect(tech.streamfusion.compat.ListCollectors.toList());
        assertTrue(actualTypes.get(1).startsWith("TIMESTAMP_LTZ(" + precision + ")"));
        if (nativeEnabled) assertEquals(stockTypes, actualTypes);
        else stockTypes = actualTypes;
        if (nativeEnabled) {
          assertTrue(scan.substitutions() > 0, scan.explainSummary());
          assertTrue(scan.fallbackReasons().isEmpty(), scan.explainSummary());
          var observations =
              metrics.nativeOperators().stream()
                  .filter(
                      row ->
                          row.get("jobId").equals(jobId)
                              && row.get("operator").toString().startsWith("NativeCalcExecNode"))
                  .collect(tech.streamfusion.compat.ListCollectors.toList());
          assertEquals(1, observations.size(), "clock marker should fuse into its consumer");
          for (var observation : observations)
            assertEquals((long) count, observation.get("recordsIn"));
          if (forceGenerated)
            assertTrue(
                scan.functionExecutions().stream()
                    .anyMatch(
                        execution ->
                            execution.contains(
                                    "reason=multiple fallible/JVM expressions require Flink's"
                                        + " row-major error order")
                                && execution.contains("PROCTIME_MATERIALIZE")),
                scan.explainSummary());
          if (!forceGenerated && precision > 3) {
            assertTrue(
                scan.functionExecutions().stream()
                    .anyMatch(
                        execution ->
                            execution.contains("COALESCE") && execution.contains("jvm=[]")),
                scan.explainSummary());
          }
          System.out.println(
              "[generated-clock-route] "
                  + zone
                  + " precision="
                  + precision
                  + " filter="
                  + filter
                  + " "
                  + scan.functionExecutions());
        }
      }
    }
  }

  static StreamTableEnvironment environment(
      StreamExecutionEnvironment env, String zone, int precision, int count) {
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneId.of(zone));
    var type = ROW(FIELD("id", INT()), FIELD("ts", TIMESTAMP_LTZ(precision)));
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < count; i++) rows.add(Row.of(i, i % 2 == 0 ? fixed(precision) : null));
    table.createTemporaryView(
        "src",
        fromData(env, rows, ExternalTypeInfo.<Row>of(type)),
        Schema.newBuilder().fromRowDataType(type).columnByExpression("pt", "PROCTIME()").build());
    return table;
  }

  private static Instant fixed(int precision) {
    return Instant.parse(
        precision == 9
            ? "1969-12-31T23:59:59.999234567Z"
            : precision == 6 ? "1969-12-31T23:59:59.999234Z" : "1969-12-31T23:59:59.999Z");
  }
}
