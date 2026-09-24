package tech.streamfusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.streamfusion.compat.FlinkTestSources.fromData;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import tech.streamfusion.planner.NativePlanner;

final class BuiltinFunctionParity {
  private BuiltinFunctionParity() {}

  static TableEnvironment environment(DataType type, List<Row> rows) {
    return environment(type, rows, "UTC");
  }

  static TableEnvironment environment(DataType type, List<Row> rows, String zone) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().setLocalTimeZone(ZoneId.of(zone));
    table.createTemporaryView(
        "src",
        fromData(env, rows, ExternalTypeInfo.<Row>of(type)),
        Schema.newBuilder().fromRowDataType(type).build());
    return table;
  }

  static void assertParity(Supplier<TableEnvironment> environment, String sql) throws Exception {
    var host = environment.get();
    var accelerated = environment.get();
    var scan = NativePlanner.install(accelerated);
    assertEquals(
        host.sqlQuery(sql).getResolvedSchema(), accelerated.sqlQuery(sql).getResolvedSchema());
    var expected = collect(host, sql);
    var actual = collect(accelerated, sql);
    assertTrue(scan.substitutions() > 0, "No native substitution: " + scan.fallbackReasons());
    assertTrue(
        scan.operatorTypes().stream().anyMatch(name -> name.contains("Calc")),
        "No native Calc: " + scan.operatorTypes());
    assertEquals(expected, actual, sql);
  }

  private static List<List<Object>> collect(TableEnvironment table, String sql) throws Exception {
    List<List<Object>> rows = new ArrayList<>();
    try (var iterator = table.executeSql(sql).collect()) {
      while (iterator.hasNext()) {
        Row row = iterator.next();
        List<Object> fields = new ArrayList<>();
        for (int i = 0; i < row.getArity(); i++) {
          fields.add(NativeParity.comparableValue(row.getField(i)));
        }
        rows.add(fields);
      }
    }
    rows.sort(Comparator.comparing(Object::toString));
    return rows;
  }
}
