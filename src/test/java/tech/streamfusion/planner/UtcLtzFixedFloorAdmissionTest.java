package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.time.ZoneId;
import java.util.List;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.junit.jupiter.api.Test;
import tech.streamfusion.compat.FlinkCompat;

class UtcLtzFixedFloorAdmissionTest {
  @Test
  void onlyVerifiedUnitsInputsAndZeroOffsetZonesLoseTheirEvaluator() throws Exception {
    var constructor = RexExpression.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    var configure = RexExpression.class.getDeclaredMethod("configure", TableConfig.class);
    configure.setAccessible(true);
    var emit = RexExpression.class.getDeclaredMethod("emit", RexNode.class);
    emit.setAccessible(true);
    for (String zone : List.of("UTC", "Z", "Etc/UTC", "GMT+05:30", "Europe/London")) {
      var table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
      table.getConfig().setLocalTimeZone(ZoneId.of(zone));
      table.executeSql("CREATE TABLE src(ts TIMESTAMP_LTZ(9),n BIGINT) WITH ('connector'='datagen')");
      for (String expression : List.of("FLOOR(ts TO DAY)", "FLOOR(ts TO HOUR)",
          "FLOOR(ts TO MINUTE)", "FLOOR(ts TO SECOND)", "FLOOR(ts TO MILLISECOND)",
          "CEIL(ts TO DAY)", "CEIL(ts TO MILLISECOND)",
          "FLOOR(TO_TIMESTAMP_LTZ(n,3) TO DAY)")) {
        var operation = ((TableImpl) table.sqlQuery("SELECT " + expression + " FROM src"))
            .getQueryOperation();
        var projection = (Project) ((PlannerQueryOperation) operation).getCalciteTree();
        var encoder = constructor.newInstance();
        configure.invoke(encoder, table.getConfig());
        boolean fixedUnitSupported = FlinkCompat.supportsSubHourLtzRounding()
            || expression.equals("FLOOR(ts TO DAY)") || expression.equals("FLOOR(ts TO HOUR)");
        boolean admitted = (Boolean) emit.invoke(encoder, projection.getProjects().get(0));
        if (!FlinkCompat.supportsSubHourLtzRounding()
            && (expression.contains("TO MINUTE") || expression.contains("TO SECOND")
                || expression.contains("TO MILLISECOND"))) {
          assertFalse(admitted, expression);
          continue;
        }
        assertTrue(admitted, expression);
        boolean pure = ZoneId.of(zone).normalized().equals(java.time.ZoneOffset.UTC)
            && expression.startsWith("FLOOR(ts") && fixedUnitSupported;
        assertEquals(pure, encoder.jvmFunctionCalls().isEmpty(), zone + " " + expression);
        if (pure) {
          assertEquals(List.of("FLOOR"), encoder.nativeFunctionCalls());
          assertEquals(154, encoder.payload()[0]);
        }
      }
    }
  }
}
