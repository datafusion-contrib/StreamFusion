package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCall;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.junit.jupiter.api.Test;

class UtcLtzCalendarFloorAdmissionTest {
  @Test
  void unverifiedShapesDoNotEnterTheUtcCalendarKernel() throws Exception {
    var constructor = RexExpression.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    var configure = RexExpression.class.getDeclaredMethod("configure", org.apache.flink.table.api.TableConfig.class);
    configure.setAccessible(true);
    var admitted = RexExpression.class.getDeclaredMethod("isNativeCalendarRound", RexCall.class);
    admitted.setAccessible(true);
    for (String expression : List.of("FLOOR(ts TO DAY)", "FLOOR(ts TO HOUR)",
        "FLOOR(ts TO MINUTE)", "FLOOR(ts TO SECOND)", "FLOOR(ts TO MILLISECOND)",
        "CEIL(ts TO DAY)", "CEIL(ts TO HOUR)", "CEIL(ts TO MINUTE)",
        "CEIL(ts TO SECOND)", "CEIL(ts TO MILLISECOND)",
        "FLOOR(TO_TIMESTAMP_LTZ(n/0,3) TO MONTH)", "CEIL(TO_TIMESTAMP_LTZ(n,3) TO MONTH)")) {
      var table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
      table.getConfig().setLocalTimeZone(java.time.ZoneOffset.UTC);
      table.executeSql("CREATE TABLE src(ts TIMESTAMP_LTZ(9),n BIGINT) WITH ('connector'='datagen')");
      var operation = ((TableImpl) table.sqlQuery("SELECT " + expression + " FROM src")).getQueryOperation();
      var call = (RexCall) ((Project) ((PlannerQueryOperation) operation).getCalciteTree()).getProjects().get(0);
      var encoder = constructor.newInstance();
      configure.invoke(encoder, table.getConfig());
      assertEquals(false, admitted.invoke(encoder, call), expression);
    }
  }
}
