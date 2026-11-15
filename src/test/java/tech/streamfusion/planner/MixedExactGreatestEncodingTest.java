package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.junit.jupiter.api.Test;

class MixedExactGreatestEncodingTest {
  @Test
  void verifiedLeavesUseTheResolvedDecimalTypeBeforeComparison() {
    for (String schema : List.of("d DECIMAL(18,2), n INT",
        "d DECIMAL(18,2) NOT NULL, n BIGINT", "d DECIMAL(19,19) NOT NULL, n INT",
        "d DECIMAL(38,18), n BIGINT")) {
      for (String expression : List.of("GREATEST(d,n)", "GREATEST(n,d)",
          "GREATEST(d,n,CAST(2 AS INT))")) {
        var encoded = encode(project(schema, expression));
        assertEquals(6, encoded.kinds()[0], schema + ": " + expression);
        assertEquals(109, encoded.payload()[0]);
        assertTrue(Arrays.stream(encoded.kinds()).anyMatch(kind -> kind == 14));
        assertEquals(List.of(), encoded.jvmFunctionCalls());
        assertTrue(encoded.nativeFunctionCalls().contains("GREATEST"));
      }
    }
  }

  @Test
  void nullableConversionsAndUnverifiedProducersStayGenerated() {
    for (String[] pair : new String[][] {
        {"d DECIMAL(4,2), n INT", "GREATEST(d,n)"},
        {"d DECIMAL(18,2), n BIGINT", "GREATEST(d,n)"},
        {"d DECIMAL(19,19), n INT", "GREATEST(n,d)"},
        {"d DECIMAL(38,38) NOT NULL, n INT", "GREATEST(d,n)"},
        {"d DECIMAL(18,2) NOT NULL, n INT", "GREATEST(ABS(d),n)"},
        {"d DECIMAL(18,2) NOT NULL, n INT", "GREATEST(d+d,n)"}}) {
      var encoded = encode(project(pair[0], pair[1]));
      assertEquals(17, encoded.kinds()[0], Arrays.toString(pair));
      assertFalse(encoded.jvmFunctionCalls().isEmpty());
    }
  }

  @Test
  void insufficientIntegralCapacityRemainsGenerated() {
    var call = (RexCall) project("d DECIMAL(38,18) NOT NULL, n BIGINT", "GREATEST(d,n)");
    var types = new org.apache.flink.table.planner.calcite.FlinkTypeFactory(
        getClass().getClassLoader(), org.apache.flink.table.planner.calcite.FlinkTypeSystem.INSTANCE);
    var narrowed = call.clone(types.createSqlType(SqlTypeName.DECIMAL, 18, 17), call.getOperands());
    assertTrue(encode(narrowed).jvmFunctionCalls().contains("GREATEST"));
  }

  private static RexNode project(String schema, String expression) {
    var table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    table.executeSql("CREATE TABLE src(" + schema + ") WITH ('connector'='datagen')");
    var query = ((TableImpl) table.sqlQuery("SELECT " + expression + " FROM src")).getQueryOperation();
    return ((Project) ((PlannerQueryOperation) query).getCalciteTree()).getProjects().get(0);
  }

  private static RexExpression encode(RexNode expression) {
    var result = RexExpression.encodeProjections(List.of(expression), List.of("v"));
    assertNotNull(result, expression.toString());
    return result;
  }
}
