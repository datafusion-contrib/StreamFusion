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

class MixedExactIfEncodingTest {
  @Test
  void verifiedLeavesUseTheResolvedDecimalTypeBeforeSelection() {
    for (String schema : List.of("d DECIMAL(19,19), n TINYINT", "d DECIMAL(19,19), n SMALLINT",
        "d DECIMAL(19,19), n INT", "d DECIMAL(19,19) NOT NULL, n INT",
        "d DECIMAL(38,18), n BIGINT")) {
      for (String expression : List.of("IF(n>=0,d,n)", "IF(n>=0,n,d)",
          "IF(n>=0,d,CAST(2 AS INT))", "IF(n>=0,CAST(NULL AS INT),d)",
          "IF(n>=0,d,CAST(NULL AS INT))")) {
        var encoded = encode(project(schema, expression));
        assertEquals(47, encoded.kinds()[0], schema + ": " + expression);
        assertEquals(3, encoded.childCounts()[0]);
        assertFalse(Arrays.stream(encoded.kinds()).anyMatch(kind -> kind == 14));
        assertEquals(List.of(), encoded.jvmFunctionCalls());
        assertTrue(encoded.nativeFunctionCalls().contains("IF"));
      }
    }
    for (String schema : List.of("d DECIMAL(4,2), n BIGINT", "d DECIMAL(18,2), n BIGINT")) {
      for (String expression : List.of("IF(n>=0,d,n)", "IF(n>=0,n,d)")) {
        var encoded = encode(project(schema, expression));
        assertEquals(47, encoded.kinds()[0]);
        assertEquals(List.of(), encoded.jvmFunctionCalls());
      }
    }
  }

  @Test
  void matchingBranchTypesKeepTheExistingCaseEncoding() {
    for (String type : List.of("DECIMAL(18,2)", "DECIMAL(38,18)")) {
      var encoded = encode(project("d " + type + ", n " + type, "IF(n>=0,d,n)"));
      assertFalse(Arrays.stream(encoded.kinds()).anyMatch(kind -> kind == 47));
      assertEquals(List.of(), encoded.jvmFunctionCalls());
    }
  }

  @Test
  void scaleReductionAndUnverifiedProducersStayGenerated() {
    for (String[] pair : new String[][] {
        {"d DECIMAL(4,2), n INT", "IF(n>=0,d,n)"},
        {"d DECIMAL(18,2), n INT", "IF(n>=0,d,n)"},
        {"d DECIMAL(18,2), n BIGINT", "IF(n>=0,d,CAST(NULL AS INT))"},
        {"d DECIMAL(38,38) NOT NULL, n INT", "IF(n>=0,d,n)"},
        {"d DECIMAL(18,2) NOT NULL, n INT", "IF(n>=0,ABS(d),n)"},
        {"d DECIMAL(18,2) NOT NULL, n INT", "IF(n>=0,d+d,n)"}}) {
      var encoded = encode(project(pair[0], pair[1]));
      assertEquals(17, encoded.kinds()[0], Arrays.toString(pair));
      assertFalse(encoded.jvmFunctionCalls().isEmpty());
    }
  }

  @Test
  void insufficientIntegralCapacityRemainsGenerated() {
    var call = (RexCall) project("d DECIMAL(38,18) NOT NULL, n BIGINT", "IF(n>=0,d,n)");
    var types = new org.apache.flink.table.planner.calcite.FlinkTypeFactory(
        getClass().getClassLoader(), org.apache.flink.table.planner.calcite.FlinkTypeSystem.INSTANCE);
    var narrowed = call.clone(types.createSqlType(SqlTypeName.DECIMAL, 18, 17), call.getOperands());
    assertTrue(encode(narrowed).jvmFunctionCalls().contains("IF"));
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
