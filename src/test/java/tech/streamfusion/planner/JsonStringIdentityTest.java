package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.junit.jupiter.api.Test;

class JsonStringIdentityTest {
  @Test
  void formattedCharactersKeepTheirUtf16IdentityForConsumers() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var function = new org.apache.calcite.sql.SqlFunction("PRINTF",
        org.apache.calcite.sql.SqlKind.OTHER_FUNCTION, null, null, null,
        org.apache.calcite.sql.SqlFunctionCategory.STRING);
    var call = rex.makeCall(types.createSqlType(SqlTypeName.VARCHAR), function,
        List.of(rex.makeLiteral("%c"), rex.makeInputRef(types.createSqlType(SqlTypeName.INTEGER), 0)));
    assertTrue(JsonStringIdentity.containsSensitiveString(call));
  }

  @Test
  void onlyFinalRootsCanExposeJavaStringsWithoutAnotherConsumer() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var cluster = RelOptCluster.create(new VolcanoPlanner(), rex);
    var string = types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
    var inputType = types.builder().add("s", string).build();
    var input = LogicalValues.createEmpty(cluster, inputType);
    var program = new RexProgramBuilder(inputType, rex);
    program.addProject(
        rex.makeCall(
            string,
            FlinkSqlOperatorTable.JSON_VALUE,
            List.of(rex.makeInputRef(string, 0), rex.makeLiteral("$"))),
        "j");
    var projection = program.getProgram();
    var calc =
        new StreamPhysicalCalc(
            cluster,
            cluster.traitSetOf(FlinkConventions.STREAM_PHYSICAL()),
            input,
            projection,
            projection.getOutputRowType());
    assertFalse(JsonStringIdentity.crossesOperatorBoundary(calc, true));
    assertTrue(JsonStringIdentity.crossesOperatorBoundary(calc, false));

    var passthrough = new RexProgramBuilder(calc.getRowType(), rex);
    passthrough.addIdentity();
    var outer =
        new StreamPhysicalCalc(
            cluster, calc.getTraitSet(), calc, passthrough.getProgram(), calc.getRowType());
    assertTrue(JsonStringIdentity.crossesOperatorBoundary(outer, true));
  }

  @Test
  void onlyUnpairedSurrogatesNeedJavaStringIdentity() {
    for (String value : List.of("\ud800", "\udc00", "a\ud800b", "\ud83d\ude00\ud800")) {
      assertTrue(JsonStringIdentity.containsUnpairedSurrogate(value));
    }
    for (String value : List.of("", "?", "�", "😀", "a😀b")) {
      assertFalse(JsonStringIdentity.containsUnpairedSurrogate(value));
    }
  }
}
