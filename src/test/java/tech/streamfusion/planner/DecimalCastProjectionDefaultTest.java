package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DecimalCastProjectionDefaultTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void projectionDefaultsMatchReleasedRowCodeWithoutChangingScalarNulls(boolean legacy)
      throws Exception {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    for (boolean nullable : new boolean[] {false, true}) {
      var input = types.createTypeWithNullability(types.createSqlType(SqlTypeName.DECIMAL, 38, 18), nullable);
      var productType = types.createTypeWithNullability(types.createSqlType(SqlTypeName.DECIMAL, 38, 6), nullable);
      var product = rex.makeCall(productType, FlinkSqlOperatorTable.MULTIPLY,
          List.of(rex.makeInputRef(input, 0), rex.makeInputRef(input, 0)));
      for (var target : List.of(SqlTypeName.CHAR, SqlTypeName.VARCHAR)) {
        var text = types.createTypeWithNullability(types.createSqlType(target, 8), nullable);
        var cast = rex.makeCall(text, FlinkSqlOperatorTable.CAST, List.of(product));
        if (!nullable) {
          assertNull(RexExpression.encodeProjections(List.of(cast), List.of("v")),
              "An unconfigured encoder must not assume modern character-width semantics");
        }
        var concat = rex.makeCall(text, FlinkSqlOperatorTable.CONCAT,
            List.of(cast, rex.makeLiteral("!")));
        for (RexNode expression : List.of(cast, concat)) {
          var config = TableConfig.getDefault();
          config.set("table.exec.legacy-cast-behaviour", legacy ? "ENABLED" : "DISABLED");
          LogicalType[] arguments = {FlinkTypeFactory.toLogicalType(input)};
          var projection = FlinkExpressionGenerator.generateProjection(expression, arguments,
              config, getClass().getClassLoader(), false);
          var binary = FlinkExpressionGenerator.generateProjection(expression, arguments,
              config, getClass().getClassLoader(), true);
          var scalar = FlinkExpressionGenerator.generate(expression, arguments, config,
              getClass().getClassLoader());
          var row = FlinkExpressionGenerator.generate(List.of(expression), null, arguments,
              RowType.of(FlinkTypeFactory.toLogicalType(text)), config, getClass().getClassLoader());
          for (var function : List.of(projection, binary, scalar, row)) function.open(context());
          try {
            for (var value : List.of(new BigDecimal("99999999999999999999.999999999999999999"),
                new BigDecimal("-99999999999999999999.999999999999999999"),
                BigDecimal.ZERO.setScale(18), BigDecimal.ONE.setScale(18))) {
              RowData expected = (RowData) row.eval(value);
              Object actual = projection.eval(value);
              assertEquals(expected.isNullAt(0) ? null : expected.getString(0), actual,
                  expression + ": " + value);
              assertArrayEquals(expected.isNullAt(0) ? null : expected.getString(0).toBytes(),
                  (byte[]) binary.eval(value));
              if (value.abs().compareTo(BigDecimal.ONE) > 0) {
                assertNull(scalar.eval(value), "Intermediate NULL flag must survive");
                if (expression == cast && !nullable) assertEquals("", actual.toString());
              }
            }
          } finally {
            for (var function : List.of(projection, binary, scalar, row)) function.close();
          }
        }
      }
    }
  }

  private FunctionContext context() {
    return new FunctionContext(null) {
      @Override
      public ClassLoader getUserCodeClassLoader() {
        return getClass().getClassLoader();
      }
    };
  }
}
