package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;

class IntegralNegateScalarNativeTest {
  @ParameterizedTest
  @ValueSource(strings = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT"})
  void constantArithmeticUsesWrappingNegationAndBroadcastsItsResolvedWidth(String name) throws Exception {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    var type = types.createSqlType(SqlTypeName.valueOf(name));
    int bits = name.equals("TINYINT") ? 8 : name.equals("SMALLINT") ? 16
        : name.equals("INTEGER") ? 32 : 64;
    long minimum = bits == 64 ? Long.MIN_VALUE : -(1L << (bits - 1));
    var min = rex.makeExactLiteral(BigDecimal.valueOf(minimum), type);
    var zero = rex.makeExactLiteral(BigDecimal.ZERO, type);
    for (var operands : List.of(List.<RexNode>of(min, zero), List.<RexNode>of(zero, min))) {
      var sum = rex.makeCall(type, FlinkSqlOperatorTable.PLUS, operands);
      var call = rex.makeCall(type, FlinkSqlOperatorTable.UNARY_MINUS, List.of(sum));
      var encoded = RexExpression.encodeProjections(List.of(call), List.of("result"));
      assertNotNull(encoded);
      assertTrue(encoded.jvmFunctionCalls().isEmpty());
      assertEquals(219, encoded.payload()[0]);
      var stock = FlinkExpressionGenerator.generate(call, new LogicalType[] {new IntType()},
          TableConfig.getDefault(), getClass().getClassLoader(), false);
      stock.open(new FunctionContext(null) {
        @Override public ClassLoader getUserCodeClassLoader() {
          return IntegralNegateScalarNativeTest.class.getClassLoader();
        }
      });
      try {
        long expected = ((Number) stock.eval(42)).longValue();
        assertEquals(minimum, expected);
        long calc = Native.createCalcExpression(encoded.kinds(), encoded.payload(), encoded.childCounts(),
            encoded.longs(), encoded.doubles(), encoded.strings(), encoded.projectionRoots(),
            encoded.conditionRoot(), new String[] {"result"});
        try (var allocator = new RootAllocator()) {
          for (int count : new int[] {0, 1, 1023}) {
            var schema = new Schema(List.of(Field.nullable("unused", new ArrowType.Int(32, true))));
            try (var inArray = ArrowArray.allocateNew(allocator);
                var inSchema = ArrowSchema.allocateNew(allocator);
                var outArray = ArrowArray.allocateNew(allocator);
                var outSchema = ArrowSchema.allocateNew(allocator)) {
              VectorSchemaRoot output;
              try (var input = VectorSchemaRoot.create(schema, allocator)) {
                input.allocateNew();
                var vector = (IntVector) input.getVector(0);
                for (int row = 0; row < count; row++) vector.setSafe(row, row);
                input.setRowCount(count);
                Data.exportVectorSchemaRoot(allocator, input, null, inArray, inSchema);
                Native.calcExpression(calc, inArray.memoryAddress(), inSchema.memoryAddress(),
                    outArray.memoryAddress(), outSchema.memoryAddress());
                output = Data.importVectorSchemaRoot(allocator, outArray, outSchema, null);
              }
              try (var result = output) {
                assertEquals(count, result.getRowCount());
                var vector = result.getVector(0);
                assertEquals(new ArrowType.Int(bits, true), vector.getField().getType());
                for (int row = 0; row < count; row++) {
                  assertFalse(vector.isNull(row));
                  assertEquals(expected, ((Number) vector.getObject(row)).longValue());
                }
              }
            }
          }
        } finally {
          Native.closeCalcExpression(calc);
        }
      } finally {
        stock.close();
      }
    }
  }
}
