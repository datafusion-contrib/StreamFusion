package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;

class TypedNullLiteralTest {
  @Test
  void unknownNullTypesAreDeclinedDuringEncoding() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var unknown = types.createSqlType(SqlTypeName.ANY);
    assertNull(
        RexExpression.encodeProjections(List.of(rex.makeNullLiteral(unknown)), List.of("v")));
    assertNull(
        RexExpression.encodeProjections(
            List.of(rex.makeNullLiteral(types.createArrayType(unknown, -1))), List.of("v")));
  }

  @Test
  void nativeSchemaPreservesNullWidthsPrecisionAndNestedFields() {
    var types = new JavaTypeFactoryImpl();
    var rex = new RexBuilder(types);
    var integer = types.createSqlType(SqlTypeName.INTEGER);
    var text = types.createTypeWithNullability(types.createSqlType(SqlTypeName.VARCHAR, 13), true);
    var decimal = types.createSqlType(SqlTypeName.DECIMAL, 38, 18);
    var nested =
        types
            .builder()
            .add("odd ' \" name", integer)
            .add("\u7528\u6237", types.createArrayType(types.createMapType(text, decimal), -1))
            .add("ts", types.createSqlType(SqlTypeName.TIMESTAMP, 9))
            .add("ltz", types.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 9))
            .build();
    for (var type :
        List.of(
            integer,
            decimal,
            types.createSqlType(SqlTypeName.BOOLEAN),
            types.createSqlType(SqlTypeName.BIGINT),
            types.createSqlType(SqlTypeName.BINARY, 7),
            types.createArrayType(integer, -1),
            types.createMapType(text, nested),
            nested)) {
      var literal = rex.makeNullLiteral(type);
      var encoded = RexExpression.encodeProjections(List.of(literal), List.of("v"));
      assertNotNull(encoded, type.toString());
      var expected =
          ArrowConversion.toArrowSchema(
              RowType.of(
                  new org.apache.flink.table.types.logical.LogicalType[] {
                    FlinkTypeFactory.toLogicalType(literal.getType())
                  },
                  new String[] {"v"}));
      try (BufferAllocator allocator = new RootAllocator();
          ArrowSchema input = ArrowSchema.allocateNew(allocator);
          ArrowSchema output = ArrowSchema.allocateNew(allocator)) {
        Data.exportSchema(
            allocator, new org.apache.arrow.vector.types.pojo.Schema(List.of()), null, input);
        String failure =
            Native.inferCalcOutputSchema(
                encoded.kinds(),
                encoded.payload(),
                encoded.childCounts(),
                encoded.longs(),
                encoded.doubles(),
                encoded.strings(),
                encoded.projectionRoots(),
                encoded.conditionRoot(),
                encoded.outputNames(),
                input.memoryAddress(),
                output.memoryAddress());
        assertNull(failure, type.toString());
        assertEquals(expected, Data.importSchema(allocator, output, null), type.toString());
      }
    }
  }
}
