package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.RowDataArrowConverter;

class ArrowSchemaViewsTest {
  @Test
  void nestedViewsRetainBuffersAndNullsWithoutLeakingTransportFields() {
    var nested =
        RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {new BigIntType()},
            new String[] {"__sf_nested_row_kind"});
    var logical = RowType.of(nested, new ArrayType(nested), new MapType(new BigIntType(), nested));
    var value = GenericRowData.ofKind(RowKind.DELETE, 42L);
    try (var allocator = new RootAllocator()) {
      VectorSchemaRoot view;
      long address;
      try (var source =
          RowDataArrowConverter.write(
              List.of(
                  GenericRowData.of(
                      value,
                      new GenericArrayData(new Object[] {value, null}),
                      new GenericMapData(Map.of(7L, value))),
                  GenericRowData.of(null, null, null)),
              logical,
              allocator)) {
        var row = (StructVector) source.getVector(0);
        assertNotNull(NestedRowKinds.vector(row));
        address = row.getChild("__sf_nested_row_kind").getDataBuffer().memoryAddress();
        var schema =
            new Schema(
                source.getSchema().getFields().stream()
                    .map(ArrowSchemaViews::logicalField)
                    .collect(Collectors.toList()));
        view = ArrowSchemaViews.retain(source.getFieldVectors(), schema, 2, allocator);
        assertEquals(1, view.getSchema().getFields().get(0).getChildren().size());
        assertEquals(
            1, view.getSchema().getFields().get(1).getChildren().get(0).getChildren().size());
        assertEquals(
            1,
            view.getSchema()
                .getFields()
                .get(2)
                .getChildren()
                .get(0)
                .getChildren()
                .get(1)
                .getChildren()
                .size());
      }
      try (var retained = view) {
        var row = (StructVector) retained.getVector(0);
        var field = (BigIntVector) row.getChild("__sf_nested_row_kind");
        assertEquals(address, field.getDataBuffer().memoryAddress());
        assertEquals(42L, field.get(0));
        assertTrue(row.isNull(1));
        assertNull(NestedRowKinds.vector(row));
        var list = (ListVector) retained.getVector(1);
        assertEquals(2, list.getElementEndIndex(0) - list.getElementStartIndex(0));
        assertTrue(list.getDataVector().isNull(1));
        assertTrue(list.isNull(1));
        var map = (MapVector) retained.getVector(2);
        var entries = (StructVector) map.getDataVector();
        assertEquals(7L, ((BigIntVector) entries.getChildrenFromFields().get(0)).get(0));
        assertEquals(
            42L,
            ((BigIntVector)
                    ((StructVector) entries.getChildrenFromFields().get(1))
                        .getChild("__sf_nested_row_kind"))
                .get(0));
        assertTrue(map.isNull(1));
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }
}
