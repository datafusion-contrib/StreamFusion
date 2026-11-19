package tech.streamfusion.arrow;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.RowDataArrowConverter;

class ArrowFileBatchTest {
  @Test
  void fileSchemaAndBorrowedValuesExcludeNestedKindsWithoutCopyingPayloads() {
    var type = RowType.of(RowType.of(new BigIntType(), new VarCharType()));
    try (var allocator = new RootAllocator()) {
      var nested = GenericRowData.of(42L, StringData.fromString("é🙂"));
      nested.setRowKind(RowKind.DELETE);
      var source = RowDataArrowConverter.write(List.of(GenericRowData.of(nested)), type, allocator);
      var original = (StructVector) source.getVector(0);
      assertNotNull(NestedRepresentation.kinds(original));
      long address = ((BigIntVector) original.getVectorById(0)).getDataBuffer().memoryAddress();
      boolean sourceClosed = false;
      try (var file = ArrowFileBatch.of(source)) {
        var values = (StructVector) file.root().getVector(0);
        assertEquals(2, values.size(), "File fields must match the two declared SQL fields");
        assertEquals(
            address, ((BigIntVector) values.getVectorById(0)).getDataBuffer().memoryAddress());
        source.close();
        sourceClosed = true;
        var row = ArrowConversion.createArrowReader(file.root(), type).read(0).getRow(0, 2);
        assertEquals(42, row.getLong(0));
        assertEquals("é🙂", row.getString(1).toString());
        assertEquals(
            RowKind.INSERT, row.getRowKind(), "File formats do not encode nested RowKinds");
      } finally {
        if (!sourceClosed) source.close();
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void arraysMapsNullParentsAndFileDescriptorsKeepOnlySqlFields() {
    var child = RowType.of(new BigIntType(), new VarCharType());
    var type =
        RowType.of(
            child,
            new org.apache.flink.table.types.logical.ArrayType(child),
            new org.apache.flink.table.types.logical.MapType(
                new org.apache.flink.table.types.logical.IntType(), child));
    try (var allocator = new RootAllocator()) {
      var nested = GenericRowData.of(9L, StringData.fromString("value"));
      nested.setRowKind(RowKind.UPDATE_BEFORE);
      try (var source =
              RowDataArrowConverter.write(
                  List.of(
                      GenericRowData.of(
                          nested,
                          new org.apache.flink.table.data.GenericArrayData(
                              new Object[] {null, nested}),
                          new org.apache.flink.table.data.GenericMapData(
                              java.util.Map.of(1, nested))),
                      GenericRowData.of(null, null, null)),
                  type,
                  allocator);
          var file = ArrowFileBatch.of(source)) {
        var schema = tech.streamfusion.compat.FileSinkCompat.encoderSchema(type);
        assertEquals(
            schema.getFields().get(0).getChildren(),
            file.root().getSchema().getFields().get(0).getChildren());
        assertEquals(2, schema.getFields().get(0).getChildren().size());
        var reader = ArrowConversion.createArrowReader(file.root(), type);
        var row = reader.read(0);
        assertTrue(row.getArray(1).isNullAt(0));
        assertEquals(9L, row.getArray(1).getRow(1, 2).getLong(0));
        assertEquals("value", row.getMap(2).valueArray().getRow(0, 2).getString(1).toString());
        row = reader.read(1);
        assertTrue(row.isNullAt(0));
        assertTrue(row.isNullAt(1));
        assertTrue(row.isNullAt(2));
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void anOrdinaryBatchKeepsItsExistingBuffersAndOwnership() {
    var type = RowType.of(new BigIntType());
    try (var allocator = new RootAllocator();
        var source =
            RowDataArrowConverter.write(List.of(GenericRowData.of(42L)), type, allocator)) {
      try (var file = ArrowFileBatch.of(source)) {
        assertSame(source, file.root());
      }
      assertEquals(42, ((BigIntVector) source.getVector(0)).get(0));
    }
  }
}
