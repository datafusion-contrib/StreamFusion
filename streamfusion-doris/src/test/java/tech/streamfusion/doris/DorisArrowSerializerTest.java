package tech.streamfusion.doris;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.ArrowBatch;

class DorisArrowSerializerTest {
  @Test
  void encodesOwnedVectorsWithDestinationNamesNullsAndSlices() throws Exception {
    RowType type =
        RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
              new IntType(), new VarCharType(VarCharType.MAX_LENGTH)
            },
            new String[] {"id", "name"});
    var schema =
        ArrowConversion.toArrowSchema(
            RowType.of(
                type.getChildren().toArray(new org.apache.flink.table.types.logical.LogicalType[0]),
                new String[] {"expression_0", "expression_1"}));
    try (var allocator = new RootAllocator()) {
      var input = VectorSchemaRoot.create(schema, allocator);
      input.allocateNew();
      ((IntVector) input.getVector(0)).setSafe(0, -1);
      ((IntVector) input.getVector(0)).setSafe(1, 11);
      ((IntVector) input.getVector(0)).setNull(2);
      ((VarCharVector) input.getVector(1)).setSafe(0, new byte[] {0});
      ((VarCharVector) input.getVector(1))
          .setSafe(1, "héllo".getBytes(java.nio.charset.StandardCharsets.UTF_8));
      ((VarCharVector) input.getVector(1)).setNull(2);
      input.setRowCount(3);
      VectorSchemaRoot sliced = input.slice(1, 2);
      input.close();
      var kinds =
          new org.apache.arrow.vector.TinyIntVector(
              tech.streamfusion.operator.RowDataArrowConverter.ROW_KIND_COLUMN, allocator);
      kinds.setSafe(0, org.apache.flink.types.RowKind.INSERT.toByteValue());
      kinds.setSafe(1, org.apache.flink.types.RowKind.INSERT.toByteValue());
      kinds.setValueCount(2);
      var fields = new java.util.ArrayList<>(sliced.getSchema().getFields());
      fields.add(kinds.getField());
      var vectors = new java.util.ArrayList<>(sliced.getFieldVectors());
      vectors.add(kinds);
      var withSidecar =
          new VectorSchemaRoot(new org.apache.arrow.vector.types.pojo.Schema(fields), vectors, 2);
      byte[] bytes = new DorisArrowSerializer(type).serialize(new ArrowBatch(withSidecar)).getRow();
      assertEquals(0, allocator.getAllocatedMemory());
      try (var reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)) {
        assertTrue(reader.loadNextBatch());
        assertEquals(2, reader.getVectorSchemaRoot().getRowCount());
        assertEquals(
            List.of("id", "name"),
            reader.getVectorSchemaRoot().getSchema().getFields().stream()
                .map(org.apache.arrow.vector.types.pojo.Field::getName)
                .collect(java.util.stream.Collectors.toList()));
        assertEquals(11, reader.getVectorSchemaRoot().getVector(0).getObject(0));
        assertEquals("héllo", reader.getVectorSchemaRoot().getVector(1).getObject(0).toString());
        assertNull(reader.getVectorSchemaRoot().getVector(1).getObject(1));
        assertFalse(reader.loadNextBatch());
      }
    }
  }

  @Test
  void timestampComponentsAndAllRowKindsBecomeWireTimestampsAndDeleteSigns() throws Exception {
    var type =
        RowType.of(
            new org.apache.flink.table.types.logical.LogicalType[] {
              new org.apache.flink.table.types.logical.TimestampType(6)
            },
            new String[] {"ts"});
    var kinds = org.apache.flink.types.RowKind.values();
    var rows = new java.util.ArrayList<org.apache.flink.table.data.RowData>();
    for (var kind : kinds) {
      var row =
          org.apache.flink.table.data.GenericRowData.of(
              org.apache.flink.table.data.TimestampData.fromLocalDateTime(
                  java.time.LocalDateTime.parse("1900-01-01T00:00:00.123456")));
      row.setRowKind(kind);
      rows.add(row);
    }
    try (var allocator = new RootAllocator()) {
      var input =
          tech.streamfusion.operator.RowDataArrowConverter.write(rows, type, allocator, true);
      byte[] bytes = new DorisArrowSerializer(type, true).serialize(new ArrowBatch(input)).getRow();
      assertEquals(0, allocator.getAllocatedMemory());
      try (var reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)) {
        assertTrue(reader.loadNextBatch());
        var root = reader.getVectorSchemaRoot();
        assertEquals(2, root.getFieldVectors().size());
        assertInstanceOf(org.apache.arrow.vector.TimeStampMicroVector.class, root.getVector(0));
        for (int row = 0; row < kinds.length; row++) {
          assertEquals(
              java.time.LocalDateTime.parse("1900-01-01T00:00:00.123456"),
              root.getVector(0).getObject(row));
          assertEquals(
              (byte)
                  (kinds[row] == org.apache.flink.types.RowKind.DELETE
                          || kinds[row] == org.apache.flink.types.RowKind.UPDATE_BEFORE
                      ? 1
                      : 0),
              root.getVector(1).getObject(row));
        }
      }
    }
  }
}
