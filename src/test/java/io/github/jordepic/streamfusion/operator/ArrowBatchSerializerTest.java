package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class ArrowBatchSerializerTest {

  private static final RowType SCHEMA =
      RowType.of(new LogicalType[] {new BigIntType(), new IntType()}, new String[] {"k", "v"});

  private static RowData row(long k, int v) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, k);
    row.setField(1, v);
    return row;
  }

  @Test
  void roundTripsABatchThroughArrowIpc() throws Exception {
    ArrowBatchSerializer serializer = new ArrowBatchSerializer();
    try (BufferAllocator allocator = new RootAllocator()) {
      List<RowData> rows = List.of(row(1L, 10), row(2L, 20), row(3L, 30));
      VectorSchemaRoot root = RowDataArrowConverter.write(rows, SCHEMA, allocator);

      DataOutputSerializer out = new DataOutputSerializer(256);
      serializer.serialize(new ArrowBatch(root), out);
      root.close();

      ArrowBatch back = serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
      try (VectorSchemaRoot result = back.root()) {
        assertEquals(3, back.rowCount());
        List<RowData> readBack = RowDataArrowConverter.read(result, SCHEMA);
        assertEquals(3, readBack.size());
        for (int i = 0; i < rows.size(); i++) {
          assertEquals(rows.get(i).getLong(0), readBack.get(i).getLong(0), "k row " + i);
          assertEquals(rows.get(i).getInt(1), readBack.get(i).getInt(1), "v row " + i);
        }
      }
    }
  }

  @Test
  void copyIsIdentity() {
    ArrowBatchSerializer serializer = new ArrowBatchSerializer();
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = RowDataArrowConverter.write(List.of(row(1L, 10)), SCHEMA, allocator)) {
      ArrowBatch batch = new ArrowBatch(root);
      // A fresh batch is handed off, never retained or mutated, so copy returns the same instance.
      assertEquals(batch, serializer.copy(batch));
    }
  }
}
