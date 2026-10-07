package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;

class WindowOutputOwnershipTest {
  @Test
  void compatibleBuffersSurviveSourceClosureWithDeclaredSchemaAndConversions() throws Exception {
    for (int properties : new int[] {0, 2}) {
    for (var shape : WindowOutputHandoffBenchmark.Shape.values()) {
      RowType output = RowType.of(new LogicalType[] {
          shape.type, new BigIntType(), new TimestampType(3), new TimestampType(3)},
          new String[] {"declared_key", "declared_result", "window_start", "window_end"});
      if (properties == 0) output = RowType.of(new LogicalType[] {shape.type, new BigIntType()},
          new String[] {"declared_key", "declared_result"});
      LogicalType nativeKey = shape == WindowOutputHandoffBenchmark.Shape.INT ? new BigIntType() : shape.type;
      RowType nativeType = RowType.of(new LogicalType[] {
          nativeKey, new BigIntType(), new BigIntType(), new BigIntType()},
          new String[] {"key0", "result0", "window_start", "window_end"});
      var fixture = new WindowOutputShapingBenchmark.Fixture(output, shape.code, shape.precision);
      try (RootAllocator allocator = new RootAllocator();
          var harness = new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
              fixture, batch -> 0, Types.INT, 128, 1, 0)) {
        harness.setup(new ArrowBatchSerializer());
        harness.open();
        // Isolate all fixture/output allocations so allocator closure checks failure cleanup too.
        fixture.allocator = allocator;
        var rows = new ArrayList<org.apache.flink.table.data.RowData>();
        for (int row = 0; row < 17; row++) {
          Object key = shape == WindowOutputHandoffBenchmark.Shape.INT ? (long) row : shape.key(row);
          if (row == 5) key = null;
          rows.add(GenericRowData.of(key, row % 3 == 0 ? null : (long) row, 0L, 1000L));
        }
        fixture.prepare(rows, nativeType);
        // Native names/metadata must not replace the independently declared output fields,
        // including timestamp component children.
        var source = fixture.prepared;
        fixture.prepared = new VectorSchemaRoot(
            new org.apache.arrow.vector.types.pojo.Schema(source.getSchema().getFields().stream()
                .map(WindowOutputOwnershipTest::withNativeMetadata).collect(java.util.stream.Collectors.toList())),
            source.getFieldVectors(), source.getRowCount());
        List<Long> keyBuffers = dataAddresses(fixture.prepared.getVector(0));
        long resultBuffer = fixture.prepared.getVector(1).getDataBuffer().memoryAddress();
        fixture.emitClosedWindows(999);
        fixture.prepared.close();
        var record = (StreamRecord<?>) harness.getOutput().poll();
        try (VectorSchemaRoot root = ((ArrowBatch) record.getValue()).root()) {
          assertEquals(tech.streamfusion.arrow.ArrowConversion.toArrowSchema(output), root.getSchema());
          for (int column = 0; column < root.getFieldVectors().size(); column++)
            assertEquals(root.getSchema().getFields().get(column), root.getVector(column).getField());
          if (properties == 0) {
            assertEquals(resultBuffer, root.getVector(1).getDataBuffer().memoryAddress());
          } else {
            assertNotEquals(resultBuffer, root.getVector(1).getDataBuffer().memoryAddress());
          }
          if (shape != WindowOutputHandoffBenchmark.Shape.INT) {
            if (properties == 0) assertEquals(keyBuffers, dataAddresses(root.getVector(0)));
            else assertNotEquals(keyBuffers, dataAddresses(root.getVector(0)));
          }
          var actual = RowDataArrowConverter.read(root, output);
          assertEquals(17, actual.size());
          for (int row = 0; row < actual.size(); row++) {
            var value = actual.get(row);
            assertEquals(row == 5, value.isNullAt(0));
            if (row != 5) {
              switch (shape) {
                case INT: assertEquals(row, value.getInt(0)); break;
                case STRING: assertEquals(shape.key(row), value.getString(0)); break;
                case TIMESTAMP: assertEquals(shape.key(row), value.getTimestamp(0, 9)); break;
                default: assertEquals(row, value.getLong(0));
              }
            }
            assertEquals(row % 3 == 0, value.isNullAt(1));
            if (row % 3 != 0) assertEquals(row, value.getLong(1));
            if (properties == 2) {
              assertEquals(TimestampData.fromEpochMillis(0), value.getTimestamp(2, 3));
              assertEquals(TimestampData.fromEpochMillis(1000), value.getTimestamp(3, 3));
            }
          }
        }
        assertEquals(0, allocator.getAllocatedMemory());
      }
    }
  }

  }

  @Test
  void partiallyCopiedOutputIsReleasedWhenShapingFails() throws Exception {
    RowType output = RowType.of(new BigIntType(), new BigIntType(), new TimestampType(3));
    RowType nativeType = RowType.of(new LogicalType[] {
        new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType()},
        new String[] {"key0", "result0", "window_start", "window_end"});
    var fixture = new WindowOutputShapingBenchmark.Fixture(output, 0, -1);
    try (RootAllocator allocator = new RootAllocator();
        var harness = new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            fixture, batch -> 0, Types.INT, 128, 1, 0)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      fixture.allocator = allocator;
      fixture.prepare(List.of(GenericRowData.of(1L, 2L, 0L, 1000L)), nativeType);
      var failure = assertThrows(IllegalStateException.class, () -> fixture.emitClosedWindows(999));
      assertEquals("window output cannot contain exactly one property", failure.getMessage());
      fixture.prepared.close();
      assertTrue(harness.getOutput().isEmpty());
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  private static org.apache.arrow.vector.types.pojo.Field withNativeMetadata(
      org.apache.arrow.vector.types.pojo.Field field) {
    var type = field.getFieldType();
    return new org.apache.arrow.vector.types.pojo.Field(field.getName(),
        new org.apache.arrow.vector.types.pojo.FieldType(type.isNullable(), type.getType(),
            type.getDictionary(), java.util.Map.of("native-only", "metadata")),
        field.getChildren().stream().map(WindowOutputOwnershipTest::withNativeMetadata)
            .collect(java.util.stream.Collectors.toList()));
  }

  private static List<Long> dataAddresses(FieldVector vector) {
    List<Long> addresses = new ArrayList<>();
    if (vector.getChildrenFromFields().isEmpty()) {
      if (vector.getDataBuffer().capacity() > 0) addresses.add(vector.getDataBuffer().memoryAddress());
    } else {
      for (FieldVector child : vector.getChildrenFromFields()) addresses.addAll(dataAddresses(child));
    }
    return addresses;
  }
}
