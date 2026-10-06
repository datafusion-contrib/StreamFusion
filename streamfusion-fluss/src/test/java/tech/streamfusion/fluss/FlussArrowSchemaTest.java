package tech.streamfusion.fluss;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import tech.streamfusion.arrow.TimestampAccessor;
import tech.streamfusion.operator.RowDataArrowConverter;

class FlussArrowSchemaTest {
  @Test
  void millisecondTimestampReadRetainsWireDataAfterInputCloses() {
    var wireSchema =
        new Schema(
            List.of(Field.nullable("time", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))));
    var nativeSchema = new Schema(List.of(TimestampAccessor.field("time", true)));
    try (var allocator = new RootAllocator()) {
      VectorSchemaRoot converted;
      long address;
      try (var input = VectorSchemaRoot.create(wireSchema, allocator)) {
        input.allocateNew();
        var timestamp = (TimeStampMilliVector) input.getVector(0);
        timestamp.setSafe(0, Long.MIN_VALUE);
        timestamp.setNull(1);
        timestamp.setSafe(2, -1);
        timestamp.setSafe(3, Long.MAX_VALUE);
        input.setRowCount(4);
        address = timestamp.getDataBuffer().memoryAddress();
        converted = FlussArrowSchema.convert(input, nativeSchema, allocator);
      }
      try (var components = converted;
          var restored = FlussArrowSchema.forAppend(components, wireSchema, allocator)) {
        var accessor = new TimestampAccessor(components.getVector(0));
        assertEquals(Long.MIN_VALUE, accessor.getTimestamp(0).getMillisecond());
        assertTrue(accessor.isNull(1));
        assertEquals(-1L, accessor.getTimestamp(2).getMillisecond());
        assertEquals(0, accessor.getTimestamp(2).getNanoOfMillisecond());
        assertEquals(Long.MAX_VALUE, accessor.getTimestamp(3).getMillisecond());
        assertEquals(
            address,
            ((StructVector) components.getVector(0))
                .getChild("millis")
                .getDataBuffer()
                .memoryAddress());
        assertEquals(address, restored.getVector(0).getDataBuffer().memoryAddress());
        assertTrue(restored.getVector(0).isNull(1));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = TimeUnit.class,
      names = {"MICROSECOND", "NANOSECOND"})
  void fractionalTimestampsRoundTripThroughComponents(TimeUnit unit) {
    Schema wireSchema =
        new Schema(List.of(Field.nullable("time", new ArrowType.Timestamp(unit, null))));
    Schema nativeSchema = new Schema(List.of(TimestampAccessor.field("time", true)));
    try (var allocator = new RootAllocator();
        var wire = VectorSchemaRoot.create(wireSchema, allocator)) {
      wire.allocateNew();
      var values = (TimeStampVector) wire.getVector(0);
      long[] expected = {
        0, -1, 1, unit == TimeUnit.MICROSECOND ? 1640000000123456L : 1640000000123456789L
      };
      for (int i = 0; i < expected.length; i++) values.setSafe(i, expected[i]);
      values.setNull(expected.length);
      wire.setRowCount(expected.length + 1);
      try (var components = FlussArrowSchema.convert(wire, nativeSchema, allocator);
          var output = FlussArrowSchema.forAppend(components, wireSchema, allocator)) {
        var restored = (TimeStampVector) output.getVector(0);
        for (int i = 0; i < expected.length; i++) assertEquals(expected[i], restored.get(i));
        assertTrue(restored.isNull(expected.length));
      }
    }
  }

  @Test
  void millisecondTimestampAppendSharesComponentsAndPreservesNullsAndRange() throws Exception {
    var inputSchema = new Schema(List.of(TimestampAccessor.field("query_time", true)));
    var wireSchema =
        new Schema(
            List.of(
                Field.nullable("event_time", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))));
    try (var allocator = new RootAllocator();
        var input = VectorSchemaRoot.create(inputSchema, allocator)) {
      input.allocateNew();
      var accessor = new TimestampAccessor(input.getVector(0));
      accessor.set(0, TimestampData.fromEpochMillis(Long.MIN_VALUE));
      accessor.set(1, null);
      accessor.set(2, TimestampData.fromEpochMillis(-1, 999999));
      accessor.set(3, TimestampData.fromEpochMillis(Long.MAX_VALUE));
      input.setRowCount(4);
      var millis = (BigIntVector) ((StructVector) input.getVector(0)).getChild("millis");
      try (var wire = FlussArrowSchema.forAppend(input, wireSchema, allocator)) {
        var timestamp = (TimeStampMilliVector) wire.getVector(0);
        assertEquals(
            millis.getDataBuffer().memoryAddress(), timestamp.getDataBuffer().memoryAddress());
        assertEquals(Long.MIN_VALUE, timestamp.get(0));
        assertTrue(timestamp.isNull(1));
        assertEquals(-1, timestamp.get(2));
        assertEquals(Long.MAX_VALUE, timestamp.get(3));
        byte[] encoded = FlussArrowLogBatch.encode(wire, 1, -1, -1);
        try (var decoded =
            FlussArrowLogBatch.decode(ByteBuffer.wrap(encoded), wireSchema, allocator)) {
          assertEquals(-1L, ((TimeStampMilliVector) decoded.root().getVector(0)).get(2));
          assertTrue(decoded.root().getVector(0).isNull(1));
        }
      }
    }
  }

  @Test
  void positionalAppendRenamesColumnsAndRetainsDataWithoutChangelogSidecar() {
    Schema input =
        new Schema(
            List.of(
                Field.nullable("alias", new ArrowType.Int(64, true)),
                Field.nullable(RowDataArrowConverter.ROW_KIND_COLUMN, new ArrowType.Int(8, true))));
    Schema target = new Schema(List.of(Field.nullable("destination", new ArrowType.Int(64, true))));
    try (RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot source = VectorSchemaRoot.create(input, allocator)) {
      source.allocateNew();
      ((BigIntVector) source.getVector(0)).setSafe(0, 91);
      ((TinyIntVector) source.getVector(1)).setSafe(0, 0);
      source.setRowCount(1);
      try (VectorSchemaRoot output = FlussArrowSchema.forAppend(source, target, allocator)) {
        assertEquals(target, output.getSchema());
        assertEquals(91L, output.getVector(0).getObject(0));
        assertEquals(
            source.getVector(0).getDataBuffer().memoryAddress(),
            output.getVector(0).getDataBuffer().memoryAddress());
        assertNull(output.getVector(RowDataArrowConverter.ROW_KIND_COLUMN));
      }
      assertEquals(91L, source.getVector(0).getObject(0));
      ((TinyIntVector) source.getVector(1)).setSafe(0, 3);
      assertThrows(
          IllegalArgumentException.class,
          () -> FlussArrowSchema.forAppend(source, target, allocator));
    }
  }
}
