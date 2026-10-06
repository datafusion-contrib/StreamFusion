package tech.streamfusion.fluss;

import static org.apache.fluss.record.LogRecordBatchFormat.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32C;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.record.MemoryLogRecords;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.RowDataArrowConverter;

class FlussArrowLogBatchTest {
  static final Schema SCHEMA =
      new Schema(List.of(Field.nullable("id", new ArrowType.Int(64, true))));

  @Test
  void appendBytesPassOfficialJavaValidationAndRoundTrip() throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot input = VectorSchemaRoot.create(SCHEMA, allocator)) {
      input.allocateNew();
      BigIntVector id = (BigIntVector) input.getVector(0);
      id.setSafe(0, 42);
      id.setNull(1);
      id.setSafe(2, -7);
      input.setRowCount(3);
      byte[] encoded = FlussArrowLogBatch.encode(input, 3, 17, 4);
      var official = MemoryLogRecords.pointToBytes(encoded).batches().iterator().next();
      official.ensureValid();
      assertEquals(3, official.getRecordCount());
      assertEquals(17, official.writerId());
      try (var decoded = FlussArrowLogBatch.decode(ByteBuffer.wrap(encoded), SCHEMA, allocator)) {
        assertEquals(3, decoded.nextOffset());
        assertEquals(42L, decoded.root().getVector(0).getObject(0));
        assertNull(decoded.root().getVector(0).getObject(1));
        assertEquals(-7L, decoded.root().getVector(0).getObject(2));
        assertNull(decoded.root().getVector(RowDataArrowConverter.ROW_KIND_COLUMN));
      }
    }
  }

  @Test
  void cdcKindsSurviveEverySupportedHeaderVersion() throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot input = VectorSchemaRoot.create(SCHEMA, allocator)) {
      input.allocateNew();
      for (int i = 0; i < 4; i++) ((BigIntVector) input.getVector(0)).setSafe(i, i);
      input.setRowCount(4);
      byte[] append = FlussArrowLogBatch.encode(input, 0, -1, -1);
      for (byte magic = 0; magic <= 2; magic++) {
        byte[] encoded = changelog(append, magic, new byte[] {1, 2, 3, 4});
        MemoryLogRecords.pointToBytes(encoded).batches().iterator().next().ensureValid();
        try (var decoded = FlussArrowLogBatch.decode(ByteBuffer.wrap(encoded), SCHEMA, allocator)) {
          TinyIntVector kinds =
              (TinyIntVector) decoded.root().getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
          for (int i = 0; i < 4; i++) assertEquals(i, kinds.get(i));
        }
      }
      byte[] invalid = changelog(append, (byte) 0, new byte[] {1, 2, 99, 4});
      assertThrows(
          IOException.class,
          () -> FlussArrowLogBatch.decode(ByteBuffer.wrap(invalid), SCHEMA, allocator));
      append[append.length - 1] ^= 1;
      assertThrows(
          IOException.class,
          () -> FlussArrowLogBatch.decode(ByteBuffer.wrap(append), SCHEMA, allocator));
    }
  }

  static byte[] changelog(byte[] append, byte magic, byte[] kinds) {
    int header = recordBatchHeaderSize(magic);
    int stats = magic == 0 ? 0 : 3;
    byte[] result =
        new byte[append.length - V0_RECORD_BATCH_HEADER_SIZE + header + stats + kinds.length];
    ByteBuffer data = ByteBuffer.wrap(result).order(java.nio.ByteOrder.LITTLE_ENDIAN);
    data.putLong(BASE_OFFSET_OFFSET, 0);
    data.putInt(LENGTH_OFFSET, result.length - LOG_OVERHEAD);
    data.put(MAGIC_OFFSET, magic);
    data.putInt(lastOffsetDeltaOffset(magic), kinds.length - 1);
    data.putLong(writeClientIdOffset(magic), -1);
    data.putInt(batchSequenceOffset(magic), -1);
    data.putInt(recordsCountOffset(magic), kinds.length);
    if (magic > 0) data.putInt(statisticsLengthOffset(magic), stats);
    System.arraycopy(kinds, 0, result, header + stats, kinds.length);
    System.arraycopy(
        append,
        V0_RECORD_BATCH_HEADER_SIZE,
        result,
        header + stats + kinds.length,
        append.length - V0_RECORD_BATCH_HEADER_SIZE);
    CRC32C crc = new CRC32C();
    crc.update(result, schemaIdOffset(magic), result.length - schemaIdOffset(magic));
    data.putInt(crcOffset(magic), (int) crc.getValue());
    return result;
  }
}
