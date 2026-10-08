package tech.streamfusion.fluss;

import static org.apache.fluss.record.LogRecordBatchFormat.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.types.RowKind;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Fluss log framing around schema-less Arrow IPC record-batch messages. */
public final class FlussArrowLogBatch {
  private FlussArrowLogBatch() {}

  public record Decoded(
      VectorSchemaRoot root, int schemaId, long baseOffset, long nextOffset, long timestamp)
      implements AutoCloseable {
    @Override
    public void close() {
      if (root != null) root.close();
    }
  }

  /** Decode exactly one complete batch; no payload buffers are exposed before CRC validation. */
  public static Decoded decode(ByteBuffer bytes, Schema schema, BufferAllocator allocator)
      throws IOException {
    return decode(bytes, schema, allocator, true);
  }

  static Decoded decode(
      ByteBuffer bytes, Schema schema, BufferAllocator allocator, boolean checkCrc)
      throws IOException {
    return decode(bytes, schema, allocator, checkCrc, null);
  }

  static Decoded decode(
      ByteBuffer bytes,
      Schema schema,
      BufferAllocator allocator,
      boolean checkCrc,
      ArrowBuf borrowedBatch)
      throws IOException {
    ByteBuffer batch = bytes.slice().order(ByteOrder.LITTLE_ENDIAN);
    require(batch.remaining() >= HEADER_SIZE_UP_TO_MAGIC, "truncated log header");
    byte magic = batch.get(MAGIC_OFFSET);
    require(magic >= LOG_MAGIC_VALUE_V0 && magic <= LOG_MAGIC_VALUE_V2, "unknown log magic");
    int headerSize = recordBatchHeaderSize(magic);
    require(batch.remaining() >= headerSize, "truncated log header");
    int length = batch.getInt(LENGTH_OFFSET);
    require(
        length >= headerSize - LOG_OVERHEAD && (long) length + LOG_OVERHEAD == batch.remaining(),
        "invalid log batch length");
    int schemaOffset = schemaIdOffset(magic);
    if (checkCrc) {
      CRC32C crc = new CRC32C();
      crc.update(batch.duplicate().position(schemaOffset));
      require(
          crc.getValue() == Integer.toUnsignedLong(batch.getInt(crcOffset(magic))),
          "log batch checksum mismatch");
    }
    int count = batch.getInt(recordsCountOffset(magic));
    int delta = batch.getInt(lastOffsetDeltaOffset(magic));
    require(count >= 0 && delta >= 0, "invalid log row count or offset delta");
    long base = batch.getLong(BASE_OFFSET_OFFSET);
    long next;
    try {
      next = Math.addExact(Math.addExact(base, delta), 1L);
    } catch (ArithmeticException overflow) {
      throw new IOException("log offset overflow", overflow);
    }
    int schemaId = Short.toUnsignedInt(batch.getShort(schemaOffset));
    long timestamp = batch.getLong(COMMIT_TIMESTAMP_OFFSET);
    int statsSize = magic == LOG_MAGIC_VALUE_V0 ? 0 : batch.getInt(statisticsLengthOffset(magic));
    require(statsSize >= 0 && statsSize <= batch.limit() - headerSize, "invalid statistics length");
    int payloadOffset = headerSize + statsSize;
    boolean appendOnly = (batch.get(attributeOffset(magic)) & 1) != 0;
    require((batch.get(attributeOffset(magic)) & ~1) == 0, "unknown log attributes");
    require(count == 0 || delta == count - 1, "non-contiguous log row offsets");
    ByteBuffer kinds = null;
    if (!appendOnly) {
      require(count <= batch.limit() - payloadOffset, "truncated change-type sidecar");
      kinds = batch.duplicate().position(payloadOffset).limit(payloadOffset + count).slice();
      for (int i = 0; i < count; i++) rowKind(kinds.get(i));
      payloadOffset += count;
    }
    if (count == 0) return new Decoded(null, schemaId, base, next, timestamp);
    ByteBuffer payload = batch.duplicate().position(payloadOffset).slice();
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    TinyIntVector kindVector = null;
    try (ReadChannel channel = new ReadChannel(new BufferChannel(payload));
        ArrowRecordBatch arrow =
            readArrow(channel, payload, payloadOffset, borrowedBatch, allocator)) {
      require(arrow != null && arrow.getLength() == count, "Arrow/log row count mismatch");
      require(!payload.hasRemaining(), "trailing Arrow payload");
      new VectorLoader(root, FlussArrowCompression.INSTANCE).load(arrow);
      alignVariableOffsets(root.getFieldVectors(), allocator);
      if (kinds != null) {
        kindVector = new TinyIntVector(RowDataArrowConverter.ROW_KIND_COLUMN, allocator);
        kindVector.allocateNew(count);
        for (int i = 0; i < count; i++) kindVector.set(i, rowKind(kinds.get(i)));
        kindVector.setValueCount(count);
        List<FieldVector> vectors = new ArrayList<>(root.getFieldVectors());
        vectors.add(kindVector);
        root = new VectorSchemaRoot(vectors);
        kindVector = null;
        root.setRowCount(count);
      }
      return new Decoded(root, schemaId, base, next, timestamp);
    } catch (Throwable failure) {
      root.close();
      if (kindVector != null) kindVector.close();
      if (failure instanceof IOException io) throw io;
      throw failure;
    }
  }

  // Arrow-rs reads the final string/binary offset during FFI import, before align_buffers runs.
  private static void alignVariableOffsets(List<FieldVector> vectors, BufferAllocator allocator) {
    for (var vector : vectors) {
      if (vector instanceof BaseVariableWidthVector variable
          && (variable.getOffsetBuffer().memoryAddress() & 3) != 0) {
        var buffers = new ArrayList<>(vector.getFieldBuffers());
        ArrowBuf offsets = variable.getOffsetBuffer();
        try (ArrowBuf aligned = allocator.buffer(offsets.capacity())) {
          aligned.setBytes(0, offsets, 0, offsets.capacity());
          buffers.set(1, aligned);
          // Loading releases the old fields before retaining replacements, which may alias them.
          for (ArrowBuf buffer : buffers) buffer.getReferenceManager().retain();
          try {
            vector.loadFieldBuffers(
                new ArrowFieldNode(vector.getValueCount(), vector.getNullCount()), buffers);
          } finally {
            for (ArrowBuf buffer : buffers) buffer.close();
          }
        }
      }
      alignVariableOffsets(vector.getChildrenFromFields(), allocator);
    }
  }

  private static ArrowRecordBatch readArrow(
      ReadChannel channel,
      ByteBuffer payload,
      int payloadOffset,
      ArrowBuf borrowedBatch,
      BufferAllocator allocator)
      throws IOException {
    if (borrowedBatch == null) return MessageSerializer.deserializeRecordBatch(channel, allocator);
    var metadata = MessageSerializer.readMessage(channel);
    require(
        metadata != null && metadata.getMessageBodyLength() == payload.remaining(),
        "invalid Arrow body length");
    ArrowBuf body = borrowedBatch.slice(payloadOffset + payload.position(), payload.remaining());
    body.getReferenceManager().retain();
    try {
      // Arrow's deserializer consumes one body reference on successful construction.
      ArrowRecordBatch arrow = MessageSerializer.deserializeRecordBatch(metadata, body);
      payload.position(payload.limit());
      return arrow;
    } catch (Throwable failure) {
      body.close();
      throw failure;
    }
  }

  /** Encode an append-only V0 batch using the broker's existing ProduceLog wire format. */
  public static byte[] encode(VectorSchemaRoot root, int schemaId, long writerId, int sequence)
      throws IOException {
    return encode(
        root,
        schemaId,
        writerId,
        sequence,
        org.apache.arrow.vector.compression.NoCompressionCodec.INSTANCE);
  }

  static byte[] encode(
      VectorSchemaRoot root,
      int schemaId,
      long writerId,
      int sequence,
      org.apache.arrow.vector.compression.CompressionCodec compression)
      throws IOException {
    return encode(root, schemaId, writerId, sequence, compression, null);
  }

  static byte[] encode(
      VectorSchemaRoot root,
      int schemaId,
      long writerId,
      int sequence,
      org.apache.arrow.vector.compression.CompressionCodec compression,
      byte[] statistics)
      throws IOException {
    byte magic = statistics == null ? LOG_MAGIC_VALUE_V0 : LOG_MAGIC_VALUE_V1;
    int headerSize = recordBatchHeaderSize(magic);
    int statisticsSize = statistics == null ? 0 : statistics.length;
    require(schemaId >= 0 && schemaId <= 65535, "schema id exceeds log header range");
    require(root.getRowCount() > 0, "empty append batch");
    require(
        root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN) == null,
        "append production cannot carry changelog kinds");
    byte[] result;
    try (ArrowRecordBatch arrow =
        new VectorUnloader(root, true, compression, true).getRecordBatch()) {
      int metadataSize = MessageSerializer.serializeMetadata(arrow).remaining();
      long paddedMetadata = (metadataSize + 7L) & ~7L;
      int size =
          Math.toIntExact(
              headerSize + statisticsSize + 8L + paddedMetadata + arrow.computeBodyLength());
      result = new byte[size];
      ByteBuffer target = ByteBuffer.wrap(result);
      target.position(headerSize);
      if (statistics != null) target.put(statistics);
      try (WriteChannel channel = new WriteChannel(new OutputBufferChannel(target))) {
        MessageSerializer.serialize(channel, arrow);
      }
      require(!target.hasRemaining(), "Arrow serialized length mismatch");
    }
    ByteBuffer header = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
    if (statistics != null) header.putInt(statisticsLengthOffset(magic), statisticsSize);
    header.putInt(LENGTH_OFFSET, result.length - LOG_OVERHEAD);
    header.put(MAGIC_OFFSET, magic);
    header.putShort(schemaIdOffset(magic), (short) schemaId);
    header.put(attributeOffset(magic), (byte) 1);
    header.putInt(lastOffsetDeltaOffset(magic), root.getRowCount() - 1);
    header.putLong(writeClientIdOffset(magic), writerId);
    header.putInt(batchSequenceOffset(magic), sequence);
    header.putInt(recordsCountOffset(magic), root.getRowCount());
    CRC32C crc = new CRC32C();
    crc.update(result, schemaIdOffset(magic), result.length - schemaIdOffset(magic));
    header.putInt(crcOffset(magic), (int) crc.getValue());
    return result;
  }

  private static byte rowKind(byte flussKind) throws IOException {
    return switch (flussKind) {
      case 0, 1 -> RowKind.INSERT.toByteValue();
      case 2 -> RowKind.UPDATE_BEFORE.toByteValue();
      case 3 -> RowKind.UPDATE_AFTER.toByteValue();
      case 4 -> RowKind.DELETE.toByteValue();
      default -> throw new IOException("invalid Fluss change type: " + flussKind);
    };
  }

  private static void require(boolean condition, String message) throws IOException {
    if (!condition) throw new IOException(message);
  }

  private static final class OutputBufferChannel implements WritableByteChannel {
    private final ByteBuffer target;
    private boolean open = true;

    OutputBufferChannel(ByteBuffer target) {
      this.target = target;
    }

    @Override
    public int write(ByteBuffer source) {
      int count = source.remaining();
      target.put(source);
      return count;
    }

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public void close() {
      open = false;
    }
  }

  private static final class BufferChannel implements ReadableByteChannel {
    private final ByteBuffer source;
    private boolean open = true;

    BufferChannel(ByteBuffer source) {
      this.source = source;
    }

    @Override
    public int read(ByteBuffer target) {
      if (!source.hasRemaining()) return -1;
      int count = Math.min(source.remaining(), target.remaining());
      ByteBuffer part = source.slice();
      part.limit(count);
      target.put(part);
      source.position(source.position() + count);
      return count;
    }

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public void close() {
      open = false;
    }
  }
}
