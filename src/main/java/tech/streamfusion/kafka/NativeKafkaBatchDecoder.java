package tech.streamfusion.kafka;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import tech.streamfusion.format.NativeMessageDecoder;
import tech.streamfusion.format.NativeMessageDecoderFactory;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.NativeSourceRecord;
import tech.streamfusion.operator.WatermarkExpression;

/**
 * Decodes one Kafka split's polled records into an Arrow batch: the task-local native decoder,
 * its reusable direct buffer, and the split's watermark candidate. Each batch takes the cheapest
 * boundary its table allows — a contiguous byte buffer, Arrow binary input, or, for a table that
 * reads connector metadata, a buffer that also carries each record's metadata.
 */
final class NativeKafkaBatchDecoder implements AutoCloseable {

  private final NativeMessageDecoder decoder;
  private final BufferAllocator allocator = NativeAllocator.SHARED;
  private final boolean keyed;
  private final boolean readsMetadata;
  private final boolean readsOffsets;
  private final boolean readsTimestamps;
  private final boolean readsLeaderEpochs;
  private final boolean readsTimestampTypes;
  private final boolean readsHeaders;
  private final int rowtimeIndex;
  private final WatermarkExpression.Evaluator watermarkExpression;
  private ArrowBuf decodeSlab;

  NativeKafkaBatchDecoder(
      RowType outputType,
      NativeMessageDecoderFactory decoderFactory,
      boolean keyed,
      List<String> metadataKeys,
      int rowtimeIndex,
      WatermarkExpression watermarkExpression) {
    this.keyed = keyed;
    this.readsMetadata = !metadataKeys.isEmpty();
    this.readsOffsets = metadataKeys.contains("offset");
    this.readsTimestamps = metadataKeys.contains("timestamp");
    this.readsLeaderEpochs = metadataKeys.contains("leader-epoch");
    this.readsTimestampTypes = metadataKeys.contains("timestamp-type");
    this.readsHeaders = metadataKeys.contains("headers");
    this.rowtimeIndex = rowtimeIndex;
    try {
      this.decoder = decoderFactory.create();
      this.decoder.open(allocator, outputType);
    } catch (Exception e) {
      throw new IllegalStateException("native Kafka decoder initialization failed", e);
    }
    this.watermarkExpression = watermarkExpression == null ? null : watermarkExpression.open();
  }

  NativeSourceRecord decode(List<ConsumerRecord<byte[], byte[]>> records) throws Exception {
    int count = records.size();
    long bodyBytes = 0;
    long keyBytes = 0;
    for (ConsumerRecord<byte[], byte[]> record : records) {
      byte[] value = record.value();
      if (value != null) {
        bodyBytes += value.length;
      }
      if (keyed) {
        byte[] key = record.key();
        if (key != null) {
          keyBytes += key.length;
        }
      }
    }
    if (readsMetadata) {
      return decodeRecords(records);
    }
    if (decoder.supportsContiguousBytes()) {
      return decodeContiguous(records, bodyBytes, keyBytes);
    }
    try (VarBinaryVector body = new VarBinaryVector("body", allocator);
        VarBinaryVector keys = keyed ? new VarBinaryVector("key", allocator) : null) {
      body.allocateNew(bodyBytes, count);
      if (keys != null) {
        keys.allocateNew(keyBytes, count);
      }
      for (int i = 0; i < count; i++) {
        ConsumerRecord<byte[], byte[]> record = records.get(i);
        set(body, i, record.value());
        if (keys != null) {
          set(keys, i, record.key());
        }
      }
      body.setValueCount(count);
      if (keys != null) {
        keys.setValueCount(count);
      }
      decoder.beforeDecode(body, count);
      if (keys != null) {
        decoder.beforeDecodeKeys(keys, count);
      }
      try (VectorSchemaRoot input =
              new VectorSchemaRoot(keys == null ? List.of(body) : List.of(keys, body));
          ArrowArray inArray = ArrowArray.allocateNew(allocator);
          ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
          ArrowArray outArray = ArrowArray.allocateNew(allocator);
          ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
        input.setRowCount(count);
        Data.exportVectorSchemaRoot(
            allocator, input, NativeAllocator.DICTIONARIES, inArray, inSchema);
        decoder.decodeInto(
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            outArray.memoryAddress(),
            outSchema.memoryAddress());
        VectorSchemaRoot output =
            Data.importVectorSchemaRoot(
                allocator, outArray, outSchema, NativeAllocator.DICTIONARIES);
        long nextOffset = records.get(count - 1).offset() + 1;
        if (output.getRowCount() == 0) {
          output.close();
          return new NativeSourceRecord(null, nextOffset, Long.MIN_VALUE);
        }
        return NativeSourceRecord.fromRoot(output, nextOffset, rowtimeIndex, watermarkExpression);
      }
    }
  }

  private NativeSourceRecord decodeContiguous(
      List<ConsumerRecord<byte[], byte[]>> records, long bodyBytes, long keyBytes) throws Exception {
    int count = records.size();
    long totalBytes = Math.addExact(bodyBytes, keyBytes);
    ensureDecodeSlab(totalBytes);
    int[] lengths = new int[Math.multiplyExact(count, 2)];
    long keyOffset = 0;
    long bodyOffset = keyBytes;
    for (int i = 0; i < count; i++) {
      ConsumerRecord<byte[], byte[]> record = records.get(i);
      byte[] key = keyed ? record.key() : null;
      byte[] value = record.value();
      lengths[i * 2] = key == null ? -1 : key.length;
      lengths[i * 2 + 1] = value == null ? -1 : value.length;
      if (key != null) {
        decodeSlab.setBytes(keyOffset, key);
        keyOffset += key.length;
      }
      if (value != null) {
        decodeSlab.setBytes(bodyOffset, value);
        bodyOffset += value.length;
      }
    }
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      decoder.decodeContiguousBytesInto(
          decodeSlab.memoryAddress(),
          totalBytes,
          keyBytes,
          lengths,
          count,
          keyed,
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      VectorSchemaRoot output =
          Data.importVectorSchemaRoot(
              allocator, outArray, outSchema, NativeAllocator.DICTIONARIES);
      return decodedRecord(records, output);
    }
  }

  // Offsets into the decode slab's regions, mirroring the native decode's layout indexes.
  private static final int LAYOUT_VALUES = 0;
  private static final int LAYOUT_VALUES_LEN = 1;
  private static final int LAYOUT_VALUE_LENGTHS = 2;
  private static final int LAYOUT_OFFSETS = 3;
  private static final int LAYOUT_TIMESTAMPS = 4;
  private static final int LAYOUT_LEADER_EPOCHS = 5;
  private static final int LAYOUT_TIMESTAMP_TYPES = 6;
  private static final int LAYOUT_HEADER_COUNTS = 7;
  private static final int LAYOUT_HEADERS = 8;
  private static final int LAYOUT_HEADERS_LEN = 9;
  private static final int LAYOUT_KEYS = 10;
  private static final int LAYOUT_KEYS_LEN = 11;
  private static final int LAYOUT_KEY_LENGTHS = 12;
  private static final int LAYOUT_LEN = 13;
  /** The leader epoch written for a record that carries none. */
  private static final long ABSENT_LEADER_EPOCH = Long.MIN_VALUE;

  /**
   * Decodes a table that reads connector metadata: the values (and a keyed table's keys) and each
   * requested per-record column go into the decode slab in one pass (eight-byte columns first, so
   * every region stays aligned), and the native decode gathers the metadata through the rows each
   * record produced. Header keys are re-encoded from Kafka's decoded strings exactly as Flink's
   * {@code StringData.fromString} does.
   */
  private NativeSourceRecord decodeRecords(List<ConsumerRecord<byte[], byte[]>> records)
      throws Exception {
    int count = records.size();
    byte[][] values = new byte[count][];
    byte[][] keys = keyed ? new byte[count][] : null;
    long valueBytes = 0;
    long keyBytes = 0;
    for (int i = 0; i < count; i++) {
      values[i] = records.get(i).value();
      if (values[i] != null) {
        valueBytes += values[i].length;
      }
      if (keyed) {
        keys[i] = records.get(i).key();
        if (keys[i] != null) {
          keyBytes += keys[i].length;
        }
      }
    }
    decoder.beforeDecode(values, count);
    if (keyed) {
      decoder.beforeDecodeKeys(keys, count);
    }
    int[] headerCounts = readsHeaders ? new int[count] : null;
    List<byte[]> headerParts = readsHeaders ? new ArrayList<>() : null;
    long headerBytes = 0;
    if (readsHeaders) {
      for (int i = 0; i < count; i++) {
        for (Header header : records.get(i).headers()) {
          byte[] key = header.key().getBytes(StandardCharsets.UTF_8);
          byte[] value = header.value();
          headerParts.add(key);
          headerParts.add(value);
          headerBytes += 8L + key.length + (value == null ? 0 : value.length);
          headerCounts[i]++;
        }
      }
    }
    long[] layout = new long[LAYOUT_LEN];
    Arrays.fill(layout, -1);
    long at = 0;
    if (readsOffsets) {
      layout[LAYOUT_OFFSETS] = at;
      at += 8L * count;
    }
    if (readsTimestamps) {
      layout[LAYOUT_TIMESTAMPS] = at;
      at += 8L * count;
    }
    if (readsLeaderEpochs) {
      layout[LAYOUT_LEADER_EPOCHS] = at;
      at += 8L * count;
    }
    layout[LAYOUT_VALUE_LENGTHS] = at;
    at += 4L * count;
    if (keyed) {
      layout[LAYOUT_KEY_LENGTHS] = at;
      at += 4L * count;
    }
    if (readsHeaders) {
      layout[LAYOUT_HEADER_COUNTS] = at;
      at += 4L * count;
    }
    if (readsTimestampTypes) {
      layout[LAYOUT_TIMESTAMP_TYPES] = at;
      at += count;
    }
    if (readsHeaders) {
      layout[LAYOUT_HEADERS] = at;
      layout[LAYOUT_HEADERS_LEN] = headerBytes;
      at += headerBytes;
    }
    if (keyed) {
      layout[LAYOUT_KEYS] = at;
      layout[LAYOUT_KEYS_LEN] = keyBytes;
      at += keyBytes;
    }
    layout[LAYOUT_VALUES] = at;
    layout[LAYOUT_VALUES_LEN] = valueBytes;
    at += valueBytes;
    ensureDecodeSlab(at);
    long valueAt = layout[LAYOUT_VALUES];
    long keyAt = layout[LAYOUT_KEYS];
    for (int i = 0; i < count; i++) {
      ConsumerRecord<byte[], byte[]> record = records.get(i);
      if (readsOffsets) {
        decodeSlab.setLong(layout[LAYOUT_OFFSETS] + 8L * i, record.offset());
      }
      if (readsTimestamps) {
        decodeSlab.setLong(layout[LAYOUT_TIMESTAMPS] + 8L * i, record.timestamp());
      }
      if (readsLeaderEpochs) {
        decodeSlab.setLong(
            layout[LAYOUT_LEADER_EPOCHS] + 8L * i,
            record.leaderEpoch().map(Integer::longValue).orElse(ABSENT_LEADER_EPOCH));
      }
      if (readsTimestampTypes) {
        decodeSlab.setByte(layout[LAYOUT_TIMESTAMP_TYPES] + i, record.timestampType().id);
      }
      if (readsHeaders) {
        decodeSlab.setInt(layout[LAYOUT_HEADER_COUNTS] + 4L * i, headerCounts[i]);
      }
      byte[] value = values[i];
      decodeSlab.setInt(layout[LAYOUT_VALUE_LENGTHS] + 4L * i, value == null ? -1 : value.length);
      if (value != null) {
        decodeSlab.setBytes(valueAt, value);
        valueAt += value.length;
      }
      if (keyed) {
        byte[] key = keys[i];
        decodeSlab.setInt(layout[LAYOUT_KEY_LENGTHS] + 4L * i, key == null ? -1 : key.length);
        if (key != null) {
          decodeSlab.setBytes(keyAt, key);
          keyAt += key.length;
        }
      }
    }
    if (readsHeaders) {
      long headerAt = layout[LAYOUT_HEADERS];
      for (byte[] part : headerParts) {
        decodeSlab.setInt(headerAt, part == null ? -1 : part.length);
        headerAt += 4;
        if (part != null) {
          decodeSlab.setBytes(headerAt, part);
          headerAt += part.length;
        }
      }
    }
    ConsumerRecord<byte[], byte[]> first = records.get(0);
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      decoder.decodeRecordsInto(
          decodeSlab.memoryAddress(),
          at,
          layout,
          count,
          first.topic(),
          first.partition(),
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      VectorSchemaRoot output =
          Data.importVectorSchemaRoot(
              allocator, outArray, outSchema, NativeAllocator.DICTIONARIES);
      return decodedRecord(records, output);
    }
  }

  private void ensureDecodeSlab(long required) {
    if (decodeSlab != null && decodeSlab.capacity() >= required) {
      return;
    }
    if (decodeSlab != null) {
      decodeSlab.close();
    }
    decodeSlab = allocator.buffer(Math.max(1L, required));
  }

  private NativeSourceRecord decodedRecord(
      List<ConsumerRecord<byte[], byte[]>> records, VectorSchemaRoot output) {
    long nextOffset = records.get(records.size() - 1).offset() + 1;
    if (output.getRowCount() == 0) {
      output.close();
      return new NativeSourceRecord(null, nextOffset, Long.MIN_VALUE);
    }
    return NativeSourceRecord.fromRoot(output, nextOffset, rowtimeIndex, watermarkExpression);
  }

  private static void set(VarBinaryVector vector, int index, byte[] value) {
    if (value == null) {
      vector.setNull(index);
    } else {
      vector.setSafe(index, value);
    }
  }

  @Override
  public void close() throws Exception {
    try {
      org.apache.flink.util.IOUtils.closeAll(
          Arrays.asList(decoder, decodeSlab, watermarkExpression), Throwable.class);
    } finally {
      decodeSlab = null;
    }
  }
}
