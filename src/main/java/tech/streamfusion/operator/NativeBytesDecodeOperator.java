package tech.streamfusion.operator;

import tech.streamfusion.format.NativeMessageDecoder;
import tech.streamfusion.format.NativeMessageDecoderFactory;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

/**
 * The shallow ingest path's format-neutral decode core. It turns raw message bodies into typed Arrow
 * batches while a format extension supplies the native decoder through the provider SPI. This class
 * owns batching, checkpoint flushing, and the Arrow C Data Interface bridge, so connector and format
 * artifacts can be installed independently.
 *
 * <p>The operator is stateless across batches. It flushes partial batches at end of input, before a
 * checkpoint barrier, and on a processing-time timer; this preserves Flink's source-checkpoint
 * contract while bounding low-volume ingest latency.
 */
public class NativeBytesDecodeOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<byte[], ArrowBatch>, BoundedOneInput {

  private final RowType outputType;
  private final int batchSize;
  private final NativeMessageDecoderFactory decoderFactory;
  private final long flushIntervalMillis;
  private final boolean keyed;

  private transient BufferAllocator allocator;
  private transient NativeMessageDecoder decoder;
  private transient VarBinaryVector keys;
  private transient VarBinaryVector body;
  private transient int count;
  private transient boolean flushTimerPending;

  public NativeBytesDecodeOperator(
      RowType outputType,
      int batchSize,
      NativeMessageDecoderFactory decoderFactory,
      long flushIntervalMillis) {
    this(outputType, batchSize, decoderFactory, flushIntervalMillis, false);
  }

  /** {@code keyed}: each element is a {@link #frame} of the record's key and value bytes, and the
   * decode receives a two-column {@code [key, body]} batch — the keyed composition (which physical
   * column the key fills, and how) rides the decoder's own option lines. */
  public NativeBytesDecodeOperator(
      RowType outputType,
      int batchSize,
      NativeMessageDecoderFactory decoderFactory,
      long flushIntervalMillis,
      boolean keyed) {
    this.outputType = outputType;
    this.batchSize = batchSize;
    this.decoderFactory = decoderFactory;
    this.flushIntervalMillis = flushIntervalMillis;
    this.keyed = keyed;
  }

  /**
   * One record's key and value bytes as a single {@code byte[]} element, so the keyed edge reuses
   * the plain nullable-bytes serializer: a flags byte (bit 0 = null key, bit 1 = null value), a
   * 4-byte big-endian key length plus the key when present, then the value bytes.
   */
  public static byte[] frame(byte[] key, byte[] value) {
    int flags = (key == null ? 1 : 0) | (value == null ? 2 : 0);
    int keyLength = key == null ? 0 : key.length + 4;
    byte[] frame = new byte[1 + keyLength + (value == null ? 0 : value.length)];
    frame[0] = (byte) flags;
    int at = 1;
    if (key != null) {
      frame[at++] = (byte) (key.length >>> 24);
      frame[at++] = (byte) (key.length >>> 16);
      frame[at++] = (byte) (key.length >>> 8);
      frame[at++] = (byte) key.length;
      System.arraycopy(key, 0, frame, at, key.length);
      at += key.length;
    }
    if (value != null) {
      System.arraycopy(value, 0, frame, at, value.length);
    }
    return frame;
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
    allocator = NativeAllocator.SHARED;
    decoder = decoderFactory.create();
    decoder.open(allocator, outputType);
    newBody();
  }

  private void newBody() {
    body = new VarBinaryVector("body", allocator);
    body.allocateNew(batchSize);
    if (keyed) {
      keys = new VarBinaryVector("key", allocator);
      keys.allocateNew(batchSize);
    }
    count = 0;
  }

  @Override
  public void processElement(StreamRecord<byte[]> element) {
    if (keyed) {
      byte[] frame = element.getValue();
      int flags = frame[0];
      int at = 1;
      if ((flags & 1) != 0) {
        keys.setNull(count);
      } else {
        int keyLength =
            ((frame[1] & 0xFF) << 24)
                | ((frame[2] & 0xFF) << 16)
                | ((frame[3] & 0xFF) << 8)
                | (frame[4] & 0xFF);
        keys.setSafe(count, frame, 5, keyLength);
        at = 5 + keyLength;
      }
      if ((flags & 2) != 0) {
        body.setNull(count++);
      } else {
        body.setSafe(count++, frame, at, frame.length - at);
      }
    } else if (element.getValue() == null) {
      // A null Kafka value (a tombstone) becomes a null body slot; each format decoder owns its
      // semantics (skip, null field, or failure — whatever Flink's deserializer does with null).
      body.setNull(count++);
    } else {
      body.setSafe(count++, element.getValue());
    }
    if (count >= batchSize) {
      flush();
    } else if (count == 1 && flushIntervalMillis > 0 && !flushTimerPending) {
      flushTimerPending = true;
      ProcessingTimeService timeService = getProcessingTimeService();
      timeService.registerTimer(
          timeService.getCurrentProcessingTime() + flushIntervalMillis,
          timestamp -> {
            flushTimerPending = false;
            if (count > 0) {
              flush();
            }
          });
    }
  }

  @Override
  public void endInput() {
    if (count > 0) {
      flush();
    }
  }

  /** Flushes before a barrier because the source checkpoint already considers buffered bytes delivered. */
  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) {
    if (count > 0) {
      flush();
    }
  }

  private void flush() {
    try {
      decoder.beforeDecode(body, count);
      body.setValueCount(count);
      if (keyed) {
        keys.setValueCount(count);
      }
      try (VectorSchemaRoot in =
              new VectorSchemaRoot(keyed ? List.of(keys, body) : List.of(body));
          ArrowArray inArray = ArrowArray.allocateNew(allocator);
          ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
          ArrowArray outArray = ArrowArray.allocateNew(allocator);
          ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
        in.setRowCount(count);
        Data.exportVectorSchemaRoot(allocator, in, NativeAllocator.DICTIONARIES, inArray, inSchema);
        decoder.decodeInto(
            inArray.memoryAddress(),
            inSchema.memoryAddress(),
            outArray.memoryAddress(),
            outSchema.memoryAddress());
        VectorSchemaRoot out =
            Data.importVectorSchemaRoot(allocator, outArray, outSchema, NativeAllocator.DICTIONARIES);
        if (out.getRowCount() > 0) {
          ColumnarRecordMetrics.emit(output, getMetricGroup(), new ArrowBatch(out));
        } else {
          out.close();
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("native format decode failed", e);
    } finally {
      body.close();
      if (keys != null) {
        keys.close();
      }
      newBody();
    }
  }

  @Override
  public void close() throws Exception {
    if (decoder != null) {
      decoder.close();
      decoder = null;
    }
    if (body != null) {
      body.close();
    }
    if (keys != null) {
      keys.close();
    }
    super.close();
  }

}
