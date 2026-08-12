package tech.streamfusion.kafka;

import tech.streamfusion.format.EncodeFormat;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeAllocator;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** Encodes each input Arrow batch once and emits the final Kafka value bytes for its rows. */
public final class NativeKafkaSerializationOperator
    extends AbstractStreamOperator<PreSerializedKafkaRecord>
    implements OneInputStreamOperator<ArrowBatch, PreSerializedKafkaRecord> {

  private final EncodeFormat valueFormat;
  private final EncodeFormat keyFormat;
  private final String[] logicalTypes;
  private final String[] fieldNames;
  private final int[] keyFields;
  private final int[] valueFields;
  private final boolean upsert;
  private transient String valueFormatOptions;
  private transient String keyFormatOptions;
  private transient Counter serializationBatches;
  private transient Counter serializationRows;
  private transient Counter serializedBytes;
  private transient Counter serializationNanos;
  private transient long encoderHandle;

  public NativeKafkaSerializationOperator(
      EncodeFormat valueFormat,
      EncodeFormat keyFormat,
      String[] logicalTypes,
      String[] fieldNames,
      int[] keyFields,
      int[] valueFields,
      boolean upsert) {
    this.valueFormat = valueFormat;
    this.keyFormat = keyFormat;
    this.logicalTypes = logicalTypes;
    this.fieldNames = fieldNames;
    this.keyFields = keyFields;
    this.valueFields = valueFields;
    this.upsert = upsert;
  }

  @Override
  public void open() throws Exception {
    super.open();
    NativeAllocator.initializeFor(this);
    valueFormatOptions = valueFormat.openOptions();
    keyFormatOptions = keyFormat == valueFormat ? valueFormatOptions : keyFormat.openOptions();
    encoderHandle =
        NativeKafka.createKafkaEncoder(
            valueFormat.format,
            valueFormatOptions,
            keyFormat.format,
            keyFormatOptions,
            logicalTypes,
            fieldNames,
            keyFields,
            valueFields,
            upsert);
    serializationBatches = getMetricGroup().counter("nativeKafkaSerializationBatches");
    serializationRows = getMetricGroup().counter("nativeKafkaSerializationRows");
    serializedBytes = getMetricGroup().counter("nativeKafkaSerializedBytes");
    serializationNanos = getMetricGroup().counter("nativeKafkaSerializationNanos");
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    int inputRows = element.getValue().rowCount();
    if (inputRows > 1) {
      getMetricGroup().getIOMetricGroup().getNumRecordsInCounter().inc(inputRows - 1L);
    }
    long started = System.nanoTime();
    try (VectorSchemaRoot root = element.getValue().root()) {
      BufferAllocator allocator =
          root.getFieldVectors().isEmpty()
              ? NativeAllocator.SHARED
              : root.getFieldVectors().get(0).getAllocator();
      try (ArrowArray array = ArrowArray.allocateNew(allocator);
          ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(
            allocator, root, NativeAllocator.DICTIONARIES, array, schema);
        NativeKafkaEncodedBatch records =
            NativeKafka.encodeKafkaRecordBatchWithHandle(
                encoderHandle, array.memoryAddress(), schema.memoryAddress());
        long bytes = 0;
        for (int index = 0; index < records.size(); index++) {
          output.collect(new StreamRecord<>(records.record(index)));
          bytes += records.serializedBytes(index);
        }
        serializationBatches.inc();
        serializationRows.inc(records.size());
        serializedBytes.inc(bytes);
        serializationNanos.inc(System.nanoTime() - started);
      }
    }
  }

  @Override
  public void close() throws Exception {
    try {
      if (encoderHandle != 0) {
        NativeKafka.closeKafkaEncoder(encoderHandle);
        encoderHandle = 0;
      }
    } finally {
      super.close();
    }
  }
}
