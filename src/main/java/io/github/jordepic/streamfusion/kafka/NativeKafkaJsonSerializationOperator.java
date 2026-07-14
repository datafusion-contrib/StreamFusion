package io.github.jordepic.streamfusion.kafka;

import io.github.jordepic.streamfusion.operator.ArrowBatch;
import io.github.jordepic.streamfusion.operator.NativeAllocator;
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
public final class NativeKafkaJsonSerializationOperator
    extends AbstractStreamOperator<PreSerializedKafkaRecord>
    implements OneInputStreamOperator<ArrowBatch, PreSerializedKafkaRecord> {

  private final boolean ignoreNullFields;
  private final String timestampFormat;
  private final String[] logicalTypes;
  private final String[] fieldNames;
  private final int[] keyFields;
  private final int[] valueFields;
  private final boolean upsert;
  private transient Counter serializationBatches;
  private transient Counter serializationRows;
  private transient Counter serializedBytes;
  private transient Counter serializationNanos;

  public NativeKafkaJsonSerializationOperator(
      boolean ignoreNullFields,
      String timestampFormat,
      String[] logicalTypes,
      String[] fieldNames,
      int[] keyFields,
      int[] valueFields,
      boolean upsert) {
    this.ignoreNullFields = ignoreNullFields;
    this.timestampFormat = timestampFormat;
    this.logicalTypes = logicalTypes;
    this.fieldNames = fieldNames;
    this.keyFields = keyFields;
    this.valueFields = valueFields;
    this.upsert = upsert;
  }

  @Override
  public void open() throws Exception {
    super.open();
    serializationBatches = getMetricGroup().counter("nativeKafkaSerializationBatches");
    serializationRows = getMetricGroup().counter("nativeKafkaSerializationRows");
    serializedBytes = getMetricGroup().counter("nativeKafkaSerializedBytes");
    serializationNanos = getMetricGroup().counter("nativeKafkaSerializationNanos");
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
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
        byte[][][] records =
            NativeKafka.encodeKafkaJsonRecords(
                array.memoryAddress(),
                schema.memoryAddress(),
                ignoreNullFields,
                timestampFormat,
                logicalTypes,
                fieldNames,
                keyFields,
                valueFields,
                upsert);
        byte[][] keys = records[0];
        byte[][] values = records[1];
        long bytes = 0;
        for (int index = 0; index < values.length; index++) {
          byte[] key = keys[index];
          byte[] value = values[index];
          if (key != null) {
            bytes += key.length;
          }
          if (value != null) {
            bytes += value.length;
          }
          output.collect(new StreamRecord<>(new PreSerializedKafkaRecord(key, value)));
        }
        serializationBatches.inc();
        serializationRows.inc(values.length);
        serializedBytes.inc(bytes);
        serializationNanos.inc(System.nanoTime() - started);
      }
    }
  }
}
