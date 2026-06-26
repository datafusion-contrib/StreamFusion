package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.types.logical.RowType;

/**
 * The shallow ingest path's decode core, generalized over message format: turns a stream of raw message
 * bodies (one {@code byte[]} per record, as Flink's Kafka connector delivers them with a value-only
 * bytes deserializer) into typed Arrow batches, batched and decoded natively by the shared
 * {@code MessageDecoder}. Replaces Flink's per-record {@code byte[] -> tree -> RowData} materialization
 * with one native decode per batch; the bytes never become a {@code RowData}.
 *
 * <p>{@code format}: 0 = JSON, 1 = Confluent-Avro, 2 = CSV, 3 = raw, 4 = bare Avro, 6 = debezium-json,
 * 7 = ogg-json, 8 = maxwell-json, 9 = canal-json, {@link #PROTOBUF} = protobuf. JSON/CSV/raw and the CDC
 * formats decode against {@code outputType} (CDC treats it as the physical columns and appends a
 * {@code $row_kind$} byte); Avro variants against {@code avroSchema} (registered at {@code schemaId} for
 * Confluent, synthetic id 0 for bare); protobuf against {@code protoDescriptor}/{@code protoMessageName}.
 * Stateless across batches; flushes the partial batch at end of input.
 */
public class NativeBytesDecodeOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<byte[], ArrowBatch>, BoundedOneInput {

  /** Operator-level format sentinel for protobuf (decoder built via {@code createProtobufDecoder}). */
  public static final int PROTOBUF = 5;

  private final RowType outputType;
  private final int batchSize;
  private final int format;
  private final String avroSchema;
  private final int schemaId;
  private final byte[] protoDescriptor;
  private final String protoMessageName;

  private transient BufferAllocator allocator;
  private transient long handle;
  private transient VarBinaryVector body;
  private transient int count;

  public NativeBytesDecodeOperator(
      RowType outputType,
      int batchSize,
      int format,
      String avroSchema,
      int schemaId,
      byte[] protoDescriptor,
      String protoMessageName) {
    this.outputType = outputType;
    this.batchSize = batchSize;
    this.format = format;
    this.avroSchema = avroSchema;
    this.schemaId = schemaId;
    this.protoDescriptor = protoDescriptor;
    this.protoMessageName = protoMessageName;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    handle = createDecoder();
    newBody();
  }

  /** Builds the native decoder for this format. Avro/protobuf derive their own schema, so they need no
   * exported target schema; JSON/CSV/raw decode against {@code outputType}. */
  private long createDecoder() {
    if (format == PROTOBUF) {
      return Native.createProtobufDecoder(protoDescriptor, protoMessageName);
    }
    if (format == 1 || format == 4) {
      return Native.createDecoder(format, 0L, 0L, avroSchema, schemaId);
    }
    try (VectorSchemaRoot template = RowDataArrowConverter.write(List.of(), outputType, allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, template, NativeAllocator.DICTIONARIES, array, schema);
      return Native.createDecoder(format, array.memoryAddress(), schema.memoryAddress(), "", 0);
    }
  }

  private void newBody() {
    body = new VarBinaryVector("body", allocator);
    body.allocateNew(batchSize);
    count = 0;
  }

  @Override
  public void processElement(StreamRecord<byte[]> element) {
    body.setSafe(count++, element.getValue());
    if (count >= batchSize) {
      flush();
    }
  }

  @Override
  public void endInput() {
    if (count > 0) {
      flush();
    }
  }

  private void flush() {
    body.setValueCount(count);
    try (VectorSchemaRoot in = new VectorSchemaRoot(List.of(body));
        ArrowArray inArray = ArrowArray.allocateNew(allocator);
        ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray outArray = ArrowArray.allocateNew(allocator);
        ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      in.setRowCount(count);
      Data.exportVectorSchemaRoot(allocator, in, NativeAllocator.DICTIONARIES, inArray, inSchema);
      Native.decodeInto(
          handle,
          inArray.memoryAddress(),
          inSchema.memoryAddress(),
          outArray.memoryAddress(),
          outSchema.memoryAddress());
      VectorSchemaRoot out =
          Data.importVectorSchemaRoot(allocator, outArray, outSchema, NativeAllocator.DICTIONARIES);
      if (out.getRowCount() > 0) {
        output.collect(new StreamRecord<>(new ArrowBatch(out)));
      } else {
        out.close();
      }
    }
    body.close();
    newBody();
  }

  @Override
  public void close() throws Exception {
    if (handle != 0) {
      Native.closeDecoder(handle);
      handle = 0;
    }
    if (body != null) {
      body.close();
    }
    super.close();
  }
}
