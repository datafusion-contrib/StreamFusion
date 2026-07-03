package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import io.github.jordepic.streamfusion.kafka.ConfluentSchemaRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.avro.Schema;
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
 * Confluent, synthetic id 0 for bare) — or, for Confluent with a {@code registry}, against writer
 * schemas fetched by frame id at runtime and resolved to {@code readerAvroSchema}; protobuf against
 * {@code protoDescriptor}/{@code protoMessageName}. Stateless across batches; flushes the partial batch
 * at end of input.
 */
public class NativeBytesDecodeOperator extends AbstractStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<byte[], ArrowBatch>, BoundedOneInput {

  /** Operator-level format sentinel for protobuf (decoder built via {@code createProtobufDecoder}). */
  public static final int PROTOBUF = 5;

  private final RowType outputType;
  private final int batchSize;
  private final int format;
  private final String avroSchema;
  private final String readerAvroSchema;
  private final int schemaId;
  private final byte[] protoDescriptor;
  private final String protoMessageName;
  // Non-null only for Confluent Avro (format 1) on the registry-driven path: writer schemas are
  // fetched by the id each message is framed with and registered into the native decoder as they
  // first appear, following the topic's schema evolution like Flink's own deserializer.
  private final ConfluentSchemaRegistry registry;
  // Flink's ignore-parse-errors: an undecodable message contributes no rows instead of failing.
  private final boolean skipParseErrors;

  private transient BufferAllocator allocator;
  private transient long handle;
  private transient VarBinaryVector body;
  private transient int count;
  private transient Set<Integer> registeredSchemaIds;
  private transient Schema readerSchema;

  public NativeBytesDecodeOperator(
      RowType outputType,
      int batchSize,
      int format,
      String avroSchema,
      String readerAvroSchema,
      int schemaId,
      byte[] protoDescriptor,
      String protoMessageName) {
    this(
        outputType,
        batchSize,
        format,
        avroSchema,
        readerAvroSchema,
        schemaId,
        protoDescriptor,
        protoMessageName,
        null,
        false);
  }

  public NativeBytesDecodeOperator(
      RowType outputType,
      int batchSize,
      int format,
      String avroSchema,
      String readerAvroSchema,
      int schemaId,
      byte[] protoDescriptor,
      String protoMessageName,
      ConfluentSchemaRegistry registry,
      boolean skipParseErrors) {
    this.outputType = outputType;
    this.batchSize = batchSize;
    this.format = format;
    this.avroSchema = avroSchema;
    this.readerAvroSchema = readerAvroSchema;
    this.schemaId = schemaId;
    this.protoDescriptor = protoDescriptor;
    this.protoMessageName = protoMessageName;
    this.registry = registry;
    this.skipParseErrors = skipParseErrors;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = NativeAllocator.SHARED;
    handle = createDecoder();
    if (registry != null) {
      registeredSchemaIds = new HashSet<>();
      readerSchema = new Schema.Parser().parse(readerAvroSchema);
    }
    newBody();
  }

  /** Builds the native decoder for this format. Avro/protobuf derive their own schema, so they need no
   * exported target schema; JSON/CSV/raw decode against {@code outputType}. */
  private long createDecoder() {
    if (format == PROTOBUF) {
      // Export the (possibly projection-narrowed) output schema so the native side prunes the
      // descriptor to the read fields; pruning to the full schema is a no-op.
      try (VectorSchemaRoot template = RowDataArrowConverter.write(List.of(), outputType, allocator);
          ArrowArray array = ArrowArray.allocateNew(allocator);
          ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, template, NativeAllocator.DICTIONARIES, array, schema);
        return Native.createProtobufDecoder(
            protoDescriptor, protoMessageName, array.memoryAddress(), schema.memoryAddress());
      }
    }
    if (format == 1 || format == 4) {
      return Native.createDecoder(format, 0L, 0L, avroSchema, readerAvroSchema, schemaId, false);
    }
    try (VectorSchemaRoot template = RowDataArrowConverter.write(List.of(), outputType, allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, template, NativeAllocator.DICTIONARIES, array, schema);
      return Native.createDecoder(
          format, array.memoryAddress(), schema.memoryAddress(), "", "", 0, skipParseErrors);
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

  /**
   * Registers any writer schema this batch is the first to carry: each Confluent-framed message names
   * its writer schema by id ({@code 0x00} + 4-byte BE id), fetched from the registry on first sight —
   * the same lazy per-id lookup Flink's deserializer makes — and patched with the reader's record
   * names as aliases (see {@link ConfluentSchemaRegistry#aliasedToReader}) before the native store
   * learns it. A malformed frame is left for the native decode to reject, like Flink's magic-byte
   * check.
   */
  private void registerNewWriterSchemas() {
    for (int i = 0; i < count; i++) {
      byte[] message = body.get(i);
      if (message == null || message.length < 5 || message[0] != 0) {
        continue;
      }
      int id =
          ((message[1] & 0xff) << 24)
              | ((message[2] & 0xff) << 16)
              | ((message[3] & 0xff) << 8)
              | (message[4] & 0xff);
      if (registeredSchemaIds.add(id)) {
        Schema writer;
        try {
          writer = registry.fetchWriterSchema(id);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        Native.registerAvroSchema(
            handle, id, ConfluentSchemaRegistry.aliasedToReader(writer, readerSchema).toString());
      }
    }
  }

  private void flush() {
    if (registry != null) {
      registerNewWriterSchemas();
    }
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
