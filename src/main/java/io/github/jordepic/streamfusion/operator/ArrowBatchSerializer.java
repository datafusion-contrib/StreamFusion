package io.github.jordepic.streamfusion.operator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Type serializer for {@link ArrowBatch}. Within a chained task it is never asked to serialize to
 * bytes — {@link #copy} is the only path, and it is identity because operators emit a fresh batch
 * per record and never retain or mutate it after emit. Across a network edge it serializes the batch
 * with Arrow's IPC stream format (length-framed so it composes with Flink's record stream).
 */
public final class ArrowBatchSerializer extends TypeSerializer<ArrowBatch> {

  private transient BufferAllocator allocator;

  private BufferAllocator allocator() {
    if (allocator == null) {
      allocator = new RootAllocator();
    }
    return allocator;
  }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<ArrowBatch> duplicate() {
    return new ArrowBatchSerializer();
  }

  @Override
  public ArrowBatch createInstance() {
    return null;
  }

  // Identity: a batch is produced fresh and handed off, so the consumer can take it as-is.
  @Override
  public ArrowBatch copy(ArrowBatch from) {
    return from;
  }

  @Override
  public ArrowBatch copy(ArrowBatch from, ArrowBatch reuse) {
    return from;
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(ArrowBatch batch, DataOutputView target) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ArrowStreamWriter writer = new ArrowStreamWriter(batch.root(), null, bytes)) {
      writer.start();
      writer.writeBatch();
      writer.end();
    } finally {
      // Serializing ships the batch onto the network edge — its terminal use on the write side, so
      // release the off-heap buffers here (the read side allocates a fresh batch on deserialize).
      batch.root().close();
    }
    byte[] encoded = bytes.toByteArray();
    target.writeInt(encoded.length);
    target.write(encoded);
  }

  @Override
  public ArrowBatch deserialize(DataInputView source) throws IOException {
    int length = source.readInt();
    byte[] encoded = new byte[length];
    source.readFully(encoded);
    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(encoded), allocator())) {
      reader.loadNextBatch();
      VectorSchemaRoot read = reader.getVectorSchemaRoot();
      // Transfer the buffers out of the reader so the batch outlives it (closing the reader then
      // frees nothing — the vectors are now owned by the returned root).
      List<FieldVector> transferred = new ArrayList<>();
      for (FieldVector vector : read.getFieldVectors()) {
        TransferPair pair = vector.getTransferPair(allocator());
        pair.transfer();
        transferred.add((FieldVector) pair.getTo());
      }
      VectorSchemaRoot root = new VectorSchemaRoot(transferred);
      root.setRowCount(read.getRowCount());
      return new ArrowBatch(root);
    }
  }

  @Override
  public ArrowBatch deserialize(ArrowBatch reuse, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    int length = source.readInt();
    byte[] encoded = new byte[length];
    source.readFully(encoded);
    target.writeInt(length);
    target.write(encoded);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof ArrowBatchSerializer;
  }

  @Override
  public int hashCode() {
    return ArrowBatchSerializer.class.hashCode();
  }

  @Override
  public TypeSerializerSnapshot<ArrowBatch> snapshotConfiguration() {
    return new ArrowBatchSerializerSnapshot();
  }

  /** Snapshot for the stateless {@link ArrowBatchSerializer}. */
  public static final class ArrowBatchSerializerSnapshot
      extends SimpleTypeSerializerSnapshot<ArrowBatch> {
    public ArrowBatchSerializerSnapshot() {
      super(ArrowBatchSerializer::new);
    }
  }
}
