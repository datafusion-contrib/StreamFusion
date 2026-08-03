package tech.streamfusion.operator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
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
 * Type serializer for {@link PartitionedArrowBatch}: the {@link ArrowBatchSerializer} contract —
 * identity {@link #copy} within a chained task, Arrow IPC across a network edge — with the bucket
 * id carried alongside the framed batch bytes.
 */
public final class PartitionedArrowBatchSerializer extends TypeSerializer<PartitionedArrowBatch> {

  private BufferAllocator allocator() {
    return NativeAllocator.SHARED;
  }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public TypeSerializer<PartitionedArrowBatch> duplicate() {
    return new PartitionedArrowBatchSerializer();
  }

  @Override
  public PartitionedArrowBatch createInstance() {
    return null;
  }

  // Identity: a batch is produced fresh and handed off, so the consumer can take it as-is.
  @Override
  public PartitionedArrowBatch copy(PartitionedArrowBatch from) {
    return from;
  }

  @Override
  public PartitionedArrowBatch copy(PartitionedArrowBatch from, PartitionedArrowBatch reuse) {
    return from;
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(PartitionedArrowBatch batch, DataOutputView target) throws IOException {
    target.writeUTF(batch.bucketId());
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ArrowStreamWriter writer = new ArrowStreamWriter(batch.root(), null, bytes)) {
      writer.start();
      writer.writeBatch();
      writer.end();
    } finally {
      // Serializing ships the batch onto the network edge — its terminal use on the write side.
      batch.root().close();
    }
    byte[] encoded = bytes.toByteArray();
    target.writeInt(encoded.length);
    target.write(encoded);
  }

  @Override
  public PartitionedArrowBatch deserialize(DataInputView source) throws IOException {
    String bucketId = source.readUTF();
    int length = source.readInt();
    byte[] encoded = new byte[length];
    source.readFully(encoded);
    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(encoded), allocator())) {
      reader.loadNextBatch();
      VectorSchemaRoot read = reader.getVectorSchemaRoot();
      // Transfer the buffers out of the reader so the batch outlives it.
      List<FieldVector> transferred = new ArrayList<>();
      for (FieldVector vector : read.getFieldVectors()) {
        TransferPair pair = vector.getTransferPair(allocator());
        pair.transfer();
        transferred.add((FieldVector) pair.getTo());
      }
      VectorSchemaRoot root = new VectorSchemaRoot(transferred);
      root.setRowCount(read.getRowCount());
      return new PartitionedArrowBatch(root, bucketId);
    }
  }

  @Override
  public PartitionedArrowBatch deserialize(PartitionedArrowBatch reuse, DataInputView source)
      throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    target.writeUTF(source.readUTF());
    int length = source.readInt();
    byte[] encoded = new byte[length];
    source.readFully(encoded);
    target.writeInt(length);
    target.write(encoded);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof PartitionedArrowBatchSerializer;
  }

  @Override
  public int hashCode() {
    return PartitionedArrowBatchSerializer.class.hashCode();
  }

  @Override
  public TypeSerializerSnapshot<PartitionedArrowBatch> snapshotConfiguration() {
    return new PartitionedArrowBatchSerializerSnapshot();
  }

  /** Snapshot for the stateless {@link PartitionedArrowBatchSerializer}. */
  public static final class PartitionedArrowBatchSerializerSnapshot
      extends SimpleTypeSerializerSnapshot<PartitionedArrowBatch> {
    public PartitionedArrowBatchSerializerSnapshot() {
      super(PartitionedArrowBatchSerializer::new);
    }
  }
}
