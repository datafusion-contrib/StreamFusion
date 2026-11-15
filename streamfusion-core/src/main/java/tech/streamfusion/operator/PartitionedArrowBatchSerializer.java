package tech.streamfusion.operator;

import java.io.IOException;
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
    ArrowIpcFrame.write(batch.root(), target);
  }

  @Override
  public PartitionedArrowBatch deserialize(DataInputView source) throws IOException {
    String bucketId = source.readUTF();
    return new PartitionedArrowBatch(ArrowIpcFrame.read(source, NativeAllocator.SHARED), bucketId);
  }

  @Override
  public PartitionedArrowBatch deserialize(PartitionedArrowBatch reuse, DataInputView source)
      throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    target.writeUTF(source.readUTF());
    ArrowIpcFrame.copy(source, target);
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
