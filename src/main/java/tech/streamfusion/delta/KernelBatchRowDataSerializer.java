package tech.streamfusion.delta;

import java.io.IOException;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.RowData;

/**
 * Prevents Flink's chained-output copy from materializing an Arrow-backed row view. The planner
 * places this type only after the partition exchange, immediately before the Delta writer; byte
 * serialization is deliberately unsupported so it cannot silently become a network type.
 */
final class KernelBatchRowDataSerializer extends TypeSerializer<RowData> {

  @Override public boolean isImmutableType() { return false; }
  @Override public TypeSerializer<RowData> duplicate() { return new KernelBatchRowDataSerializer(); }
  @Override public RowData createInstance() { return null; }
  @Override public RowData copy(RowData from) { return from; }
  @Override public RowData copy(RowData from, RowData reuse) { return from; }
  @Override public int getLength() { return -1; }

  @Override
  public void serialize(RowData record, DataOutputView target) throws IOException {
    throw new IOException("Arrow-backed Delta row views cannot cross a serialized edge");
  }

  @Override
  public RowData deserialize(DataInputView source) throws IOException {
    throw new IOException("Arrow-backed Delta row views cannot cross a serialized edge");
  }

  @Override
  public RowData deserialize(RowData reuse, DataInputView source) throws IOException {
    return deserialize(source);
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    throw new IOException("Arrow-backed Delta row views cannot cross a serialized edge");
  }

  @Override public boolean equals(Object other) {
    return other instanceof KernelBatchRowDataSerializer;
  }
  @Override public int hashCode() { return KernelBatchRowDataSerializer.class.hashCode(); }

  @Override
  public TypeSerializerSnapshot<RowData> snapshotConfiguration() {
    return new Snapshot();
  }

  public static final class Snapshot extends SimpleTypeSerializerSnapshot<RowData> {
    public Snapshot() {
      super(KernelBatchRowDataSerializer::new);
    }
  }
}
