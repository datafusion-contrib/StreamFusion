package tech.streamfusion.operator;

import java.io.IOException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.compat.FixedSerializerTypeInformation;

/** Consumer-local borrowing, only for the synchronous row-to-Arrow writer. */
public final class SynchronousArrowInput {
  private SynchronousArrowInput() {}

  @SuppressWarnings("unchecked")
  public static Transformation<RowData> forTranspose(
      Transformation<RowData> input, RowType sourceType) {
    if (!(input instanceof PhysicalTransformation<?>)
        || !(input.getOutputType() instanceof InternalTypeInfo<?> info)
        || !info.toLogicalType().equals(sourceType)) {
      return input;
    }
    var edge = new PartitionTransformation<>(input, new ForwardPartitioner<RowData>());
    edge.setOutputType(new InputType((InternalTypeInfo<RowData>) info));
    return edge;
  }

  static final class InputType extends FixedSerializerTypeInformation<RowData> {
    private final InternalTypeInfo<RowData> original;

    InputType(InternalTypeInfo<RowData> original) {
      this.original = original;
    }

    @Override
    public boolean isBasicType() {
      return original.isBasicType();
    }

    @Override
    public boolean isTupleType() {
      return original.isTupleType();
    }

    @Override
    public int getArity() {
      return original.getArity();
    }

    @Override
    public int getTotalFields() {
      return original.getTotalFields();
    }

    @Override
    public Class<RowData> getTypeClass() {
      return RowData.class;
    }

    @Override
    public boolean isKeyType() {
      return original.isKeyType();
    }

    @Override
    public TypeSerializer<RowData> createSerializer() {
      return new InputSerializer(original.toRowSerializer());
    }

    @Override
    public String toString() {
      return original.toString();
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof InputType type && original.equals(type.original);
    }

    @Override
    public int hashCode() {
      return original.hashCode();
    }

    @Override
    public boolean canEqual(Object other) {
      return other instanceof InputType;
    }
  }

  private static final class InputSerializer extends TypeSerializer<RowData> {
    private final TypeSerializer<RowData> original;

    InputSerializer(TypeSerializer<RowData> original) {
      this.original = original;
    }

    @Override
    public boolean isImmutableType() {
      return false;
    }

    @Override
    public TypeSerializer<RowData> duplicate() {
      return new InputSerializer(original.duplicate());
    }

    @Override
    public RowData createInstance() {
      return original.createInstance();
    }

    @Override
    public RowData copy(RowData row) {
      return row;
    }

    @Override
    public RowData copy(RowData row, RowData reuse) {
      return row;
    }

    @Override
    public int getLength() {
      return original.getLength();
    }

    @Override
    public void serialize(RowData row, DataOutputView target) throws IOException {
      original.serialize(row, target);
    }

    @Override
    public RowData deserialize(DataInputView source) throws IOException {
      return original.deserialize(source);
    }

    @Override
    public RowData deserialize(RowData reuse, DataInputView source) throws IOException {
      return original.deserialize(reuse, source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
      original.copy(source, target);
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof InputSerializer serializer && original.equals(serializer.original);
    }

    @Override
    public int hashCode() {
      return original.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<RowData> snapshotConfiguration() {
      // Restoring the original serializer conservatively restores copying, with identical wire
      // data.
      return original.snapshotConfiguration();
    }
  }
}
