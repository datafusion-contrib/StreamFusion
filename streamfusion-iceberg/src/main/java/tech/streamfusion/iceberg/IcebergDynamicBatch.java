package tech.streamfusion.iceberg;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import tech.streamfusion.compat.FixedSerializerTypeInformation;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchSerializer;

/** One target and host routing key, with the payload still owned by an Arrow batch. */
final class IcebergDynamicBatch {
  final Target target;
  final ArrowBatch batch;

  IcebergDynamicBatch(Target target, ArrowBatch batch) {
    this.target = target;
    this.batch = batch;
  }

  record Target(
      String table,
      String branch,
      String schemaJson,
      String specJson,
      int writerKey,
      boolean upsert,
      Set<Integer> equalityFields)
      implements Serializable {
    Target {
      equalityFields = equalityFields == null ? Set.of() : Set.copyOf(equalityFields);
    }

    Schema schema() {
      return SchemaParser.fromJson(schemaJson);
    }

    PartitionSpec spec(Schema schema) {
      return PartitionSpecParser.fromJson(schema, specJson);
    }
  }

  static final TypeInfo TYPE = new TypeInfo();

  public static final class TypeInfo extends FixedSerializerTypeInformation<IcebergDynamicBatch> {
    @Override
    public boolean isBasicType() {
      return false;
    }

    @Override
    public boolean isTupleType() {
      return false;
    }

    @Override
    public int getArity() {
      return 1;
    }

    @Override
    public int getTotalFields() {
      return 1;
    }

    @Override
    public Class<IcebergDynamicBatch> getTypeClass() {
      return IcebergDynamicBatch.class;
    }

    @Override
    public boolean isKeyType() {
      return false;
    }

    @Override
    public TypeSerializer<IcebergDynamicBatch> createSerializer() {
      return new Serializer();
    }

    @Override
    public String toString() {
      return "IcebergDynamicArrowBatch";
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof TypeInfo;
    }

    @Override
    public int hashCode() {
      return TypeInfo.class.hashCode();
    }

    @Override
    public boolean canEqual(Object other) {
      return other instanceof TypeInfo;
    }
  }

  public static final class Serializer extends TypeSerializer<IcebergDynamicBatch> {
    private final ArrowBatchSerializer payload = new ArrowBatchSerializer();

    @Override
    public boolean isImmutableType() {
      return false;
    }

    @Override
    public TypeSerializer<IcebergDynamicBatch> duplicate() {
      return new Serializer();
    }

    @Override
    public IcebergDynamicBatch createInstance() {
      return null;
    }

    @Override
    public IcebergDynamicBatch copy(IcebergDynamicBatch value) {
      return value;
    }

    @Override
    public IcebergDynamicBatch copy(IcebergDynamicBatch value, IcebergDynamicBatch reuse) {
      return value;
    }

    @Override
    public int getLength() {
      return -1;
    }

    @Override
    public void serialize(IcebergDynamicBatch value, DataOutputView out) throws IOException {
      Target target = value.target;
      for (String text :
          new String[] {target.table(), target.branch(), target.schemaJson(), target.specJson()})
        StringSerializer.INSTANCE.serialize(text, out);
      out.writeInt(target.writerKey());
      out.writeBoolean(target.upsert());
      out.writeInt(target.equalityFields().size());
      for (int id : target.equalityFields()) out.writeInt(id);
      payload.serialize(value.batch, out);
    }

    @Override
    public IcebergDynamicBatch deserialize(DataInputView in) throws IOException {
      String table = StringSerializer.INSTANCE.deserialize(in);
      String branch = StringSerializer.INSTANCE.deserialize(in);
      String schema = StringSerializer.INSTANCE.deserialize(in);
      String spec = StringSerializer.INSTANCE.deserialize(in);
      int key = in.readInt();
      boolean upsert = in.readBoolean();
      int size = in.readInt();
      if (size < 0 || size > 10000) throw new IOException("Invalid equality-field count");
      var ids = new java.util.HashSet<Integer>();
      for (int i = 0; i < size; i++) ids.add(in.readInt());
      return new IcebergDynamicBatch(
          new Target(table, branch, schema, spec, key, upsert, ids), payload.deserialize(in));
    }

    @Override
    public IcebergDynamicBatch deserialize(IcebergDynamicBatch reuse, DataInputView in)
        throws IOException {
      return deserialize(in);
    }

    @Override
    public void copy(DataInputView in, DataOutputView out) throws IOException {
      for (int i = 0; i < 4; i++) StringSerializer.INSTANCE.copy(in, out);
      out.writeInt(in.readInt());
      out.writeBoolean(in.readBoolean());
      int count = in.readInt();
      if (count < 0 || count > 10000) throw new IOException("Invalid equality-field count");
      out.writeInt(count);
      for (int i = 0; i < count; i++) out.writeInt(in.readInt());
      payload.copy(in, out);
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Serializer;
    }

    @Override
    public int hashCode() {
      return Serializer.class.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<IcebergDynamicBatch> snapshotConfiguration() {
      return new Snapshot();
    }
  }

  public static final class Snapshot extends SimpleTypeSerializerSnapshot<IcebergDynamicBatch> {
    public Snapshot() {
      super(Serializer::new);
    }
  }
}
