package tech.streamfusion.operator;

import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.NestedRepresentation;

/** The serialized keyBy boundary that gives retained ROW payloads one physical identity. */
final class SerializedJoinInput implements AutoCloseable {
  private final VectorSchemaRoot root;
  private final boolean owned;
  private final TaskOffHeapMemory.Reservation workspace;

  private SerializedJoinInput(
      VectorSchemaRoot root, boolean owned, TaskOffHeapMemory.Reservation workspace) {
    this.root = root;
    this.owned = owned;
    this.workspace = workspace;
  }

  static SerializedJoinInput prepare(VectorSchemaRoot input, RowType type) {
    if (input.getRowCount() == 0
        || type.getChildren().stream().noneMatch(SerializedJoinInput::containsRow))
      return new SerializedJoinInput(input, false, null);
    long bytes =
        Math.addExact(
            65536L,
            Math.addExact(
                Math.multiplyExact(
                    input.getFieldVectors().stream().mapToLong(v -> v.getBufferSize()).sum(), 4L),
                Math.multiplyExact(input.getRowCount(), 512L)));
    var workspace = TaskOffHeapMemory.reserve("state", "join serialized ROW workspace", bytes);
    VectorSchemaRoot data = null;
    TinyIntVector kinds = null;
    try {
      var allocator = input.getFieldVectors().get(0).getAllocator();
      data =
          VectorSchemaRoot.create(
              NestedRepresentation.withBinaryRows(ArrowConversion.toArrowSchema(type)), allocator);
      var writer = ArrowConversion.createRowDataArrowWriter(data, type);
      var reader = ArrowConversion.createArrowReader(input, type);
      var serializer = new RowDataSerializer(type);
      var originalKinds = input.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
      if (originalKinds != null) {
        kinds = new TinyIntVector(RowDataArrowConverter.ROW_KIND_COLUMN, allocator);
        kinds.allocateNew(input.getRowCount());
      }
      for (int row = 0; row < input.getRowCount(); row++) {
        // The writer copies this row before the serializer reuses its backing storage.
        writer.write(serializer.toBinaryRow(reader.read(row)));
        if (kinds != null) kinds.set(row, ((TinyIntVector) originalKinds).get(row));
      }
      writer.finish();
      VectorSchemaRoot result = data;
      if (kinds != null) {
        kinds.setValueCount(input.getRowCount());
        result =
            RowDataArrowConverter.withExtraColumns(
                data, java.util.List.of(kinds), input.getRowCount());
      }
      return new SerializedJoinInput(result, true, workspace);
    } catch (RuntimeException | Error failure) {
      try {
        org.apache.flink.util.IOUtils.closeAll(data, kinds);
      } catch (Exception | Error cleanup) {
        failure.addSuppressed(cleanup);
      } finally {
        workspace.close();
      }
      throw failure;
    }
  }

  private static boolean containsRow(LogicalType type) {
    return type.is(LogicalTypeRoot.ROW)
        || type.getChildren().stream().anyMatch(SerializedJoinInput::containsRow);
  }

  VectorSchemaRoot root() {
    return root;
  }

  @Override
  public void close() {
    try {
      if (owned) root.close();
    } finally {
      if (workspace != null) workspace.close();
    }
  }
}
