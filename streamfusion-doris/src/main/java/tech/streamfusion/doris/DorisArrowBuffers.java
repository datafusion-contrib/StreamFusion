package tech.streamfusion.doris;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ForeignAllocation;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import tech.streamfusion.arrow.TimestampAccessor;

/** Bridges Doris's shaded Arrow allocator without serializing or copying the vector buffers. */
final class DorisArrowBuffers {
  private DorisArrowBuffers() {}

  static VectorSchemaRoot borrow(
      org.apache.doris.shaded.org.apache.arrow.vector.VectorSchemaRoot source,
      Schema expected,
      BufferAllocator allocator,
      Runnable retain,
      Runnable release)
      throws IOException {
    return borrow(source, expected, allocator, retain, release, false);
  }

  static VectorSchemaRoot borrow(
      org.apache.doris.shaded.org.apache.arrow.vector.VectorSchemaRoot source,
      Schema expected,
      BufferAllocator allocator,
      Runnable retain,
      Runnable release,
      boolean incremental)
      throws IOException {
    Schema actual = Schema.fromJSON(source.getSchema().toJson());
    if (!expected.getFields().isEmpty()
        && actual.getFields().size() != expected.getFields().size() + (incremental ? 3 : 0))
      throw new IOException("Doris returned a different Arrow field count");
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < expected.getFields().size(); i++)
      fields.add(layout(actual.getFields().get(i), expected.getFields().get(i)));
    VectorSchemaRoot result = VectorSchemaRoot.create(new Schema(fields), allocator);
    try {
      for (int i = 0; i < expected.getFields().size(); i++) {
        borrowVector(source.getVector(i), result.getVector(i), allocator, retain, release);
      }
      result.setRowCount(source.getRowCount());
      return result;
    } catch (Throwable failure) {
      result.close();
      throw failure;
    }
  }

  static Field layout(Field actual, Field expected) throws IOException {
    if (actual.getDictionary() != null)
      throw new IOException("Doris dictionary vectors are not supported");
    if (actual.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Timestamp
        && TimestampAccessor.isComponentTimestamp(expected))
      return new Field(expected.getName(), actual.getFieldType(), actual.getChildren());
    var expectedChildren = logicalFields(expected.getChildren());
    if (!actual.getType().equals(expected.getType())
        || actual.getChildren().size() != expectedChildren.size())
      throw new IOException(
          "Doris returned an incompatible Arrow layout: " + actual + "; expected: " + expected);
    List<Field> children = new ArrayList<>();
    for (int i = 0; i < actual.getChildren().size(); i++)
      children.add(layout(actual.getChildren().get(i), expectedChildren.get(i)));
    return new Field(expected.getName(), actual.getFieldType(), children);
  }

  static List<Field> logicalFields(List<Field> fields) {
    return fields.stream()
        .filter(
            field ->
                !"1".equals(field.getMetadata().get("streamfusion.nested-row-kind"))
                    && !tech.streamfusion.arrow.NestedRepresentation.isBinaryRowBytes(field))
        .collect(java.util.stream.Collectors.toList());
  }

  private static void borrowVector(
      org.apache.doris.shaded.org.apache.arrow.vector.FieldVector from,
      FieldVector to,
      BufferAllocator allocator,
      Runnable retain,
      Runnable release) {
    List<ArrowBuf> buffers = new ArrayList<>();
    try {
      for (var buffer : from.getFieldBuffers()) {
        if (buffer.capacity() == 0) {
          buffers.add(allocator.getEmpty());
          continue;
        }
        retain.run();
        buffer.getReferenceManager().retain();
        var released = new AtomicBoolean();
        Runnable free =
            () -> {
              if (released.compareAndSet(false, true)) {
                try {
                  buffer.getReferenceManager().release();
                } finally {
                  release.run();
                }
              }
            };
        ForeignAllocation allocation =
            new ForeignAllocation(buffer.capacity(), buffer.memoryAddress()) {
              @Override
              protected void release0() {
                free.run();
              }
            };
        ArrowBuf borrowed;
        try {
          borrowed = allocator.wrapForeignAllocation(allocation);
        } catch (Throwable failure) {
          free.run();
          throw failure;
        }
        borrowed.writerIndex(buffer.writerIndex());
        buffers.add(borrowed);
      }
      to.loadFieldBuffers(new ArrowFieldNode(from.getValueCount(), from.getNullCount()), buffers);
    } finally {
      buffers.forEach(ArrowBuf::close);
    }
    for (int i = 0; i < from.getChildrenFromFields().size(); i++)
      borrowVector(
          from.getChildrenFromFields().get(i),
          to.getChildrenFromFields().get(i),
          allocator,
          retain,
          release);
  }
}
