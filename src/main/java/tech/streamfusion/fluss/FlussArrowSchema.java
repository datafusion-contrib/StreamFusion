package tech.streamfusion.fluss;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fluss.utils.ArrowUtils;
import tech.streamfusion.arrow.TimestampAccessor;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Shares compatible buffers and converts Fluss timestamp vectors to our component layout. */
public final class FlussArrowSchema {
  private static final boolean SHARE_MILLIS_INPUT =
      Boolean.parseBoolean(System.getProperty("streamfusion.fluss.millis-sharing.enabled", "true"));

  private FlussArrowSchema() {}

  public static Schema wireSchema(org.apache.fluss.types.RowType rowType) throws IOException {
    // JSON is schema metadata only; Fluss's shaded Arrow objects never enter our Arrow allocator.
    return Schema.fromJSON(ArrowUtils.toArrowSchema(rowType).toJson());
  }

  public static boolean compatible(Field source, Field target) {
    boolean sourceTimestamp =
        source.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Timestamp
            || TimestampAccessor.isComponentTimestamp(source);
    boolean targetTimestamp =
        target.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Timestamp
            || TimestampAccessor.isComponentTimestamp(target);
    if (sourceTimestamp && targetTimestamp) return true;
    if (!source.getType().equals(target.getType())
        || source.getChildren().size() != target.getChildren().size()) return false;
    for (int i = 0; i < source.getChildren().size(); i++) {
      if (!compatible(source.getChildren().get(i), target.getChildren().get(i))) return false;
    }
    return true;
  }

  public static VectorSchemaRoot convert(
      VectorSchemaRoot input, Schema target, BufferAllocator allocator) {
    List<FieldVector> vectors = new ArrayList<>();
    try {
      for (Field field : target.getFields()) {
        FieldVector source = input.getVector(field.getName());
        FieldVector vector = field.createVector(allocator);
        vectors.add(vector);
        if (source == null) {
          if (!field.isNullable())
            throw new IllegalArgumentException("Missing required Fluss field " + field.getName());
          vector.allocateNew();
          vector.setValueCount(input.getRowCount());
        } else {
          convertVector(source, vector);
        }
      }
      FieldVector kinds = input.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
      if (kinds != null
          && target.getFields().stream()
              .noneMatch(field -> field.getName().equals(RowDataArrowConverter.ROW_KIND_COLUMN))) {
        FieldVector copy = kinds.getField().createVector(allocator);
        vectors.add(copy);
        convertVector(kinds, copy);
      }
      VectorSchemaRoot root = new VectorSchemaRoot(vectors);
      root.setRowCount(input.getRowCount());
      return root;
    } catch (RuntimeException | Error failure) {
      vectors.forEach(FieldVector::close);
      throw failure;
    }
  }

  /** Sink columns are positional; query aliases need not match the destination names. */
  public static VectorSchemaRoot forAppend(
      VectorSchemaRoot input, Schema target, BufferAllocator allocator) {
    FieldVector kinds = input.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
    if (kinds != null) {
      if (!(kinds instanceof org.apache.arrow.vector.TinyIntVector changeTypes))
        throw new IllegalArgumentException("Invalid changelog sidecar type");
      for (int i = 0; i < input.getRowCount(); i++) {
        if (changeTypes.isNull(i) || changeTypes.get(i) != 0)
          throw new IllegalArgumentException("Append-only production received a changelog");
      }
    }
    int physicalCount =
        input.getFieldVectors().size()
            - (input.getVector(RowDataArrowConverter.ROW_KIND_COLUMN) == null ? 0 : 1);
    if (physicalCount != target.getFields().size()) {
      throw new IllegalArgumentException("Fluss sink field count differs from input");
    }
    List<FieldVector> vectors = new ArrayList<>();
    try {
      int sourceIndex = 0;
      for (Field field : target.getFields()) {
        while (input
            .getVector(sourceIndex)
            .getName()
            .equals(RowDataArrowConverter.ROW_KIND_COLUMN)) {
          sourceIndex++;
        }
        FieldVector vector = field.createVector(allocator);
        vectors.add(vector);
        convertVector(input.getVector(sourceIndex++), vector);
      }
      VectorSchemaRoot root = new VectorSchemaRoot(vectors);
      root.setRowCount(input.getRowCount());
      return root;
    } catch (RuntimeException | Error failure) {
      vectors.forEach(FieldVector::close);
      throw failure;
    }
  }

  private static void convertVector(FieldVector source, FieldVector target) {
    int count = source.getValueCount();
    if (source.getField().getType()
            instanceof org.apache.arrow.vector.types.pojo.ArrowType.Timestamp timestamp
        && timestamp.getUnit() == org.apache.arrow.vector.types.TimeUnit.MILLISECOND
        && TimestampAccessor.isComponentTimestamp(target.getField())
        && SHARE_MILLIS_INPUT) {
      var struct = (org.apache.arrow.vector.complex.StructVector) target;
      var millis = (org.apache.arrow.vector.BigIntVector) struct.getChild("millis");
      var nanos = (org.apache.arrow.vector.IntVector) struct.getChild("nano_of_milli");
      try (var valid = target.getAllocator().buffer((count + 7L) / 8);
          var fractions = target.getAllocator().buffer(count * 4L)) {
        valid.setOne(0L, valid.capacity());
        fractions.setZero(0L, fractions.capacity());
        struct.loadFieldBuffers(
            new ArrowFieldNode(count, source.getNullCount()), List.of(source.getValidityBuffer()));
        millis.loadFieldBuffers(
            new ArrowFieldNode(count, 0), List.of(valid, source.getDataBuffer()));
        nanos.loadFieldBuffers(new ArrowFieldNode(count, 0), List.of(valid, fractions));
        target.setValueCount(count);
      }
      return;
    }
    if (TimestampAccessor.isComponentTimestamp(source.getField())
        && target.getField().getType()
            instanceof org.apache.arrow.vector.types.pojo.ArrowType.Timestamp timestamp
        && timestamp.getUnit() == org.apache.arrow.vector.types.TimeUnit.MILLISECOND) {
      var struct = (org.apache.arrow.vector.complex.StructVector) source;
      var millis = (org.apache.arrow.vector.BigIntVector) struct.getChild("millis");
      target.loadFieldBuffers(
          new ArrowFieldNode(count, source.getNullCount()),
          List.of(struct.getValidityBuffer(), millis.getDataBuffer()));
      target.setValueCount(count);
      return;
    }
    if (TimestampAccessor.isTimestamp(source)
        && TimestampAccessor.isTimestamp(target)
        && !source.getField().getType().equals(target.getField().getType())) {
      target.allocateNew();
      TimestampAccessor from = new TimestampAccessor(source);
      TimestampAccessor to = new TimestampAccessor(target);
      for (int i = 0; i < count; i++) to.set(i, from.isNull(i) ? null : from.getTimestamp(i));
      target.setValueCount(count);
      return;
    }
    if (!source.getField().getType().equals(target.getField().getType())) {
      throw new IllegalArgumentException(
          "Unsupported Fluss field conversion: " + source.getField() + " -> " + target.getField());
    }
    target.loadFieldBuffers(
        new ArrowFieldNode(count, source.getNullCount()), source.getFieldBuffers());
    List<FieldVector> from = source.getChildrenFromFields();
    List<FieldVector> to = target.getChildrenFromFields();
    if (from.size() != to.size())
      throw new IllegalArgumentException("Fluss nested field count changed");
    for (int i = 0; i < from.size(); i++) convertVector(from.get(i), to.get(i));
    target.setValueCount(count);
  }
}
