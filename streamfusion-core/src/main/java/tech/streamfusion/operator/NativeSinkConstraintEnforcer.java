package tech.streamfusion.operator;

import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.compat.FlinkStreamOperator;

/** Runs released Flink constraint semantics on borrowed rows with Arrow batch boundaries. */
public final class NativeSinkConstraintEnforcer extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {
  @FunctionalInterface
  public interface Enforcer extends Serializable {
    RowData enforce(RowData row);
  }

  private final RowType type;
  private final Enforcer enforcer;
  private final boolean[] lengthColumns;
  private final RowData.FieldGetter[] getters;

  public NativeSinkConstraintEnforcer(RowType type, Enforcer enforcer, boolean[] lengthColumns) {
    this.type = type;
    this.enforcer = enforcer;
    this.lengthColumns = lengthColumns.clone();
    getters = new RowData.FieldGetter[type.getFieldCount()];
    for (int i = 0; i < getters.length; i++)
      if (lengthColumns[i]) getters[i] = RowData.createFieldGetter(type.getTypeAt(i), i);
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ArrowBatch batch = element.getValue();
    VectorSchemaRoot root = batch.inspectRoot();
    ColumnarRecordMetrics.countIngested(getMetricGroup(), root.getRowCount());
    boolean needsCheck = false;
    for (int i = 0; i < type.getFieldCount(); i++)
      needsCheck |=
          lengthColumns[i]
              || !type.getTypeAt(i).isNullable() && root.getVector(i).getNullCount() != 0;
    if (!needsCheck) {
      output.collect(element);
      return;
    }
    var reader = ArrowConversion.createArrowReader(root, type);
    var view = new ConstraintRow();
    var kinds =
        (org.apache.arrow.vector.TinyIntVector)
            root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
    BitSet keep = new BitSet(root.getRowCount());
    Object[][] changes = new Object[type.getFieldCount()][];
    BitSet[] changed = new BitSet[type.getFieldCount()];
    int row = 0;
    try {
      for (; row < root.getRowCount(); row++) {
        RowData input = view.replace(reader.read(row), ConstraintRow.EMPTY);
        input.setRowKind(
            kinds == null
                ? org.apache.flink.types.RowKind.INSERT
                : org.apache.flink.types.RowKind.fromByteValue(kinds.get(row)));
        RowData result = enforcer.enforce(input);
        if (result == null) continue;
        keep.set(row);
        if (result == input) continue;
        for (int column = 0; column < getters.length; column++) {
          if (getters[column] == null) continue;
          Object before = getters[column].getFieldOrNull(input);
          Object after = getters[column].getFieldOrNull(result);
          boolean equal =
              before instanceof byte[] && after instanceof byte[]
                  ? Arrays.equals((byte[]) before, (byte[]) after)
                  : java.util.Objects.equals(before, after);
          if (equal) continue;
          if (changes[column] == null) {
            changes[column] = new Object[root.getRowCount()];
            changed[column] = new BitSet(root.getRowCount());
          }
          changes[column][row] = after;
          changed[column].set(row);
        }
      }
    } catch (RuntimeException | Error failure) {
      emit(element, keep, changes, changed);
      throw failure;
    }
    if (keep.cardinality() == root.getRowCount()
        && Arrays.stream(changes).allMatch(values -> values == null)) {
      output.collect(element);
    } else {
      emit(element, keep, changes, changed);
    }
  }

  private static final class ConstraintRow extends org.apache.flink.table.data.utils.JoinedRowData {
    private static final RowData EMPTY = new GenericRowData(0);

    // Released enforcers can read nullable fields without first checking nullness. Preserve
    // GenericRowData's null getter contract rather than interpreting spare Arrow bytes as values.
    @Override
    public org.apache.flink.table.data.StringData getString(int position) {
      return isNullAt(position) ? null : super.getString(position);
    }

    @Override
    public byte[] getBinary(int position) {
      return isNullAt(position) ? null : super.getBinary(position);
    }
  }

  private void emit(
      StreamRecord<ArrowBatch> element, BitSet keep, Object[][] changes, BitSet[] changed) {
    ArrowBatch batch = element.getValue();
    try (VectorSchemaRoot input = batch.root()) {
      if (keep.isEmpty()) return;
      var allocator = input.getVector(0).getAllocator();
      try (VectorSchemaRoot selected = VectorSchemaRoot.create(input.getSchema(), allocator)) {
        var reader = ArrowConversion.createArrowReader(input, type);
        boolean filtered = keep.cardinality() != input.getRowCount();
        for (int column = 0; column < input.getFieldVectors().size(); column++) {
          FieldVector from = input.getVector(column);
          FieldVector to = selected.getVector(column);
          if (column < changes.length && changes[column] != null) {
            RowType columnType =
                RowType.of(
                    new org.apache.flink.table.types.logical.LogicalType[] {type.getTypeAt(column)},
                    new String[] {type.getFieldNames().get(column)});
            var borrowed = new VectorSchemaRoot(List.of(to.getField()), List.of(to), 0);
            var writer = ArrowConversion.createRowDataArrowWriter(borrowed, columnType);
            for (int row = keep.nextSetBit(0); row >= 0; row = keep.nextSetBit(row + 1)) {
              Object value =
                  changed[column].get(row)
                      ? changes[column][row]
                      : getters[column].getFieldOrNull(reader.read(row));
              writer.write(GenericRowData.of(value));
            }
            writer.finish();
          } else if (from.getField().getMetadata() != null
              && "v1"
                  .equals(
                      from.getField()
                          .getMetadata()
                          .get(tech.streamfusion.arrow.StringCacheColumns.METADATA_KEY))) {
            int field =
                Integer.parseInt(
                    from.getName()
                        .substring("$string_java_".length(), from.getName().length() - 1));
            if (changes[field] == null) {
              if (filtered) {
                to.allocateNew();
                int target = 0;
                for (int row = keep.nextSetBit(0); row >= 0; row = keep.nextSetBit(row + 1))
                  to.copyFromSafe(row, target++, from);
              } else from.makeTransferPair(to).splitAndTransfer(0, input.getRowCount());
            } else {
              to.allocateNew();
              var cacheWriter =
                  new tech.streamfusion.arrow.Utf16Cache.Writer(
                      (org.apache.arrow.vector.complex.ListVector) to);
              int target = 0;
              for (int row = keep.nextSetBit(0); row >= 0; row = keep.nextSetBit(row + 1)) {
                Object value =
                    changed[field].get(row)
                        ? changes[field][row]
                        : getters[field].getFieldOrNull(reader.read(row));
                cacheWriter.write(
                    target++,
                    value == null
                        ? null
                        : ((org.apache.flink.table.data.binary.BinaryStringData) value)
                            .getJavaObject());
              }
            }
          } else if (filtered) {
            to.allocateNew();
            int target = 0;
            for (int row = keep.nextSetBit(0); row >= 0; row = keep.nextSetBit(row + 1))
              to.copyFromSafe(row, target++, from);
          } else {
            from.makeTransferPair(to).splitAndTransfer(0, input.getRowCount());
          }
        }
        selected.setRowCount(keep.cardinality());
        ArrowBatch result = batch.filtered(selected.slice(0, selected.getRowCount()), keep::get);
        try {
          output.collect(element.copy(result));
        } catch (RuntimeException | Error failure) {
          result.closeUnclaimed();
          throw failure;
        }
      }
    }
  }
}
