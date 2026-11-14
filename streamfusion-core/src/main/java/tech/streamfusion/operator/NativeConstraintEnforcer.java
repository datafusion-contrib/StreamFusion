package tech.streamfusion.operator;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import tech.streamfusion.compat.FlinkConstraintExceptions;
import tech.streamfusion.compat.FlinkStreamOperator;

/**
 * Connector-neutral constraints without leaving the columnar pipeline.
 *
 * <p>Currently implements only Flink's top-level NOT NULL ERROR policy. Admission for other
 * constraints remains the planner's responsibility.
 */
public final class NativeConstraintEnforcer extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {
  private static final String NOT_NULL_ERROR =
      "Column '%s' is NOT NULL, however, a null value is being written into it. "
          + "You can set job configuration 'table.exec.sink.not-null-enforcer'='DROP' "
          + "to suppress this exception and drop such records silently.";

  private final String[] fieldNames;

  public NativeConstraintEnforcer(String[] fieldNames) {
    this.fieldNames = fieldNames.clone();
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ArrowBatch batch = element.getValue();
    VectorSchemaRoot root = batch.inspectRoot();
    boolean hasNulls = false;
    for (int column = 0; column < fieldNames.length; column++) {
      if (fieldNames[column] != null && root.getVector(column).getNullCount() != 0) {
        hasNulls = true;
        break;
      }
    }
    if (hasNulls) {
      for (int row = 0; row < root.getRowCount(); row++) {
        for (int column = 0; column < fieldNames.length; column++) {
          if (fieldNames[column] != null && root.getVector(column).isNull(row)) {
            // Stock Flink forwards the valid prefix before the first failing record.
            try (VectorSchemaRoot owned = batch.root()) {
              if (row != 0) {
                ArrowBatch prefix = batch.prefix(owned, row);
                try {
                  output.collect(element.copy(prefix));
                } catch (RuntimeException | Error failure) {
                  prefix.closeUnclaimed();
                  throw failure;
                }
              }
            }
            throw FlinkConstraintExceptions.notNull(NOT_NULL_ERROR, fieldNames[column]);
          }
        }
      }
    }
    output.collect(element);
  }
}
