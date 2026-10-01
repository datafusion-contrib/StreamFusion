package tech.streamfusion.paimon;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.TableException;
import tech.streamfusion.compat.FlinkStreamOperator;
import tech.streamfusion.operator.ArrowBatch;

/** Enforces Flink's NOT NULL error semantics on Arrow before Paimon can merge rows. */
public final class NativePaimonNotNullEnforcer extends FlinkStreamOperator<ArrowBatch>
    implements OneInputStreamOperator<ArrowBatch, ArrowBatch> {
  private final String[] fieldNames;

  public NativePaimonNotNullEnforcer(String[] fieldNames) {
    this.fieldNames = fieldNames.clone();
  }

  @Override
  public void processElement(StreamRecord<ArrowBatch> element) {
    ArrowBatch batch = element.getValue();
    VectorSchemaRoot root = batch.root();
    try {
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
              throw new TableException(
                  "Column '"
                      + fieldNames[column]
                      + "' is NOT NULL, however, a null value is being written into it. "
                      + "You can set job configuration 'table.exec.sink.not-null-enforcer'='DROP' "
                      + "to suppress this exception and drop such records silently.");
            }
          }
        }
      }
    } catch (RuntimeException | Error failure) {
      root.close();
      throw failure;
    }
    output.collect(element.replace(new ArrowBatch(root, batch.keyGroup())));
  }
}
