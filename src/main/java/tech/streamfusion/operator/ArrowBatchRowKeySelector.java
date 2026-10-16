package tech.streamfusion.operator;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.arrow.ArrowConversion;

/** Establishes a real logical-key backend before a batch is processed key by key. */
public final class ArrowBatchRowKeySelector implements KeySelector<ArrowBatch, RowData> {
  private final RowType inputType;
  private final RowDataKeySelector selector;

  public ArrowBatchRowKeySelector(RowType inputType, RowDataKeySelector selector) {
    this.inputType = inputType;
    this.selector = selector;
  }

  @Override
  public RowData getKey(ArrowBatch batch) throws Exception {
    var root = batch.inspectRoot();
    RowData first =
        root.getRowCount() == 0
            ? new GenericRowData(inputType.getFieldCount())
            : ArrowConversion.createArrowReader(root, inputType).read(0);
    return selector.getKey(first);
  }
}
