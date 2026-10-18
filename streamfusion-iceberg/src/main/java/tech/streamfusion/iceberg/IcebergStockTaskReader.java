package tech.streamfusion.iceberg;

import java.io.IOException;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.source.reader.RecordAndPosition;
import org.apache.iceberg.flink.source.reader.RowDataReaderFunction;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.CloseableIterator;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;

/** Explicit host fallback for tasks outside the admitted Parquet contract. */
final class IcebergStockTaskReader implements IcebergTaskBatchReader {
  private final CloseableIterator<RecordsWithSplitIds<RecordAndPosition<RowData>>> input;
  private final org.apache.flink.table.types.logical.RowType type;
  private final int batchRows;
  private RecordsWithSplitIds<RecordAndPosition<RowData>> records;
  private boolean closed;

  IcebergStockTaskReader(
      FileScanTask task,
      Schema projection,
      RowDataReaderFunction function,
      int batchRows,
      long skip) {
    type = FlinkSchemaUtil.convert(projection);
    this.batchRows = batchRows;
    input =
        function.apply(
            IcebergSourceSplit.fromCombinedScanTask(
                new BaseCombinedScanTask(List.of(task)), 0, skip));
  }

  private RowData row() {
    while (!closed) {
      if (records != null) {
        var row = records.nextRecordFromSplit();
        if (row != null) return row.record();
        if (records.nextSplit() != null) continue;
        records.recycle();
        records = null;
      }
      if (!input.hasNext()) return null;
      records = input.next();
      if (records.nextSplit() == null) {
        records.recycle();
        records = null;
      }
    }
    return null;
  }

  @Override
  public VectorSchemaRoot next() {
    if (closed) throw new IllegalStateException("Iceberg fallback reader is closed");
    var root = VectorSchemaRoot.create(ArrowConversion.toArrowSchema(type), NativeAllocator.SHARED);
    try {
      var writer = ArrowConversion.createRowDataArrowWriter(root, type);
      int count = 0;
      RowData row;
      while (count < batchRows && (row = row()) != null) {
        writer.write(row);
        count++;
      }
      if (count == 0) {
        root.close();
        return null;
      }
      writer.finish();
      return root;
    } catch (Throwable failure) {
      root.close();
      throw failure;
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) return;
    closed = true;
    try {
      if (records != null) {
        records.recycle();
        records = null;
      }
    } finally {
      input.close();
    }
  }
}
