package tech.streamfusion.delta;

import io.delta.flink.sink.mergestrategy.MoRUpsert;
import io.delta.flink.table.AbstractKernelTable;
import io.delta.flink.table.DeltaTable;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;
import java.util.function.BiPredicate;

/** Narrow adapter exposing Delta 4.4's published MOR file-removal implementation. */
final class NativeMoRUpsert extends MoRUpsert {
  void bind(DeltaTable deltaTable) {
    table = (AbstractKernelTable) deltaTable;
  }

  CloseableIterator<Row> removeRows(
      Row addFile, BiPredicate<ColumnarBatch, Integer> filter) {
    return deleteRecords(addFile, filter);
  }
}
