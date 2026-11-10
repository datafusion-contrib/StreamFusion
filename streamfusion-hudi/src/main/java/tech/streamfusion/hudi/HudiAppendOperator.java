package tech.streamfusion.hudi;

import org.apache.arrow.vector.TinyIntVector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.sink.append.AppendWriteFunction;
import org.apache.hudi.sink.common.AbstractWriteOperator;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Uses Hudi's original append function and checkpoint coordinator with borrowed Arrow views. */
final class HudiAppendOperator extends AbstractWriteOperator<Object> {
  HudiAppendOperator(Configuration conf, RowType type) {
    super(new Function(conf, type));
  }

  @Override
  public void open() throws Exception {
    TaskOffHeapMemory.initialize(
        getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration());
    TaskOffHeapMemory.registerMetrics(getMetricGroup());
    super.open();
  }

  @Override
  public void processElement(StreamRecord<Object> record) throws Exception {
    int rows = ((ArrowBatch) record.getValue()).rowCount();
    super.processElement(record);
    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter().inc(rows - 1L);
  }

  private static final class Function extends AppendWriteFunction<Object> {
    Function(Configuration conf, RowType type) {
      super(conf, type);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      writeClient
          .getHoodieTable(false)
          .getContext()
          .getStorageConf()
          .set(
              HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS.key(),
              HudiAppendIOFactory.class.getName());
    }

    @Override
    public void processElement(Object value, Context context, Collector<RowData> output)
        throws Exception {
      try (var root = ((ArrowBatch) value).root()) {
        var kinds = (TinyIntVector) root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
        if (kinds != null)
          for (int i = 0; i < root.getRowCount(); i++)
            if (kinds.isNull(i) || kinds.get(i) != RowKind.INSERT.toByteValue())
              throw new IllegalArgumentException("Hudi native append requires INSERT rows");
        var batch = new HudiAppendIOFactory.Batch(root, rowType);
        try {
          for (int i = 0; i < root.getRowCount(); i++) {
            batch.cursor.setRowId(i);
            super.processElement(batch.cursor, context, output);
          }
        } finally {
          batch.finished = true;
        }
      }
    }
  }
}
