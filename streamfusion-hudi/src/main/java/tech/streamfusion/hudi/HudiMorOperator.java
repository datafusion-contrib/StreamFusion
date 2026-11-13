package tech.streamfusion.hudi;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.StreamWriteFunction;
import org.apache.hudi.sink.bucket.BucketStreamWriteFunction;
import org.apache.hudi.sink.buffer.RowDataBucket;
import org.apache.hudi.sink.common.AbstractWriteOperator;
import org.apache.hudi.sink.utils.TimeWait;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.FlinkWriteHelper;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Keeps Hudi's index, flush policy and coordinator, retaining payloads as Arrow batches. */
final class HudiMorOperator extends AbstractWriteOperator<HoodieFlinkInternalRow> {
  HudiMorOperator(Configuration conf, RowType type) {
    super(new Function(conf, type));
  }

  @Override
  public void open() throws Exception {
    var task = getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration();
    TaskOffHeapMemory.initialize(task);
    TaskOffHeapMemory.registerMetrics(getMetricGroup());
    super.open();
  }

  @Override
  public void processElement(StreamRecord<HoodieFlinkInternalRow> record) throws Exception {
    var batch = (ArrowBatch) (Object) record.getValue();
    int rows = batch.rowCount();
    super.processElement(record.replace(new Envelope(batch)));
    getMetricGroup().getIOMetricGroup().getNumRecordsInCounter().inc(rows - 1L);
  }

  /** Control envelope only; the original row-facing function never reads its dummy row. */
  private static final class Envelope extends HoodieFlinkInternalRow {
    final ArrowBatch batch;

    Envelope(ArrowBatch batch) {
      super("", "", new GenericRowData(0));
      this.batch = batch;
    }
  }

  private static final class Function extends BucketStreamWriteFunction {
    private static final Field BUCKETS = bucketsField();
    private static final Method BUCKET_INFO = method("getBucketInfo", HoodieFlinkInternalRow.class);
    private transient BufferedBatch currentBatch;
    private transient HoodieFlinkInternalRow currentControl;
    private transient int currentRow;
    private transient HudiArrowWriteContext mergeContext;
    private transient BufferedRecordMerger<RowData> merger;
    private transient String[] orderingFields;
    private transient boolean recovering;

    Function(Configuration conf, RowType type) {
      super(conf, type);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      super.initializeState(context);
      recovering = context.isRestored();
    }

    @Override
    public void open(Configuration parameters) throws IOException {
      super.open(parameters);
      if (config.get(FlinkOptions.PRE_COMBINE)) {
        mergeContext = new HudiArrowWriteContext(metaClient);
        var props = writeClient.getConfig().getProps();
        mergeContext.initRecordMergerForIngestion(props);
        orderingFields =
            HoodieRecordUtils.getOrderingFieldNames(mergeContext.getMergeMode(), metaClient)
                .toArray(new String[0]);
        merger =
            BufferedRecordMergerFactory.create(
                mergeContext,
                mergeContext.getMergeMode(),
                false,
                mergeContext.getRecordMerger(),
                HoodieSchema.parse(writeClient.getConfig().getSchema()),
                mergeContext.getPayloadClasses(props),
                props,
                metaClient.getTableConfig().getPartialUpdateMode());
      }
      Map<String, RowDataBucket> buckets =
          new LinkedHashMap<String, RowDataBucket>() {
            @Override
            public RowDataBucket computeIfAbsent(
                String key,
                java.util.function.Function<? super String, ? extends RowDataBucket> ignored) {
              return super.computeIfAbsent(
                  key,
                  absent ->
                      new ArrowBucket(
                          absent,
                          bucketInfo(currentControl),
                          config.get(FlinkOptions.WRITE_BATCH_SIZE)));
            }
          };
      try {
        BUCKETS.set(this, buckets);
      } catch (IllegalAccessException failure) {
        throw new IOException("Unsupported Hudi write-buffer boundary", failure);
      }
      writeClient
          .getEngineContext()
          .getStorageConf()
          .set(HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS.key(), HudiMorIOFactory.class.getName());
    }

    @Override
    public void processElement(
        HoodieFlinkInternalRow record, Context context, Collector<RowData> out) throws Exception {
      var root = ((Envelope) record).batch.root();
      final BufferedBatch input;
      try {
        input = new BufferedBatch(root);
      } catch (Throwable failure) {
        root.close();
        throw failure;
      }
      currentBatch = input;
      try {
        if (recovering) {
          // The coordinator must finish replayed commits before Java bootstraps file groups.
          var wait =
              TimeWait.builder()
                  .timeout(config.get(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT))
                  .interval(50)
                  .action("restored Hudi commits before bucket bootstrap")
                  .build();
          while (correspondent.requestInflightInstants().keySet().stream()
              .anyMatch(id -> id < checkpointId)) wait.waitFor();
          recovering = false;
        }
        var shape = HudiRowBufferControl.shape(rowType.getFieldCount());
        for (currentRow = 0; currentRow < root.getRowCount(); currentRow++) {
          var cursor = input.batch.cursor;
          cursor.setRowId(currentRow);
          cursor.setRowKind(kind(input.kinds, currentRow));
          shape.setTotalSize(HudiRowBufferControl.rowBytes(root, currentRow, rowType));
          currentControl =
              new HoodieFlinkInternalRow(
                  keyGen.getRecordKey(cursor), keyGen.getPartitionPath(cursor), shape);
          super.processElement(currentControl, context, out);
        }
      } finally {
        currentControl = null;
        currentBatch = null;
        input.release();
      }
    }

    @Override
    protected List<WriteStatus> writeRecords(String instant, RowDataBucket bucket) {
      var arrowBucket = (ArrowBucket) bucket;
      writeMetrics.startFileFlush();
      try {
        var statuses =
            writeFunction.write(
                deduplicateRecordsIfNeeded(
                    new Records(arrowBucket.selections, bucket.getBucketInfo())),
                bucket.getBucketInfo(),
                instant);
        writeMetrics.endFileFlush();
        writeMetrics.increaseNumOfFilesWritten();
        return statuses;
      } finally {
        arrowBucket.releasePayloads();
      }
    }

    @Override
    protected Iterator<HoodieRecord> deduplicateRecordsIfNeeded(Iterator<HoodieRecord> records) {
      if (!config.get(FlinkOptions.PRE_COMBINE)) return records;
      return FlinkWriteHelper.newInstance()
          .deduplicateRecords(
              records,
              null,
              -1,
              writeClient.getConfig().getSchema(),
              writeClient.getConfig().getProps(),
              merger,
              mergeContext,
              orderingFields);
    }

    private final class BufferedBatch {
      final VectorSchemaRoot root;
      final HudiAppendIOFactory.Batch batch;
      final TinyIntVector kinds;
      final TaskOffHeapMemory.Reservation memory;
      private int references = 1;

      BufferedBatch(VectorSchemaRoot root) {
        this.root = root;
        batch = new HudiAppendIOFactory.Batch(root, rowType);
        kinds = (TinyIntVector) root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
        long bytes = root.getFieldVectors().stream().mapToLong(v -> v.getBufferSize()).sum();
        memory = TaskOffHeapMemory.reserve("hudi-buffer", "MOR Arrow batches", bytes);
      }

      void retain() {
        references++;
      }

      void release() {
        if (--references != 0) return;
        try {
          root.close();
        } finally {
          memory.close();
        }
      }
    }

    private final class Selection implements AutoCloseable {
      final BufferedBatch batch;
      final BitSet rows = new BitSet();

      Selection(BufferedBatch batch) {
        this.batch = batch;
        batch.retain();
      }

      @Override
      public void close() {
        batch.release();
      }
    }

    private final class Records implements Iterator<HoodieRecord> {
      private final Iterator<Selection> batches;
      private final BucketInfo info;
      private Selection current;
      private int nextRow = -1;

      Records(List<Selection> selections, BucketInfo info) {
        batches = selections.iterator();
        this.info = info;
      }

      @Override
      public boolean hasNext() {
        while (nextRow < 0 && batches.hasNext()) {
          current = batches.next();
          nextRow = current.rows.nextSetBit(0);
        }
        return nextRow >= 0;
      }

      @Override
      public HoodieRecord next() {
        if (!hasNext()) throw new NoSuchElementException();
        var batch = current.batch;
        var view = new HudiAppendIOFactory.Cursor(batch.batch, batch.batch.vectors);
        view.setRowId(nextRow);
        view.setRowKind(kind(batch.kinds, nextRow));
        nextRow = current.rows.nextSetBit(nextRow + 1);
        return recordConverter.convert(view, info);
      }
    }

    private final class ArrowBucket extends RowDataBucket {
      final List<Selection> selections = new ArrayList<>();
      private Selection current;

      ArrowBucket(String key, BucketInfo info, double batchSize) {
        super(
            key,
            HudiRowBufferControl.create(rowType, preemptiveMemorySegmentPool),
            info,
            batchSize);
      }

      @Override
      public boolean writeRow(RowData row) throws IOException {
        if (!super.writeRow(row)) return false;
        if (current == null || current.batch != currentBatch) {
          current = new Selection(currentBatch);
          selections.add(current);
        }
        current.rows.set(currentRow);
        return true;
      }

      @Override
      public org.apache.flink.util.MutableObjectIterator<
              org.apache.flink.table.data.binary.BinaryRowData>
          getDataIterator() {
        throw new UnsupportedOperationException("Hudi payloads live in Arrow batches");
      }

      void releasePayloads() {
        try {
          org.apache.flink.util.IOUtils.closeAll(selections);
        } catch (Exception failure) {
          throw new HoodieException("Cannot release Hudi Arrow batches", failure);
        } finally {
          selections.clear();
          current = null;
        }
      }

      @Override
      public void dispose() {
        try {
          releasePayloads();
        } finally {
          super.dispose();
        }
      }
    }

    private static BucketInfo bucketInfo(HoodieFlinkInternalRow control) {
      try {
        return (BucketInfo) BUCKET_INFO.invoke(null, control);
      } catch (ReflectiveOperationException failure) {
        throw new HoodieException("Unsupported released Hudi bucket hook", failure);
      }
    }

    private static Field bucketsField() {
      try {
        var field = StreamWriteFunction.class.getDeclaredField("buckets");
        field.setAccessible(true);
        return field;
      } catch (ReflectiveOperationException failure) {
        throw new IllegalStateException("Unsupported released Hudi buffer", failure);
      }
    }

    private static Method method(String name, Class<?> parameter) {
      try {
        var method = StreamWriteFunction.class.getDeclaredMethod(name, parameter);
        method.setAccessible(true);
        return method;
      } catch (ReflectiveOperationException failure) {
        throw new IllegalStateException("Unsupported released Hudi writer", failure);
      }
    }
  }

  private static RowKind kind(TinyIntVector kinds, int row) {
    if (kinds == null) return RowKind.INSERT;
    if (kinds.isNull(row))
      throw new IllegalArgumentException("Hudi MOR requires a valid changelog row kind");
    return RowKind.fromByteValue(kinds.get(row));
  }
}
