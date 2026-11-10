package tech.streamfusion.hudi;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
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
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.StreamWriteFunction;
import org.apache.hudi.sink.bucket.BucketStreamWriteFunction;
import org.apache.hudi.sink.buffer.RowDataBucket;
import org.apache.hudi.sink.common.AbstractWriteOperator;
import org.apache.hudi.sink.transform.RowDataToHoodieFunction;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.util.StreamerUtil;
import tech.streamfusion.Native;
import tech.streamfusion.format.ColumnarFileCodec;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Keeps Hudi's index, flush policy and coordinator, replacing its binary payload buffer. */
final class HudiMorOperator extends AbstractWriteOperator<HoodieFlinkInternalRow> {
  private final Function function;

  HudiMorOperator(Configuration conf, RowType type) {
    this(new Function(conf, type));
  }

  private HudiMorOperator(Function function) {
    super(function);
    this.function = function;
  }

  @Override
  public void open() throws Exception {
    var task = getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration();
    TaskOffHeapMemory.initialize(task);
    TaskOffHeapMemory.registerMetrics(getMetricGroup());
    function.directories = ConfigurationUtils.splitPaths(task.get(CoreOptions.TMP_DIRS));
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
    private transient Map<String, RowDataBucket> buckets;
    private transient Map<String, Integer> ids;
    private transient RowDataToHoodieFunction<RowData, HoodieFlinkInternalRow> keys;
    private transient Map<Integer, HoodieFlinkInternalRow> controls;
    private transient int assigned;
    private transient long buffer;
    private transient long memoryOwner;
    private transient long charged;
    private transient HoodieFlinkInternalRow currentControl;
    private transient VectorSchemaRoot currentRoot;
    private transient int currentRow;
    private transient String[] directories;

    Function(Configuration conf, RowType type) {
      super(conf, type);
    }

    @Override
    public void open(Configuration parameters) throws IOException {
      super.open(parameters);
      try {
        buckets =
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
                            ids.get(absent),
                            bucketInfo(currentControl),
                            config.get(FlinkOptions.WRITE_BATCH_SIZE)));
              }
            };
        BUCKETS.set(this, buckets);
      } catch (IllegalAccessException failure) {
        throw new IOException("Unsupported Hudi write-buffer boundary", failure);
      }
      ids = new LinkedHashMap<>();
      keys = new RowDataToHoodieFunction<>(rowType, config);
      memoryOwner = TaskOffHeapMemory.registerOwner("hudi-buffer", "MOR Arrow staging");
      writeClient
          .getHoodieTable(false)
          .getContext()
          .getStorageConf()
          .set(HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS.key(), HudiMorIOFactory.class.getName());
      buffer = Native.createAppendBuffer(directories, 0, 0, 0, null);
    }

    @Override
    public void processElement(
        HoodieFlinkInternalRow record, Context context, Collector<RowData> out) throws Exception {
      long route;
      int[] destinations;
      controls = new HashMap<>();
      try (var root = ((Envelope) record).batch.root()) {
        if (root.getRowCount() == 0) {
          controls = null;
          return;
        }
        var batch = new HudiAppendIOFactory.Batch(root, rowType);
        var kinds = (TinyIntVector) root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
        destinations = new int[root.getRowCount()];
        for (int i = 0; i < destinations.length; i++) {
          batch.cursor.setRowId(i);
          batch.cursor.setRowKind(kind(kinds, i));
          super.processElement(keys.map(batch.cursor), context, out);
          destinations[i] = assigned;
        }
        try (var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
            var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
          Data.exportVectorSchemaRoot(
              NativeAllocator.SHARED, payload(root), NativeAllocator.DICTIONARIES, array, schema);
          try {
            route =
                Native.routeByAssignedBuckets(
                    array.memoryAddress(), schema.memoryAddress(), destinations);
          } finally {
            if (array.snapshot().release != 0) array.release();
            if (schema.snapshot().release != 0) schema.release();
          }
        }
      } catch (Throwable failure) {
        controls = null;
        throw failure;
      }
      var routed = new HashMap<Integer, VectorSchemaRoot>();
      try {
        int[] destination = new int[1];
        while (true) {
          var root =
              ColumnarFileCodec.importBatch(
                  (array, schema) ->
                      (destination[0] = Native.nextBucketRoute(route, array, schema)) >= 0);
          if (root == null) break;
          routed.put(destination[0], root);
        }
        int[] offsets = new int[ids.size()];
        var shape = HudiRowBufferControl.shape(rowType.getFieldCount());
        var descriptors = new HashMap<Integer, HoodieFlinkInternalRow>();
        controls.forEach(
            (id, control) ->
                descriptors.put(
                    id,
                    new HoodieFlinkInternalRow(
                        control.getRecordKey(),
                        control.getPartitionPath(),
                        control.getFileId(),
                        control.getInstantTime(),
                        control.getOperationType(),
                        false,
                        shape)));
        for (int id : destinations) {
          currentRoot = routed.get(id);
          currentRow = offsets[id]++;
          shape.setTotalSize(
              HudiRowBufferControl.rowBytes(currentRoot, currentRow, rowType.getFieldCount()));
          currentControl = descriptors.get(id);
          super.bufferRecord(currentControl);
        }
        for (var bucket : buckets.values()) ((ArrowBucket) bucket).publish();
        updateMemory();
      } finally {
        currentControl = null;
        currentRoot = null;
        controls = null;
        try {
          org.apache.flink.util.IOUtils.closeAll(routed.values());
        } finally {
          Native.closeBucketRoute(route);
        }
      }
    }

    private VectorSchemaRoot payload(VectorSchemaRoot root) {
      var fields = new ArrayList<>(root.getFieldVectors().subList(0, rowType.getFieldCount()));
      var kinds = root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
      if (kinds != null) fields.add(kinds);
      return new VectorSchemaRoot(
          fields.stream().map(v -> v.getField()).collect(java.util.stream.Collectors.toList()),
          fields,
          root.getRowCount());
    }

    @Override
    protected void bufferRecord(HoodieFlinkInternalRow record) {
      String key = StreamerUtil.generateBucketKey(record.getPartitionPath(), record.getFileId());
      assigned = ids.computeIfAbsent(key, ignored -> ids.size());
      controls.putIfAbsent(assigned, record);
    }

    @Override
    protected List<WriteStatus> writeRecords(String instant, RowDataBucket bucket) {
      ((ArrowBucket) bucket).publish();
      writeMetrics.startFileFlush();
      try (var records = new Records(buffer, ((ArrowBucket) bucket).id, bucket.getBucketInfo())) {
        var statuses = writeFunction.write(records, bucket.getBucketInfo(), instant);
        writeMetrics.endFileFlush();
        writeMetrics.increaseNumOfFilesWritten();
        return statuses;
      } finally {
        updateMemory();
      }
    }

    @Override
    public void snapshotState() {
      super.snapshotState();
      updateMemory();
    }

    @Override
    public void endInput() {
      super.endInput();
      updateMemory();
    }

    private void updateMemory() {
      long bytes = Native.appendBufferBytes(buffer);
      if (!TaskOffHeapMemory.tryReserve(memoryOwner, Math.max(0, bytes - charged)))
        throw TaskOffHeapMemory.exhausted("Hudi Arrow staging", bytes - charged);
      if (bytes < charged) TaskOffHeapMemory.release(memoryOwner, charged - bytes);
      charged = bytes;
    }

    @Override
    public void close() throws Exception {
      try {
        super.close();
      } finally {
        try {
          if (buffer != 0) Native.closeAppendBuffer(buffer);
        } finally {
          buffer = 0;
          if (memoryOwner != 0) TaskOffHeapMemory.closeOwner(memoryOwner);
          memoryOwner = 0;
        }
      }
    }

    private final class Records implements Iterator<HoodieRecord>, AutoCloseable {
      private final long handle;
      private final int id;
      private final BucketInfo info;
      private final List<VectorSchemaRoot> roots = new ArrayList<>();
      private HudiAppendIOFactory.Batch batch;
      private TinyIntVector kinds;
      private int row;

      Records(long handle, int id, BucketInfo info) {
        this.handle = handle;
        this.id = id;
        this.info = info;
      }

      @Override
      public boolean hasNext() {
        while (batch == null || row == batch.data.getRowCount()) {
          var root =
              ColumnarFileCodec.importBatch(
                  (array, schema) -> Native.appendBufferNext(handle, id, array, schema));
          if (root == null) return false;
          roots.add(root);
          batch = new HudiAppendIOFactory.Batch(root, rowType);
          kinds = (TinyIntVector) root.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
          row = 0;
        }
        return true;
      }

      @Override
      public HoodieRecord next() {
        if (!hasNext()) throw new NoSuchElementException();
        var view = new HudiAppendIOFactory.Cursor(batch, batch.vectors);
        view.setRowId(row);
        view.setRowKind(kind(kinds, row++));
        return recordConverter.convert(view, info);
      }

      @Override
      public void close() {
        try {
          org.apache.flink.util.IOUtils.closeAll(roots);
        } catch (Exception failure) {
          throw new HoodieException("Cannot release borrowed Hudi Arrow rows", failure);
        }
      }
    }

    private final class ArrowBucket extends RowDataBucket {
      final int id;
      private VectorSchemaRoot pending;
      private int start;
      private int end;

      ArrowBucket(String key, int id, BucketInfo info, double batchSize) {
        super(
            key,
            HudiRowBufferControl.create(rowType, preemptiveMemorySegmentPool),
            info,
            batchSize);
        this.id = id;
      }

      @Override
      public boolean writeRow(RowData row) throws IOException {
        if (!super.writeRow(row)) return false;
        if (pending == null) {
          pending = currentRoot;
          start = currentRow;
        } else if (pending != currentRoot || end != currentRow) {
          throw new IllegalStateException("Non-contiguous Hudi Arrow payload positions");
        }
        end = currentRow + 1;
        return true;
      }

      void publish() {
        if (pending == null) return;
        try (var slice = pending.slice(start, end - start)) {
          push(buffer, id, slice);
        }
        pending = null;
      }

      @Override
      public org.apache.flink.util.MutableObjectIterator<
              org.apache.flink.table.data.binary.BinaryRowData>
          getDataIterator() {
        throw new UnsupportedOperationException("Hudi payloads live in the Arrow buffer");
      }

      @Override
      public void dispose() {
        pending = null;
        super.dispose();
      }
    }

    private static BucketInfo bucketInfo(HoodieFlinkInternalRow control) {
      return (BucketInfo) invoke(BUCKET_INFO, null, control);
    }

    private static Object invoke(Method method, Object target, Object argument) {
      try {
        return method.invoke(target, argument);
      } catch (ReflectiveOperationException failure) {
        throw new HoodieException(
            "Unsupported released Hudi write hook: " + method.getName(), failure);
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
    if (kinds.isNull(row) || kinds.get(row) == RowKind.UPDATE_BEFORE.toByteValue())
      throw new IllegalArgumentException("Hudi MOR requires INSERT, UPDATE_AFTER or DELETE");
    return RowKind.fromByteValue(kinds.get(row));
  }

  private static void push(long handle, int bucket, VectorSchemaRoot root) {
    try (var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      try {
        Native.appendBufferPush(handle, bucket, array.memoryAddress(), schema.memoryAddress());
      } finally {
        if (array.snapshot().release != 0) array.release();
        if (schema.snapshot().release != 0) schema.release();
      }
    }
  }
}
