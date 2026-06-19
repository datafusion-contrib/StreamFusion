package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Native columnar sink: buffers rows into a batch, converts the whole batch to Arrow, and writes it
 * to a Parquet file natively, so the columnar encoding happens directly rather than through the
 * host's row-to-Parquet path. Each flush writes one file ({@code part-<subtask>-<n>.parquet}) into
 * the output directory.
 *
 * <p>This is the first cut for a bounded job: it writes a file per batch and flushes the remainder
 * on close. Checkpoint-aligned commit for exactly-once streaming is a later step.
 */
public class NativeParquetSinkOperator extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<RowData, Void>, BoundedOneInput {

  private final RowType inputRowType;
  private final String outputDirectory;
  private final int batchSize;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient List<RowData> buffer;
  private transient int fileCounter;
  private transient int subtask;

  public NativeParquetSinkOperator(RowType inputRowType, String outputDirectory, int batchSize) {
    this.inputRowType = inputRowType;
    this.outputDirectory = outputDirectory;
    this.batchSize = batchSize;
  }

  @Override
  public void open() throws Exception {
    super.open();
    allocator = new RootAllocator();
    dictionaries = new CDataDictionaryProvider();
    buffer = new ArrayList<>(batchSize);
    fileCounter = 0;
    subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
  }

  @Override
  public void processElement(StreamRecord<RowData> element) {
    buffer.add(element.getValue());
    if (buffer.size() >= batchSize) {
      flush();
    }
  }

  @Override
  public void endInput() {
    flush();
  }

  @Override
  public void close() throws Exception {
    flush();
    if (dictionaries != null) {
      dictionaries.close();
    }
    if (allocator != null) {
      allocator.close();
    }
    super.close();
  }

  private void flush() {
    if (buffer.isEmpty()) {
      return;
    }
    String path = outputDirectory + "/part-" + subtask + "-" + (fileCounter++) + ".parquet";
    try (VectorSchemaRoot batch = RowDataArrowConverter.write(buffer, inputRowType, allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, batch, dictionaries, array, schema);
      Native.writeParquet(array.memoryAddress(), schema.memoryAddress(), path);
    }
    buffer.clear();
  }
}
