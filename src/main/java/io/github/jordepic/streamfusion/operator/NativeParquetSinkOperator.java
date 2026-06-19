package io.github.jordepic.streamfusion.operator;

import io.github.jordepic.streamfusion.Native;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Native columnar sink: buffers rows into a batch, converts the whole batch to Arrow, and writes it
 * to a Parquet file natively, so the columnar encoding happens directly rather than through the
 * host's row-to-Parquet path. Each flush writes one file.
 *
 * <p>Files are committed exactly once via two-phase commit, mirroring the host's file sink: a flush
 * writes an in-progress file, the in-progress set is checkpointed, and a file is renamed to its
 * visible name only once the checkpoint that recorded it completes. Pending files restored from a
 * checkpoint are committed on recovery (the rename is idempotent), so a row appears in the output
 * exactly once across failures.
 */
public class NativeParquetSinkOperator extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<RowData, Void>, BoundedOneInput, CheckpointListener {

  private static final String IN_PROGRESS_PREFIX = "_";
  private static final String IN_PROGRESS_SUFFIX = ".inprogress";

  private final RowType inputRowType;
  private final String outputDirectory;
  private final int batchSize;

  private transient BufferAllocator allocator;
  private transient CDataDictionaryProvider dictionaries;
  private transient List<RowData> buffer;
  private transient int fileCounter;
  private transient int subtask;
  private transient ListState<String> pendingState;
  // In-progress files awaiting commit, grouped by the checkpoint that recorded them (ascending).
  private transient TreeMap<Long, List<String>> pendingByCheckpoint;
  // Files written since the last checkpoint, not yet tied to one.
  private transient List<String> uncommitted;

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
    subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    pendingByCheckpoint = new TreeMap<>();
    uncommitted = new ArrayList<>();
    fileCounter = 0;
    pendingState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "native-parquet-sink-pending", BasicTypeInfo.STRING_TYPE_INFO));
    // Files recorded by the restored checkpoint were made durable by it, so make them visible now;
    // the rename is idempotent, so re-committing an already-committed file is a no-op.
    for (String inProgress : pendingState.get()) {
      commit(inProgress);
    }
    pendingState.clear();
  }

  @Override
  public void processElement(StreamRecord<RowData> element) {
    buffer.add(element.getValue());
    if (buffer.size() >= batchSize) {
      flush();
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    flush();
    if (!uncommitted.isEmpty()) {
      pendingByCheckpoint.put(context.getCheckpointId(), new ArrayList<>(uncommitted));
      uncommitted.clear();
    }
    pendingState.clear();
    for (List<String> files : pendingByCheckpoint.values()) {
      for (String file : files) {
        pendingState.add(file);
      }
    }
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    // Commit every file recorded at this checkpoint or earlier; they are now durable.
    while (!pendingByCheckpoint.isEmpty() && pendingByCheckpoint.firstKey() <= checkpointId) {
      Map.Entry<Long, List<String>> entry = pendingByCheckpoint.pollFirstEntry();
      for (String inProgress : entry.getValue()) {
        commit(inProgress);
      }
    }
  }

  @Override
  public void endInput() {
    flush();
  }

  @Override
  public void close() throws Exception {
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
    String inProgress =
        outputDirectory
            + "/"
            + IN_PROGRESS_PREFIX
            + "part-"
            + subtask
            + "-"
            + (fileCounter++)
            + ".parquet"
            + IN_PROGRESS_SUFFIX;
    try (VectorSchemaRoot batch = RowDataArrowConverter.write(buffer, inputRowType, allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, batch, dictionaries, array, schema);
      Native.writeParquet(array.memoryAddress(), schema.memoryAddress(), inProgress);
    }
    uncommitted.add(inProgress);
    buffer.clear();
  }

  /** Renames an in-progress file to its visible name; a no-op if it was already committed. */
  private void commit(String inProgress) {
    Path source = Paths.get(inProgress);
    String name = source.getFileName().toString();
    String visible =
        name.substring(IN_PROGRESS_PREFIX.length(), name.length() - IN_PROGRESS_SUFFIX.length());
    try {
      if (Files.exists(source)) {
        Files.move(source, source.resolveSibling(visible), StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (java.io.IOException e) {
      throw new RuntimeException("failed to commit parquet file " + inProgress, e);
    }
  }
}
