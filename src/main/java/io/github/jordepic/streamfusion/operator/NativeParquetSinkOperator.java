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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Native columnar sink: writes incoming Arrow batches to Parquet files natively, so a columnar
 * producer feeds it without converting to rows. Batches are coalesced into one open Parquet file and
 * a new file is rolled only when a row target is reached (or on checkpoint), so the output file
 * count tracks total size rather than the upstream batch size — small read batches no longer produce
 * a file each.
 *
 * <p>Files are committed exactly once via two-phase commit, mirroring the host's file sink: writing
 * fills an in-progress file, the closed in-progress set is checkpointed, and a file is renamed to
 * its visible name only once the checkpoint that recorded it completes. A checkpoint closes the
 * currently open file so its rows are durable in the recorded set. Pending files restored from a
 * checkpoint are committed on recovery (the rename is idempotent), so a row appears in the output
 * exactly once across failures.
 */
public class NativeParquetSinkOperator extends AbstractStreamOperator<Object>
    implements OneInputStreamOperator<ArrowBatch, Object>, CheckpointListener {

  private static final String IN_PROGRESS_PREFIX = "_";
  private static final String IN_PROGRESS_SUFFIX = ".inprogress";
  /** Default rows per output file: roll once a file holds this many, independent of batch size. */
  public static final long DEFAULT_TARGET_ROWS = 1_000_000L;

  private final String outputDirectory;
  private final long targetRows;

  private transient CDataDictionaryProvider dictionaries;
  private transient int fileCounter;
  private transient int subtask;
  private transient ListState<String> pendingState;
  // In-progress files awaiting commit, grouped by the checkpoint that recorded them (ascending).
  private transient TreeMap<Long, List<String>> pendingByCheckpoint;
  // Closed files written since the last checkpoint, not yet tied to one.
  private transient List<String> uncommitted;
  // The currently open file: a native writer handle (0 = none), its path, and rows written so far.
  private transient long openWriter;
  private transient String openPath;
  private transient long openRows;

  public NativeParquetSinkOperator(String outputDirectory) {
    this(outputDirectory, DEFAULT_TARGET_ROWS);
  }

  public NativeParquetSinkOperator(String outputDirectory, long targetRows) {
    this.outputDirectory = outputDirectory;
    this.targetRows = targetRows;
  }

  @Override
  public void open() throws Exception {
    super.open();
    dictionaries = new CDataDictionaryProvider();
    subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    pendingByCheckpoint = new TreeMap<>();
    uncommitted = new ArrayList<>();
    fileCounter = 0;
    openWriter = 0;
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
  public void processElement(StreamRecord<ArrowBatch> element) {
    VectorSchemaRoot batch = element.getValue().root();
    // The batch's buffers belong to the upstream operator's allocator; export with that allocator
    // (buffers associate only within one allocator root).
    BufferAllocator batchAllocator =
        batch.getFieldVectors().isEmpty() ? null : batch.getFieldVectors().get(0).getAllocator();
    long rowCount = batch.getRowCount();
    if (openWriter == 0) {
      openPath =
          outputDirectory + "/" + IN_PROGRESS_PREFIX + "part-" + subtask + "-" + (fileCounter++)
              + ".parquet" + IN_PROGRESS_SUFFIX;
      openWriter = Native.createParquetWriter(openPath);
      openRows = 0;
    }
    try (ArrowArray array = ArrowArray.allocateNew(batchAllocator);
        ArrowSchema schema = ArrowSchema.allocateNew(batchAllocator)) {
      Data.exportVectorSchemaRoot(batchAllocator, batch, dictionaries, array, schema);
      Native.parquetWriterWrite(openWriter, array.memoryAddress(), schema.memoryAddress());
    } finally {
      batch.close();
    }
    openRows += rowCount;
    if (openRows >= targetRows) {
      closeOpenFile();
    }
  }

  /** Finalizes the open file (if any) and queues it for commit at the next checkpoint. */
  private void closeOpenFile() {
    if (openWriter == 0) {
      return;
    }
    Native.closeParquetWriter(openWriter);
    uncommitted.add(openPath);
    openWriter = 0;
    openPath = null;
    openRows = 0;
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    // Close the open file so its rows are durable in this checkpoint's recorded set.
    closeOpenFile();
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
  public void close() throws Exception {
    // Finalize any open file (a no-op if none) so the handle is freed and the file is well-formed;
    // like the closed files in `uncommitted`, it stays in-progress until a checkpoint commits it.
    closeOpenFile();
    if (dictionaries != null) {
      dictionaries.close();
    }
    super.close();
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
