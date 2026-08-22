package tech.streamfusion.delta;

import io.delta.flink.sink.Conversions;
import io.delta.flink.sink.DeltaSinkConf;
import io.delta.flink.sink.DeltaWriterResult;
import io.delta.flink.sink.MergeStrategy;
import io.delta.flink.sink.WriterResultContext;
import io.delta.flink.kernel.ColumnVectorUtils;
import io.delta.flink.sink.mergestrategy.ScanLocator;
import io.delta.flink.table.DeltaTable;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.function.BiPredicate;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

/** Writes Arrow-backed Delta data files while reusing Delta's published merge and commit APIs. */
final class NativeDeltaSinkWriter implements CommittingSinkWriter<RowData, DeltaWriterResult> {
  private final String pathSuffix;
  private final DeltaTable table;
  private final DeltaSinkConf conf;
  private final boolean upsertMode;
  private final NativeMoRUpsert morUpsert;
  private final Map<Map<String, String>, PartitionBuffer> partitions = new LinkedHashMap<>();
  private final Map<String, BufferedRow> currentRows = new HashMap<>();
  private final List<List<Literal>> persistedKeys = new ArrayList<>();
  private final Set<String> persistedKeyIndex = new HashSet<>();
  private long lowWatermark = Long.MAX_VALUE;
  private long highWatermark = -1;

  NativeDeltaSinkWriter(
      String jobId,
      int subtaskId,
      int attemptNumber,
      DeltaTable table,
      DeltaSinkConf conf) {
    this.pathSuffix = jobId + "-" + subtaskId + "-" + attemptNumber;
    this.table = table;
    this.conf = conf;
    this.upsertMode = conf.isUpsert();
    this.morUpsert = upsertMode ? new NativeMoRUpsert() : null;
    if (morUpsert != null) {
      morUpsert.bind(table);
    }
  }

  @Override
  public void write(RowData element, SinkWriter.Context context) {
    if (!(element instanceof ArrowKernelRows)) {
      throw new IllegalArgumentException(
          "The native Delta writer requires an Arrow batch carrier, got "
              + element.getClass().getName());
    }
    ArrowKernelRows batch = (ArrowKernelRows) element;
    if (upsertMode && !batch.hasRowKinds()) {
      throw new IllegalStateException(
          "Delta upsert input lost its RowKind column before reaching the native writer");
    }
    try {
      for (int rowId = 0; rowId < batch.rowCount(); rowId++) {
        RowData row = batch.rowView(rowId);
        Map<String, Literal> partitionValues =
            Conversions.FlinkToDelta.partitionValues(
                table.getSchema(), table.getPartitionColumns(), row);
        PartitionBuffer partition =
            partitions.computeIfAbsent(
                writerKey(partitionValues), ignored -> new PartitionBuffer(partitionValues));
        List<Literal> primaryKey = upsertMode ? primaryKey(row) : List.of();
        String key = primaryKey.isEmpty() ? null : encodeKey(primaryKey);
        switch (row.getRowKind()) {
          case INSERT:
            bufferRow(partition, batch, rowId, key);
            break;
          case UPDATE_AFTER:
            if (!bufferRow(partition, batch, rowId, key)) {
              recordPersistedKey(primaryKey);
            }
            break;
          case UPDATE_BEFORE:
            break;
          case DELETE:
            if (!removeBufferedRow(key)) {
              recordPersistedKey(primaryKey);
            }
            break;
          default:
            throw new IllegalStateException("Unexpected RowKind: " + row.getRowKind());
        }
      }
      lowWatermark = Math.min(lowWatermark, context.currentWatermark());
      highWatermark = Math.max(highWatermark, context.currentWatermark());
    } finally {
      batch.close();
    }
  }

  private boolean bufferRow(
      PartitionBuffer partition, ArrowKernelRows batch, int rowId, String primaryKey) {
    BufferedSelection selection = partition.selectionFor(batch);
    BufferedRow next = selection.add(rowId);
    if (primaryKey != null) {
      BufferedRow previous = currentRows.put(primaryKey, next);
      if (previous != null) {
        previous.remove();
        return true;
      }
    }
    return false;
  }

  private boolean removeBufferedRow(String primaryKey) {
    if (primaryKey == null) {
      return false;
    }
    BufferedRow previous = currentRows.remove(primaryKey);
    if (previous != null) {
      previous.remove();
      return true;
    }
    return false;
  }

  private void recordPersistedKey(List<Literal> primaryKey) {
    if (persistedKeyIndex.add(MergeStrategy.keyString(primaryKey))) {
      persistedKeys.add(primaryKey);
    }
  }

  private List<Literal> primaryKey(RowData row) {
    int[] ordinals = conf.getPrimaryKeyOrdinals();
    List<Literal> key = new ArrayList<>(ordinals.length);
    for (int ordinal : ordinals) {
      key.add(Conversions.FlinkToDelta.data(table.getSchema(), row, ordinal));
    }
    return key;
  }

  @Override
  public Collection<DeltaWriterResult> prepareCommit() {
    try {
      List<Row> actions = new ArrayList<>();
      for (PartitionBuffer partition : partitions.values()) {
        List<FilteredColumnarBatch> batches = partition.drain();
        if (batches.isEmpty()) {
          continue;
        }
        try (CloseableIterator<Row> written =
            table.writeParquet(
                pathSuffix,
                Utils.toCloseableIterator(batches.iterator()),
                partition.partitionValues)) {
          actions.addAll(written.toInMemoryList());
        }
      }
      actions.addAll(mergePersistedRows());
      partitions.clear();
      currentRows.clear();
      persistedKeys.clear();
      persistedKeyIndex.clear();
      if (actions.isEmpty()) {
        return List.of();
      }
      WriterResultContext context = new WriterResultContext(lowWatermark, highWatermark);
      lowWatermark = Long.MAX_VALUE;
      highWatermark = -1;
      return List.of(new DeltaWriterResult(actions, context));
    } catch (IOException failure) {
      closeBufferedBatches();
      throw new UncheckedIOException("Failed to prepare native Delta files", failure);
    }
  }

  @Override
  public void flush(boolean endOfInput) {}

  @Override
  public void close() throws Exception {
    closeBufferedBatches();
    table.close();
  }

  private void closeBufferedBatches() {
    partitions.values().forEach(PartitionBuffer::close);
    partitions.clear();
    currentRows.clear();
    persistedKeys.clear();
    persistedKeyIndex.clear();
  }

  private List<Row> mergePersistedRows() throws IOException {
    if (!upsertMode || persistedKeys.isEmpty()) {
      return List.of();
    }
    int[] ordinals = conf.getPrimaryKeyOrdinals();
    BiPredicate<ColumnarBatch, Integer> filter =
        (batch, rowId) -> persistedKeyIndex.contains(encodedBatchKey(batch, rowId, ordinals));
    CloseableIterator<Row> files = new ScanLocator().find(table, ordinals, persistedKeys);
    return files.flatMap(file -> morUpsert.removeRows(file, filter)).toInMemoryList();
  }

  private static String encodedBatchKey(ColumnarBatch batch, int rowId, int[] ordinals) {
    List<String> values = new ArrayList<>(ordinals.length);
    for (int ordinal : ordinals) {
      values.add(
          MergeStrategy.encodeObject(ColumnVectorUtils.get(batch.getColumnVector(ordinal), rowId)));
    }
    return String.join(";", values);
  }

  private static Map<String, String> writerKey(Map<String, Literal> partitionValues) {
    Map<String, String> key = new LinkedHashMap<>();
    partitionValues.forEach((name, value) -> key.put(name, encodeLiteral(value)));
    return Map.copyOf(key);
  }

  private static String encodeKey(List<Literal> values) {
    StringBuilder encoded = new StringBuilder(values.size() * 16);
    for (Literal value : values) {
      String part = encodeLiteral(value);
      encoded.append(part.length()).append(':').append(part);
    }
    return encoded.toString();
  }

  private static String encodeLiteral(Literal value) {
    return value.getValue() == null ? "N" : "V" + value;
  }

  private static final class PartitionBuffer {
    private final Map<String, Literal> partitionValues;
    private final List<BufferedSelection> selections = new ArrayList<>();
    private ArrowKernelRows currentBatch;
    private BufferedSelection currentSelection;

    private PartitionBuffer(Map<String, Literal> partitionValues) {
      this.partitionValues = Map.copyOf(partitionValues);
    }

    private BufferedSelection selectionFor(ArrowKernelRows batch) {
      if (currentBatch != batch) {
        batch.retain();
        currentBatch = batch;
        currentSelection = new BufferedSelection(batch);
        selections.add(currentSelection);
      }
      return currentSelection;
    }

    private List<FilteredColumnarBatch> drain() {
      List<FilteredColumnarBatch> batches = new ArrayList<>(selections.size());
      for (BufferedSelection selection : selections) {
        FilteredColumnarBatch batch = selection.drain();
        if (batch != null) {
          batches.add(batch);
        }
      }
      selections.clear();
      currentBatch = null;
      currentSelection = null;
      return batches;
    }

    private void close() {
      selections.forEach(BufferedSelection::close);
      selections.clear();
    }
  }

  private static final class BufferedSelection {
    private final ArrowKernelRows batch;
    private final List<Integer> rowIds = new ArrayList<>();
    private final List<Boolean> live = new ArrayList<>();
    private boolean closed;

    private BufferedSelection(ArrowKernelRows batch) {
      this.batch = batch;
    }

    private BufferedRow add(int rowId) {
      int position = rowIds.size();
      rowIds.add(rowId);
      live.add(true);
      return new BufferedRow(this, position);
    }

    private void remove(int position) {
      live.set(position, false);
    }

    private FilteredColumnarBatch drain() {
      int count = 0;
      for (boolean keep : live) {
        count += keep ? 1 : 0;
      }
      if (count == 0) {
        close();
        return null;
      }
      int[] selected = new int[count];
      int output = 0;
      for (int i = 0; i < live.size(); i++) {
        if (live.get(i)) {
          selected[output++] = rowIds.get(i);
        }
      }
      FilteredColumnarBatch result = batch.selectRows(selected);
      close();
      return result;
    }

    private void close() {
      if (!closed) {
        closed = true;
        batch.close();
      }
    }
  }

  private static final class BufferedRow {
    private final BufferedSelection selection;
    private final int position;

    private BufferedRow(BufferedSelection selection, int position) {
      this.selection = selection;
      this.position = position;
    }

    private void remove() {
      selection.remove(position);
    }
  }
}
