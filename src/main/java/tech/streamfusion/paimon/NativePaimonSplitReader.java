package tech.streamfusion.paimon;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.NativeSourceRecord;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.operator.WatermarkExpression;

/** Native and Java Paimon split reads under one logical-row checkpoint. */
public final class NativePaimonSplitReader
    implements SplitReader<NativeSourceRecord, FileStoreSourceSplit> {
  private final FileStoreTable table;
  private final TableRead stock;
  private FileStoreSourceReaderMetrics metrics;
  private final RowType outputType;
  private final boolean primaryKey;
  private final int batchRows;
  private final int rowtimeIndex;
  private final WatermarkExpression.Evaluator watermarkExpression;
  private final Queue<FileStoreSourceSplit> splits = new ArrayDeque<>();
  private final RowDataSerializer copy;
  private FileStoreSourceSplit current;
  private long position;
  private long skip;
  private RecordReader<InternalRow> rows;
  private RecordReader.RecordIterator<InternalRow> rowBatch;
  private Queue<DataFileMeta> files;
  private NativePaimonFileReader decoder;
  private NativePaimonSnapshotReader snapshot;
  private boolean nativeSnapshotsEnabled = true;
  private int nativeSnapshots;
  private volatile boolean wakeup;
  private int nativeFiles;

  public int nativeFilesRead() {
    return nativeFiles;
  }

  public int nativeSnapshotsRead() {
    return nativeSnapshots;
  }

  NativePaimonSplitReader withNativeSnapshots(boolean enabled) {
    nativeSnapshotsEnabled = enabled;
    return this;
  }

  public NativePaimonSplitReader(
      FileStoreTable table, ReadBuilder read, TableRead stock, int batchRows, int rowtimeIndex) {
    this(table, read, stock, batchRows, rowtimeIndex, WatermarkExpression.rowtime(rowtimeIndex));
  }

  public NativePaimonSplitReader(
      FileStoreTable table,
      ReadBuilder read,
      TableRead stock,
      int batchRows,
      int rowtimeIndex,
      WatermarkExpression watermarkExpression) {
    this.table = table;
    this.stock = stock;
    this.outputType = LogicalTypeConversion.toLogicalType(read.readType());
    this.primaryKey = !table.primaryKeys().isEmpty();
    this.batchRows = batchRows;
    this.rowtimeIndex = rowtimeIndex;
    this.copy = new RowDataSerializer(outputType);
    this.watermarkExpression = watermarkExpression == null ? null : watermarkExpression.open();
  }

  NativePaimonSplitReader withMetrics(FileStoreSourceReaderMetrics metrics) {
    this.metrics = metrics;
    return this;
  }

  @Override
  public RecordsWithSplitIds<NativeSourceRecord> fetch() throws IOException {
    if (wakeup) {
      wakeup = false;
      return new Records(null, null, false);
    }
    if (current == null) {
      current = splits.remove();
      if (metrics != null && current.split() instanceof DataSplit) {
        metrics.recordSnapshotUpdate(
            ((DataSplit) current.split())
                .earliestFileCreationEpochMillis()
                .orElse(FileStoreSourceReaderMetrics.UNDEFINED));
      }
      position = current.recordsToSkip();
      skip = position;
      if (canDecode(current)) {
        files = new ArrayDeque<>(((DataSplit) current.split()).dataFiles());
      } else {
        if (nativeSnapshotsEnabled && current.split() instanceof DataSplit) {
          snapshot =
              NativePaimonSnapshotReader.create(
                  table, (DataSplit) current.split(), outputType, batchRows);
        }
        if (snapshot != null) {
          nativeSnapshots++;
        } else {
          rows = stock.createReader(current.split());
        }
      }
    }
    while (true) {
      VectorSchemaRoot root =
          snapshot != null ? snapshot.next() : files != null ? nativeBatch() : javaBatch();
      if (root == null) {
        if (skip > 0) {
          throw new IOException(
              "Paimon checkpoint skips beyond the end of split " + current.splitId());
        }
        String id = current.splitId();
        closeCurrent();
        return new Records(id, null, true);
      }
      if (skip >= root.getRowCount()) {
        skip -= root.getRowCount();
        root.close();
        continue;
      }
      if (skip > 0) {
        // A restart may land inside a different batch size; offsets count logical rows, not
        // batches.
        VectorSchemaRoot sliced =
            new VectorSchemaRoot(
                root.getFieldVectors().stream()
                    .map(
                        vector -> {
                          var transfer = vector.getTransferPair(NativeAllocator.SHARED);
                          transfer.splitAndTransfer(
                              (int) skip, vector.getValueCount() - (int) skip);
                          return (FieldVector) transfer.getTo();
                        })
                    .collect(Collectors.toList()));
        sliced.setRowCount(root.getRowCount() - (int) skip);
        root.close();
        root = sliced;
        skip = 0;
      }
      position += root.getRowCount();
      return new Records(
          current.splitId(),
          NativeSourceRecord.fromRoot(root, position, rowtimeIndex, watermarkExpression),
          false);
    }
  }

  private boolean canDecode(FileStoreSourceSplit split) throws IOException {
    if (!(split.split() instanceof DataSplit)) {
      return false;
    }
    DataSplit data = (DataSplit) split.split();
    // Historical schemas and deletion-vector selections retain Java's mapping and filtering.
    boolean eligible =
        (!primaryKey
                || data.isStreaming()
                || (nativeSnapshotsEnabled
                    && data.rawConvertible()
                    && data.dataFiles().stream().allMatch(f -> f.deleteRowCount().isPresent())))
            && data.deletionFiles().isEmpty()
            && data.dataFiles().stream()
                .allMatch(
                    f ->
                        f.schemaId() == table.schema().id()
                            && PaimonCodecs.available(f.fileFormat()));
    if (!eligible) return false;
    for (DataFileMeta file : data.dataFiles()) {
      if (file.fileFormat().equals("orc")) {
        try (NativePaimonFileReader reader = openFile(file, data)) {
          if (reader.readerMemory() == Long.MAX_VALUE) return false;
        }
      }
    }
    return true;
  }

  private VectorSchemaRoot nativeBatch() throws IOException {
    while (true) {
      if (decoder == null) {
        DataFileMeta file = files.poll();
        if (file == null) {
          return null;
        }
        DataSplit split = (DataSplit) current.split();
        nativeFiles++;
        decoder = openFile(file, split);
      }
      VectorSchemaRoot root = decoder.next();
      if (root != null) {
        return root;
      }
      decoder.close();
      decoder = null;
    }
  }

  private NativePaimonFileReader openFile(DataFileMeta file, DataSplit split) throws IOException {
    List<Field> fields = new ArrayList<>(ArrowConversion.toArrowSchema(outputType).getFields());
    List<String> names = new ArrayList<>(outputType.getFieldNames());
    if (primaryKey && split.isStreaming()) {
      fields.add(Field.nullable(RowDataArrowConverter.ROW_KIND_COLUMN, new ArrowType.Int(8, true)));
      names.add("_VALUE_KIND");
    }
    return new NativePaimonFileReader(
        PaimonCodecs.reader(
            file.fileFormat(),
            table
                .coreOptions()
                .toConfiguration()
                .get(org.apache.paimon.format.OrcOptions.ORC_TIMESTAMP_LTZ_LEGACY_TYPE)),
        table.fileIO(),
        new Path(file.externalPath().orElse(split.bucketPath() + "/" + file.fileName())),
        file.fileSize(),
        new Schema(fields),
        names.toArray(String[]::new),
        batchRows);
  }

  private VectorSchemaRoot javaBatch() throws IOException {
    List<RowData> batch = new ArrayList<>();
    while (batch.size() < batchRows) {
      if (rowBatch == null) {
        rowBatch = rows.readBatch();
        if (rowBatch == null) {
          break;
        }
      }
      InternalRow row = rowBatch.next();
      if (row == null) {
        rowBatch.releaseBatch();
        rowBatch = null;
      } else {
        batch.add(tech.streamfusion.compat.RuntimeCompat.copyRow(copy, new FlinkRowData(row)));
      }
    }
    return batch.isEmpty()
        ? null
        : RowDataArrowConverter.write(batch, outputType, NativeAllocator.SHARED, primaryKey);
  }

  private void closeCurrent() throws IOException {
    try {
      if (rowBatch != null) {
        rowBatch.releaseBatch();
      }
    } finally {
      rowBatch = null;
      try {
        if (rows != null) {
          rows.close();
        }
      } finally {
        rows = null;
        try {
          if (decoder != null) {
            decoder.close();
          }
        } finally {
          try {
            if (snapshot != null) {
              snapshot.close();
            }
          } finally {
            snapshot = null;
            decoder = null;
            files = null;
            current = null;
          }
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    try {
      closeCurrent();
    } finally {
      if (watermarkExpression != null) {
        watermarkExpression.close();
      }
    }
  }

  @Override
  public void wakeUp() {
    wakeup = true;
  }

  @Override
  public void handleSplitsChanges(SplitsChange<FileStoreSourceSplit> changes) {
    if (!(changes instanceof SplitsAddition)) {
      throw new UnsupportedOperationException("Paimon split removal");
    }
    splits.addAll(changes.splits());
  }

  private static final class Records implements RecordsWithSplitIds<NativeSourceRecord> {
    private final String id;
    private NativeSourceRecord record;
    private final boolean finished;
    private boolean visited;

    Records(String id, NativeSourceRecord record, boolean finished) {
      this.id = id;
      this.record = record;
      this.finished = finished;
    }

    @Override
    public String nextSplit() {
      if (visited || finished) {
        return null;
      }
      visited = true;
      return id;
    }

    @Override
    public NativeSourceRecord nextRecordFromSplit() {
      NativeSourceRecord result = record;
      record = null;
      return result;
    }

    @Override
    public Set<String> finishedSplits() {
      return finished ? Set.of(id) : Collections.emptySet();
    }

    @Override
    public void recycle() {
      if (record != null && record.batch() != null) {
        record.batch().root().close();
        record = null;
      }
    }
  }
}
