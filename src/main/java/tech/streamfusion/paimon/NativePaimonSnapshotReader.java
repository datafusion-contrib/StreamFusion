package tech.streamfusion.paimon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.IntervalPartition;
import org.apache.paimon.mergetree.compact.PartialUpdateMergeFunction;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowType;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

/** Java plans runs and owns file access; Rust decodes and merges into Arrow. */
public final class NativePaimonSnapshotReader implements AutoCloseable {
  private final FileStoreTable table;
  private final DataSplit split;
  private final List<List<SortedRun>> sections;
  private final Schema input;
  private final Schema output;
  private final String[] names;
  private final int keyCount;
  private final int[] sequenceColumns;
  private final int batchRows;
  private final long budget;
  private int sectionIndex;
  private List<SortedRun> runs;
  private NativePaimonFileReader[] readers;
  private int[] fileIndices;
  private long handle;
  private long peakBytes;

  /** Returns null before emission when the split retains Java snapshot semantics. */
  static NativePaimonSnapshotReader create(
      FileStoreTable table,
      DataSplit split,
      org.apache.flink.table.types.logical.RowType outputType,
      int batchRows)
      throws IOException {
    CoreOptions options = table.coreOptions();
    boolean partial = options.mergeEngine() == CoreOptions.MergeEngine.PARTIAL_UPDATE;
    boolean removeOnDelete =
        options.toConfiguration().get(CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE);
    if (partial) {
      if (options.toConfiguration().toMap().keySet().stream()
          .anyMatch(k -> k.endsWith("sequence-group") || k.contains("aggregate-function")))
        return null;
      // Keep configuration validation with the released Java implementation.
      PartialUpdateMergeFunction.factory(
          options.toConfiguration(), table.rowType(), table.primaryKeys());
    }
    if (!PaimonVersion.hasKeyValueStore(table)
        || PaimonVersion.hasBeforeFiles(split)
        || split.isStreaming()
        || split.rawConvertible()
        || (table.bucketMode() != BucketMode.HASH_FIXED
            && table.bucketMode() != BucketMode.HASH_DYNAMIC)
        || (options.mergeEngine() != CoreOptions.MergeEngine.DEDUPLICATE
            && options.mergeEngine() != CoreOptions.MergeEngine.FIRST_ROW
            && !partial)
        || options.sortEngine() != CoreOptions.SortEngine.LOSER_TREE
        || split.deletionFiles().isPresent()
        || split.dataFiles().stream()
            .anyMatch(
                f ->
                    f.schemaId() != table.schema().id()
                        || !PaimonCodecs.available(f.fileFormat())
                        || f.minSequenceNumber() < 0
                        || f.maxSequenceNumber() < f.minSequenceNumber()
                        || ((options.mergeEngine() == CoreOptions.MergeEngine.FIRST_ROW
                                || (partial && !removeOnDelete))
                            && !options.ignoreDelete()
                            && f.deleteRowCount().orElse(1L) != 0))) {
      return null;
    }
    var keys = table.schema().trimmedPrimaryKeysFields();
    if (keys.isEmpty()
        || keys.stream().anyMatch(f -> !comparable(f.type()))
        || PaimonArrowFields.unsupportedTypeReason(new RowType(keys)) != null) {
      return null;
    }
    for (String name : options.sequenceField()) {
      var type = table.rowType().getTypeAt(table.rowType().getFieldNames().indexOf(name));
      if (!comparable(type)
          || PaimonArrowFields.unsupportedTypeReason(
                  new RowType(List.of(new org.apache.paimon.types.DataField(0, name, type))))
              != null) return null;
    }
    for (DataFileMeta file : split.dataFiles()) {
      if (orcTimestampOrderingRisk(file, new RowType(keys))) return null;
    }
    List<List<SortedRun>> sections =
        new IntervalPartition(split.dataFiles(), PaimonVersion.keyComparator(table)).partition();
    for (List<SortedRun> section : sections) {
      if (section.size() > options.sortSpillThreshold()) {
        return null;
      }
    }
    NativePaimonSnapshotReader reader =
        new NativePaimonSnapshotReader(table, split, sections, outputType, batchRows);
    if (!reader.withinRowGroupBudget()) {
      return null;
    }
    return reader;
  }

  private static boolean comparable(org.apache.paimon.types.DataType type) {
    return PaimonKeyValueLayout.comparableNatively(type)
        || type.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.FLOAT
        || type.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.DOUBLE;
  }

  private static boolean orcTimestampOrderingRisk(DataFileMeta file, RowType keys) {
    if (!file.fileFormat().equals("orc")) return false;
    for (int i = 0; i < keys.getFieldCount(); i++) {
      var type = keys.getTypeAt(i);
      int precision =
          type instanceof org.apache.paimon.types.TimestampType
              ? ((org.apache.paimon.types.TimestampType) type).getPrecision()
              : type instanceof org.apache.paimon.types.LocalZonedTimestampType
                  ? ((org.apache.paimon.types.LocalZonedTimestampType) type).getPrecision()
                  : 0;
      // Java ORC aliases fractional timestamps in the last negative second to the first
      // positive second. Decoded keys can therefore violate a sorted run's ordering.
      if (precision > 0
          && (i != 0
              || (file.minKey().getTimestamp(i, precision).getMillisecond() < 0
                  && file.maxKey().getTimestamp(i, precision).getMillisecond() >= -1000)))
        return true;
    }
    return false;
  }

  private NativePaimonSnapshotReader(
      FileStoreTable table,
      DataSplit split,
      List<List<SortedRun>> sections,
      org.apache.flink.table.types.logical.RowType outputType,
      int batchRows) {
    this.table = table;
    this.split = split;
    this.sections = sections;
    this.batchRows = batchRows;
    this.budget = table.coreOptions().sortSpillBufferSize();
    RowType keys =
        new RowType(
            PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR.keyFields(table.schema()));
    keyCount = keys.getFieldCount();
    RowType values = (RowType) LogicalTypeConversion.toDataType(outputType);
    var valueFields = new ArrayList<>(values.getFields());
    for (String name : table.coreOptions().sequenceField()) {
      if (!outputType.getFieldNames().contains(name)) {
        valueFields.add(
            table.rowType().getFields().get(table.rowType().getFieldNames().indexOf(name)));
      }
    }
    values = new RowType(valueFields);
    var valueNames = values.getFieldNames();
    sequenceColumns =
        table.coreOptions().sequenceField().stream()
            .mapToInt(name -> keyCount + 2 + valueNames.indexOf(name))
            .toArray();
    RowType internal = KeyValue.schema(keys, values);
    input = ArrowConversion.toArrowSchema(LogicalTypeConversion.toLogicalType(internal));
    names = internal.getFieldNames().toArray(String[]::new);
    List<Field> fields = new ArrayList<>(ArrowConversion.toArrowSchema(outputType).getFields());
    fields.add(Field.nullable(RowDataArrowConverter.ROW_KIND_COLUMN, new ArrowType.Int(8, true)));
    output = new Schema(fields);
  }

  private NativePaimonFileReader open(DataFileMeta file) throws IOException {
    return new NativePaimonFileReader(
        PaimonCodecs.reader(
            file.fileFormat(),
            PaimonVersion.legacyOrcTimestamp(table.coreOptions().toConfiguration())),
        table.fileIO(),
        new Path(file.externalPath().orElse(split.bucketPath() + "/" + file.fileName())),
        file.fileSize(),
        input,
        names,
        batchRows);
  }

  private boolean withinRowGroupBudget() throws IOException {
    for (List<SortedRun> section : sections) {
      long total = 0;
      for (SortedRun run : section) {
        long maximum = 0;
        for (DataFileMeta file : run.files()) {
          try (NativePaimonFileReader reader = open(file)) {
            maximum = Math.max(maximum, reader.readerMemory());
          }
        }
        if (maximum > budget / 2 - total) {
          return false;
        }
        total += maximum;
      }
    }
    return true;
  }

  public VectorSchemaRoot next() throws IOException {
    while (true) {
      if (handle == 0) {
        if (sectionIndex == sections.size()) {
          return null;
        }
        runs = sections.get(sectionIndex++);
        readers = new NativePaimonFileReader[runs.size()];
        fileIndices = new int[runs.size()];
        try (ArrowSchema in = ArrowSchema.allocateNew(NativeAllocator.SHARED);
            ArrowSchema out = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
          try {
            Data.exportSchema(NativeAllocator.SHARED, input, NativeAllocator.DICTIONARIES, in);
            Data.exportSchema(NativeAllocator.SHARED, output, NativeAllocator.DICTIONARIES, out);
            handle =
                NativePaimon.createSnapshotMerger(
                    this,
                    in.memoryAddress(),
                    out.memoryAddress(),
                    keyCount,
                    runs.size(),
                    batchRows,
                    budget,
                    sequenceColumns,
                    table.coreOptions().sequenceFieldSortOrderIsAscending(),
                    table.coreOptions().mergeEngine() == CoreOptions.MergeEngine.FIRST_ROW,
                    table.coreOptions().ignoreDelete(),
                    table.coreOptions().mergeEngine() == CoreOptions.MergeEngine.PARTIAL_UPDATE,
                    table
                        .coreOptions()
                        .toConfiguration()
                        .get(CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE));
          } finally {
            if (in.snapshot().release != 0) {
              in.release();
            }
            if (out.snapshot().release != 0) {
              out.release();
            }
          }
        }
      }
      VectorSchemaRoot result =
          NativePaimonFileReader.importBatch(
              (array, schema) -> NativePaimon.snapshotMergerNext(handle, array, schema));
      peakBytes = Math.max(peakBytes, NativePaimon.snapshotMergerPeakBytes(handle));
      if (result != null) {
        return result;
      }
      closeSection();
    }
  }

  /** Native callback. All reads stay on the calling source fetcher thread. */
  public boolean nextRunBatch(int run, long array, long schema) throws IOException {
    if (Thread.currentThread().isInterrupted()) {
      throw new java.io.InterruptedIOException("Paimon snapshot read cancelled");
    }
    while (true) {
      if (readers[run] == null) {
        List<DataFileMeta> files = runs.get(run).files();
        if (fileIndices[run] == files.size()) {
          return false;
        }
        readers[run] = open(files.get(fileIndices[run]++));
      }
      if (readers[run].exportNext(array, schema)) {
        return true;
      }
      readers[run].close();
      readers[run] = null;
    }
  }

  public long peakRetainedBytes() {
    return peakBytes;
  }

  private void closeSection() throws IOException {
    try {
      if (handle != 0) {
        NativePaimon.closeSnapshotMerger(handle);
        handle = 0;
      }
    } finally {
      IOException failure = null;
      if (readers != null) {
        for (NativePaimonFileReader reader : readers) {
          if (reader != null) {
            try {
              reader.close();
            } catch (IOException e) {
              if (failure == null) {
                failure = e;
              } else {
                failure.addSuppressed(e);
              }
            }
          }
        }
        readers = null;
      }
      if (failure != null) {
        throw failure;
      }
    }
  }

  @Override
  public void close() throws IOException {
    closeSection();
  }
}
