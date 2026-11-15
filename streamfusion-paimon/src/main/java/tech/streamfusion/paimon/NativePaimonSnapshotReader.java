package tech.streamfusion.paimon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.flink.LogicalTypeConversion;
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

/** Java checks split admission; released paimon-rust plans runs, decodes and merges into Arrow. */
public final class NativePaimonSnapshotReader implements AutoCloseable {
  private final FileStoreTable table;
  private final DataSplit split;
  private final List<List<SortedRun>> sections;
  private final Schema input;
  private final org.apache.flink.table.types.logical.RowType outputType;
  private PaimonRustReader reader;
  private final String[] names;
  private final int batchRows;
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
                        || !PaimonRustReader.supports(table, f)
                        || f.minSequenceNumber() < 0
                        || f.maxSequenceNumber() < f.minSequenceNumber()
                        || f.rowCount() - 1 > f.maxSequenceNumber() - f.minSequenceNumber()
                        || (partial
                            && (options.ignoreDelete()
                                || (removeOnDelete && PaimonVersion.partialUpdateDeleteRemovalNeedsJava()))
                            && f.deleteRowCount().orElse(1L) != 0)
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
      if (section.size() > Math.min(256, options.sortSpillThreshold())
          || PaimonReadPreflight.overlappingSequences(section)) {
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
    this.outputType = outputType;
    this.table = table;
    this.split = split;
    this.sections = sections;
    this.batchRows = batchRows;
    RowType keys =
        new RowType(
            PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR.keyFields(table.schema()));
    RowType values = (RowType) LogicalTypeConversion.toDataType(outputType);
    var valueFields = new ArrayList<>(values.getFields());
    for (String name : table.coreOptions().sequenceField()) {
      if (!outputType.getFieldNames().contains(name)) {
        valueFields.add(
            table.rowType().getFields().get(table.rowType().getFieldNames().indexOf(name)));
      }
    }
    values = new RowType(valueFields);
    RowType internal = KeyValue.schema(keys, values);
    input =
        tech.streamfusion.arrow.ArrowFileBatch.schema(
            ArrowConversion.toArrowSchema(LogicalTypeConversion.toLogicalType(internal)));
    names = internal.getFieldNames().toArray(String[]::new);
  }

  private boolean withinRowGroupBudget() throws IOException {
    return PaimonReadPreflight.withinBudget(
        table, sections, input, names, split.bucketPath(), batchRows);
  }

  public VectorSchemaRoot next() throws IOException {
    if (reader == null) reader = new PaimonRustReader(table, split, outputType, batchRows, false);
    VectorSchemaRoot result = reader.next();
    peakBytes = Math.max(peakBytes, reader.reservedBytes());
    return result;
  }

  public long peakRetainedBytes() {
    return peakBytes;
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      try {
        reader.close();
      } finally {
        reader = null;
      }
    }
  }
}
