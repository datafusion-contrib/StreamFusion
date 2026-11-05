package tech.streamfusion.paimon;

import java.lang.reflect.Method;
import java.util.List;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.BucketedAppendCompactManager.CompactRewriter;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RowDataRollingFileWriter;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.LongCounter;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ArrowFileBatch;

/** Copies Java-selected append files in order through released Rust readers and stock writers. */
final class NativePaimonAppendRewriter implements CompactRewriter {
  private static final Method WRITER = PaimonVersion.appendWriterFactory();
  private final FileStoreTable table;
  private final Object write;
  private final BinaryRow partition;
  private final int bucket;
  private final CompactRewriter stock;
  private final NativePaimonCompactRewriter.Counters counters;
  String fallbackReason;

  NativePaimonAppendRewriter(
      FileStoreTable table,
      Object write,
      BinaryRow partition,
      int bucket,
      CompactRewriter stock,
      NativePaimonCompactRewriter.Counters counters) {
    this.table = table;
    this.write = write;
    this.partition = partition.copy();
    this.bucket = bucket;
    this.stock = stock;
    this.counters = counters;
  }

  static Method writerFactory(Class<?> type, Class<?> sequence) {
    try {
      var method =
          type.getDeclaredMethod("createRollingFileWriter", BinaryRow.class, int.class, sequence);
      return method.getReturnType() == RowDataRollingFileWriter.class && method.trySetAccessible()
          ? method
          : null;
    } catch (ReflectiveOperationException | RuntimeException | LinkageError unavailable) {
      return null;
    }
  }

  static boolean available() {
    return WRITER != null;
  }

  @Override
  public List<DataFileMeta> rewrite(List<DataFileMeta> files) throws Exception {
    fallbackReason = unsupportedReason(files);
    if (fallbackReason != null) return stockRewrite(files);
    var type = LogicalTypeConversion.toLogicalType(table.rowType());
    String bucketPath = table.store().pathFactory().bucketPath(partition, bucket).toString();
    int batchRows = table.coreOptions().toConfiguration().get(CoreOptions.READ_BATCH_SIZE);
    if (fallbackReason == null
        && !PaimonReadPreflight.rawWithinBudget(
            table,
            files,
            ArrowFileBatch.schema(ArrowConversion.toArrowSchema(type)),
            table.rowType().getFieldNames().toArray(String[]::new),
            bucketPath,
            batchRows)) fallbackReason = "projected-row-group-budget";
    if (fallbackReason != null) return stockRewrite(files);
    var sequence = new LongCounter(files.get(0).minSequenceNumber());
    var output =
        (RowDataRollingFileWriter)
            WRITER.invoke(write, partition, bucket, PaimonVersion.appendSequenceCounter(sequence));
    try (output) {
      try {
        long written = 0;
        for (var file : files) {
          var split =
              DataSplit.builder()
                  .withPartition(partition)
                  .withBucket(bucket)
                  .withBucketPath(bucketPath)
                  .withDataFiles(List.of(file))
                  .isStreaming(false)
                  .rawConvertible(true)
                  .build();
          try (var reader = new PaimonRustReader(table, split, type, batchRows, true)) {
            for (var batch = reader.next(); batch != null; batch = reader.next())
              try (var root = batch) {
                PaimonVersion.advanceAppendSequence(sequence, root.getRowCount());
                output.writeBundle(new ArrowBatchBundle(root, type));
                written += root.getRowCount();
              }
          }
        }
        output.close();
        var result = output.result();
        counters.nativeGroups.inc();
        counters.rows.inc(written);
        counters.files.inc(files.size());
        counters.bytes.inc(files.stream().mapToLong(DataFileMeta::fileSize).sum());
        return result;
      } catch (Throwable failure) {
        counters.errors.inc();
        output.abort();
        throw failure;
      }
    }
  }

  private List<DataFileMeta> stockRewrite(List<DataFileMeta> files) throws Exception {
    counters.stockGroups.inc();
    return stock.rewrite(files);
  }

  private String unsupportedReason(List<DataFileMeta> files) {
    if (!available()) return "writer-factory-unavailable";
    if (!PaimonVersion.supportsAppendWrite(write)) return "writer-shape";
    if (files.isEmpty()) return "empty-work";
    if (!table.primaryKeys().isEmpty()) return "primary-key-table";
    var options = table.coreOptions();
    if (options.deletionVectorsEnabled()) return "deletion-vectors";
    if (options.recordLevelExpireTime() != null) return "record-expiry";
    String features = PaimonVersion.compactionFeatures(options);
    if (features != null) return features;
    if (!new FileIndexOptions(options).isEmpty()) return "file-index";
    if (PaimonArrowFields.unsupportedTypeReason(table.rowType()) != null) return "data-types";
    String format = options.fileFormatString();
    if (!List.of("parquet", "orc").contains(format)) return "output-format";
    var writer = FileFormat.fromIdentifier(format, options.toConfiguration());
    if (!(writer instanceof NativePaimonFileFormat) || !PaimonCodecs.available(format))
      return "native-format-unavailable";
    if (((NativePaimonFileFormat) writer)
            .nativeWriterFallbackReason(table.rowType(), options.fileCompression())
        != null) return "writer-settings";
    for (var file : files) {
      if (!PaimonRustReader.supports(table, file)) return "input-format";
      if (file.schemaId() != table.schema().id()) return "schema-evolution";
      if (file.embeddedIndex() != null || !file.extraFiles().isEmpty())
        return "input-index-or-sidecar";
      if (PaimonVersion.specialFileMetadata(file)) return "input-row-tracking-or-evolution";
      if (file.minSequenceNumber() < 0 || file.rowCount() < 0) return "ambiguous-sequences";
    }
    return null;
  }
}
