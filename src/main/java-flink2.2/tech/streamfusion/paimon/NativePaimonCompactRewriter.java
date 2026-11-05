package tech.streamfusion.paimon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.CompactRewriter;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.arrow.ArrowFileBatch;

/** Paimon selects and commits the work; the public Rust reader and format writer process files. */
final class NativePaimonCompactRewriter implements CompactRewriter {
  private static final Logger LOG = LoggerFactory.getLogger(NativePaimonCompactRewriter.class);
  private final FileStoreTable table;
  private final BinaryRow partition;
  private final int bucket;
  private final CompactRewriter stock;
  final Counter nativeGroups, stockGroups, rows, files, bytes, errors;
  private boolean closed;
  String fallbackReason;

  NativePaimonCompactRewriter(
      FileStoreTable table,
      BinaryRow partition,
      int bucket,
      CompactRewriter stock,
      MetricGroup metrics) {
    this(table, partition, bucket, stock, new Counters(metrics));
  }

  NativePaimonCompactRewriter(
      FileStoreTable table,
      BinaryRow partition,
      int bucket,
      CompactRewriter stock,
      Counters metrics) {
    this.table = table;
    this.partition = partition;
    this.bucket = bucket;
    this.stock = stock;
    nativeGroups = metrics.nativeGroups;
    stockGroups = metrics.stockGroups;
    rows = metrics.rows;
    files = metrics.files;
    bytes = metrics.bytes;
    errors = metrics.errors;
  }

  static final class Counters {
    final Counter nativeGroups, stockGroups, rows, files, bytes, errors;

    Counters(MetricGroup group) {
      nativeGroups = group.counter("nativeRewriteGroups", new ThreadSafeSimpleCounter());
      stockGroups = group.counter("stockRewriteGroups", new ThreadSafeSimpleCounter());
      rows = group.counter("nativeRewriteRows", new ThreadSafeSimpleCounter());
      files = group.counter("nativeRewriteFiles", new ThreadSafeSimpleCounter());
      bytes = group.counter("nativeRewriteBytes", new ThreadSafeSimpleCounter());
      errors = group.counter("nativeRewriteErrors", new ThreadSafeSimpleCounter());
    }
  }

  @Override
  public CompactResult rewrite(int outputLevel, boolean dropDelete, List<List<SortedRun>> sections)
      throws Exception {
    if (closed) throw new IllegalStateException("Paimon compaction rewriter is closed");
    fallbackReason = unsupportedReason(outputLevel, sections);
    if (fallbackReason != null) {
      return stockRewrite(outputLevel, dropDelete, sections);
    }
    var layout = PaimonKeyValueLayout.of(table);
    String bucketPath = table.store().pathFactory().bucketPath(partition, bucket).toString();
    int batchRows = table.coreOptions().toConfiguration().get(CoreOptions.READ_BATCH_SIZE);
    var fileLayout = PaimonKeyValueLayout.of(table, outputLevel);
    if (!PaimonReadPreflight.withinBudget(
        table,
        sections,
        ArrowFileBatch.schema(ArrowConversion.toArrowSchema(fileLayout.flinkFileType)),
        fileLayout.fileType.getFieldNames().toArray(String[]::new),
        bucketPath,
        batchRows)) {
      fallbackReason = "projected-row-group-budget";
      return stockRewrite(outputLevel, dropDelete, sections);
    }
    var writer = new NativePaimonKeyValueFileWriter(table, layout);
    try (var output = writer.compaction(partition, bucket, outputLevel)) {
      try {
        List<DataFileMeta> before = new ArrayList<>();
        long writtenRows = 0;
        for (var section : sections) {
          var selected =
              section.stream().flatMap(run -> run.files().stream()).collect(Collectors.toList());
          before.addAll(selected);
          // Compaction has no scan snapshot. Paimon's public split defaults to its unbound id;
          // explicit file descriptors are the entire read, including pending uncommitted L0 files.
          var split =
              DataSplit.builder()
                  .withPartition(partition)
                  .withBucket(bucket)
                  .withBucketPath(bucketPath)
                  .withDataFiles(selected)
                  .isStreaming(false)
                  .rawConvertible(false)
                  .build();
          try (var reader =
              PaimonRustReader.compaction(table, split, layout, batchRows, dropDelete)) {
            for (var batch = reader.next(); batch != null; batch = reader.next()) {
              try (var root = batch) {
                output.write(root);
                writtenRows += root.getRowCount();
              }
            }
          }
        }
        output.close();
        var after = output.result();
        nativeGroups.inc();
        rows.inc(writtenRows);
        files.inc(before.size());
        bytes.inc(before.stream().mapToLong(DataFileMeta::fileSize).sum());
        return new CompactResult(before, after);
      } catch (Throwable failure) {
        errors.inc();
        output.abort();
        throw failure;
      }
    }
  }

  private CompactResult stockRewrite(int level, boolean dropDelete, List<List<SortedRun>> sections)
      throws Exception {
    LOG.debug("Using Java Paimon compaction: {}", fallbackReason);
    stockGroups.inc();
    return stock.rewrite(level, dropDelete, sections);
  }

  private String unsupportedReason(int level, List<List<SortedRun>> sections) {
    if (sections.isEmpty()
        || sections.stream()
            .anyMatch(
                section -> section.isEmpty() || section.stream().anyMatch(SortedRun::isEmpty)))
      return "empty-work";
    var options = table.coreOptions();
    if (table.primaryKeys().isEmpty()) return "append-table";
    if (table.bucketMode() != BucketMode.HASH_FIXED
        && table.bucketMode() != BucketMode.HASH_DYNAMIC) return "bucket-mode";
    if (options.mergeEngine() != CoreOptions.MergeEngine.DEDUPLICATE) return "merge-engine";
    if (options.changelogProducer() != CoreOptions.ChangelogProducer.NONE
        && options.changelogProducer() != CoreOptions.ChangelogProducer.INPUT)
      return "changelog-producer";
    if (!options.sequenceField().isEmpty()) return "user-sequence";
    if (options.ignoreDelete()) return "ignore-delete";
    if (options.snapshotSequenceOrdering()) return "snapshot-sequence-ordering";
    if (options.sortEngine() != CoreOptions.SortEngine.LOSER_TREE) return "sort-engine";
    if (options.deletionVectorsEnabled()) return "deletion-vectors";
    if (options.recordLevelExpireTime() != null) return "record-expiry";
    if (options.rowTrackingEnabled()) return "row-tracking";
    if (options.dataEvolutionEnabled()) return "data-evolution";
    if (!CoreOptions.blobField(table.options()).isEmpty()) return "blob-fields";
    if (!new FileIndexOptions(options).isEmpty()
        || options.primaryKeyVectorIndexEnabled()
        || options.primaryKeyFullTextIndexEnabled()
        || options.primaryKeyFMIndexEnabled()
        || !options.primaryKeyBTreeIndexColumns().isEmpty()
        || !options.primaryKeyBitmapIndexColumns().isEmpty()
        || !options.primaryKeyMultiValueIndexColumns().isEmpty()) return "file-index";
    if (PaimonKeyValueLayout.unsupportedKeyReason(table) != null
        || PaimonArrowFields.unsupportedTypeReason(table.rowType()) != null) return "data-types";
    if (!options.fileFormatString().equals("parquet")
        || !options
            .fileFormatPerLevel()
            .getOrDefault(level, options.fileFormatString())
            .equals("parquet")) return "output-format";
    var format = FileFormat.fromIdentifier("parquet", options.toConfiguration());
    if (!(format instanceof NativePaimonFileFormat) || !PaimonCodecs.available("parquet"))
      return "native-format-unavailable";
    String writerReason =
        ((NativePaimonFileFormat) format)
            .nativeWriterFallbackReason(
                table.rowType(),
                options.fileCompressionPerLevel().getOrDefault(level, options.fileCompression()));
    if (writerReason != null) return "writer-settings: " + writerReason;
    for (var section : sections) {
      if (section.size() > Math.min(256, options.sortSpillThreshold())) return "sorted-run-count";
      if (PaimonReadPreflight.overlappingSequences(section)) return "overlapping-sequences";
      for (var run : section)
        for (var file : run.files()) {
          if (!file.fileFormat().equals("parquet") || !PaimonRustReader.supports(table, file))
            return "input-format";
          if (file.schemaId() != table.schema().id()) return "schema-evolution";
          if (file.minSequenceNumber() < 0
              || file.maxSequenceNumber() < file.minSequenceNumber()
              || file.rowCount() - 1 > file.maxSequenceNumber() - file.minSequenceNumber())
            return "ambiguous-sequences";
          if (file.embeddedIndex() != null || !file.extraFiles().isEmpty())
            return "input-index-or-sidecar";
          if (file.firstRowId() != null || file.writeCols() != null)
            return "input-row-tracking-or-evolution";
        }
    }
    return null;
  }

  @Override
  public CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception {
    if (closed) throw new IllegalStateException("Paimon compaction rewriter is closed");
    return stock.upgrade(outputLevel, file);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      stock.close();
    }
  }
}
