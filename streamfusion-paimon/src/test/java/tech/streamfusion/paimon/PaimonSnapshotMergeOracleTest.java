package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.paimon.PaimonSnapshotMergeTest.collect;
import static tech.streamfusion.paimon.PaimonSnapshotMergeTest.stock;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Exercise the public file reader, including admission of stock tie ordering. */
class PaimonSnapshotMergeOracleTest {
  private static final RowType VALUES =
      new RowType(
          List.of(
              new DataField(0, "id", DataTypes.INT().notNull()),
              new DataField(1, "seq", DataTypes.DOUBLE()),
              new DataField(2, "a", DataTypes.STRING()),
              new DataField(3, "b", DataTypes.INT())));

  @ParameterizedTest
  @ValueSource(
      strings = {
        "deduplicate",
        "deduplicate-ignore",
        "first-row",
        "first-row-ignore",
        "partial-update",
        "partial-update-ignore",
        "partial-update-remove"
      })
  void releasedFileReadsMatchJavaAcrossSequencesAndBatchBoundaries(String mode) throws Exception {
    boolean first = mode.startsWith("first-row");
    boolean partial = mode.startsWith("partial-update");
    boolean ignore = mode.endsWith("ignore");
    boolean remove = mode.endsWith("remove");
    for (int sequenceMode = 0; sequenceMode < 3; sequenceMode++) {
      var options = new java.util.HashMap<String, String>();
      options.put("bucket", "1");
      options.put("merge-engine", first ? "first-row" : partial ? "partial-update" : "deduplicate");
      options.put("ignore-delete", Boolean.toString(ignore));
      options.put("partial-update.remove-record-on-delete", Boolean.toString(remove));
      if (sequenceMode != 0 && !first) {
        options.put("sequence.field", "seq");
        options.put("sequence.field.sort-order", sequenceMode == 1 ? "ascending" : "descending");
      }
      var table = (PrimaryKeyFileStoreTable) PaimonMergeEngineTest.table(options, VALUES);
      var store = table.store();
      var factory =
          KeyValueFileWriterFactory.builder(
                  table.fileIO(),
                  table.schema().id(),
                  new RowType(
                      org.apache.paimon.table.PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor
                          .EXTRACTOR
                          .keyFields(table.schema())),
                  VALUES,
                  FileFormat.fromIdentifier(
                      table.coreOptions().fileFormatString(),
                      table.coreOptions().toConfiguration()),
                  ignored -> store.pathFactory(),
                  Long.MAX_VALUE)
              .build(BinaryRow.EMPTY_ROW, 0, table.coreOptions());
      for (int fanIn : new int[] {1, 2, 7, 16}) {
        for (boolean tied : new boolean[] {false, true}) {
          var random = new Random(7919L * fanIn + sequenceMode);
          var files = new ArrayList<DataFileMeta>();
          for (int run = 0; run < fanIn; run++) {
            // Empty runs and absent keys must not change output or leak streams.
            if (run % 7 == 5) continue;
            var writer = factory.createRollingMergeTreeFileWriter(0, FileSource.APPEND);
            try (writer) {
              for (int key = 0; key < 37; key++) {
                if (random.nextInt(4) == 0) continue;
                int copies = tied ? 1 + random.nextInt(2) : 1;
                for (int duplicate = 0; duplicate < copies; duplicate++) {
                  byte kind = (byte) (random.nextInt(2) * 2);
                  if (ignore || remove || (!first && !partial)) kind = (byte) random.nextInt(4);
                  Double sequence =
                      new Double[] {null, -0.0, 0.0, Double.NaN, 1.0}[random.nextInt(5)];
                  writer.write(
                      new KeyValue()
                          .replace(
                              GenericRow.of(key),
                              tied ? 0 : run * 100L + key,
                              RowKind.fromByteValue(kind),
                              GenericRow.of(
                                  key,
                                  sequence,
                                  random.nextBoolean()
                                      ? null
                                      : BinaryString.fromString("r" + run + "-k" + key),
                                  random.nextBoolean() ? null : random.nextInt())));
                }
              }
            }
            files.addAll(writer.result());
          }
          var split =
              DataSplit.builder()
                  .withSnapshot(1)
                  .withPartition(BinaryRow.EMPTY_ROW)
                  .withBucket(0)
                  .withBucketPath(store.pathFactory().bucketPath(BinaryRow.EMPTY_ROW, 0).toString())
                  .withDataFiles(files)
                  .isStreaming(false)
                  .rawConvertible(false)
                  .build();
          for (int[] projection : List.of(new int[] {0, 1, 2, 3}, new int[] {3, 2})) {
            var read = table.newReadBuilder().withProjection(projection);
            var expected = stock(read, split);
            for (int batchRows : new int[] {1, 7}) {
              var actual = collect(table, read, split, true, batchRows, 0, Integer.MAX_VALUE);
              assertEquals(
                  expected, actual.rows(), mode + "/" + sequenceMode + "/" + fanIn + "/" + tied);
              assertEquals(
                  tied
                          || (partial
                              && (ignore || remove)
                              && files.stream().anyMatch(f -> f.deleteRowCount().orElse(1L) != 0))
                      ? 0
                      : 1,
                  actual.merged(),
                  "Overlapping file sequence ranges retain Java; proven reads must execute"
                      + " natively");
            }
          }
        }
      }
    }
    assertEquals("", NativePaimon.liveNativeHandles());
  }
}
