package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.mergetree.compact.FirstRowMergeFunction;
import org.apache.paimon.mergetree.compact.PartialUpdateMergeFunction;
import org.apache.paimon.mergetree.compact.ReducerMergeFunctionWrapper;
import org.apache.paimon.mergetree.compact.SortMergeReaderWithLoserTree;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.UserDefinedSeqComparator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.compat.ListCollectors;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

class PaimonSnapshotMergeOracleTest {
  static final RowType VALUES =
      new RowType(
          List.of(
              new DataField(0, "id", DataTypes.INT().notNull()),
              new DataField(1, "seq", DataTypes.DOUBLE()),
              new DataField(2, "a", DataTypes.STRING()),
              new DataField(3, "b", DataTypes.INT())));
  static final org.apache.flink.table.types.logical.RowType OUTPUT =
      LogicalTypeConversion.toLogicalType(VALUES);
  static final org.apache.flink.table.types.logical.RowType INPUT =
      LogicalTypeConversion.toLogicalType(
          KeyValue.schema(
              new RowType(List.of(new DataField(0, "_KEY_id", DataTypes.INT().notNull()))),
              VALUES));

  static final class Version {
    private final int key;
    private final long sequence;
    private final byte kind;
    private final Double userSequence;
    private final String a;
    private final Integer b;

    Version(int key, long sequence, byte kind, Double userSequence, String a, Integer b) {
      this.key = key;
      this.sequence = sequence;
      this.kind = kind;
      this.userSequence = userSequence;
      this.a = a;
      this.b = b;
    }

    public int key() {
      return key;
    }

    public long sequence() {
      return sequence;
    }

    public byte kind() {
      return kind;
    }

    public Double userSequence() {
      return userSequence;
    }

    public String a() {
      return a;
    }

    public Integer b() {
      return b;
    }

    KeyValue javaRow() {
      return new KeyValue()
          .replace(
              GenericRow.of(key),
              sequence,
              RowKind.fromByteValue(kind),
              GenericRow.of(key, userSequence, a == null ? null : BinaryString.fromString(a), b));
    }

    RowData arrowRow() {
      return GenericRowData.of(
          key, sequence, kind, key, userSequence, a == null ? null : StringData.fromString(a), b);
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
      Version that = (Version) other;
      return key == that.key
          && sequence == that.sequence
          && kind == that.kind
          && java.util.Objects.equals(userSequence, that.userSequence)
          && java.util.Objects.equals(a, that.a)
          && java.util.Objects.equals(b, that.b);
    }

    @Override
    public int hashCode() {
      int result = 0;
      result = 31 * result + key;
      result = 31 * result + Long.hashCode(sequence);
      result = 31 * result + kind;
      result = 31 * result + java.util.Objects.hashCode(userSequence);
      result = 31 * result + java.util.Objects.hashCode(a);
      result = 31 * result + java.util.Objects.hashCode(b);
      return result;
    }

    @Override
    public String toString() {
      return "Version[key="
          + key
          + ", sequence="
          + sequence
          + ", kind="
          + kind
          + ", userSequence="
          + userSequence
          + ", a="
          + a
          + ", b="
          + b
          + "]";
    }
  }

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
  void exactJavaReductionAcrossTiesEmptyRunsAndBatchBoundaries(String mode) throws Exception {
    boolean first = mode.startsWith("first-row");
    boolean partial = mode.startsWith("partial-update");
    boolean ignore = mode.endsWith("ignore");
    boolean remove = mode.endsWith("remove");
    var options =
        Options.fromMap(
            Map.of(
                "ignore-delete",
                Boolean.toString(ignore),
                "partial-update.remove-record-on-delete",
                Boolean.toString(remove)));
    for (int sequenceMode = 0; sequenceMode < 3; sequenceMode++) {
      for (int fanIn : new int[] {1, 2, 3, 4, 5, 7, 8, 11, 16}) {
        for (int batchRows : new int[] {1, 7}) {
          var random = new Random(7919L * fanIn + sequenceMode);
          var runs = new ArrayList<List<Version>>();
          for (int run = 0; run < fanIn; run++) {
            var rows = new ArrayList<Version>();
            // Entirely empty runs, missing keys, duplicate keys within a run, and exact ties.
            if (run % 7 != 5 && !(fanIn == 16 && batchRows == 7 && sequenceMode == 2)) {
              for (int key = 0; key < 37; key++) {
                for (int duplicate = 0; duplicate < random.nextInt(3); duplicate++) {
                  byte kind = (byte) (random.nextInt(2) * 2);
                  if (ignore || remove || (!first && !partial)) kind = (byte) random.nextInt(4);
                  Double seq = new Double[] {null, -0.0, 0.0, Double.NaN, 1.0}[random.nextInt(5)];
                  rows.add(
                      new Version(
                          key,
                          random.nextInt(3),
                          kind,
                          seq,
                          random.nextBoolean() ? null : "r" + run + "-k" + key + "-" + duplicate,
                          random.nextBoolean() ? null : random.nextInt()));
                }
              }
            }
            runs.add(rows);
          }
          var function =
              first
                  ? FirstRowMergeFunction.factory(options).create(null)
                  : partial
                      ? PartialUpdateMergeFunction.factory(options, VALUES, List.of("id"))
                          .create(null)
                      : DeduplicateMergeFunction.factory(options).create(null);
          var readers = new ArrayList<RecordReader<KeyValue>>();
          for (var run : runs)
            readers.add(new IteratorRecordReader<>(run.stream().map(Version::javaRow).iterator()));
          var expected = new ArrayList<String>();
          try (var java =
              new SortMergeReaderWithLoserTree<KeyValue>(
                  readers,
                  (a, b) -> Integer.compare(a.getInt(0), b.getInt(0)),
                  UserDefinedSeqComparator.create(
                      VALUES, sequenceMode == 0 ? List.of() : List.of("seq"), sequenceMode != 2),
                  new ReducerMergeFunctionWrapper(function))) {
            RecordReader.RecordIterator<KeyValue> batch;
            while ((batch = java.readBatch()) != null) {
              try {
                KeyValue row;
                while ((row = batch.next()) != null) {
                  if (row.valueKind().isAdd())
                    expected.add(
                        row.valueKind().toByteValue()
                            + ":"
                            + PaimonTestTables.render(row.value(), VALUES));
                }
              } finally {
                batch.releaseBatch();
              }
            }
          }
          var actual = new ArrayList<String>();
          try (var allocator = new RootAllocator()) {
            try (var in = ArrowSchema.allocateNew(allocator);
                var out = ArrowSchema.allocateNew(allocator);
                var template = RowDataArrowConverter.write(List.of(), OUTPUT, allocator, true)) {
              Data.exportSchema(
                  allocator,
                  ArrowConversion.toArrowSchema(INPUT),
                  NativeAllocator.DICTIONARIES,
                  in);
              Data.exportSchema(allocator, template.getSchema(), NativeAllocator.DICTIONARIES, out);
              long handle =
                  NativePaimon.createSnapshotMerger(
                      new Provider(runs, batchRows, allocator),
                      in.memoryAddress(),
                      out.memoryAddress(),
                      1,
                      fanIn,
                      batchRows,
                      16 << 20,
                      sequenceMode == 0 ? new int[0] : new int[] {4},
                      sequenceMode != 2,
                      first,
                      ignore,
                      partial,
                      remove);
              try {
                while (true) {
                  try (var batch =
                      NativePaimonFileReader.importBatch(
                          (a, s) -> NativePaimon.snapshotMergerNext(handle, a, s))) {
                    if (batch == null) break;
                    for (var row : RowDataArrowConverter.read(batch, OUTPUT)) {
                      actual.add(
                          row.getRowKind().toByteValue()
                              + ":"
                              + PaimonTestTables.render(
                                  new org.apache.paimon.flink.FlinkRowWrapper(row), VALUES));
                    }
                  }
                }
              } finally {
                NativePaimon.closeSnapshotMerger(handle);
              }
            }
            assertEquals(0, allocator.getAllocatedMemory());
          }
          assertEquals(expected, actual, mode + "/" + sequenceMode + "/" + fanIn + "/" + batchRows);
        }
      }
    }
    assertEquals("", NativePaimon.liveNativeHandles());
  }

  public static final class Provider {
    private final List<List<Version>> runs;
    private final int[] offsets;
    private final int batchRows;
    private final RootAllocator allocator;

    Provider(List<List<Version>> runs, int batchRows, RootAllocator allocator) {
      this.runs = runs;
      this.offsets = new int[runs.size()];
      this.batchRows = batchRows;
      this.allocator = allocator;
    }

    public boolean nextRunBatch(int run, long array, long schema) {
      var rows = runs.get(run);
      if (offsets[run] == rows.size()) return false;
      int end = Math.min(rows.size(), offsets[run] + batchRows);
      try (var root =
          RowDataArrowConverter.write(
              rows.subList(offsets[run], end).stream()
                  .map(Version::arrowRow)
                  .collect(ListCollectors.toList()),
              INPUT,
              allocator)) {
        offsets[run] = end;
        Data.exportVectorSchemaRoot(
            allocator,
            root,
            NativeAllocator.DICTIONARIES,
            ArrowArray.wrap(array),
            ArrowSchema.wrap(schema));
      }
      return true;
    }
  }
}
