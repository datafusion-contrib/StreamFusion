package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.operator.RowDataArrowConverter;

class PaimonMergeEngineTest {
  static final RowType TYPE =
      new RowType(
          List.of(
              new DataField(0, "id", DataTypes.INT().notNull()),
              new DataField(1, "seq", DataTypes.BIGINT()),
              new DataField(2, "seq2", DataTypes.INT()),
              new DataField(3, "v", DataTypes.BIGINT()),
              new DataField(4, "txt", DataTypes.STRING()),
              new DataField(5, "flag", DataTypes.BOOLEAN()),
              new DataField(6, "amount", DataTypes.DECIMAL(6, 2)),
              new DataField(7, "nested", DataTypes.ARRAY(DataTypes.INT())),
              new DataField(8, "op", DataTypes.STRING())));

  static Map<String, String> options(String... pairs) {
    Map<String, String> options = new LinkedHashMap<>();
    for (int i = 0; i < pairs.length; i += 2) {
      options.put(pairs[i], pairs[i + 1]);
    }
    return options;
  }

  static Stream<Map<String, String>> modes() {
    List<Map<String, String>> modes = new ArrayList<>();
    modes.add(options("sequence.field", "seq,seq2", "changelog-producer", "input"));
    modes.add(options("sequence.field", "seq,seq2", "sequence.field.sort-order", "descending"));
    modes.add(options("rowkind.field", "op", "changelog-producer", "input"));
    modes.add(options("rowkind.field", "op", "ignore-delete", "true"));
    modes.add(options("rowkind.field", "op", "ignore-update-before", "true"));
    modes.add(
        options(
            "merge-engine", "first-row", "ignore-delete", "true", "changelog-producer", "lookup"));
    modes.add(options("merge-engine", "partial-update", "ignore-delete", "true"));
    modes.add(
        options(
            "merge-engine", "partial-update", "partial-update.remove-record-on-delete", "true"));
    modes.add(
        options(
            "merge-engine",
            "partial-update",
            "fields.seq,seq2.sequence-group",
            "v,txt,flag,amount,nested"));
    modes.add(
        options(
            "merge-engine",
            "partial-update",
            "fields.seq.sequence-group",
            "v,txt,flag,amount,nested",
            "partial-update.remove-record-on-sequence-group",
            "seq"));
    modes.add(
        options(
            "merge-engine",
            "partial-update",
            "fields.seq.sequence-group",
            "v,txt,flag,amount,nested",
            "fields.v.aggregate-function",
            "sum",
            "fields.txt.aggregate-function",
            "last_non_null_value",
            "fields.txt.ignore-retract",
            "true"));
    Map<String, String> aggregates =
        options(
            "merge-engine",
            "aggregation",
            "fields.v.aggregate-function",
            "sum",
            "fields.amount.aggregate-function",
            "sum",
            "fields.flag.aggregate-function",
            "bool_or",
            "fields.txt.aggregate-function",
            "listagg",
            "fields.txt.list-agg-delimiter",
            "|");
    for (String name : List.of("seq", "seq2", "flag", "txt", "nested", "op")) {
      aggregates.put("fields." + name + ".ignore-retract", "true");
    }
    modes.add(aggregates);
    modes.add(options("merge-engine", "aggregation"));
    modes.add(options("merge-engine", "aggregation", "fields.v.aggregate-function", "last_value"));
    Map<String, String> removeAgg = new LinkedHashMap<>(aggregates);
    removeAgg.keySet().removeIf(k -> k.endsWith("ignore-retract"));
    removeAgg.put("aggregation.remove-record-on-delete", "true");
    removeAgg.put("ignore-update-before", "true");
    modes.add(removeAgg);
    for (String function :
        List.of(
            "first_value",
            "last_value",
            "first_non_null_value",
            "first_not_null_value",
            "last_non_null_value",
            "min",
            "max",
            "product")) {
      modes.add(
          options(
              "merge-engine",
              "aggregation",
              "ignore-delete",
              "true",
              "fields.v.aggregate-function",
              function));
    }
    return modes.stream();
  }

  @ParameterizedTest
  @MethodSource("modes")
  void matchesReleasedWriterAcrossCheckpointsCompactionAndRestart(Map<String, String> mode)
      throws Exception {
    FileStoreTable table = table(mode);
    FileStoreTable stock = table(mode);
    assertNull(PaimonMergeOptions.unsupportedReason(table));
    List<InternalRow> rows = rows(240, mode.containsKey("rowkind.field"));
    var nativeState = new PaimonChangelogSinkWriteTest.MemoryState();
    var stockState = new PaimonChangelogSinkWriteTest.MemoryState();
    for (int run = 0; run < 2; run++) {
      try (Writer ours = new Writer(table, true, nativeState, 7);
          Writer twin = new Writer(stock, false, stockState, 7)) {
        for (int checkpoint = run * 2 + 1; checkpoint <= run * 2 + 2; checkpoint++) {
          List<InternalRow> input = rows.subList((checkpoint - 1) * 60, checkpoint * 60);
          ours.write(input);
          twin.write(input);
          var actual = ours.commit(checkpoint);
          var expected = twin.commit(checkpoint);
          assertEquals(
              metadata(expected), metadata(actual), "level-0 sequences, kinds and row counts");
          assertEquals(
              PaimonTestTables.readRows(stock, TYPE),
              PaimonTestTables.readRows(table, TYPE),
              "checkpoint " + checkpoint);
          if (table.coreOptions().changelogProducer() != CoreOptions.ChangelogProducer.NONE) {
            assertEquals(
                PaimonChangelogSinkWriteTest.changelogRows(stock),
                PaimonChangelogSinkWriteTest.changelogRows(table));
          }
        }
      }
    }
  }

  static List<InternalRow> rows(int count, boolean fieldKinds) {
    List<InternalRow> rows = new ArrayList<>();
    Random random = new Random(47);
    for (int i = 0; i < count; i++) {
      int kind = i % 13 == 0 ? 3 : i % 11 == 0 ? 1 : i % 2 == 0 ? 0 : 2;
      rows.add(
          GenericRow.ofKind(
              fieldKinds ? RowKind.INSERT : RowKind.fromByteValue((byte) kind),
              i % 17,
              i % 9 == 0 ? null : (long) random.nextInt(7),
              i % 8 == 0 ? null : random.nextInt(3),
              i % 5 == 0 ? null : (long) (i % 3 + 1),
              i % 4 == 0 ? null : BinaryString.fromString(i % 7 == 0 ? " " : "v" + i),
              i % 6 == 0 ? null : i % 3 == 0,
              i % 5 == 0 ? null : Decimal.fromUnscaledLong(i % 19, 6, 2),
              i % 3 == 0 ? null : new GenericArray(new Integer[] {i, null, -i}),
              BinaryString.fromString(RowKind.fromByteValue((byte) kind).shortString())));
    }
    return rows;
  }

  @Test
  void fillsParsedColumnDefaultsBeforePartialMerging() throws Exception {
    List<DataField> fields = new ArrayList<>(TYPE.getFields());
    fields.set(3, new DataField(3, "v", DataTypes.BIGINT(), null, "42"));
    fields.set(4, new DataField(4, "txt", DataTypes.STRING(), null, "'default'"));
    fields.set(6, new DataField(6, "amount", DataTypes.DECIMAL(6, 2), null, "1.25"));
    RowType type = new RowType(fields);
    List<List<String>> contents = new ArrayList<>();
    for (boolean nativeWriter : new boolean[] {false, true}) {
      FileStoreTable table =
          table(options("merge-engine", "partial-update", "ignore-delete", "true"), type);
      try (Writer writer =
          new Writer(table, nativeWriter, new PaimonChangelogSinkWriteTest.MemoryState(), 7)) {
        writer.write(rows(100, false));
        writer.commit(1);
      }
      contents.add(PaimonTestTables.readRows(table, type));
    }
    assertEquals(contents.get(0), contents.get(1));
  }

  @Test
  void numericAggregatesMatchReleasedTypesRetractsAndDecimalOverflow() throws Exception {
    for (var type :
        List.of(
            DataTypes.TINYINT(),
            DataTypes.SMALLINT(),
            DataTypes.INT(),
            DataTypes.BIGINT(),
            DataTypes.FLOAT(),
            DataTypes.DOUBLE(),
            DataTypes.DECIMAL(6, 2),
            DataTypes.DECIMAL(38, 2))) {
      for (String function :
          type.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.DECIMAL
              ? List.of("sum")
              : List.of("sum", "product")) {
        RowType schema =
            new RowType(
                List.of(
                    new DataField(0, "id", DataTypes.INT().notNull()),
                    new DataField(1, "v", type)));
        List<List<String>> contents = new ArrayList<>();
        for (boolean nativeWriter : new boolean[] {false, true}) {
          FileStoreTable table =
              table(
                  options("merge-engine", "aggregation", "fields.v.aggregate-function", function),
                  schema);
          List<InternalRow> rows = new ArrayList<>();
          for (int i = 0; i < 4; i++) {
            Object value =
                i == 0
                    ? null
                    : org.apache.paimon.utils.DefaultValueUtils.convertDefaultValue(
                        type, i == 3 ? "3" : "2");
            rows.add(GenericRow.ofKind(i == 2 ? RowKind.DELETE : RowKind.INSERT, 1, value));
          }
          if (type.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.DECIMAL) {
            int precision = ((org.apache.paimon.types.DecimalType) type).getPrecision();
            Object maximum =
                org.apache.paimon.utils.DefaultValueUtils.convertDefaultValue(
                    type, "9".repeat(precision - 2) + ".99");
            rows.add(GenericRow.of(2, maximum));
            rows.add(
                GenericRow.of(
                    2,
                    org.apache.paimon.utils.DefaultValueUtils.convertDefaultValue(type, "0.01")));
          }
          try (Writer writer =
              new Writer(table, nativeWriter, new PaimonChangelogSinkWriteTest.MemoryState(), 3)) {
            writer.write(rows);
            writer.commit(1);
          }
          contents.add(PaimonTestTables.readRows(table, schema));
        }
        assertEquals(contents.get(0), contents.get(1), type + " " + function);
      }
    }
  }

  @Test
  void rejectsTheSameInvalidInputAsReleasedPaimon() throws Exception {
    for (String mode :
        List.of(
            "first-row", "partial-update", "aggregation", "overflow", "null-kind", "bad-kind")) {
      Map<String, String> options = new HashMap<>();
      if (mode.endsWith("kind")) {
        options.put("rowkind.field", "op");
      } else if (mode.equals("overflow")) {
        options.put("merge-engine", "aggregation");
        options.put("fields.v.aggregate-function", "sum");
      } else {
        options.put("merge-engine", mode);
        if (mode.equals("aggregation")) {
          options.put("fields.v.aggregate-function", "min");
        }
        if (mode.equals("first-row")) {
          options.put("changelog-producer", "lookup");
        }
      }
      for (boolean nativeWriter : new boolean[] {false, true}) {
        FileStoreTable table = table(options);
        GenericRow first = (GenericRow) rows(2, false).get(1);
        first.setField(0, 1);
        first.setField(3, Long.MAX_VALUE);
        GenericRow second = (GenericRow) rows(2, false).get(1);
        second.setField(0, 1);
        second.setField(3, 1L);
        if (!mode.equals("overflow") && !mode.endsWith("kind")) {
          second.setRowKind(RowKind.DELETE);
        }
        if (mode.endsWith("kind")) {
          second.setField(8, mode.equals("null-kind") ? null : BinaryString.fromString("insert"));
        }
        try (Writer writer =
            new Writer(table, nativeWriter, new PaimonChangelogSinkWriteTest.MemoryState(), 7)) {
          assertThrows(
              Exception.class,
              () -> {
                writer.write(List.of(first, second));
                writer.commit(1);
              },
              mode + " native=" + nativeWriter);
        }
      }
    }
  }

  static FileStoreTable table(Map<String, String> mode) throws Exception {
    return table(mode, TYPE);
  }

  static FileStoreTable table(Map<String, String> mode, RowType type) throws Exception {
    Map<String, String> options = new HashMap<>(mode);
    options.put("bucket", "2");
    options.put("file.format", "parquet");
    options.put("num-sorted-run.compaction-trigger", "100");
    options.put("commit.force-compact", "true");
    Path path = new Path(Files.createTempDirectory("paimon-merge").toUri());
    new SchemaManager(LocalFileIO.create(), path)
        .createTable(new Schema(type.getFields(), List.of(), List.of("id"), options, null));
    return FileStoreTableFactory.create(LocalFileIO.create(), path);
  }

  private static List<String> metadata(List<CommitMessage> messages) {
    List<String> result = new ArrayList<>();
    for (CommitMessage message : messages) {
      var impl = (CommitMessageImpl) message;
      for (DataFileMeta file : impl.newFilesIncrement().newFiles()) {
        result.add(
            impl.bucket()
                + ":"
                + file.rowCount()
                + ":"
                + file.deleteRowCount()
                + ":"
                + file.minSequenceNumber()
                + ":"
                + file.maxSequenceNumber());
      }
    }
    result.sort(String::compareTo);
    return result;
  }

  static final class Writer implements AutoCloseable {
    private final IOManagerAsync io = new IOManagerAsync();
    private final BufferAllocator allocator = new RootAllocator();
    private final StreamTableWrite router;
    private final StoreSinkWrite writer;
    private final StreamTableCommit commit;
    private final int batchRows;
    private final org.apache.flink.table.types.logical.RowType type;

    Writer(
        FileStoreTable table,
        boolean nativeWriter,
        PaimonChangelogSinkWriteTest.MemoryState state,
        int batchRows) {
      this.batchRows = batchRows;
      this.type = LogicalTypeConversion.toLogicalType(table.rowType());
      CoreOptions options = table.coreOptions();
      CheckpointConfig checkpoints = new CheckpointConfig();
      checkpoints.setCheckpointInterval(1000);
      StoreSinkWrite delegate =
          StoreSinkWrite.createWriteProvider(table, checkpoints, true, false, false)
              .provide(
                  table,
                  "writer",
                  state,
                  io,
                  new MemoryPoolFactory(
                      new HeapMemorySegmentPool(options.writeBufferSize(), options.pageSize())),
                  null);
      writer = nativeWriter ? new NativeKeyValueSinkWrite(table, delegate) : delegate;
      router = table.newStreamWriteBuilder().newWrite();
      commit = table.newStreamWriteBuilder().withCommitUser("writer").newCommit();
    }

    void write(List<InternalRow> input) throws Exception {
      Map<Integer, List<RowData>> batches = new LinkedHashMap<>();
      BinaryRow partition = null;
      for (InternalRow row : input) {
        if (!(writer instanceof NativeKeyValueSinkWrite)) {
          writer.write(row);
          continue;
        }
        partition = router.getPartition(row).copy();
        int bucket = router.getBucket(row);
        var batch = batches.computeIfAbsent(bucket, b -> new ArrayList<>());
        batch.add(new FlinkRowData(row));
        if (batch.size() == batchRows) {
          ((NativeKeyValueSinkWrite) writer)
              .writeBundle(
                  partition, bucket, RowDataArrowConverter.write(batch, type, allocator, true));
          batch.clear();
        }
      }
      for (var batch : batches.entrySet()) {
        if (!batch.getValue().isEmpty()) {
          ((NativeKeyValueSinkWrite) writer)
              .writeBundle(
                  partition,
                  batch.getKey(),
                  RowDataArrowConverter.write(batch.getValue(), type, allocator, true));
        }
      }
    }

    List<CommitMessage> commit(long checkpoint) throws Exception {
      List<CommitMessage> messages = new ArrayList<>();
      for (var c : writer.prepareCommit(true, checkpoint)) {
        messages.add(c.commitMessage());
      }
      writer.snapshotState();
      commit.commit(checkpoint, messages);
      return messages;
    }

    @Override
    public void close() throws Exception {
      try {
        writer.close();
        router.close();
        commit.close();
      } finally {
        io.close();
        allocator.close();
      }
    }
  }
}
