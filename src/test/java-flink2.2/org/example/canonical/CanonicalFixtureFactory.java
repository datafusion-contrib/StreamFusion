package org.example.canonical;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/** Independent user connector fixture: its JAR has no StreamFusion dependencies. */
public final class CanonicalFixtureFactory
    implements DynamicTableSourceFactory, DynamicTableSinkFactory {
  private static final ConfigOption<String> INPUT =
      ConfigOptions.key("input").stringType().noDefaultValue();
  private static final ConfigOption<String> READY =
      ConfigOptions.key("ready").stringType().noDefaultValue();
  private static final ConfigOption<Integer> LIMIT =
      ConfigOptions.key("limit").intType().defaultValue(Integer.MAX_VALUE);
  private static final ConfigOption<String> OUTPUT =
      ConfigOptions.key("output").stringType().noDefaultValue();
  private static final ConfigOption<String> WITNESS =
      ConfigOptions.key("witness").stringType().noDefaultValue();
  private static final ConfigOption<Boolean> INSERT_ONLY =
      ConfigOptions.key("insert-only").booleanType().defaultValue(false);
  private static final ConfigOption<Boolean> UPSERT =
      ConfigOptions.key("upsert").booleanType().defaultValue(false);

  private static final ConfigOption<Integer> PARALLELISM =
      ConfigOptions.key("parallelism").intType().noDefaultValue();

  @Override
  public String factoryIdentifier() {
    return "canonical-fixture";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Set.of(INPUT, READY, LIMIT, OUTPUT, WITNESS, INSERT_ONLY, UPSERT, PARALLELISM);
  }

  @Override
  public DynamicTableSource createDynamicTableSource(DynamicTableSourceFactory.Context context) {
    var options = context.getCatalogTable().getOptions();
    return new TableSource(
        options.get("input"),
        options.get("ready"),
        Integer.parseInt(options.getOrDefault("limit", Integer.toString(Integer.MAX_VALUE))),
        Boolean.parseBoolean(options.getOrDefault("insert-only", "false")),
        Boolean.parseBoolean(options.getOrDefault("upsert", "false")),
        options.containsKey("parallelism") ? Integer.valueOf(options.get("parallelism")) : null);
  }

  @Override
  public DynamicTableSink createDynamicTableSink(DynamicTableSinkFactory.Context context) {
    var options = context.getCatalogTable().getOptions();
    return new TableSink(
        options.get("output"),
        options.get("witness"),
        (RowType) context.getPhysicalRowDataType().getLogicalType(),
        options.containsKey("parallelism") ? Integer.valueOf(options.get("parallelism")) : null,
        Boolean.parseBoolean(options.getOrDefault("upsert", "false")));
  }

  public static final class TableSource implements ScanTableSource {
    private final String input;
    private final String ready;
    private final int limit;
    private final boolean insertOnly;
    private final boolean upsert;
    private final Integer parallelism;

    TableSource(
        String input,
        String ready,
        int limit,
        boolean insertOnly,
        boolean upsert,
        Integer parallelism) {
      this.parallelism = parallelism;
      this.input = input;
      this.ready = ready;
      this.limit = limit;
      this.insertOnly = insertOnly;
      this.upsert = upsert;
    }

    @Override
    public ChangelogMode getChangelogMode() {
      if (insertOnly) return ChangelogMode.insertOnly();
      return upsert
          ? ChangelogMode.newBuilder()
              .addContainedKind(RowKind.INSERT)
              .addContainedKind(RowKind.UPDATE_AFTER)
              .addContainedKind(RowKind.DELETE)
              .build()
          : ChangelogMode.all();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
      return parallelism == null
          ? SourceProvider.of(new RowsSource(input, ready, limit))
          : new FixedSourceProvider();
    }

    private final class FixedSourceProvider
        implements org.apache.flink.table.connector.source.DataStreamScanProvider {
      @Override
      public boolean isBounded() {
        return true;
      }

      @Override
      public org.apache.flink.streaming.api.datastream.DataStream<RowData> produceDataStream(
          org.apache.flink.table.connector.ProviderContext provider,
          org.apache.flink.streaming.api.environment.StreamExecutionEnvironment environment) {
        var stream =
            environment
                .fromSource(
                    new RowsSource(input, ready, limit),
                    org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                    "CanonicalFixtureSource",
                    org.apache.flink.table.runtime.typeutils.InternalTypeInfo.ofFields(
                        new org.apache.flink.table.types.logical.BigIntType(),
                        new org.apache.flink.table.types.logical.BigIntType()))
                .setParallelism(parallelism);
        provider.generateUid("source").ifPresent(stream::uid);
        return stream;
      }
    }

    @Override
    public DynamicTableSource copy() {
      return new TableSource(input, ready, limit, insertOnly, upsert, parallelism);
    }

    @Override
    public String asSummaryString() {
      return "CanonicalFixtureSource";
    }
  }

  public static final class TableSink implements DynamicTableSink {
    private final String output;
    private final String witness;
    private final RowType type;
    private final Integer parallelism;
    private final boolean upsert;

    TableSink(String output, String witness, RowType type, Integer parallelism, boolean upsert) {
      this.upsert = upsert;
      this.parallelism = parallelism;
      this.output = output;
      this.witness = witness;
      this.type = type;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requested) {
      return upsert
          ? ChangelogMode.newBuilder()
              .addContainedKind(RowKind.INSERT)
              .addContainedKind(RowKind.UPDATE_AFTER)
              .addContainedKind(RowKind.DELETE)
              .build()
          : requested;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
      return parallelism == null
          ? SinkV2Provider.of(new RowsSink(output, witness, type))
          : new FixedSinkProvider();
    }

    private final class FixedSinkProvider
        implements org.apache.flink.table.connector.sink.DataStreamSinkProvider {
      @Override
      public org.apache.flink.streaming.api.datastream.DataStreamSink<?> consumeDataStream(
          org.apache.flink.table.connector.ProviderContext provider,
          org.apache.flink.streaming.api.datastream.DataStream<RowData> stream) {
        var sink = stream.sinkTo(new RowsSink(output, witness, type)).setParallelism(parallelism);
        provider.generateUid("sink").ifPresent(sink::uid);
        return sink;
      }
    }

    @Override
    public DynamicTableSink copy() {
      return new TableSink(output, witness, type, parallelism, upsert);
    }

    @Override
    public String asSummaryString() {
      return "CanonicalFixtureSink";
    }
  }

  public static final class OffsetSplit implements SourceSplit {
    final int offset;

    OffsetSplit(int offset) {
      this.offset = offset;
    }

    @Override
    public String splitId() {
      return "rows";
    }
  }

  public static final class OffsetSerializer implements SimpleVersionedSerializer<OffsetSplit> {
    @Override
    public int getVersion() {
      return 1;
    }

    @Override
    public byte[] serialize(OffsetSplit split) throws IOException {
      var bytes = new ByteArrayOutputStream();
      try (var output = new DataOutputStream(bytes)) {
        output.writeInt(split.offset);
      }
      return bytes.toByteArray();
    }

    @Override
    public OffsetSplit deserialize(int version, byte[] bytes) throws IOException {
      if (version != 1 || bytes.length != 4) throw new IOException("invalid source offset state");
      try (var input = new DataInputStream(new ByteArrayInputStream(bytes))) {
        return new OffsetSplit(input.readInt());
      }
    }
  }

  public static final class EnumeratorSerializer implements SimpleVersionedSerializer<Integer> {
    private final OffsetSerializer offsets = new OffsetSerializer();

    @Override
    public int getVersion() {
      return 1;
    }

    @Override
    public byte[] serialize(Integer state) throws IOException {
      return offsets.serialize(new OffsetSplit(state));
    }

    @Override
    public Integer deserialize(int version, byte[] bytes) throws IOException {
      return offsets.deserialize(version, bytes).offset;
    }
  }

  public static final class RowsSource implements Source<RowData, OffsetSplit, Integer> {
    private final String input;
    private final String ready;
    private final int limit;

    RowsSource(String input, String ready, int limit) {
      this.input = input;
      this.ready = ready;
      this.limit = limit;
    }

    @Override
    public Boundedness getBoundedness() {
      return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, OffsetSplit> createReader(SourceReaderContext context)
        throws Exception {
      return new RowsReader(context, Files.readAllLines(Path.of(input)), ready, limit);
    }

    @Override
    public SplitEnumerator<OffsetSplit, Integer> createEnumerator(
        SplitEnumeratorContext<OffsetSplit> context) {
      return new RowsEnumerator(context, 0);
    }

    @Override
    public SplitEnumerator<OffsetSplit, Integer> restoreEnumerator(
        SplitEnumeratorContext<OffsetSplit> context, Integer state) {
      return new RowsEnumerator(context, state);
    }

    @Override
    public SimpleVersionedSerializer<OffsetSplit> getSplitSerializer() {
      return new OffsetSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Integer> getEnumeratorCheckpointSerializer() {
      return new EnumeratorSerializer();
    }
  }

  public static final class RowsEnumerator implements SplitEnumerator<OffsetSplit, Integer> {
    private final SplitEnumeratorContext<OffsetSplit> context;
    private int pending;

    RowsEnumerator(SplitEnumeratorContext<OffsetSplit> context, int pending) {
      this.context = context;
      this.pending = pending;
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtask, String hostname) {
      assign(subtask);
    }

    @Override
    public void addReader(int subtask) {
      assign(subtask);
    }

    private void assign(int subtask) {
      if (pending >= 0) {
        context.assignSplit(new OffsetSplit(pending), subtask);
        pending = -1;
      }
      context.signalNoMoreSplits(subtask);
    }

    @Override
    public void addSplitsBack(List<OffsetSplit> splits, int subtask) {
      if (!splits.isEmpty()) pending = splits.get(0).offset;
    }

    @Override
    public Integer snapshotState(long checkpoint) {
      return pending;
    }

    @Override
    public void close() {}
  }

  public static final class RowsReader implements SourceReader<RowData, OffsetSplit> {
    private final SourceReaderContext context;
    private final List<String> rows;
    private final String ready;
    private final int limit;
    private OffsetSplit split;
    private boolean noMoreSplits;
    private boolean markedReady;
    private CompletableFuture<Void> available = new CompletableFuture<>();

    RowsReader(SourceReaderContext context, List<String> rows, String ready, int limit) {
      this.context = context;
      this.rows = rows;
      this.ready = ready;
      this.limit = limit;
    }

    @Override
    public void start() {
      context.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
      if (split == null)
        return noMoreSplits && limit >= rows.size()
            ? InputStatus.END_OF_INPUT
            : InputStatus.NOTHING_AVAILABLE;
      if (split.offset >= rows.size()) return InputStatus.END_OF_INPUT;
      if (split.offset >= limit) {
        markReady();
        return InputStatus.NOTHING_AVAILABLE;
      }
      String[] fields = rows.get(split.offset).split(",", -1);
      output.collect(
          GenericRowData.ofKind(
              RowKind.valueOf(fields[0]),
              fields[1].equals("null") ? null : Long.parseLong(fields[1]),
              fields[2].equals("null") ? null : Long.parseLong(fields[2])));
      split = new OffsetSplit(split.offset + 1);
      if (split.offset == limit && limit < rows.size()) markReady();
      return split.offset < Math.min(limit, rows.size())
          ? InputStatus.MORE_AVAILABLE
          : split.offset == rows.size() ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
    }

    private void markReady() throws IOException {
      if (!markedReady) {
        Files.writeString(Path.of(ready), Integer.toString(split.offset));
        markedReady = true;
      }
    }

    @Override
    public List<OffsetSplit> snapshotState(long checkpoint) {
      return split == null ? List.of() : List.of(split);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
      if (split != null && split.offset < Math.min(limit, rows.size())
          || split != null && split.offset >= rows.size()
          || split == null && noMoreSplits && limit >= rows.size()) {
        return CompletableFuture.completedFuture(null);
      }
      return available;
    }

    @Override
    public void addSplits(List<OffsetSplit> splits) {
      if (splits.size() != 1 || split != null)
        throw new IllegalStateException("unexpected split assignment");
      split = splits.get(0);
      available.complete(null);
      available = new CompletableFuture<>();
    }

    @Override
    public void notifyNoMoreSplits() {
      noMoreSplits = true;
      available.complete(null);
      available = new CompletableFuture<>();
    }

    @Override
    public void close() {}
  }

  public static final class WriterState {
    final long transaction;
    final List<byte[]> rows;

    WriterState(long transaction, List<byte[]> rows) {
      this.transaction = transaction;
      this.rows = List.copyOf(rows);
    }
  }

  public static final class RowsSerializer implements SimpleVersionedSerializer<WriterState> {
    @Override
    public int getVersion() {
      return 1;
    }

    @Override
    public byte[] serialize(WriterState state) throws IOException {
      var bytes = new ByteArrayOutputStream();
      try (var output = new DataOutputStream(bytes)) {
        output.writeLong(state.transaction);
        output.writeInt(state.rows.size());
        for (byte[] row : state.rows) {
          output.writeInt(row.length);
          output.write(row);
        }
      }
      return bytes.toByteArray();
    }

    @Override
    public WriterState deserialize(int version, byte[] bytes) throws IOException {
      if (version != 1) throw new IOException("unsupported sink state version");
      try (var input = new DataInputStream(new ByteArrayInputStream(bytes))) {
        long transaction = input.readLong();
        int count = input.readInt();
        if (transaction < 0 || count < 0 || count > bytes.length / 4)
          throw new IOException("invalid sink state");
        List<byte[]> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
          int size = input.readInt();
          if (size < 0 || size > input.available())
            throw new IOException("invalid sink row length");
          rows.add(input.readNBytes(size));
        }
        if (input.available() != 0) throw new IOException("trailing sink state bytes");
        return new WriterState(transaction, rows);
      }
    }
  }

  public static final class RowsSink
      implements Sink<RowData>,
          SupportsWriterState<RowData, WriterState>,
          SupportsCommitter<WriterState> {
    private final String output;
    private final String witness;
    private final RowType type;

    RowsSink(String output, String witness, RowType type) {
      this.output = output;
      this.witness = witness;
      this.type = type;
    }

    @Override
    public RowsWriter createWriter(WriterInitContext context) throws IOException {
      return new RowsWriter(type, witness, List.of());
    }

    @Override
    public RowsWriter restoreWriter(WriterInitContext context, Collection<WriterState> state)
        throws IOException {
      return new RowsWriter(type, witness, state);
    }

    @Override
    public SimpleVersionedSerializer<WriterState> getWriterStateSerializer() {
      return new RowsSerializer();
    }

    @Override
    public SimpleVersionedSerializer<WriterState> getCommittableSerializer() {
      return new RowsSerializer();
    }

    @Override
    public Committer<WriterState> createCommitter(CommitterInitContext context) {
      return new RowsCommitter(output);
    }
  }

  public static final class RowsWriter
      implements StatefulSinkWriter<RowData, WriterState>,
          CommittingSinkWriter<RowData, WriterState> {
    private final RowDataSerializer serializer;
    private final String witness;
    private final List<byte[]> pending = new ArrayList<>();
    private long transaction;
    private boolean witnessed;
    private boolean outputWitnessed;

    RowsWriter(RowType type, String witness, Collection<WriterState> restored) throws IOException {
      serializer = new RowDataSerializer(type);
      this.witness = witness;
      for (WriterState state : restored) {
        transaction = Math.max(transaction, state.transaction);
        pending.addAll(state.rows);
      }
      Files.writeString(Path.of(witness + "-sink-ready"), "ready");
    }

    @Override
    public void write(RowData row, Context context) throws IOException {
      var encoded = new DataOutputSerializer(64);
      serializer.serialize(row, encoded);
      pending.add(encoded.getCopyOfBuffer());
      if (!outputWitnessed) {
        Files.writeString(Path.of(witness + "-sink-output-ready"), "row received");
        outputWitnessed = true;
      }
      if (!witnessed) {
        for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
          if (frame
                  .getClassName()
                  .equals("tech.streamfusion.operator.NativeColumnarGroupAggregateOperator")
              || frame
                  .getClassName()
                  .equals("tech.streamfusion.operator.NativeColumnarJvmGroupOperator")
              || frame
                  .getClassName()
                  .equals("tech.streamfusion.operator.NativeColumnarKeepLastDeduplicateOperator")) {
            Files.writeString(Path.of(witness), frame.toString());
            witnessed = true;
            break;
          }
        }
      }
    }

    @Override
    public void flush(boolean endOfInput) {}

    @Override
    public Collection<WriterState> prepareCommit() {
      if (pending.isEmpty()) return List.of();
      WriterState commit = new WriterState(transaction++, pending);
      pending.clear();
      return List.of(commit);
    }

    @Override
    public List<WriterState> snapshotState(long checkpoint) {
      return List.of(new WriterState(transaction, pending));
    }

    @Override
    public void close() {}
  }

  public static final class RowsCommitter implements Committer<WriterState> {
    private final String output;

    RowsCommitter(String output) {
      this.output = output;
    }

    @Override
    public void commit(Collection<CommitRequest<WriterState>> requests) throws IOException {
      Path directory = Path.of(output);
      Files.createDirectories(directory);
      for (CommitRequest<WriterState> request : requests) {
        failCommit("before");
        WriterState commit = request.getCommittable();
        byte[] bytes = new RowsSerializer().serialize(commit);
        Path target = directory.resolve(String.format("transaction-%020d", commit.transaction));
        if (Files.exists(target)) {
          if (!java.util.Arrays.equals(bytes, Files.readAllBytes(target)))
            throw new IOException("transaction reused with different contents");
          continue;
        }
        Path temporary = Files.createTempFile(directory, "pending-", ".tmp");
        Files.write(temporary, bytes);
        Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
        failCommit("after");
      }
    }

    private void failCommit(String position) throws IOException {
      Path marker = Path.of(output).getParent().resolve("fail-" + position + "-commit");
      try {
        Files.delete(marker);
      } catch (java.nio.file.NoSuchFileException absent) {
        return;
      }
      throw new IOException("fixture commit failure " + position);
    }

    @Override
    public void close() {}
  }

  public static final class HistoryAccumulator {
    public long sum;
    public long rows;
    public org.apache.flink.table.api.dataview.MapView<Long, Long> counts =
        new org.apache.flink.table.api.dataview.MapView<>();
  }

  public static final class HistoryAggregate
      extends org.apache.flink.table.functions.AggregateFunction<Long, HistoryAccumulator> {
    @Override
    public HistoryAccumulator createAccumulator() {
      return new HistoryAccumulator();
    }

    @Override
    public Long getValue(HistoryAccumulator accumulator) {
      try {
        long distinct = 0;
        for (var ignored : accumulator.counts.entries()) distinct++;
        return accumulator.sum + accumulator.rows + 1000L * distinct;
      } catch (Exception error) {
        throw new IllegalStateException(error);
      }
    }

    public void accumulate(HistoryAccumulator accumulator, Long value) throws Exception {
      accumulator.rows++;
      if (value != null) {
        accumulator.sum += value;
        Long count = accumulator.counts.get(value);
        long next = count == null ? 1L : count + 1;
        if (next == 0) accumulator.counts.remove(value);
        else accumulator.counts.put(value, next);
      }
    }

    public void retract(HistoryAccumulator accumulator, Long value) throws Exception {
      accumulator.rows--;
      if (value != null) {
        accumulator.sum -= value;
        Long stored = accumulator.counts.get(value);
        long count = stored == null ? 0 : stored;
        if (count == 1) accumulator.counts.remove(value);
        else accumulator.counts.put(value, count - 1);
      }
    }

    public void merge(HistoryAccumulator accumulator, Iterable<HistoryAccumulator> partials)
        throws Exception {
      for (HistoryAccumulator partial : partials) {
        accumulator.sum += partial.sum;
        accumulator.rows += partial.rows;
        for (var entry : partial.counts.entries()) {
          Long previous = accumulator.counts.get(entry.getKey());
          long next = (previous == null ? 0 : previous) + entry.getValue();
          if (next == 0) accumulator.counts.remove(entry.getKey());
          else accumulator.counts.put(entry.getKey(), next);
        }
      }
    }
  }

  public static List<String> committedRows(Path directory, RowType type) throws IOException {
    List<String> result = new ArrayList<>();
    RowDataSerializer serializer = new RowDataSerializer(type);
    try (var files = Files.list(directory)) {
      for (Path file :
          files
              .filter(path -> path.getFileName().toString().startsWith("transaction-"))
              .sorted()
              .collect(java.util.stream.Collectors.toList())) {
        WriterState state = new RowsSerializer().deserialize(1, Files.readAllBytes(file));
        for (byte[] bytes : state.rows) {
          RowData row = serializer.deserialize(new DataInputDeserializer(bytes));
          StringBuilder value = new StringBuilder(row.getRowKind().shortString());
          for (int i = 0; i < row.getArity(); i++)
            value.append('|').append(row.isNullAt(i) ? "null" : row.getLong(i));
          result.add(value.toString());
        }
      }
    }
    return result;
  }
}
