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
        options.containsKey("parallelism") ? Integer.valueOf(options.get("parallelism")) : null,
        (RowType) context.getPhysicalRowDataType().getLogicalType());
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

  public static final class TableSource
      implements ScanTableSource,
          org.apache.flink.table.connector.source.abilities.SupportsSourceWatermark,
          tech.streamfusion.planner.Utf8StringSource {
    private final String input;
    private final String ready;
    private final int limit;
    private final boolean insertOnly;
    private final boolean upsert;
    private final Integer parallelism;
    private final RowType type;
    private boolean sourceWatermarks;

    TableSource(
        String input,
        String ready,
        int limit,
        boolean insertOnly,
        boolean upsert,
        Integer parallelism,
        RowType type) {
      this.parallelism = parallelism;
      this.input = input;
      this.ready = ready;
      this.limit = limit;
      this.insertOnly = insertOnly;
      this.upsert = upsert;
      this.type = type;
    }

    @Override
    public boolean guaranteesUtf8Strings() {
      return true;
    }

    @Override
    public void applySourceWatermark() {
      sourceWatermarks = true;
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
          ? SourceProvider.of(new RowsSource(input, ready, limit, type, sourceWatermarks))
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
                    new RowsSource(input, ready, limit, type, sourceWatermarks),
                    org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                    "CanonicalFixtureSource",
                    org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(type))
                .setParallelism(parallelism);
        provider.generateUid("source").ifPresent(stream::uid);
        return stream;
      }
    }

    @Override
    public DynamicTableSource copy() {
      var copy = new TableSource(input, ready, limit, insertOnly, upsert, parallelism, type);
      copy.sourceWatermarks = sourceWatermarks;
      return copy;
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
    private final RowType type;
    private final boolean sourceWatermarks;

    RowsSource(String input, String ready, int limit, RowType type, boolean sourceWatermarks) {
      this.input = input;
      this.ready = ready;
      this.limit = limit;
      this.type = type;
      this.sourceWatermarks = sourceWatermarks;
    }

    @Override
    public Boundedness getBoundedness() {
      return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, OffsetSplit> createReader(SourceReaderContext context)
        throws Exception {
      return new RowsReader(
          context, Files.readAllLines(Path.of(input)), ready, limit, type, sourceWatermarks);
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
    public void addReader(int subtask) {}

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
    private final RowType type;
    private final boolean sourceWatermarks;
    private final RowDataSerializer serializer;
    private OffsetSplit split;
    private boolean noMoreSplits;
    private boolean markedReady;
    private CompletableFuture<Void> available = new CompletableFuture<>();

    RowsReader(
        SourceReaderContext context,
        List<String> rows,
        String ready,
        int limit,
        RowType type,
        boolean sourceWatermarks) {
      this.context = context;
      this.rows = rows;
      this.ready = ready;
      this.limit = limit;
      this.type = type;
      this.sourceWatermarks = sourceWatermarks;
      this.serializer = new RowDataSerializer(type);
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
      if (split.offset >= rows.size())
        return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
      if (split.offset >= limit) {
        markReady();
        return InputStatus.NOTHING_AVAILABLE;
      }
      String[] fields = rows.get(split.offset).split(",", -1);
      if (fields[0].equals("WATERMARK")) {
        if (!sourceWatermarks || fields.length != 2)
          throw new IOException("source watermark was not requested or is malformed");
        output.emitWatermark(
            new org.apache.flink.api.common.eventtime.Watermark(Long.parseLong(fields[1])));
      } else if (fields[0].equals("BINARY")) {
        if (fields.length != 2) throw new IOException("malformed binary source row");
        output.collect(
            serializer.deserialize(
                new DataInputDeserializer(java.util.Base64.getDecoder().decode(fields[1]))));
      } else {
        if (fields.length != type.getFieldCount() + 1)
          throw new IOException("source row does not match its physical schema");
        var row = new GenericRowData(RowKind.valueOf(fields[0]), type.getFieldCount());
        for (int i = 0; i < type.getFieldCount(); i++) {
          String value = fields[i + 1];
          row.setField(i, scalar(type.getTypeAt(i), value));
        }
        output.collect(serializer.toBinaryRow(row).copy());
      }
      split = new OffsetSplit(split.offset + 1);
      if (split.offset == limit && limit < rows.size()) markReady();
      return split.offset < Math.min(limit, rows.size())
          ? InputStatus.MORE_AVAILABLE
          : split.offset == rows.size() && noMoreSplits
              ? InputStatus.END_OF_INPUT
              : InputStatus.NOTHING_AVAILABLE;
    }

    private static Object scalar(
        org.apache.flink.table.types.logical.LogicalType type, String value) {
      if (value.equals("null")) return null;
      switch (type.getTypeRoot()) {
        case TINYINT:
          return Byte.parseByte(value);
        case SMALLINT:
          return Short.parseShort(value);
        case INTEGER:
        case DATE:
        case TIME_WITHOUT_TIME_ZONE:
        case INTERVAL_YEAR_MONTH:
          return Integer.parseInt(value);
        case BIGINT:
        case INTERVAL_DAY_TIME:
          return Long.parseLong(value);
        case BOOLEAN:
          return Boolean.parseBoolean(value);
        case FLOAT:
          return Float.parseFloat(value);
        case DOUBLE:
          return Double.parseDouble(value);
        case CHAR:
        case VARCHAR:
          return org.apache.flink.table.data.StringData.fromString(value);
        case DECIMAL:
          var decimal = (org.apache.flink.table.types.logical.DecimalType) type;
          return org.apache.flink.table.data.DecimalData.fromBigDecimal(
              new java.math.BigDecimal(value), decimal.getPrecision(), decimal.getScale());
        case TIMESTAMP_WITHOUT_TIME_ZONE:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          return org.apache.flink.table.data.TimestampData.fromEpochMillis(Long.parseLong(value));
        case BINARY:
        case VARBINARY:
          return java.util.Base64.getDecoder().decode(value);
        default:
          throw new IllegalArgumentException("Use BINARY rows for fixture type " + type);
      }
    }

    private void markReady() throws IOException {
      if (!markedReady) {
        Path target = Path.of(ready).toAbsolutePath();
        Path temporary = Files.createTempFile(target.getParent(), "source-ready-", ".tmp");
        try {
          Files.writeString(temporary, Integer.toString(split.offset));
          Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
        } finally {
          Files.deleteIfExists(temporary);
        }
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
          || split != null && split.offset >= rows.size() && noMoreSplits
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
    final long generation;
    final int writer;
    final long transaction;
    final List<byte[]> rows;

    WriterState(long transaction, List<byte[]> rows) {
      this(0, 0, transaction, rows);
    }

    WriterState(long generation, int writer, long transaction, List<byte[]> rows) {
      this.generation = generation;
      this.writer = writer;
      this.transaction = transaction;
      this.rows = List.copyOf(rows);
    }
  }

  public static final class RowsSerializer implements SimpleVersionedSerializer<WriterState> {
    private static final long OWNER_HEADER = 0xC516_7A11_0000_0002L;

    @Override
    public int getVersion() {
      return 2;
    }

    @Override
    public byte[] serialize(WriterState state) throws IOException {
      var bytes = new ByteArrayOutputStream();
      try (var output = new DataOutputStream(bytes)) {
        output.writeLong(OWNER_HEADER);
        output.writeLong(state.generation);
        output.writeInt(state.writer);
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
      if (version != 1 && version != 2) throw new IOException("unsupported sink state version");
      try (var input = new DataInputStream(new ByteArrayInputStream(bytes))) {
        if (version == 2 && input.readLong() != OWNER_HEADER)
          throw new IOException("invalid fixture writer ownership header");
        long generation = version == 1 ? 0 : input.readLong();
        int writer = version == 1 ? 0 : input.readInt();
        long transaction = input.readLong();
        int count = input.readInt();
        if (generation < 0
            || writer < 0
            || transaction < 0
            || count < 0
            || count > bytes.length / 4) throw new IOException("invalid sink state");
        List<byte[]> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
          int size = input.readInt();
          if (size < 0 || size > input.available())
            throw new IOException("invalid sink row length");
          rows.add(input.readNBytes(size));
        }
        if (input.available() != 0) throw new IOException("trailing sink state bytes");
        return new WriterState(generation, writer, transaction, rows);
      }
    }

    WriterState committed(byte[] bytes) throws IOException {
      try (var input = new DataInputStream(new ByteArrayInputStream(bytes))) {
        return deserialize(input.readLong() == OWNER_HEADER ? 2 : 1, bytes);
      }
    }
  }

  public static final class RowsSink extends CanonicalSinkCompat {
    private final String output;
    private final String witness;
    private final RowType type;

    RowsSink(String output, String witness, RowType type) {
      this.output = output;
      this.witness = witness;
      this.type = type;
    }

    @Override
    protected RowsWriter writer(Collection<WriterState> restored) throws IOException {
      return new RowsWriter(type, witness, restored);
    }

    @Override
    protected RowsCommitter committer() {
      return new RowsCommitter(output);
    }
  }

  public static final class RowsWriter implements CanonicalWriterCompat {
    private final RowDataSerializer serializer;
    private final String witness;
    private final List<byte[]> pending = new ArrayList<>();
    private long generation;
    private int writer;
    private long transaction;
    private boolean witnessed;
    private boolean outputWitnessed;
    private long received;
    private final boolean countEnabled;

    RowsWriter(RowType type, String witness, Collection<WriterState> restored) throws IOException {
      serializer = new RowDataSerializer(type);
      this.witness = witness;
      countEnabled = Files.exists(Path.of(witness + "-count-enabled"));
      if (countEnabled) Files.writeString(Path.of(witness + "-sink-count"), "0");
      for (WriterState state : restored) {
        generation = Math.max(generation, Math.addExact(state.generation, 1));
        transaction = Math.max(transaction, state.transaction);
        pending.addAll(state.rows);
      }
      Files.writeString(Path.of(witness + "-sink-ready"), "ready");
    }

    RowsWriter withIdentity(long restoredCheckpoint, int writer) {
      if (restoredCheckpoint < -1 || writer < 0)
        throw new IllegalArgumentException("invalid fixture writer identity");
      generation = Math.max(generation, Math.addExact(restoredCheckpoint, 1));
      this.writer = writer;
      return this;
    }

    @Override
    public void write(RowData row, Context context) throws IOException {
      var encoded = new DataOutputSerializer(64);
      serializer.serialize(row, encoded);
      pending.add(encoded.getCopyOfBuffer());
      if (countEnabled)
        Files.writeString(Path.of(witness + "-sink-count"), Long.toString(++received));
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
      WriterState commit = new WriterState(generation, writer, transaction++, pending);
      pending.clear();
      return List.of(commit);
    }

    @Override
    public List<WriterState> snapshotState(long checkpoint) {
      return List.of(new WriterState(generation, writer, transaction, pending));
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
        if (commit.generation == 0 && commit.writer == 0) {
          Path legacy =
              directory.resolve(
                  String.format(java.util.Locale.ROOT, "transaction-%020d", commit.transaction));
          if (Files.exists(legacy)) {
            var serializer = new RowsSerializer();
            if (!java.util.Arrays.equals(
                bytes, serializer.serialize(serializer.committed(Files.readAllBytes(legacy)))))
              throw new IOException("transaction reused with different contents");
            continue;
          }
        }
        Path target =
            directory.resolve(
                String.format(
                    java.util.Locale.ROOT,
                    "transaction-%020d-%010d-%020d",
                    commit.generation,
                    commit.writer,
                    commit.transaction));
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

  public static final class NestedRankChecksum
      extends org.apache.flink.table.functions.ScalarFunction {
    public Long eval(
        @org.apache.flink.table.annotation.DataTypeHint(NestedJoinChecksum.TYPE)
            org.apache.flink.types.Row row,
        Long rank) {
      return NestedJoinChecksum.checksum(row) * 31 + rank;
    }
  }

  public static final class NestedRowRankChecksum
      extends org.apache.flink.table.functions.ScalarFunction {
    public static final String TYPE = "ROW<a ARRAY<BIGINT>,r ROW<v BIGINT,s STRING>>";

    public Long eval(
        @org.apache.flink.table.annotation.DataTypeHint(TYPE) org.apache.flink.types.Row row,
        Long rank) {
      if (row == null) return -7 * 31 + rank;
      Object[] array = (Object[]) row.getField(0);
      long result = 17;
      if (array != null)
        for (Object value : array) result = result * 37 + (value == null ? -1 : (Long) value);
      var nested = (org.apache.flink.types.Row) row.getField(1);
      if (nested != null) {
        result = result * 41 + (nested.getField(0) == null ? -5 : (Long) nested.getField(0));
        result += nested.getField(1) == null ? -11 : nested.getField(1).hashCode();
      }
      return result * 31 + rank;
    }
  }

  public static final class NestedJoinChecksum
      extends org.apache.flink.table.functions.ScalarFunction {
    public static final String TYPE =
        "ROW<a ARRAY<BIGINT>,m MAP<STRING,BIGINT>,r ROW<v BIGINT,s STRING>>";

    public Long eval(
        @org.apache.flink.table.annotation.DataTypeHint(TYPE) org.apache.flink.types.Row left,
        @org.apache.flink.table.annotation.DataTypeHint(TYPE) org.apache.flink.types.Row right) {
      return checksum(left) * 31 + checksum(right);
    }

    private static long checksum(org.apache.flink.types.Row row) {
      if (row == null) return -7;
      long result = 17;
      Object[] array = (Object[]) row.getField(0);
      if (array != null)
        for (Object value : array) result = result * 37 + (value == null ? -1 : (Long) value);
      @SuppressWarnings("unchecked")
      var map = (java.util.Map<String, Long>) row.getField(1);
      if (map != null)
        for (var entry : map.entrySet())
          result +=
              entry.getKey().hashCode() * 19L + (entry.getValue() == null ? -3 : entry.getValue());
      var nested = (org.apache.flink.types.Row) row.getField(2);
      if (nested != null) {
        var value = (Long) nested.getField(0);
        var text = (String) nested.getField(1);
        result = result * 41 + (value == null ? -5 : value);
        result += text == null ? -11 : text.hashCode();
      }
      return result;
    }
  }

  public static final class MergeHistoryAccumulator {
    public long sum;
    public long rows;
    public org.apache.flink.table.api.dataview.MapView<Long, Long> counts =
        new org.apache.flink.table.api.dataview.MapView<>();
    public org.apache.flink.table.api.dataview.ListView<
            org.apache.flink.api.java.tuple.Tuple2<Long, Long>>
        history = new org.apache.flink.table.api.dataview.ListView<>();
  }

  public static final class MergeHistoryAggregate
      extends org.apache.flink.table.functions.AggregateFunction<Long, MergeHistoryAccumulator> {
    @Override
    public MergeHistoryAccumulator createAccumulator() {
      return new MergeHistoryAccumulator();
    }

    @Override
    public Long getValue(MergeHistoryAccumulator accumulator) {
      try {
        long distinct = 0, mappedSum = 0, mappedCount = 0, listedSum = 0, listedCount = 0;
        var entries = accumulator.counts.entries();
        if (entries != null)
          for (var entry : entries) {
            if (entry.getValue() > 0) distinct++;
            mappedSum += entry.getKey() * entry.getValue();
            mappedCount += entry.getValue();
          }
        var history = accumulator.history.get();
        if (history != null)
          for (var entry : history) {
            listedSum += entry.f0 * entry.f1;
            listedCount += entry.f1;
          }
        if (mappedSum != accumulator.sum
            || listedSum != accumulator.sum
            || listedCount != mappedCount)
          throw new IllegalStateException("Signed accumulator and managed views disagree");
        return accumulator.sum + accumulator.rows + 1000L * distinct;
      } catch (Exception failure) {
        throw new IllegalStateException(failure);
      }
    }

    public void accumulate(MergeHistoryAccumulator accumulator, Long value) throws Exception {
      change(accumulator, value, 1);
    }

    public void retract(MergeHistoryAccumulator accumulator, Long value) throws Exception {
      change(accumulator, value, -1);
    }

    private void change(MergeHistoryAccumulator accumulator, Long value, long delta)
        throws Exception {
      accumulator.rows += delta;
      if (value == null) return;
      accumulator.sum += value * delta;
      // A local bundle can retract a value owned by an earlier global bundle.
      accumulator.history.add(org.apache.flink.api.java.tuple.Tuple2.of(value, delta));
      Long previous = accumulator.counts.get(value);
      long next = (previous == null ? 0 : previous) + delta;
      if (next == 0) accumulator.counts.remove(value);
      else accumulator.counts.put(value, next);
    }

    public void merge(
        MergeHistoryAccumulator accumulator, Iterable<MergeHistoryAccumulator> partials)
        throws Exception {
      for (var partial : partials) {
        accumulator.sum += partial.sum;
        accumulator.rows += partial.rows;
        var history = partial.history.get();
        if (history != null)
          for (var entry : history)
            accumulator.history.add(org.apache.flink.api.java.tuple.Tuple2.of(entry.f0, entry.f1));
        var entries = partial.counts.entries();
        if (entries != null)
          for (var entry : entries) {
            Long previous = accumulator.counts.get(entry.getKey());
            long next = (previous == null ? 0 : previous) + entry.getValue();
            if (next == 0) accumulator.counts.remove(entry.getKey());
            else accumulator.counts.put(entry.getKey(), next);
          }
      }
    }
  }

  public static final class HistoryAccumulator {
    public long sum;
    public long rows;
    public org.apache.flink.table.api.dataview.MapView<Long, Long> counts =
        new org.apache.flink.table.api.dataview.MapView<>();
    public org.apache.flink.table.api.dataview.ListView<Long> history =
        new org.apache.flink.table.api.dataview.ListView<>();
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
        long distinct = 0, mappedSum = 0, mappedCount = 0, listedSum = 0, listedCount = 0;
        var entries = accumulator.counts.entries();
        if (entries != null)
          for (var entry : entries) {
            distinct++;
            mappedSum += entry.getKey() * entry.getValue();
            mappedCount += entry.getValue();
          }
        var history = accumulator.history.get();
        if (history != null)
          for (long value : history) {
            listedSum += value;
            listedCount++;
          }
        if (mappedSum != accumulator.sum
            || listedSum != accumulator.sum
            || listedCount != mappedCount)
          throw new IllegalStateException("Accumulator and managed views disagree");
        return accumulator.sum + accumulator.rows + 1000L * distinct;
      } catch (Exception error) {
        throw new IllegalStateException(error);
      }
    }

    public void accumulate(HistoryAccumulator accumulator, Long value) throws Exception {
      accumulator.rows++;
      if (value != null) {
        accumulator.sum += value;
        accumulator.history.add(value);
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
        if (!accumulator.history.remove(value))
          throw new IllegalStateException("Missing history value on retract");
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
        var history = partial.history.get();
        if (history != null) for (Long value : history) accumulator.history.add(value);
        for (var entry : partial.counts.entries()) {
          Long previous = accumulator.counts.get(entry.getKey());
          long next = (previous == null ? 0 : previous) + entry.getValue();
          if (next == 0) accumulator.counts.remove(entry.getKey());
          else accumulator.counts.put(entry.getKey(), next);
        }
      }
    }
  }

  public static List<byte[]> committedRecords(Path directory) throws IOException {
    List<byte[]> result = new ArrayList<>();
    try (var files = Files.list(directory)) {
      for (Path file :
          files
              .filter(path -> path.getFileName().toString().startsWith("transaction-"))
              .sorted()
              .collect(java.util.stream.Collectors.toList())) {
        var serializer = new RowsSerializer();
        WriterState state = serializer.committed(Files.readAllBytes(file));
        result.addAll(state.rows);
      }
    }
    return result;
  }

  public static List<String> committedRows(Path directory, RowType type) throws IOException {
    List<String> result = new ArrayList<>();
    RowDataSerializer serializer = new RowDataSerializer(type);
    for (byte[] bytes : committedRecords(directory)) {
      RowData row = serializer.deserialize(new DataInputDeserializer(bytes));
      StringBuilder value = new StringBuilder(row.getRowKind().shortString());
      for (int i = 0; i < row.getArity(); i++)
        value.append('|').append(row.isNullAt(i) ? "null" : row.getLong(i));
      result.add(value.toString());
    }
    return result;
  }
}
