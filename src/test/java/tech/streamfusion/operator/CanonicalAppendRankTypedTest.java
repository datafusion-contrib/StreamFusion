package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.FirstNCanonicalTestSupport.snapshot;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.*;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.*;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.StockAppendTopNStateContract;

class CanonicalAppendRankTypedTest {
  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(allCases());
  }

  private static Stream<Arguments> allCases() {
    return Stream.of("released", "released-nn", "long", "string", "collision", "composite")
        .flatMap(
            shape ->
                Stream.of(false, true)
                    .flatMap(
                        fast ->
                            Stream.of(false, true)
                                .flatMap(
                                    rocks ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                ttl ->
                                                    Stream.of(false, true)
                                                        .map(
                                                            rank ->
                                                                Arguments.of(
                                                                    shape, fast, rocks, ttl,
                                                                    rank))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void typedRetainedRowsSurviveBothCanonicalDirections(
      String shape, boolean fast, boolean rocks, boolean ttl, boolean rank) throws Exception {
    RowType type =
        shape.startsWith("released")
            ? RowType.of(
                new IntType(), new VarCharType(100), new IntType(!shape.equals("released-nn")))
            : RowType.of(
                shape.equals("long") ? new VarCharType(100) : new IntType(),
                shape.equals("long") ? new BigIntType() : new VarCharType(100),
                new VarCharType(100));
    if (shape.equals("collision"))
      type =
          RowType.of(
              new LogicalType[] {new IntType(), new VarCharType(100), new IntType()},
              new String[] {"binary_key", "__rank_end__", "__ttl_ts__"});
    if (shape.equals("composite"))
      type = RowType.of(new BigIntType(), new BigIntType(), new IntType(), new VarCharType(100));
    int[] key =
        shape.equals("composite")
            ? new int[] {3, 2}
            : new int[] {shape.startsWith("released") || shape.equals("collision") ? 1 : 0};
    int sort = shape.startsWith("released") || shape.equals("collision") ? 2 : 1;
    RowType output = rank ? appendRank(type) : type;
    var selector = selector(type, key);
    var sortSelector = selector(type, sort);
    var keyType = (RowType) selector.getProducedType().toLogicalType();
    long retention = ttl ? 1000 : 0;
    var contract =
        new StockAppendTopNStateContract(
            new RowDataSerializer(keyType),
            selector,
            sortSelector,
            keyType,
            type,
            new int[] {sort},
            fast,
            false,
            ttl);
    try (var stock = stock(type, output, key, sort, retention, fast, rank, rocks);
        var nativeHarness =
            nativeHarness(type, key, sort, retention, fast, rank, rocks, contract)) {
      stock.open();
      synchronizeCacheClock(stock, fast);
      time(stock, 1000);
      feed(
          stock,
          type,
          List.of(
              row(shape, 1, 20, 1),
              row(shape, 1, 10, 2),
              row(shape, 1, 10, 3),
              row(shape, 2, 30, 4),
              row(shape, null, 40, 5)));
      nativeHarness.initializeState(snapshot(stock));
      nativeHarness.open();
      assertEquals(
          rocks ? "rocksdb" : "memory", ((CanonicalRank) nativeHarness.getOperator()).storage());
      var suffix = List.of(row(shape, 1, 12, 6), row(shape, 2, 25, 7), row(shape, null, 35, 8));
      time(stock, 1800);
      feed(stock, type, suffix);
      push(nativeHarness, type, suffix, 1800);
      assertEquals(
          bytes(output, stock.extractOutputValues()),
          bytes(output, collect(nativeHarness, output)));
      try (var restored = stock(type, output, key, sort, retention, fast, rank, rocks)) {
        nativeHarness.prepareSnapshotPreBarrier(2);
        restored.initializeState(
            nativeHarness
                .snapshotWithLocalState(
                    2,
                    1800,
                    org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                        org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                .getJobManagerOwnedState());
        restored.open();
        synchronizeCacheClock(restored, fast);
        var future =
            List.of(
                row(shape, 1, 9, 9),
                row(shape, 2, shape.equals("released-nn") ? 0 : null, 10),
                row(shape, null, 34, 11));
        time(stock, 1900);
        time(restored, 1900);
        feed(stock, type, future);
        feed(restored, type, future);
        push(nativeHarness, type, future, 1900);
        var expected = bytes(output, stock.extractOutputValues());
        assertFalse(expected.isEmpty());
        assertEquals(expected, bytes(output, restored.extractOutputValues()));
        assertEquals(expected, bytes(output, collect(nativeHarness, output)));
        var expired = List.of(row(shape, 1, 50, 12), row(shape, 2, 60, 13));
        time(stock, 2900);
        time(restored, 2900);
        feed(stock, type, expired);
        feed(restored, type, expired);
        push(nativeHarness, type, expired, 2900);
        expected = bytes(output, stock.extractOutputValues());
        assertEquals(expected, bytes(output, restored.extractOutputValues()));
        assertEquals(expected, bytes(output, collect(nativeHarness, output)));
      }
    }
  }

  private static void synchronizeCacheClock(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness, boolean fast)
      throws Exception {
    var process = (KeyedProcessOperator<?, ?, ?>) harness.getOneInputOperator();
    var function = process.getUserFunction();
    Object owner = function;
    if (tech.streamfusion.compat.CanonicalTestCompat.flink22()) owner = field(function, "helper");
    var cache = field(owner, fast ? "kvCache" : "kvSortedMap");
    var localField = cache.getClass().getDeclaredField("localCache");
    localField.setAccessible(true);
    var local = localField.get(cache);
    var ticker = local.getClass().getDeclaredField("ticker");
    ticker.setAccessible(true);
    // Flink's state TTL clock and Guava's default wall clock must advance together in this harness.
    ticker.set(
        local,
        tech.streamfusion.compat.CanonicalTestCompat.ticker(
            () ->
                java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(
                    harness.getProcessingTimeService().getCurrentProcessingTime())));
  }

  private static Object field(Object owner, String name) throws Exception {
    for (Class<?> type = owner.getClass(); type != null; type = type.getSuperclass()) {
      try {
        var field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(owner);
      } catch (NoSuchFieldException inherited) {
      }
    }
    throw new NoSuchFieldException(name);
  }

  private static RowType appendRank(RowType type) {
    var fields = new ArrayList<>(type.getFields());
    fields.add(new RowType.RowField("rank", new BigIntType()));
    return new RowType(fields);
  }

  private static RowData row(String shape, Integer key, Integer score, int id) {
    if (shape.equals("composite"))
      return GenericRowData.of(
          (long) id,
          score == null ? null : score.longValue(),
          key,
          key == null ? null : StringData.fromString("分区-" + key));
    Object k =
        key == null
            ? null
            : shape.equals("long") || shape.startsWith("released") || shape.equals("collision")
                ? StringData.fromString("分区-" + key)
                : key;
    if (shape.startsWith("released") || shape.equals("collision"))
      return GenericRowData.of(id, k, score);
    Object value =
        score == null
            ? null
            : shape.equals("long")
                ? Long.valueOf(score)
                : StringData.fromString(String.format(java.util.Locale.ROOT, "%03d", score));
    return GenericRowData.of(k, value, StringData.fromString("payload-" + id));
  }

  private static RowDataKeySelector selector(RowType type, int... columns) {
    return KeySelectorUtil.getRowDataSelector(
        CanonicalAppendRankTypedTest.class.getClassLoader(), columns, InternalTypeInfo.of(type));
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(
      RowType type,
      RowType output,
      int[] key,
      int sort,
      long ttl,
      boolean fast,
      boolean rank,
      boolean rocks)
      throws Exception {
    var comparator =
        new GeneratedRecordComparator("", "", new Object[0]) {
          @Override
          public RecordComparator newInstance(ClassLoader loader) {
            return (left, right) -> {
              if (left.isNullAt(0)) return right.isNullAt(0) ? 0 : -1;
              if (right.isNullAt(0)) return 1;
              switch (type.getTypeAt(sort).getTypeRoot()) {
                case INTEGER:
                  return Integer.compare(left.getInt(0), right.getInt(0));
                case BIGINT:
                  return Long.compare(left.getLong(0), right.getLong(0));
                default:
                  return left.getString(0).compareTo(right.getString(0));
              }
            };
          }
        };
    AbstractTopNFunction function =
        fast
            ? new FastTop1Function(
                StateConfigUtil.createTtlConfig(ttl),
                InternalTypeInfo.of(type),
                comparator,
                selector(type, sort),
                RankType.ROW_NUMBER,
                new ConstantRankRange(1, 1),
                true,
                rank,
                10000)
            : new AppendOnlyTopNFunction(
                StateConfigUtil.createTtlConfig(ttl),
                InternalTypeInfo.of(type),
                comparator,
                selector(type, sort),
                RankType.ROW_NUMBER,
                new ConstantRankRange(1, 3),
                true,
                rank,
                10000);
    var process = new KeyedProcessOperator<RowData, RowData, RowData>(function);
    function.setKeyContext(process);
    var selector = selector(type, key);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            process, selector, selector.getProducedType(), 128, 1, 0);
    harness.setStateBackend(
        rocks
            ? tech.streamfusion.compat.CanonicalTestCompat.rocksBackend()
            : new HashMapStateBackend());
    harness.setup(new RowDataSerializer(output));
    return harness;
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      nativeHarness(
          RowType type,
          int[] key,
          int sort,
          long ttl,
          boolean fast,
          boolean rank,
          boolean rocks,
          StockAppendTopNStateContract contract)
          throws Exception {
    var descriptorMethod =
        tech.streamfusion.planner.FlinkKeyGroupUtils.class.getDeclaredMethod(
            "stockKeyDescriptor", RowType.class, int[].class);
    descriptorMethod.setAccessible(true);
    var descriptor = (int[]) descriptorMethod.invoke(null, type, key);
    var operator = new CanonicalRank(type, key, sort, descriptor, ttl, fast, rank);
    operator.setStockStateContract(contract);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            operator, batch -> 0, Types.INT, 128, 1, 0);
    var config = new Configuration();
    if (rocks)
      config.setString(
          org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND.key(),
          tech.streamfusion.state.RocksDBNativeStateBackendFactory.class.getName());
    tech.streamfusion.state.CanonicalStateBackend.install(
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
            .getExecutionEnvironment(),
        config,
        harness.getStreamConfig().getOperatorID().toHexString(),
        operator.getClass().getSimpleName(),
        contract,
        CanonicalAppendRankTypedTest.class.getClassLoader());
    harness.setStateBackend(
        new tech.streamfusion.state.CanonicalStateBackendFactory()
            .createFromConfig(config, CanonicalAppendRankTypedTest.class.getClassLoader()));
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }

  static void time(KeyedOneInputStreamOperatorTestHarness<?, ?, ?> harness, long time)
      throws Exception {
    harness.setProcessingTime(time);
    harness.setStateTtlProcessingTime(time);
  }

  static void feed(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness,
      RowType type,
      List<RowData> rows)
      throws Exception {
    harness.getOutput().clear();
    var serializer = new RowDataSerializer(type);
    for (var row : rows)
      harness.processElement(new StreamRecord<>(serializer.toBinaryRow(row).copy()));
  }

  static void push(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RowType type,
      List<RowData> rows,
      long now)
      throws Exception {
    time(harness, now);
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED))));
    harness.prepareSnapshotPreBarrier(0);
  }

  static List<RowData> collect(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RowType type) {
    var rows = new ArrayList<RowData>();
    var serializer = new RowDataSerializer(type);
    while (!harness.getOutput().isEmpty()) {
      var event = harness.getOutput().poll();
      if (!(event instanceof StreamRecord)) continue;
      try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
        var reader = ArrowConversion.createArrowReader(root, type);
        var kinds = (org.apache.arrow.vector.TinyIntVector) root.getVector(type.getFieldCount());
        for (int i = 0; i < root.getRowCount(); i++) {
          var row = serializer.copy(reader.read(i));
          row.setRowKind(RowKind.fromByteValue(kinds.get(i)));
          rows.add(row);
        }
      }
    }
    return rows;
  }

  static List<String> bytes(RowType type, List<RowData> rows) {
    var serializer = new RowDataSerializer(type);
    return rows.stream()
        .map(
            row -> {
              var binary = serializer.toBinaryRow(row);
              return Base64.getEncoder()
                  .encodeToString(
                      org.apache.flink.table.data.binary.BinarySegmentUtils.copyToBytes(
                          binary.getSegments(), binary.getOffset(), binary.getSizeInBytes()));
            })
        .collect(java.util.stream.Collectors.toList());
  }

  private static final class CanonicalRank extends NativeColumnarTopNOperator {
    private boolean canonical;

    CanonicalRank(
        RowType type, int[] key, int sort, int[] descriptor, long ttl, boolean fast, boolean rank) {
      super(
          key,
          descriptor,
          type,
          new int[] {sort},
          new int[] {1},
          new int[] {1},
          0,
          fast ? 1 : 3,
          rank,
          false,
          null,
          null,
          true,
          false,
          -1,
          ttl,
          128);
    }

    String storage() {
      return rocksdbState() ? directRocksDBState() ? "rocksdb" : "rocksdb-snapshot" : "memory";
    }

    @Override
    protected org.apache.flink.streaming.api.operators.OperatorSnapshotFutures
        snapshotCanonicalState(
            long id,
            long time,
            org.apache.flink.runtime.checkpoint.CheckpointOptions options,
            org.apache.flink.runtime.state.CheckpointStreamFactory streams)
            throws Exception {
      canonical = true;
      try {
        return super.snapshotCanonicalState(id, time, options, streams);
      } finally {
        canonical = false;
      }
    }

    @Override
    protected byte[][] snapshotRawPartitions() {
      if (canonical)
        throw new AssertionError("Canonical append export serialized raw native state");
      return super.snapshotRawPartitions();
    }

    @Override
    protected String[] checkpointRocksDBHandle(String directory) {
      if (canonical && !directory.isEmpty())
        throw new AssertionError("Canonical append export created a native checkpoint");
      return super.checkpointRocksDBHandle(directory);
    }

    @Override
    protected byte[][] snapshotCanonicalPartitions() {
      if (canonical)
        throw new AssertionError("Canonical append export serialized a native snapshot");
      return super.snapshotCanonicalPartitions();
    }
  }
}
