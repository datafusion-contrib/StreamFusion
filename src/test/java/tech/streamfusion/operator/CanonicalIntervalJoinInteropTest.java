package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.*;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.interval.IntervalJoinFunction;
import org.apache.flink.table.runtime.operators.join.interval.RowTimeIntervalJoin;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.state.*;

class CanonicalIntervalJoinInteropTest {
  static final SavepointType CANONICAL = SavepointType.savepoint(SavepointFormatType.CANONICAL);

  static Stream<Arguments> cases() {
    return tech.streamfusion.CanonicalTestMatrix.select(allCases());
  }

  private static Stream<Arguments> allCases() {
    var cases = new ArrayList<Arguments>();
    for (int kind = 0; kind < 4; kind++)
      for (boolean stockDisk : new boolean[] {false, true})
        for (boolean nativeDisk : new boolean[] {false, true})
          for (boolean timestamp : new boolean[] {false, true})
            cases.add(Arguments.of(kind, stockDisk, nativeDisk, timestamp));
    return Boolean.getBoolean("streamfusion.test.intervalJoinSmoke")
        ? cases.stream().limit(1)
        : cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void stockListsAndTimersReexportBeforeInputAndSurviveNativeRecovery(
      int kind, boolean stockDisk, boolean nativeDisk, boolean timestamp) throws Exception {
    var plan = new Plan(kind, timestamp);
    OperatorSubtaskState saved;
    try (var stock = plan.stock(stockDisk)) {
      stock.open();
      plan.prefix(stock);
      assertEquals(2, plan.stockOutput(stock).size());
      saved = snapshot(stock);
    }
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var nativeOp = plan.nativeHarness(nativeDisk)) {
      nativeOp.initializeState(saved);
      nativeOp.open();
      assertEquals(
          allocated, NativeAllocator.SHARED.getAllocatedMemory(), "import retained Arrow buffers");
      assertTrue(plan.nativeOutput(nativeOp).isEmpty());
      try (var stock = plan.stock(!stockDisk)) {
        stock.initializeState(snapshot(nativeOp));
        stock.open();
        plan.verifyMetadata(stock);
        plan.extra(stock);
        plan.extraNative(nativeOp);
        assertEquals(plan.stockOutput(stock), plan.nativeOutput(nativeOp));
        nativeOp.prepareSnapshotPreBarrier(7);
        var checkpoint = nativeOp.snapshot(7, 0);
        assertTrue(
            checkpoint.getManagedKeyedState().stream()
                .noneMatch(
                    handle ->
                        handle
                            instanceof org.apache.flink.runtime.state.SavepointKeyedStateHandle));
        try (var recovered = plan.nativeHarness(nativeDisk)) {
          recovered.initializeState(checkpoint);
          recovered.open();
          try (var again = plan.stock(stockDisk)) {
            again.initializeState(snapshot(recovered));
            again.open();
            for (long mark : new long[] {200, 201, 251, 301, 401, Long.MAX_VALUE}) {
              marks(stock, mark);
              marks(again, mark);
              marks(recovered, mark);
              var expected = plan.stockOutput(stock);
              assertEquals(expected, plan.stockOutput(again));
              assertEquals(expected, plan.nativeOutput(recovered));
            }
          }
        }
      }
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @ParameterizedTest
  @MethodSource("cases")
  void nativeIngressExportsToTheReleasedStockCache(
      int kind, boolean stockDisk, boolean nativeDisk, boolean timestamp) throws Exception {
    var plan = new Plan(kind, timestamp);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var reference = plan.stock(stockDisk);
        var nativeOp = plan.nativeHarness(nativeDisk)) {
      reference.open();
      nativeOp.open();
      plan.prefix(reference);
      plan.prefixNative(nativeOp);
      assertEquals(plan.stockOutput(reference), plan.nativeOutput(nativeOp));
      try (var switched = plan.stock(!stockDisk)) {
        switched.initializeState(snapshot(nativeOp));
        switched.open();
        plan.verifyMetadata(switched);
        plan.extra(reference);
        plan.extra(switched);
        plan.extraNative(nativeOp);
        var expected = plan.stockOutput(reference);
        assertEquals(expected, plan.stockOutput(switched));
        assertEquals(expected, plan.nativeOutput(nativeOp));
        for (long mark : new long[] {200, 201, 251, 301, 401, Long.MAX_VALUE}) {
          marks(reference, mark);
          marks(switched, mark);
          marks(nativeOp, mark);
          expected = plan.stockOutput(reference);
          assertEquals(expected, plan.stockOutput(switched));
          assertEquals(expected, plan.nativeOutput(nativeOp));
        }
      }
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @ParameterizedTest
  @MethodSource("cases")
  void emptyAndTimerOnlyKeysSurviveNoInputHandoffs(
      int kind, boolean stockDisk, boolean nativeDisk, boolean timestamp) throws Exception {
    var plan = new Plan(kind, timestamp);
    try (var stock = plan.stock(stockDisk);
        var nativeOp = plan.nativeHarness(nativeDisk)) {
      stock.open();
      var backend = backend(stock);
      backend.setCurrentKey(plan.selector.getKey(plan.row(99L, 0L, 100)));
      backend
          .getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new org.apache.flink.api.common.state.ValueStateDescriptor<Long>(
                  "IntervalJoinRightTimerState", Long.class))
          .update(350L);
      var operator =
          (KeyedCoProcessOperator<RowData, RowData, RowData, RowData>) stock.getOperator();
      operator
          .getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, operator)
          .registerEventTimeTimer(VoidNamespace.INSTANCE, 350);
      nativeOp.initializeState(snapshot(stock));
      nativeOp.open();
      try (var restored = plan.stock(!stockDisk)) {
        restored.initializeState(snapshot(nativeOp));
        restored.open();
        var restoredBackend = backend(restored);
        restoredBackend.setCurrentKey(plan.selector.getKey(plan.row(99L, 0L, 100)));
        assertEquals(
            350L,
            restoredBackend
                .getPartitionedState(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    new org.apache.flink.api.common.state.ValueStateDescriptor<Long>(
                        "IntervalJoinRightTimerState", Long.class))
                .value());
        marks(restored, 350);
        marks(nativeOp, 350);
        assertTrue(plan.stockOutput(restored).isEmpty());
        assertTrue(plan.nativeOutput(nativeOp).isEmpty());
        var emptyState = snapshot(nativeOp);
        try (var empty = plan.stock(stockDisk);
            var freshNative = plan.nativeHarness(nativeDisk)) {
          empty.initializeState(emptyState);
          empty.open();
          freshNative.initializeState(emptyState);
          freshNative.open();
          plan.prefix(empty);
          plan.prefixNative(freshNative);
          assertEquals(plan.stockOutput(empty), plan.nativeOutput(freshNative));
        }
      }
    }
  }

  static Stream<Arguments> typedKeys() {
    var cases = new ArrayList<Arguments>();
    for (String key :
        List.of(
            "long",
            "int",
            "string",
            "decimal",
            "timestamp",
            "compactTimestamp",
            "boolean",
            "double",
            "binary"))
      for (int kind = 0; kind < 4; kind++)
        for (boolean stockDisk : new boolean[] {false, true})
          for (boolean nativeDisk : new boolean[] {false, true})
            cases.add(Arguments.of(key, kind, stockDisk, nativeDisk));
    return tech.streamfusion.CanonicalTestMatrix.select(cases.stream());
  }

  @ParameterizedTest
  @MethodSource("typedKeys")
  void actualTypedKeysAndNullSlotsPreserveTimerOwnership(
      String key, int kind, boolean stockDisk, boolean nativeDisk) throws Exception {
    var plan = new Plan(kind, true, key);
    try (var reference = plan.stock(stockDisk);
        var original = plan.nativeHarness(nativeDisk)) {
      reference.open();
      original.open();
      assertEquals(
          nativeDisk, ((NativeIntervalJoinOperator) original.getOperator()).directRocksDBState());
      plan.prefix(reference);
      plan.prefixNative(original);
      var expected = plan.stockOutput(reference);
      assertFalse(expected.isEmpty());
      assertEquals(expected, plan.nativeOutput(original));
      try (var stock = plan.stock(!stockDisk)) {
        stock.initializeState(snapshot(original));
        stock.open();
        plan.verifyMetadata(stock);
        try (var restored = plan.nativeHarness(nativeDisk)) {
          restored.initializeState(snapshot(stock));
          restored.open();
          restored.prepareSnapshotPreBarrier(9);
          var nativeCheckpoint = restored.snapshot(9, 0);
          try (var resumed = plan.nativeHarness(nativeDisk)) {
            resumed.initializeState(nativeCheckpoint);
            resumed.open();
            plan.extra(reference);
            plan.extra(stock);
            plan.extraNative(original);
            plan.extraNative(restored);
            plan.extraNative(resumed);
            expected = plan.stockOutput(reference);
            assertEquals(expected, plan.stockOutput(stock));
            assertEquals(expected, plan.nativeOutput(original));
            assertEquals(expected, plan.nativeOutput(restored));
            assertEquals(expected, plan.nativeOutput(resumed));
            for (long mark : new long[] {201, 251, 301, 401, Long.MAX_VALUE}) {
              marks(reference, mark);
              marks(stock, mark);
              marks(original, mark);
              marks(restored, mark);
              marks(resumed, mark);
              expected = plan.stockOutput(reference);
              assertEquals(expected, plan.stockOutput(stock));
              assertEquals(expected, plan.nativeOutput(original));
              assertEquals(expected, plan.nativeOutput(restored));
              assertEquals(expected, plan.nativeOutput(resumed));
            }
          }
        }
      }
    }
  }

  static final class Plan {
    final int kind;
    final RowType input, output, keyType;
    final boolean timestamp;
    final boolean processingTime;
    final RowDataKeySelector selector;
    final String keyEncoding;

    Plan(int kind, boolean timestamp) {
      this(kind, timestamp, "long");
    }

    Plan(int kind, boolean timestamp, String keyEncoding) {
      this(kind, timestamp, keyEncoding, false);
    }

    Plan(int kind, boolean timestamp, String keyEncoding, boolean processingTime) {
      this.processingTime = processingTime;
      this.kind = kind;
      this.timestamp = timestamp;
      this.keyEncoding = keyEncoding;
      LogicalType key;
      switch (keyEncoding) {
        case "int":
          key = new IntType();
          break;
        case "string":
          key = new VarCharType(VarCharType.MAX_LENGTH);
          break;
        case "decimal":
          key = new DecimalType(38, 2);
          break;
        case "timestamp":
          key = new TimestampType(6);
          break;
        case "compactTimestamp":
          key = new TimestampType(3);
          break;
        case "boolean":
          key = new BooleanType();
          break;
        case "double":
          key = new DoubleType();
          break;
        case "binary":
          key = new VarBinaryType(VarBinaryType.MAX_LENGTH);
          break;
        default:
          key = new BigIntType();
      }
      input =
          RowType.of(
              key,
              new BigIntType(),
              timestamp
                  ? processingTime
                      ? new org.apache.flink.table.types.logical.LocalZonedTimestampType(
                          false, org.apache.flink.table.types.logical.TimestampKind.PROCTIME, 3)
                      : new TimestampType(3)
                  : new BigIntType());
      var fields = new ArrayList<RowType.RowField>();
      for (int side = 0; side < 2; side++)
        for (int column = 0; column < input.getFieldCount(); column++)
          fields.add(new RowType.RowField("f" + side + column, input.getTypeAt(column).copy(true)));
      output = new RowType(fields);
      selector =
          KeySelectorUtil.getRowDataSelector(
              getClass().getClassLoader(), new int[] {0}, InternalTypeInfo.of(input));
      keyType = (RowType) selector.getProducedType().toLogicalType();
    }

    RowDataSerializer keySerializer() {
      return new RowDataSerializer(keyType);
    }

    RowData row(Long key, Long value, long time) {
      return new RowDataSerializer(input)
          .toBinaryRow(
              GenericRowData.of(
                  keyValue(key), value, timestamp ? TimestampData.fromEpochMillis(time) : time))
          .copy();
    }

    private Object keyValue(Long key) {
      if (key == null) return null;
      switch (keyEncoding) {
        case "int":
          return Math.toIntExact(key);
        case "string":
          return StringData.fromString("key-" + key);
        case "decimal":
          return DecimalData.fromBigDecimal(java.math.BigDecimal.valueOf(key).setScale(2), 38, 2);
        case "timestamp":
          return TimestampData.fromEpochMillis(key * 1000, 123456);
        case "compactTimestamp":
          return TimestampData.fromEpochMillis(key * 1000);
        case "boolean":
          return key % 2 == 0;
        case "double":
          return key + 0.25;
        case "binary":
          return ("key-" + key).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        default:
          return key;
      }
    }

    void prefix(KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h)
        throws Exception {
      for (var row :
          List.of(
              row(1L, 10L, 100),
              row(1L, 11L, 100),
              row(2L, 20L, 100),
              row(4L, null, 300),
              row(null, 7L, 100))) h.processElement1(new StreamRecord<>(row));
      h.processElement2(new StreamRecord<>(row(1L, 30L, 150)));
      h.processElement2(new StreamRecord<>(row(3L, 40L, 200)));
    }

    void prefixNative(
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> h)
        throws Exception {
      for (var row :
          List.of(
              row(1L, 10L, 100),
              row(1L, 11L, 100),
              row(2L, 20L, 100),
              row(4L, null, 300),
              row(null, 7L, 100))) push(h, true, row);
      push(h, false, row(1L, 30L, 150));
      push(h, false, row(3L, 40L, 200));
    }

    void extra(KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h)
        throws Exception {
      h.processElement1(new StreamRecord<>(row(3L, 41L, 250)));
      h.processElement2(new StreamRecord<>(row(2L, 21L, 120)));
    }

    void extraNative(
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> h)
        throws Exception {
      push(h, true, row(3L, 41L, 250));
      push(h, false, row(2L, 21L, 120));
    }

    void push(
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> h,
        boolean left,
        RowData row)
        throws Exception {
      var record =
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(List.of(row), input, NativeAllocator.SHARED)));
      if (left) h.processElement1(record);
      else h.processElement2(record);
    }

    KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> stock(boolean disk)
        throws Exception {
      String code =
          "public class CanonicalIntervalCondition extends"
              + " org.apache.flink.api.common.functions.AbstractRichFunction implements"
              + " org.apache.flink.table.runtime.generated.JoinCondition {public"
              + " CanonicalIntervalCondition(Object[] refs){}public boolean"
              + " apply(org.apache.flink.table.data.RowData l,org.apache.flink.table.data.RowData"
              + " r){return true;}}";
      var join =
          new IntervalJoinFunction(
              new GeneratedJoinCondition("CanonicalIntervalCondition", code, new Object[0]),
              InternalTypeInfo.of(output),
              new boolean[] {true});
      var kinds =
          new FlinkJoinType[] {
            FlinkJoinType.INNER, FlinkJoinType.LEFT, FlinkJoinType.RIGHT, FlinkJoinType.FULL
          };
      org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction<
              RowData, RowData, RowData, RowData>
          interval =
              processingTime
                  ? new org.apache.flink.table.runtime.operators.join.interval.ProcTimeIntervalJoin(
                      kinds[kind],
                      -100,
                      100,
                      0,
                      InternalTypeInfo.of(input),
                      InternalTypeInfo.of(input),
                      join)
                  : new RowTimeIntervalJoin(
                      kinds[kind],
                      -100,
                      100,
                      0,
                      0,
                      InternalTypeInfo.of(input),
                      InternalTypeInfo.of(input),
                      join,
                      2,
                      2);
      var h =
          new KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>(
              new KeyedCoProcessOperator<>(interval),
              selector,
              selector,
              selector.getProducedType(),
              128,
              1,
              0);
      if (disk) h.setStateBackend(tech.streamfusion.compat.CanonicalTestCompat.rocksBackend());
      h.setup(new RowDataSerializer(output));
      return h;
    }

    StockIntervalJoinStateContract contract() throws Exception {
      var contract =
          new StockIntervalJoinStateContract(
              keySerializer(),
              keyType,
              input,
              input,
              new int[] {0},
              new int[] {0},
              2,
              2,
              processingTime);
      var loader =
          new ClassLoader(getClass().getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve)
                throws ClassNotFoundException {
              if (name.startsWith("org.apache.flink.table.planner."))
                throw new ClassNotFoundException(name);
              return super.loadClass(name, resolve);
            }
          };
      return InstantiationUtil.deserializeObject(
          InstantiationUtil.serializeObject(contract), loader);
    }

    KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>
        nativeHarness(boolean disk) throws Exception {
      var operator = new CanonicalInterval(input, kind, processingTime);
      var contract = contract();
      operator.setStockStateContract(contract);
      var h =
          new KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch>(
              operator, batch -> 0, batch -> 0, Types.INT, 128, 1, 0);
      var config = new Configuration();
      if (disk)
        config.setString(
            org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND.key(),
            RocksDBNativeStateBackendFactory.class.getName());
      CanonicalStateBackend.install(
          StreamExecutionEnvironment.getExecutionEnvironment(),
          config,
          h.getStreamConfig().getOperatorID().toHexString(),
          operator.getClass().getSimpleName(),
          contract,
          getClass().getClassLoader());
      h.setStateBackend(
          new CanonicalStateBackendFactory().createFromConfig(config, getClass().getClassLoader()));
      h.setup(new ArrowBatchSerializer());
      return h;
    }

    List<String> stockOutput(
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h) {
      var result = new ArrayList<String>();
      while (!h.getOutput().isEmpty()) {
        var event = h.getOutput().poll();
        if (event instanceof StreamRecord)
          result.add(format((RowData) ((StreamRecord<?>) event).getValue()));
      }
      result.sort(String::compareTo);
      return result;
    }

    List<String> nativeOutput(
        KeyedTwoInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch, ArrowBatch> h) {
      var result = new ArrayList<String>();
      while (!h.getOutput().isEmpty()) {
        var event = h.getOutput().poll();
        if (event instanceof StreamRecord) {
          try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
            for (var row : RowDataArrowConverter.read(root, output)) result.add(format(row));
          }
        }
      }
      result.sort(String::compareTo);
      return result;
    }

    String format(RowData row) {
      var result = new StringBuilder(row.getRowKind().shortString());
      for (int i = 0; i < 6; i++) {
        Object value = RowData.createFieldGetter(output.getTypeAt(i), i).getFieldOrNull(row);
        result
            .append('|')
            .append(value instanceof byte[] ? Arrays.toString((byte[]) value) : value);
      }
      return result.toString();
    }

    void verifyMetadata(
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h)
        throws Exception {
      var resources = backend(h).savepoint().getSnapshotResources();
      var clean =
          new ClassLoader(getClass().getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve)
                throws ClassNotFoundException {
              if (name.startsWith("tech.streamfusion.")
                  || name.startsWith("org.apache.flink.table.planner."))
                throw new ClassNotFoundException(name);
              return super.loadClass(name, resolve);
            }
          };
      try (AutoCloseable release = resources::release) {
        for (var meta : resources.getMetaInfoSnapshots())
          for (var snapshot : meta.getSerializerSnapshotsImmutable().values()) {
            var output = new org.apache.flink.core.memory.DataOutputSerializer(128);
            org.apache.flink.api.common.typeutils.TypeSerializerSnapshot.writeVersionedSnapshot(
                output, snapshot);
            var input =
                new org.apache.flink.core.memory.DataInputDeserializer(output.getCopyOfBuffer());
            assertNotNull(
                org.apache.flink.api.common.typeutils.TypeSerializerSnapshot.readVersionedSnapshot(
                        input, clean)
                    .restoreSerializer());
          }
      }
    }
  }

  private static final class CanonicalInterval extends NativeIntervalJoinOperator {
    private boolean canonical;

    CanonicalInterval(RowType input, int kind, boolean processingTime) {
      super(
          new int[] {0},
          new int[] {0},
          2,
          2,
          -100,
          100,
          kind,
          input,
          input,
          EncodedPredicate.NONE,
          processingTime,
          new int[] {-1},
          128);
    }

    @Override
    protected org.apache.flink.streaming.api.operators.OperatorSnapshotFutures
        snapshotCanonicalState(
            long id,
            long timestamp,
            org.apache.flink.runtime.checkpoint.CheckpointOptions options,
            org.apache.flink.runtime.state.CheckpointStreamFactory streams)
            throws Exception {
      canonical = true;
      try {
        return super.snapshotCanonicalState(id, timestamp, options, streams);
      } finally {
        canonical = false;
      }
    }

    @Override
    protected byte[][] snapshotCanonicalPartitions() {
      if (canonical)
        throw new AssertionError("Canonical interval export serialized a native snapshot");
      return super.snapshotCanonicalPartitions();
    }
  }

  static OperatorSubtaskState snapshot(
      org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness<?> h) throws Exception {
    h.prepareSnapshotPreBarrier(1);
    return h.snapshotWithLocalState(1, 0, CANONICAL).getJobManagerOwnedState();
  }

  static void marks(KeyedTwoInputStreamOperatorTestHarness<?, ?, ?, ?> h, long mark)
      throws Exception {
    h.processWatermark1(new Watermark(mark));
    h.processWatermark2(new Watermark(mark));
  }

  @SuppressWarnings("unchecked")
  static CheckpointableKeyedStateBackend<RowData> backend(
      KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h) {
    return (CheckpointableKeyedStateBackend<RowData>)
        h.getOperator().<RowData>getKeyedStateBackend();
  }
}
