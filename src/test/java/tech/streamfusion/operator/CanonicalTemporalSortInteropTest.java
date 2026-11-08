package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.*;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.*;
import org.apache.flink.runtime.state.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.*;
import org.apache.flink.table.data.*;
import org.apache.flink.table.runtime.keyselector.EmptyRowDataKeySelector;
import org.apache.flink.table.runtime.operators.sort.RowTimeSortOperator;
import org.apache.flink.table.runtime.typeutils.*;
import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.*;

/** Released temporal sort canonical descriptors and ordered output, across both stores. */
class CanonicalTemporalSortInteropTest {
  @BeforeAll
  static void memory() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> modes() {
    return tech.streamfusion.CanonicalTestMatrix.select(allModes());
  }

  private static Stream<Arguments> allModes() {
    var out = new ArrayList<Arguments>();
    for (String type :
        List.of("long", "string", "decimal", "array", "row", "double", "binary", "map", "reserved"))
      for (boolean stock : new boolean[] {false, true})
        for (boolean nativeStore : new boolean[] {false, true})
          for (int maximum : new int[] {1, 128})
            for (int layout = 0; layout < 7; layout++)
              out.add(Arguments.of(type, stock, nativeStore, layout, maximum));
    return out.stream();
  }

  @ParameterizedTest
  @MethodSource("modes")
  void pendingRowsCutoffAndOrderedPeersContinueAcrossRepeatedEngineSwitches(
      String type, boolean stockStore, boolean nativeStore, int layout, int maximum)
      throws Exception {
    assertInterchange(new Plan(type, maximum), stockStore, nativeStore, layout);
  }

  void assertInterchange(Plan p, boolean stockStore, boolean nativeStore, int layout)
      throws Exception {
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    OperatorSubtaskState original;
    List<String> expected, expectedState;
    try (var stock = p.stock(stockStore)) {
      stock.open();
      prefixStock(p, stock, layout);
      original = snapshot(stock);
      suffixStock(p, stock, layout);
      expected = p.stockOutput(stock);
      assertEquals(layout == 6, expected.isEmpty());
      expectedState = p.digest(snapshot(stock));
    }
    OperatorSubtaskState exported;
    try (var target = p.nativeHarness(nativeStore)) {
      target.initializeState(original);
      target.open();
      exported = snapshot(target);
      assertEquals(
          p.digest(original), p.digest(exported), "Untouched stock/native/stock projection");
      suffixNative(p, target, layout);
      assertEquals(expected, p.nativeOutput(target), "Ordered rows after stock import");
      assertEquals(expectedState, p.digest(snapshot(target)));
    }
    try (var stock = p.stock(stockStore)) {
      stock.initializeState(exported);
      stock.open();
      suffixStock(p, stock, layout);
      assertEquals(expected, p.stockOutput(stock));
    }
    try (var nativeOp = p.nativeHarness(nativeStore)) {
      nativeOp.open();
      prefixNative(p, nativeOp, layout);
      exported = snapshot(nativeOp);
      assertEquals(p.digest(original), p.digest(exported), "Direct native state projection");
      suffixNative(p, nativeOp, layout);
      assertEquals(expected, p.nativeOutput(nativeOp));
      assertEquals(expectedState, p.digest(snapshot(nativeOp)));
    }
    try (var stock = p.stock(stockStore)) {
      stock.initializeState(exported);
      stock.open();
      suffixStock(p, stock, layout);
      assertEquals(expected, p.stockOutput(stock));
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void receiverFailureReleasesArrowOwnershipAndLeavesLiveStateAvailableForRetry(boolean nativeStore)
      throws Exception {
    var p = new Plan("long", 128);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var nativeOp = p.nativeHarness(nativeStore)) {
      nativeOp.open();
      prefixNative(p, nativeOp, 0);
      var expected = p.digest(snapshot(nativeOp));
      long handle = ((NativeColumnarTemporalSortOperator) nativeOp.getOperator()).handle;
      assertThrows(
          IllegalStateException.class,
          () ->
              tech.streamfusion.Native.exportTemporalSortCanonicalRows(
                  handle,
                  (a, s) -> {
                    try (var array = org.apache.arrow.c.ArrowArray.wrap(a);
                        var schema = org.apache.arrow.c.ArrowSchema.wrap(s);
                        var root =
                            org.apache.arrow.c.Data.importVectorSchemaRoot(
                                NativeAllocator.SHARED,
                                array,
                                schema,
                                NativeAllocator.DICTIONARIES)) {
                      assertTrue(root.getRowCount() > 0);
                      throw new IllegalStateException("receiver failed");
                    }
                  }));
      assertEquals(expected, p.digest(snapshot(nativeOp)));
      suffixNative(p, nativeOp, 0);
      assertFalse(p.nativeOutput(nativeOp).isEmpty());
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  static Stream<Arguments> corruptions() {
    var out = new ArrayList<Arguments>();
    for (String mode :
        List.of(
            "missingTimer",
            "extraTimer",
            "processingTimer",
            "emptyBucket",
            "alreadyEmitted",
            "wrongTime",
            "nullTime",
            "retraction",
            "unknownState"))
      for (boolean stock : new boolean[] {false, true})
        for (boolean nativeStore : new boolean[] {false, true})
          out.add(Arguments.of(mode, stock, nativeStore));
    return out.stream();
  }

  @ParameterizedTest
  @MethodSource("corruptions")
  @SuppressWarnings("unchecked")
  void malformedStateRejectsBeforeInstallingNativeRows(
      String mode, boolean stockStore, boolean nativeStore) throws Exception {
    var p = new Plan("long");
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var stock = p.stock(stockStore);
        var target = p.nativeHarness(nativeStore);
        var view =
            new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
                new StateView(), p.selector, p.selector.getProducedType(), 1, 1, 0)) {
      stock.open();
      prefixStock(p, stock, 0);
      p.stockBackend(view, stockStore);
      view.setup(new RowDataSerializer(p.input));
      view.initializeState(snapshot(stock));
      view.open();
      var backend =
          (CheckpointableKeyedStateBackend<RowData>)
              view.getOperator().<RowData>getKeyedStateBackend();
      var ns = VoidNamespace.INSTANCE;
      var namespaces = VoidNamespaceSerializer.INSTANCE;
      var longs = org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE;
      backend.setCurrentKey(p.selector.getKey(p.row(0, 0)));
      var rows =
          backend.getPartitionedState(
              ns,
              namespaces,
              new org.apache.flink.api.common.state.MapStateDescriptor<>(
                  "dataState",
                  longs,
                  new org.apache.flink.api.common.typeutils.base.ListSerializer<>(
                      new RowDataSerializer(p.input))));
      var timers =
          backend.<TimerHeapInternalTimer<RowData, VoidNamespace>>create(
              "_timer_state/event_user-timers",
              new TimerSerializer<>(p.selector.getProducedType().toSerializer(), namespaces));
      String expected = "Temporal sort";
      switch (mode) {
        case "missingTimer":
          timers.remove(new TimerHeapInternalTimer<>(100L, backend.getCurrentKey(), ns));
          break;
        case "extraTimer":
          timers.add(new TimerHeapInternalTimer<>(400L, backend.getCurrentKey(), ns));
          break;
        case "processingTimer":
          backend
              .<TimerHeapInternalTimer<RowData, VoidNamespace>>create(
                  "_timer_state/processing_user-timers",
                  new TimerSerializer<>(p.selector.getProducedType().toSerializer(), namespaces))
              .add(new TimerHeapInternalTimer<>(400L, backend.getCurrentKey(), ns));
          expected = "pending timers";
          break;
        case "emptyBucket":
          rows.put(100L, new ArrayList<>());
          break;
        case "alreadyEmitted":
          backend
              .getPartitionedState(
                  ns,
                  namespaces,
                  new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                      "lastTriggeringTsState", longs))
              .update(100L);
          break;
        case "wrongTime":
          rows.put(100L, List.of(p.row(9, 101)));
          break;
        case "nullTime":
          expected = "NULL temporal sort rowtime";
          rows.put(100L, List.of(GenericRowData.of(9L, 1L, null)));
          break;
        case "retraction":
          {
            var row = p.row(9, 100);
            row.setRowKind(org.apache.flink.types.RowKind.DELETE);
            rows.put(100L, List.of(row));
            break;
          }
        case "unknownState":
          backend
              .getPartitionedState(
                  ns,
                  namespaces,
                  new org.apache.flink.api.common.state.ValueStateDescriptor<>("unowned", longs))
              .update(1L);
          expected = "not owned";
          break;
        default:
          throw new AssertionError(mode);
      }
      var malformed = snapshot(view);
      var error = assertThrows(Exception.class, () -> target.initializeState(malformed));
      var messages = new StringBuilder();
      for (Throwable cause = error; cause != null; cause = cause.getCause())
        messages.append(cause.getMessage()).append('\n');
      assertTrue(messages.toString().contains(expected), messages.toString());
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  static OperatorSubtaskState snapshot(AbstractStreamOperatorTestHarness<?> h) throws Exception {
    return h.snapshotWithLocalState(1, 100, SavepointType.savepoint(SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }

  static long[] prefix(int layout) {
    switch (layout) {
      case 0:
        return new long[] {300, 100, 300};
      case 1:
      case 2:
        return new long[] {-2, -1};
      case 3:
        return new long[0];
      case 4:
        return new long[] {Long.MIN_VALUE, -100, -100, 0, 100};
      case 5:
        return new long[] {Long.MIN_VALUE};
      case 6:
        return new long[] {Long.MAX_VALUE};
      default:
        throw new AssertionError(layout);
    }
  }

  static long[] suffix(int layout) {
    if (layout == 5) return new long[] {Long.MIN_VALUE, Long.MIN_VALUE + 1, 0, 0};
    if (layout == 6) return new long[] {Long.MAX_VALUE, 100};
    return new long[] {300, 0, 100, 100, Long.MIN_VALUE};
  }

  static void prefixStock(
      Plan p, KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h, int layout)
      throws Exception {
    long[] times = prefix(layout);
    for (int i = 0; i < times.length; i++) p.pushStock(h, p.row(i, times[i]));
    if (layout == 1 || layout == 2 || layout == 3) h.processWatermark(new Watermark(500));
    if (layout == 5) h.processWatermark(new Watermark(Long.MIN_VALUE));
    if (layout == 6) h.processWatermark(new Watermark(Long.MAX_VALUE));
    if (layout == 1) p.pushStock(h, p.row(20, 300));
    p.stockOutput(h);
  }

  static void prefixNative(
      Plan p, KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h, int layout)
      throws Exception {
    long[] times = prefix(layout);
    for (int i = 0; i < times.length; i++) p.push(h, p.row(i, times[i]));
    if (layout == 1 || layout == 2 || layout == 3) h.processWatermark(new Watermark(500));
    if (layout == 5) h.processWatermark(new Watermark(Long.MIN_VALUE));
    if (layout == 6) h.processWatermark(new Watermark(Long.MAX_VALUE));
    if (layout == 1) p.push(h, p.row(20, 300));
    p.nativeOutput(h);
  }

  static void suffixStock(
      Plan p, KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h, int layout)
      throws Exception {
    long[] times = suffix(layout);
    for (int i = 0; i < times.length; i++) p.pushStock(h, p.row(100 + i, times[i]));
    h.processWatermark(new Watermark(Long.MAX_VALUE));
  }

  static void suffixNative(
      Plan p, KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h, int layout)
      throws Exception {
    long[] times = suffix(layout);
    for (int i = 0; i < times.length; i++) p.push(h, p.row(100 + i, times[i]));
    h.processWatermark(new Watermark(Long.MAX_VALUE));
  }

  static final class Plan {
    final String type;
    final int maximum;
    final RowType input;
    final EmptyRowDataKeySelector selector = EmptyRowDataKeySelector.INSTANCE;

    Plan(String type) {
      this(type, 1);
    }

    Plan(String type, int maximum) {
      this(type, maximum, 3, true);
    }

    Plan(String type, int maximum, int precision, boolean localZoned) {
      this.maximum = maximum;
      this.type = type;
      LogicalType payload;
      switch (type) {
        case "binary":
          payload = new VarBinaryType(VarBinaryType.MAX_LENGTH);
          break;
        case "map":
          payload = new MapType(new VarCharType(100), new BigIntType());
          break;
        case "reserved":
        case "long":
          payload = new BigIntType();
          break;
        case "string":
          payload = new VarCharType(VarCharType.MAX_LENGTH);
          break;
        case "decimal":
          payload = new DecimalType(38, 2);
          break;
        case "array":
          payload = new ArrayType(new BigIntType());
          break;
        case "row":
          payload = RowType.of(new BigIntType(), new VarCharType(100));
          break;
        case "double":
          payload = new DoubleType();
          break;
        default:
          throw new AssertionError(type);
      }
      input =
          RowType.of(
              new LogicalType[] {
                new BigIntType(),
                payload,
                localZoned ? new LocalZonedTimestampType(precision) : new TimestampType(precision)
              },
              new String[] {
                "id",
                type.equals("reserved") ? RowDataArrowConverter.ROW_KIND_COLUMN : "value",
                "rt"
              });
    }

    StockTemporalSortStateContract contract() {
      return new StockTemporalSortStateContract(
          selector.getProducedType().toSerializer(), input, 2);
    }

    KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(boolean rocks)
        throws Exception {
      var h =
          new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
              new RowTimeSortOperator(InternalTypeInfo.of(input), 2, null),
              selector,
              selector.getProducedType(),
              maximum,
              1,
              0);
      stockBackend(h, rocks);
      h.setup(new RowDataSerializer(input));
      return h;
    }

    void stockBackend(AbstractStreamOperatorTestHarness<?> h, boolean rocks) throws Exception {
      var config = new Configuration();
      config.set(StateBackendOptions.STATE_BACKEND, rocks ? "rocksdb" : "hashmap");
      h.setStateBackend(
          StateBackendLoader.loadStateBackendFromConfig(config, getClass().getClassLoader(), null));
    }

    KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness(
        boolean rocks) throws Exception {
      var operator = new NativeColumnarTemporalSortOperator(2, input, maximum);
      operator.setStockStateContract(contract());
      var h =
          new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
              operator, ignored -> 0, Types.INT, maximum, 1, 0);
      var config = new Configuration();
      if (rocks)
        config.setString(
            org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND.key(),
            RocksDBNativeStateBackendFactory.class.getName());
      CanonicalStateBackend.install(
          StreamExecutionEnvironment.getExecutionEnvironment(),
          config,
          h.getStreamConfig().getOperatorID().toHexString(),
          operator.getClass().getSimpleName(),
          contract(),
          getClass().getClassLoader());
      h.setStateBackend(
          new CanonicalStateBackendFactory().createFromConfig(config, getClass().getClassLoader()));
      h.setup(new ArrowBatchSerializer());
      return h;
    }

    RowData row(int id, long time) {
      Object payload = null;
      if (id % 3 != 0) {
        switch (type) {
          case "binary":
            payload = new byte[] {(byte) id, 0, -1};
            break;
          case "map":
            {
              var values = new LinkedHashMap<StringData, Long>();
              values.put(StringData.fromString("é"), (long) id);
              values.put(StringData.fromString("null"), null);
              payload = new GenericMapData(values);
              break;
            }
          case "reserved":
          case "long":
            payload = (long) id;
            break;
          case "string":
            payload = StringData.fromString("é-" + id);
            break;
          case "decimal":
            payload = DecimalData.fromBigDecimal(java.math.BigDecimal.valueOf(id, 2), 38, 2);
            break;
          case "array":
            payload = new GenericArrayData(new Long[] {(long) id, null, -1L});
            break;
          case "row":
            payload = GenericRowData.of((long) id, StringData.fromString("中"));
            break;
          case "double":
            payload = id % 2 == 0 ? Double.NaN : Double.NEGATIVE_INFINITY;
            break;
          default:
            throw new AssertionError(type);
        }
      }
      return GenericRowData.of((long) id, payload, TimestampData.fromEpochMillis(time));
    }

    void pushStock(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h, RowData row)
        throws Exception {
      h.processElement(new StreamRecord<>(new RowDataSerializer(input).toBinaryRow(row).copy()));
    }

    void push(
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h, RowData row)
        throws Exception {
      h.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(List.of(row), input, NativeAllocator.SHARED))));
    }

    List<String> stockOutput(KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h)
        throws Exception {
      var out = new ArrayList<String>();
      for (var row : h.extractOutputValues()) out.add(encode(row));
      h.getOutput().clear();
      return out;
    }

    List<String> nativeOutput(
        KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> h)
        throws Exception {
      var out = new ArrayList<String>();
      for (var batch : h.extractOutputValues()) {
        try (var root = batch.root()) {
          var reader = ArrowConversion.createArrowReader(root, input);
          for (int i = 0; i < root.getRowCount(); i++) out.add(encode(reader.read(i)));
        }
      }
      h.getOutput().clear();
      return out;
    }

    String encode(RowData row) throws Exception {
      var bytes = new org.apache.flink.core.memory.DataOutputSerializer(128);
      new RowDataSerializer(input).serialize(row, bytes);
      return Base64.getEncoder().encodeToString(bytes.getCopyOfBuffer());
    }

    @SuppressWarnings("unchecked")
    List<String> digest(OperatorSubtaskState state) throws Exception {
      try (var view =
          new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
              new StateView(), selector, selector.getProducedType(), maximum, 1, 0)) {
        stockBackend(view, false);
        view.setup(new RowDataSerializer(input));
        view.initializeState(state);
        view.open();
        var backend =
            (CheckpointableKeyedStateBackend<RowData>)
                view.getOperator().<RowData>getKeyedStateBackend();
        backend.setCurrentKey(selector.getKey(row(0, 0)));
        var rows =
            backend.getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new org.apache.flink.api.common.state.MapStateDescriptor<>(
                    "dataState",
                    org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE,
                    new org.apache.flink.api.common.typeutils.base.ListSerializer<>(
                        new RowDataSerializer(input))));
        var out = new ArrayList<String>();
        var entries = rows.entries();
        if (entries != null)
          for (var entry : entries) {
            int index = 0;
            for (var row : entry.getValue())
              out.add(entry.getKey() + ":" + index++ + ":" + encode(row));
          }
        var last =
            backend.getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                    "lastTriggeringTsState",
                    org.apache.flink.api.common.typeutils.base.LongSerializer.INSTANCE));
        out.add("last:" + last.value());
        for (String domain : List.of("event", "processing")) {
          var timers =
              backend.<TimerHeapInternalTimer<RowData, VoidNamespace>>create(
                  "_timer_state/" + domain + "_user-timers",
                  new TimerSerializer<>(
                      selector.getProducedType().toSerializer(), VoidNamespaceSerializer.INSTANCE));
          try (var iterator = timers.iterator()) {
            while (iterator.hasNext()) out.add(domain + ":" + iterator.next().getTimestamp());
          }
        }
        Collections.sort(out);
        return out;
      }
    }
  }

  static final class StateView extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    @Override
    public void processElement(StreamRecord<RowData> row) {}
  }
}
