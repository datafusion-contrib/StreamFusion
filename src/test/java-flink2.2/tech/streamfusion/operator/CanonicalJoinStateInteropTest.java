package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.StreamingSemiAntiJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.FlinkRocksDBOptions;
import tech.streamfusion.state.StockJoinStateContract;

class CanonicalJoinStateInteropTest {
  private static final RowType INPUT =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType());
  private static final RowType KEY = RowType.of(new BigIntType());
  private static final RowType EMPTY_KEY = RowType.of();
  @TempDir Path directory;

  @ParameterizedTest
  @CsvSource({
    "count,false,false", "count,true,false", "key,false,false", "key,true,false",
    "count,false,true", "count,true,true", "key,false,true", "key,true,true"
  })
  void completeRightSideValidationPrecedesAnyNativeImport(
      String malformed, boolean persistent, boolean late) throws Exception {
    int shape = late && malformed.equals("key") ? 1 : 2;
    long handle = create(0, shape, persistent, 0);
    try (var view = view()) {
      view.open();
      var backend = backend(view);
      backend.setCurrentKey(key(1));
      var left = map(backend, 0, shape, true, false);
      left.put(
          shape == 2 ? binary(INPUT, row(RowKind.INSERT, 1, 10, 100)) : key(100),
          shape == 2 ? 1 : row(RowKind.INSERT, 1, 10, 100));
      if (late) {
        var right = map(backend, 0, shape, false, false);
        for (int i = 0; i < 1026; i++) {
          RowData value = row(RowKind.INSERT, 1, 1000 + i, 2000 + i);
          right.put(shape == 2 ? binary(INPUT, value) : key(2000 + i), shape == 2 ? 1 : value);
        }
        java.util.Map.Entry<RowData, Object> last = null;
        for (var entry : right.entries()) last = entry;
        assertNotNull(last);
        RowData badKey = new RowDataSerializer(shape == 2 ? INPUT : KEY).copy(last.getKey());
        Object badValue = 0;
        if (shape == 1) {
          RowData payload = (RowData) last.getValue();
          badValue = row(RowKind.INSERT, 99, payload.getLong(1), payload.getLong(2));
        }
        right.put(badKey, badValue);
        int position = 0;
        for (var entry : right.entries()) {
          if (entry.getKey().equals(badKey)) assertEquals(1025, position);
          position++;
        }
      } else {
        if (malformed.equals("key")) backend.setCurrentKey(key(99));
        map(backend, 0, shape, false, false)
            .put(binary(INPUT, row(RowKind.INSERT, 1, 20, 200)), malformed.equals("count") ? 0 : 1);
      }
      var contract = contract(0, shape, 0);
      assertThrows(RuntimeException.class, () -> contract.importState(backend, handle, persistent));
      contract.exportState(backend, handle, persistent);
      for (boolean side : new boolean[] {true, false})
        try (var keys = backend.getKeys(name(side), VoidNamespace.INSTANCE)) {
          assertEquals(
              0, keys.count(), "Invalid right state must not partially import either side");
        }
    } finally {
      close(handle, persistent);
    }
  }

  static Stream<Arguments> continuations() {
    var cases = new ArrayList<Arguments>();
    for (int kind = 0; kind < 6; kind++)
      for (int shape = 0; shape < 3; shape++)
        for (boolean persistent : new boolean[] {false, true})
          cases.add(Arguments.of(kind, shape, persistent, false));
    for (int kind = 0; kind < 6; kind++)
      for (boolean persistent : new boolean[] {false, true})
        cases.add(Arguments.of(kind, 2, persistent, true));
    return cases.stream();
  }

  static Stream<Arguments> chunks() {
    var cases = new ArrayList<Arguments>();
    for (int kind : new int[] {0, 1})
      for (int shape = 0; shape < 3; shape++)
        for (boolean persistent : new boolean[] {false, true})
          cases.add(Arguments.of(kind, shape, persistent));
    return cases.stream();
  }

  @ParameterizedTest
  @CsvSource({
    "unique_key,false", "unique_key,true",
    "join_key,false", "join_key,true",
    "row_kind,false", "row_kind,true",
    "zero_count,false", "zero_count,true",
    "negative_count,false", "negative_count,true"
  })
  void malformedStockKeysKindsAndCountsAreRejected(String mode, boolean persistent)
      throws Exception {
    int shape = mode.equals("unique_key") ? 1 : 2;
    long handle = create(0, shape, persistent, 0);
    try (var view = view()) {
      view.open();
      var backend = backend(view);
      backend.setCurrentKey(key(mode.equals("join_key") ? 99 : 1));
      RowData payload =
          row(mode.equals("row_kind") ? RowKind.UPDATE_AFTER : RowKind.INSERT, 1, 10, 100);
      Object body =
          shape == 1
              ? payload
              : mode.equals("zero_count") ? 0 : mode.equals("negative_count") ? -1 : 1;
      map(backend, 0, shape, true, false).put(shape == 1 ? key(999) : binary(INPUT, payload), body);
      var contract = contract(0, shape, 0);
      assertThrows(RuntimeException.class, () -> contract.importState(backend, handle, persistent));
      contract.exportState(backend, handle, persistent);
      for (boolean left : new boolean[] {true, false}) {
        try (var keys = backend.getKeys(name(left), VoidNamespace.INSTANCE)) {
          assertEquals(0, keys.count(), "Rejected row must not have reached native state");
        }
      }
    } finally {
      close(handle, persistent);
    }
  }

  @ParameterizedTest(name = "kind={0}, shape={1}, rocks={2}, keyless={3}")
  @MethodSource("continuations")
  void releasedStockPrefixContinuesNativelyAndThenRestoresBackIntoStock(
      int kind, int shape, boolean persistent, boolean keyless) throws Exception {
    long handle = create(kind, shape, persistent, 0, keyless);
    try (var stock = stock(kind, shape, 0, keyless);
        var stockRestored = stock(kind, shape, 0, keyless);
        var view = view(keyless)) {
      stock.open();
      send(stock, true, row(RowKind.INSERT, 1, 10, 100));
      send(stock, true, row(RowKind.INSERT, 2, 20, 200));
      send(stock, false, row(RowKind.INSERT, 1, 30, 300));
      send(stock, false, row(RowKind.INSERT, 3, 40, 400));
      if (shape != 0) {
        send(stock, true, row(RowKind.INSERT, 1, 11, 101));
        send(stock, false, row(RowKind.INSERT, 1, 31, 301));
      }
      if (shape == 2) {
        send(stock, true, row(RowKind.INSERT, 1, 10, 100));
        send(stock, false, row(RowKind.INSERT, 1, 30, 300));
      }
      var snapshot =
          stock
              .snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      view.initializeState(snapshot);
      view.open();
      stockRestored.initializeState(snapshot);
      stockRestored.open();
      var contract = contract(kind, shape, 0, keyless);
      contract.importState(backend(view), handle, persistent);
      stock.getOutput().clear();
      var changes =
          List.of(
              new Event(false, row(RowKind.DELETE, 1, 30, 300)),
              new Event(true, row(RowKind.UPDATE_BEFORE, 1, 10, 100)),
              new Event(true, row(RowKind.UPDATE_AFTER, 1, 12, 100)),
              new Event(false, row(RowKind.UPDATE_AFTER, 1, 32, 300)),
              new Event(false, row(RowKind.INSERT, 2, 50, 500)),
              new Event(true, row(RowKind.DELETE, 2, 20, 200)),
              new Event(false, row(RowKind.DELETE, 3, 40, 400)));
      for (var event : changes) {
        send(stock, event.left, event.row);
        send(stockRestored, event.left, event.row);
        var expected = drain(stock);
        assertEquals(expected, drain(stockRestored), "stock -> stock: " + event);
        assertEquals(
            expected,
            push(handle, persistent, kind, event.left, event.row),
            "stock -> native: " + event);
      }
      contract.exportState(backend(view), handle, persistent);
      var exported =
          view.snapshotWithLocalState(2, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var resumed = stock(kind, shape, 0, keyless)) {
        resumed.initializeState(exported);
        resumed.open();
        for (var event :
            List.of(
                new Event(true, row(RowKind.INSERT, 3, 60, 600)),
                new Event(false, row(RowKind.DELETE, 2, 50, 500)),
                new Event(false, row(RowKind.UPDATE_BEFORE, 1, 32, 300)),
                new Event(false, row(RowKind.UPDATE_AFTER, 1, 33, 300)),
                new Event(true, row(RowKind.DELETE, 1, 12, 100)))) {
          send(stock, event.left, event.row);
          send(resumed, event.left, event.row);
          assertEquals(drain(stock), drain(resumed), "native -> stock: " + event);
        }
      }
    } finally {
      close(handle, persistent);
    }
  }

  @ParameterizedTest(name = "kind={0}, shape={1}, rocks={2}")
  @MethodSource("chunks")
  void manyRowsAndOriginalTtlAgesSurviveCanonicalImportExportAndStockSnapshot(
      int kind, int shape, boolean persistent) throws Exception {
    long handle = create(kind, shape, persistent, 1000);
    try (var view = view()) {
      view.open();
      view.setStateTtlProcessingTime(10000);
      var backend = backend(view);
      for (boolean left : new boolean[] {true, false}) {
        var map = shape == 0 ? null : map(backend, kind, shape, left, true);
        var value =
            shape == 0
                ? backend.getPartitionedState(
                    VoidNamespace.INSTANCE,
                    VoidNamespaceSerializer.INSTANCE,
                    new ValueStateDescriptor<>(name(left), serializer(kind, shape, left, true)))
                : null;
        for (int index = 0; index < 1539; index++) {
          long group = shape == 0 ? index : 1;
          backend.setCurrentKey(key(group));
          RowData payload = row(RowKind.INSERT, group, 10 + index, index);
          Object body = shape == 2 ? index % 3 + 1 : payload;
          if (outer(kind, left)) body = Tuple2.of(body, index % 4);
          Object raw = new TtlValue<>(body, 1500 + index % 2);
          if (shape == 0) value.update(raw);
          else map.put(shape == 2 ? binary(INPUT, payload) : key(index), raw);
        }
      }
      var contract = contract(kind, shape, 1000);
      contract.importState(backend, handle, persistent);
      contract.exportState(backend, handle, persistent);
      var saved =
          view.snapshotWithLocalState(1, 0, SavepointType.savepoint(SavepointFormatType.CANONICAL))
              .getJobManagerOwnedState();
      try (var restored = view()) {
        restored.initializeState(saved);
        restored.open();
        var actual = backend(restored);
        for (boolean left : new boolean[] {true, false}) {
          var map = shape == 0 ? null : map(actual, kind, shape, left, true);
          var value =
              shape == 0
                  ? actual.getPartitionedState(
                      VoidNamespace.INSTANCE,
                      VoidNamespaceSerializer.INSTANCE,
                      new ValueStateDescriptor<>(name(left), serializer(kind, shape, left, true)))
                  : null;
          if (shape != 0) {
            actual.setCurrentKey(key(1));
            int count = 0;
            for (var ignored : map.entries()) count++;
            assertEquals(1539, count);
          }
          for (int index = 0; index < 1539; index++) {
            long group = shape == 0 ? index : 1;
            actual.setCurrentKey(key(group));
            Object raw =
                shape == 0
                    ? value.value()
                    : map.get(
                        shape == 2
                            ? binary(INPUT, row(RowKind.INSERT, group, 10 + index, index))
                            : key(index));
            assertNotNull(raw);
            var ttl = (TtlValue<?>) raw;
            assertEquals(1500 + index % 2, ttl.getLastAccessTimestamp());
            Object body = ttl.getUserValue();
            if (outer(kind, left)) {
              assertEquals(index % 4, ((Tuple2<?, ?>) body).f1);
              body = ((Tuple2<?, ?>) body).f0;
            }
            if (shape == 2) assertEquals(index % 3 + 1, body);
            else {
              assertEquals(index, ((RowData) body).getLong(2));
              assertEquals(RowKind.INSERT, ((RowData) body).getRowKind());
            }
          }
        }
      }
    } finally {
      close(handle, persistent);
    }
  }

  private static final class Event {
    final boolean left;
    final RowData row;

    Event(boolean left, RowData row) {
      this.left = left;
      this.row = row;
    }

    @Override
    public String toString() {
      return (left ? "left " : "right ") + value(row);
    }
  }

  private static RowData row(RowKind kind, long group, long payload, long unique) {
    var row = GenericRowData.of(group, payload, unique);
    row.setRowKind(kind);
    return row;
  }

  private static RowData binary(RowType type, RowData row) {
    return new RowDataSerializer(type).toBinaryRow(row).copy();
  }

  private static RowData key(long value) {
    return binary(KEY, GenericRowData.of(value));
  }

  private static boolean outer(int kind, boolean left) {
    return left ? kind == 1 || kind == 3 || kind >= 4 : kind == 2 || kind == 3;
  }

  private static String name(boolean left) {
    return left ? "left-records" : "right-records";
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static TypeSerializer<Object> serializer(int kind, int shape, boolean left, boolean ttl) {
    TypeSerializer<?> value = shape == 2 ? IntSerializer.INSTANCE : new RowDataSerializer(INPUT);
    if (outer(kind, left))
      value =
          new TupleSerializer(
              (Class) Tuple2.class, new TypeSerializer<?>[] {value, IntSerializer.INSTANCE});
    if (ttl) value = new TtlSerializer<>(LongSerializer.INSTANCE, value);
    return (TypeSerializer<Object>) value;
  }

  private static MapState<RowData, Object> map(
      CheckpointableKeyedStateBackend<RowData> backend,
      int kind,
      int shape,
      boolean left,
      boolean ttl)
      throws Exception {
    return backend.getPartitionedState(
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE,
        new MapStateDescriptor<>(
            name(left),
            new RowDataSerializer(shape == 2 ? INPUT : KEY),
            serializer(kind, shape, left, ttl)));
  }

  private static StockJoinStateContract contract(int kind, int shape, long ttl) {
    return contract(kind, shape, ttl, false);
  }

  private static StockJoinStateContract contract(int kind, int shape, long ttl, boolean keyless) {
    var selector =
        KeySelectorUtil.getRowDataSelector(
            CanonicalJoinStateInteropTest.class.getClassLoader(),
            new int[] {2},
            InternalTypeInfo.of(INPUT));
    return new StockJoinStateContract(
        new RowDataSerializer(keyless ? EMPTY_KEY : KEY),
        keyless ? EMPTY_KEY : KEY,
        new StockJoinStateContract.SideSpec(
            INPUT,
            shape == 0,
            shape == 1 ? KEY : null,
            shape == 1 ? selector : null,
            outer(kind, true),
            ttl),
        new StockJoinStateContract.SideSpec(
            INPUT,
            shape == 0,
            shape == 1 ? KEY : null,
            shape == 1 ? selector : null,
            outer(kind, false),
            ttl));
  }

  private long create(int kind, int shape, boolean persistent, long ttl) throws Exception {
    return create(kind, shape, persistent, ttl, false);
  }

  private long create(int kind, int shape, boolean persistent, long ttl, boolean keyless)
      throws Exception {
    try (var left = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        var right = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        AutoCloseable release =
            () -> {
              if (left.snapshot().release != 0) left.release();
              if (right.snapshot().release != 0) right.release();
            }) {
      for (var schema : List.of(left, right))
        Data.exportSchema(
            NativeAllocator.SHARED,
            ArrowConversion.toArrowSchema(INPUT),
            NativeAllocator.DICTIONARIES,
            schema);
      int[] keys = keyless ? new int[0] : new int[] {0};
      int[] precision =
          keyless
              ? new int[] {Native.STOCK_KEY_PROJECTION}
              : new int[] {Native.STOCK_KEY_PROJECTION, -1};
      int[] filter = keyless ? new int[0] : new int[] {1}, empty = {};
      if (!persistent)
        return Native.createUpdatingJoiner(
            keys,
            keys,
            filter,
            precision,
            kind,
            left.memoryAddress(),
            right.memoryAddress(),
            empty,
            empty,
            empty,
            new long[0],
            new double[0],
            new String[0],
            shape == 0,
            shape == 0,
            false,
            ttl,
            ttl,
            1 << 26);
      return Native.createRocksDBUpdatingJoiner(
          keys,
          keys,
          filter,
          precision,
          kind,
          left.memoryAddress(),
          right.memoryAddress(),
          empty,
          empty,
          empty,
          new long[0],
          new double[0],
          new String[0],
          shape == 0,
          shape == 0,
          false,
          ttl,
          ttl,
          10000,
          1 << 26,
          directory.resolve("state").toString(),
          keyless ? 1 : 128,
          FlinkRocksDBOptions.from(new Configuration()).json(),
          0,
          new String[0],
          new String[0],
          0,
          keyless ? 0 : 127,
          false,
          new byte[0][]);
    }
  }

  private static void close(long handle, boolean persistent) {
    if (persistent) Native.closeRocksDBUpdatingJoiner(handle);
    else Native.closeUpdatingJoiner(handle);
  }

  private static List<String> push(
      long handle, boolean persistent, int kind, boolean left, RowData row) {
    var output = new ArrayList<String>();
    Native.BatchReceiver receiver =
        (arrayAddress, schemaAddress) -> {
          try (var array = ArrowArray.wrap(arrayAddress);
              var schema = ArrowSchema.wrap(schemaAddress);
              var root =
                  Data.importVectorSchemaRoot(
                      NativeAllocator.SHARED, array, schema, NativeAllocator.DICTIONARIES)) {
            var types =
                kind >= 4
                    ? INPUT
                    : RowType.of(
                        new BigIntType(),
                        new BigIntType(),
                        new BigIntType(),
                        new BigIntType(),
                        new BigIntType(),
                        new BigIntType());
            var reader = ArrowConversion.createArrowReader(root, types);
            var kinds = (TinyIntVector) root.getVector(types.getFieldCount());
            for (int index = 0; index < root.getRowCount(); index++) {
              var value = reader.read(index);
              value.setRowKind(RowKind.fromByteValue(kinds.get(index)));
              output.add(value(value));
            }
          }
        };
    try (var root = RowDataArrowConverter.write(List.of(row), INPUT, NativeAllocator.SHARED, true);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (persistent && left)
        Native.pushLeftRocksDBUpdatingJoiner(
            handle, array.memoryAddress(), schema.memoryAddress(), 0, receiver);
      else if (persistent)
        Native.pushRightRocksDBUpdatingJoiner(
            handle, array.memoryAddress(), schema.memoryAddress(), 0, receiver);
      else if (left)
        Native.pushLeftUpdatingJoiner(
            handle, array.memoryAddress(), schema.memoryAddress(), 0, receiver);
      else
        Native.pushRightUpdatingJoiner(
            handle, array.memoryAddress(), schema.memoryAddress(), 0, receiver);
    }
    output.sort(String::compareTo);
    return output;
  }

  private static void send(
      KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> stock,
      boolean left,
      RowData row)
      throws Exception {
    var record = new StreamRecord<>(binary(INPUT, row));
    if (left) stock.processElement1(record);
    else stock.processElement2(record);
  }

  private static List<String> drain(
      KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> stock) {
    var output = new ArrayList<String>();
    for (var row : stock.extractOutputValues()) output.add(value(row));
    stock.getOutput().clear();
    output.sort(String::compareTo);
    return output;
  }

  private static String value(RowData row) {
    var value = new StringBuilder(row.getRowKind().toString());
    for (int field = 0; field < row.getArity(); field++)
      value.append(':').append(row.isNullAt(field) ? "null" : row.getLong(field));
    return value.toString();
  }

  private static KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> stock(
      int kind, int shape, long ttl, boolean keyless) throws Exception {
    var type = InternalTypeInfo.<RowData>of(INPUT);
    var selector =
        KeySelectorUtil.getRowDataSelector(
            CanonicalJoinStateInteropTest.class.getClassLoader(),
            new int[] {shape == 0 ? 0 : 2},
            type);
    var side =
        shape == 0
            ? JoinInputSideSpec.withUniqueKeyContainedByJoinKey(InternalTypeInfo.of(KEY), selector)
            : shape == 1
                ? JoinInputSideSpec.withUniqueKey(InternalTypeInfo.of(KEY), selector)
                : JoinInputSideSpec.withoutUniqueKey();
    var condition =
        new GeneratedJoinCondition("unused", "", new Object[0]) {
          @Override
          public JoinCondition newInstance(ClassLoader loader) {
            return new AlwaysTrue();
          }
        };
    TwoInputStreamOperator<RowData, RowData, RowData> operator =
        kind < 4
            ? new StreamingJoinOperator(
                type,
                type,
                condition,
                side,
                side,
                outer(kind, true),
                outer(kind, false),
                keyless ? new boolean[0] : new boolean[] {true},
                ttl,
                ttl)
            : new StreamingSemiAntiJoinOperator(
                kind == 5,
                type,
                type,
                condition,
                side,
                side,
                keyless ? new boolean[0] : new boolean[] {true},
                ttl,
                ttl);
    var harness =
        new KeyedTwoInputStreamOperatorTestHarness<>(
            operator,
            r -> keyless ? binary(EMPTY_KEY, new GenericRowData(0)) : key(r.getLong(0)),
            r -> keyless ? binary(EMPTY_KEY, new GenericRowData(0)) : key(r.getLong(0)),
            InternalTypeInfo.<RowData>of(keyless ? EMPTY_KEY : KEY),
            keyless ? 1 : 128,
            1,
            0);
    harness.setStateBackend(new HashMapStateBackend());
    harness.setup(
        new RowDataSerializer(
            kind >= 4
                ? INPUT
                : RowType.of(
                    new BigIntType(),
                    new BigIntType(),
                    new BigIntType(),
                    new BigIntType(),
                    new BigIntType(),
                    new BigIntType())));
    return harness;
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> view()
      throws Exception {
    return view(false);
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> view(
      boolean keyless) throws Exception {
    var selector =
        KeySelectorUtil.getRowDataSelector(
            CanonicalJoinStateInteropTest.class.getClassLoader(),
            keyless ? new int[0] : new int[] {0},
            InternalTypeInfo.of(INPUT));
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            new Projection(), selector, selector.getProducedType(), keyless ? 1 : 128, 1, 0);
    harness.setStateBackend(new HashMapStateBackend());
    harness.setup(new RowDataSerializer(INPUT));
    return harness;
  }

  @SuppressWarnings("unchecked")
  private static CheckpointableKeyedStateBackend<RowData> backend(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> view) {
    return (CheckpointableKeyedStateBackend<RowData>)
        view.getOperator().<RowData>getKeyedStateBackend();
  }

  public static class AlwaysTrue extends AbstractRichFunction implements JoinCondition {
    @Override
    public boolean apply(RowData left, RowData right) {
      return true;
    }
  }

  private static final class Projection extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    @Override
    public void processElement(StreamRecord<RowData> row) {}
  }
}
