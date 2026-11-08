package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.planner.CanonicalGroupArithmeticInitializationTest.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRawValueData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.util.Collector;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.NativeMemoryLimitException;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.state.StockStateContract;

class CanonicalRetractingExtremaValidationTest {

  static Stream<Arguments> modes() {
    return Stream.of("extrema", "mixed-extrema")
        .flatMap(
            mode ->
                Stream.of(false, true)
                    .flatMap(
                        rocks -> Stream.of(0L, 1000L).map(ttl -> Arguments.of(mode, rocks, ttl))));
  }

  static Stream<Arguments> invalidSources() {
    return modes()
        .flatMap(
            args ->
                Stream.of(
                        "late-null-count",
                        "late-null-view",
                        "bad-placeholder",
                        "row-kind",
                        "foreign")
                    .map(
                        reason ->
                            Arguments.of(args.get()[0], args.get()[1], args.get()[2], reason)));
  }

  @ParameterizedTest
  @MethodSource("invalidSources")
  void completeValidationKeepsCorruptSourceAndEmptyNativeTarget(
      String mode, boolean rocks, long ttl, String reason) throws Exception {
    try (var fixture = fixture(mode, rocks, ttl);
        var source = new Projection(fixture, rocks)) {
      int groups = reason.startsWith("late") ? 1025 : 1;
      for (int i = 0; i < groups; i++) source.put(i, header(fixture, false, false), 1L);
      var keys = source.keys();
      var last = keys.get(keys.size() - 1);
      source.backend.setCurrentKey(last);
      if (reason.equals("late-null-count")) {
        var bad = header(fixture, false, false);
        bad.setField(
            (int) GroupAggregateStockTestFixture.field(source.contract, "recordCountField"), null);
        source.state.update(source.wrap(bad));
      } else if (reason.equals("late-null-view")) {
        if (rocks && !tech.streamfusion.compat.CanonicalTestCompat.flink22()) {
          var expected = source.bytes();
          assertThrows(
              Exception.class, () -> source.maps[source.firstMap].put(value(fixture, 7), null));
          assertEquals(
              expected, source.bytes(), "released backend rejects the invalid value before import");
          return;
        }
        source.maps[source.firstMap].put(value(fixture, 7), null);
      } else if (reason.equals("bad-placeholder"))
        source.state.update(source.wrap(header(fixture, true, false)));
      else if (reason.equals("row-kind"))
        source.state.update(source.wrap(header(fixture, false, true)));
      else
        source
            .backend
            .getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new ValueStateDescriptor<>("foreign", LongSerializer.INSTANCE))
            .update(99L);
      boolean nullView = reason.equals("late-null-view");
      var expected = nullView ? source.logicalEntries() : source.bytes();
      long target = create(fixture, 16_000_000);
      try {
        assertThrows(
            Exception.class, () -> source.contract.importState(source.backend, target, false));
        assertEquals(
            expected,
            nullView ? source.logicalEntries() : source.bytes(),
            "all source descriptors and entries survive failure");
        Native.BatchReceiver populated =
            (array, schema) -> {
              throw new AssertionError("invalid source populated native state");
            };
        Native.exportGroupExtremaCanonical(target, false, populated, populated);
      } finally {
        Native.closeGroupAggregator(target);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("modes")
  void deniedImportRetainsAllHeadersAndViewsForFreshRetry(String mode, boolean rocks, long ttl)
      throws Exception {
    String handles = Native.liveNativeHandles();
    try (var fixture = fixture(mode, rocks, ttl);
        var source = new Projection(fixture, rocks)) {
      for (int i = 0; i < 1025; i++) source.put(i, header(fixture, false, false), 1L);
      var expected = source.bytes();
      assertEquals(3075, expected.size());
      long denied = create(fixture, 64);
      try {
        assertThrows(
            NativeMemoryLimitException.class,
            () -> source.contract.importState(source.backend, denied, false));
        assertEquals(expected, source.bytes());
      } finally {
        Native.closeGroupAggregator(denied);
      }
      assertEquals(handles, Native.liveNativeHandles());
      long retry = create(fixture, 16_000_000);
      try {
        source.contract.importState(source.backend, retry, false);
        assertTrue(
            source.bytes().isEmpty(), "consume all headers and views only after complete success");
        source.contract.exportState(source.backend, retry, false);
        assertEquals(expected, source.bytes());
      } finally {
        Native.closeGroupAggregator(retry);
      }
    }
    assertEquals(handles, Native.liveNativeHandles());
  }

  static Stream<Arguments> receivers() {
    return modes()
        .flatMap(
            args ->
                Stream.of(false, true)
                    .map(
                        entries ->
                            Arguments.of(args.get()[0], args.get()[1], args.get()[2], entries)));
  }

  @ParameterizedTest
  @MethodSource("receivers")
  void receiverFailurePreservesExceptionAndLiveState(
      String mode, boolean rocks, long ttl, boolean entries) throws Exception {
    String handles = Native.liveNativeHandles();
    try (var reference = fixture(mode, rocks, ttl);
        var target = nativeHarness(reference, rocks)) {
      reference.harness.open();
      target.open();
      assertEquals(
          stockStep(reference, input(reference, 7, 0), 1000),
          nativeStep(
              target, input(reference, 7, 0), 1000, reference.outputType, reference.inputType));
      assertEquals(
          stockStep(reference, input(reference, 4, 0), 1500),
          nativeStep(
              target, input(reference, 4, 0), 1500, reference.outputType, reference.inputType));
      var owner = (DirectGroup) target.getOperator();
      var sentinel = new IllegalArgumentException("extrema receiver sentinel");
      Native.BatchReceiver fail =
          (array, schema) -> {
            throw sentinel;
          };
      Native.BatchReceiver consume =
          (array, schema) -> {
            try (var a = ArrowArray.wrap(array);
                var s = ArrowSchema.wrap(schema);
                var root =
                    Data.importVectorSchemaRoot(
                        NativeAllocator.SHARED, a, s, NativeAllocator.DICTIONARIES)) {
              assertTrue(root.getRowCount() > 0);
            }
          };
      assertSame(
          sentinel,
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  Native.exportGroupExtremaCanonical(
                      owner.address(),
                      owner.persistent(),
                      entries ? consume : fail,
                      entries ? fail : consume)));
      assertProjectedState(reference, snapshot(target), mode, rocks, ttl);
      assertEquals(
          stockStep(reference, input(reference, 7, 3), 1600),
          nativeStep(
              target, input(reference, 7, 3), 1600, reference.outputType, reference.inputType));
      assertProjectedState(reference, snapshot(target), mode, rocks, ttl);
    }
    assertEquals(handles, Native.liveNativeHandles());
  }

  GroupAggregateStockTestFixture fixture(String mode, boolean rocks, long ttl) throws Exception {
    return new GroupAggregateStockTestFixture(mode, rocks, ttl, true, false);
  }

  private void assertProjectedState(
      GroupAggregateStockTestFixture reference,
      org.apache.flink.runtime.checkpoint.OperatorSubtaskState saved,
      String mode,
      boolean rocks,
      long ttl)
      throws Exception {
    try (var returned = fixture(mode, rocks, ttl)) {
      returned.harness.initializeState(saved);
      returned.harness.open();
      assertEquals(reference.rawStateBytes(), returned.rawStateBytes());
    }
  }

  private static Object value(GroupAggregateStockTestFixture fixture, long number) {
    switch (fixture.inputType.getTypeAt(1).getTypeRoot()) {
      case BIGINT:
        return number;
      case INTEGER:
      case DATE:
        return Math.toIntExact(number);
      case TIME_WITHOUT_TIME_ZONE:
        return Math.toIntExact(number * 1000);
      case SMALLINT:
        return (short) number;
      case TINYINT:
        return (byte) number;
      case BOOLEAN:
        return number > 5;
      default:
        throw new IllegalArgumentException("Unqualified extrema fixture type");
    }
  }

  private static RowData input(GroupAggregateStockTestFixture fixture, long number, int kind) {
    var row = GenericRowData.of(1L, value(fixture, number));
    row.setRowKind(org.apache.flink.types.RowKind.fromByteValue((byte) kind));
    return row;
  }

  private long create(GroupAggregateStockTestFixture fixture, long budget) {
    int[] empty = new int[0];
    return Native.createGroupAggregator(
        fixture.kinds,
        fixture.valueTypes(),
        fixture.valueColumns,
        new int[] {0},
        new int[] {Native.STOCK_KEY_PROJECTION, -1},
        empty,
        empty,
        empty,
        -1,
        true,
        false,
        fixture.ttl,
        budget);
  }

  private static GenericRowData header(
      GroupAggregateStockTestFixture fixture, boolean badPlaceholder, boolean badKind)
      throws Exception {
    var row = new GenericRowData(fixture.accumulatorType.getFieldCount());
    row.setField(
        (int) GroupAggregateStockTestFixture.field(fixture.contract(), "recordCountField"), 1L);
    for (int i = 0; i < fixture.kinds.length; i++) {
      if (fixture.kinds[i] == 38 || fixture.kinds[i] == 39)
        row.setField(
            fixture.scalarFields[i],
            GenericRowData.of(
                value(fixture, 7),
                1L,
                badPlaceholder ? null : BinaryRawValueData.fromBytes(new byte[] {0})));
      else row.setField(fixture.scalarFields[i], fixture.kinds[i] == 22 ? 7L : 1L);
      if (fixture.countFields[i] >= 0) row.setField(fixture.countFields[i], 1L);
    }
    if (badKind) row.setRowKind(org.apache.flink.types.RowKind.DELETE);
    return row;
  }

  private static final class Projection implements AutoCloseable {
    final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness;
    final CheckpointableKeyedStateBackend<RowData> backend;
    final StockStateContract contract;
    final GroupAggregateStockTestFixture fixture;
    final ValueState<Object> state;
    final MapState<Object, Object>[] maps;
    final int firstMap;

    @SuppressWarnings({"unchecked", "rawtypes"})
    Projection(GroupAggregateStockTestFixture fixture, boolean rocks) throws Exception {
      this.fixture = fixture;
      contract = fixture.contract();
      var operator =
          new KeyedProcessOperator<RowData, RowData, RowData>(
              new KeyedProcessFunction<RowData, RowData, RowData>() {
                @Override
                public void processElement(
                    RowData row, Context context, Collector<RowData> output) {}
              });
      harness =
          new KeyedOneInputStreamOperatorTestHarness<>(
              operator,
              fixture.selector,
              fixture.selector.getProducedType(),
              fixture.maximum,
              1,
              0);
      harness.setStateBackend(
          rocks
              ? tech.streamfusion.compat.CanonicalTestCompat.rocksBackend()
              : new HashMapStateBackend());
      harness.setup(new RowDataSerializer(fixture.outputType));
      harness.open();
      backend = (CheckpointableKeyedStateBackend<RowData>) operator.<RowData>getKeyedStateBackend();
      TypeSerializer<?> values = new RowDataSerializer(fixture.accumulatorType);
      if (fixture.ttl > 0) values = new TtlSerializer(LongSerializer.INSTANCE, values);
      state =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new ValueStateDescriptor<>("accState", (TypeSerializer<Object>) values));
      maps = (MapState<Object, Object>[]) new MapState<?, ?>[fixture.kinds.length];
      var names = (String[]) GroupAggregateStockTestFixture.field(contract, "mapNames");
      var keySerializers =
          (TypeSerializer<?>[]) GroupAggregateStockTestFixture.field(contract, "mapKeySerializers");
      var valueSerializers =
          (TypeSerializer<?>[])
              GroupAggregateStockTestFixture.field(contract, "mapValueSerializers");
      int first = -1;
      for (int i = 0; i < maps.length; i++)
        if (names[i] != null) {
          if (first < 0) first = i;
          var serializer =
              fixture.ttl > 0
                  ? new TtlSerializer(LongSerializer.INSTANCE, valueSerializers[i])
                  : valueSerializers[i];
          maps[i] =
              backend.getPartitionedState(
                  VoidNamespace.INSTANCE,
                  VoidNamespaceSerializer.INSTANCE,
                  new MapStateDescriptor<>(
                      names[i],
                      (TypeSerializer<Object>) keySerializers[i],
                      (TypeSerializer<Object>) serializer));
        }
      firstMap = first;
    }

    Object wrap(Object value) {
      return fixture.ttl > 0 ? new TtlValue<>(value, 1000) : value;
    }

    void put(long key, RowData header, Long count) throws Exception {
      backend.setCurrentKey(fixture.selector.getKey(row(key, 7L, 0)));
      state.update(wrap(header));
      for (var map : maps) if (map != null) map.put(value(fixture, 7), wrap(count));
    }

    List<RowData> keys() throws Exception {
      try (var stream = backend.getKeys("accState", VoidNamespace.INSTANCE)) {
        return stream
            .map(backend.getKeySerializer()::copy)
            .collect(java.util.stream.Collectors.toList());
      }
    }

    Map<String, String> bytes() throws Exception {
      var resources = backend.savepoint().getSnapshotResources();
      try (AutoCloseable release = resources::release;
          var iterator = resources.createKVStateIterator()) {
        var result = new java.util.TreeMap<String, String>();
        var metadata = resources.getMetaInfoSnapshots();
        while (iterator.isValid()) {
          result.put(
              metadata.get(iterator.kvStateId()).getName()
                  + ":"
                  + java.util.Base64.getEncoder().encodeToString(iterator.key()),
              java.util.Base64.getEncoder().encodeToString(iterator.value()));
          iterator.next();
        }
        return result;
      }
    }

    Map<String, String> logicalEntries() throws Exception {
      // Flink's heap canonical writer cannot serialize a corrupt NULL map value.
      var result = new java.util.TreeMap<String, String>();
      var names = (String[]) GroupAggregateStockTestFixture.field(contract, "mapNames");
      for (var key : keys()) {
        backend.setCurrentKey(key);
        var keyBytes = new org.apache.flink.core.memory.DataOutputSerializer(32);
        backend.getKeySerializer().serialize(key, keyBytes);
        var encoded = java.util.Base64.getEncoder().encodeToString(keyBytes.getCopyOfBuffer());
        Object raw = state.value();
        long age = 0;
        if (raw instanceof TtlValue) {
          age = ((TtlValue<?>) raw).getLastAccessTimestamp();
          raw = ((TtlValue<?>) raw).getUserValue();
        }
        var header = new org.apache.flink.core.memory.DataOutputSerializer(128);
        new RowDataSerializer(fixture.accumulatorType).serialize((RowData) raw, header);
        result.put(
            "accState:" + encoded,
            age + ":" + java.util.Base64.getEncoder().encodeToString(header.getCopyOfBuffer()));
        for (int i = 0; i < maps.length; i++)
          if (maps[i] != null) {
            for (var entry : maps[i].entries()) {
              Object value = entry.getValue();
              long written = 0;
              if (value instanceof TtlValue) {
                written = ((TtlValue<?>) value).getLastAccessTimestamp();
                value = ((TtlValue<?>) value).getUserValue();
              }
              result.put(names[i] + ":" + encoded + ":" + entry.getKey(), written + ":" + value);
            }
          }
      }
      return result;
    }

    @Override
    public void close() throws Exception {
      harness.close();
    }
  }
}
