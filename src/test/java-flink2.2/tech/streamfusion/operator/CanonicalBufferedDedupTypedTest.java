package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.FirstNCanonicalTestSupport.snapshot;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.*;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.operators.deduplicate.RowTimeDeduplicateKeepFirstRowFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.state.StockBufferedDedupStateContract;

class CanonicalBufferedDedupTypedTest {
  private static final RowType PAYLOAD =
      RowType.of(
          new VarCharType(VarCharType.MAX_LENGTH),
          new ArrayType(new DecimalType(38, 2)),
          new TimestampType(9));

  static Stream<Arguments> cases() {
    return Stream.of(
            "long",
            "int",
            "string",
            "decimal",
            "timestamp",
            "compact-timestamp",
            "boolean",
            "double",
            "binary",
            "row",
            "array",
            "composite")
        .flatMap(
            shape ->
                Stream.of(false, true)
                    .flatMap(
                        rocks ->
                            Stream.of(false, true).map(ttl -> Arguments.of(shape, rocks, ttl))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void typedKeysAndNestedPayloadsSurviveBothCanonicalDirections(
      String shape, boolean rocks, boolean ttl) throws Exception {
    LogicalType key = keyType(shape);
    RowType type =
        shape.equals("composite")
            ? RowType.of(
                key, new BigIntType(), new TimestampType(false, 3), PAYLOAD, new VarCharType(100))
            : RowType.of(key, new BigIntType(), new TimestampType(false, 3), PAYLOAD);
    int[] keys = shape.equals("composite") ? new int[] {4, 0} : new int[] {0};
    var selector =
        KeySelectorUtil.getRowDataSelector(
            getClass().getClassLoader(), keys, InternalTypeInfo.of(type));
    RowType keyType = (RowType) selector.getProducedType().toLogicalType();
    long retention = ttl ? 1000 : 0;
    var contract =
        new StockBufferedDedupStateContract(
            new RowDataSerializer(keyType), selector, keyType, type, keys, 2, retention);
    try (var stock = harness(type, keys, retention, true);
        var nativeHarness = nativeHarness(type, keys, retention, rocks, contract)) {
      stock.open();
      time(stock, 1500);
      feed(
          stock,
          type,
          List.of(
              row(shape, 1L, 10, 100),
              row(shape, 2L, 20, 300),
              row(shape, null, 30, 400),
              row(shape, 1L, 11, 90)));
      stock.processWatermark(new Watermark(100));
      assertEquals(List.of(11L), ids(stock.extractOutputValues()));
      nativeHarness.initializeState(snapshot(stock));
      nativeHarness.open();
      assertEquals(
          rocks ? "rocksdb" : "memory",
          ((CanonicalBuffered) nativeHarness.getOperator()).storage());
      var continued =
          List.of(row(shape, 1L, 98, 150), row(shape, 2L, 21, 200), row(shape, null, 31, 450));
      time(stock, 1800);
      feed(stock, type, continued);
      stock.processWatermark(new Watermark(250));
      push(nativeHarness, type, continued, 1800);
      var nativeOutput = flush(nativeHarness, type, 250);
      assertEquals(List.of(21L), ids(nativeOutput));
      assertEquals(bytes(type, stock.extractOutputValues()), bytes(type, nativeOutput));
      try (var restored = harness(type, keys, retention, true)) {
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
        var future =
            List.of(row(shape, 1L, 99, 600), row(shape, 2L, 97, 700), row(shape, null, 32, 440));
        time(stock, 2600);
        time(restored, 2600);
        feed(stock, type, future);
        feed(restored, type, future);
        stock.processWatermark(new Watermark(1000));
        restored.processWatermark(new Watermark(1000));
        push(nativeHarness, type, future, 2600);
        var output = flush(nativeHarness, type, 1000);
        assertEquals(ttl ? List.of(30L, 99L) : List.of(30L), ids(output));
        assertEquals(bytes(type, stock.extractOutputValues()), bytes(type, output));
        assertEquals(
            bytes(type, stock.extractOutputValues()), bytes(type, restored.extractOutputValues()));
      }
    }
  }

  static Stream<Arguments> capturedKeys() {
    return Stream.of("row", "array", "composite")
        .flatMap(shape -> Stream.of(false, true).map(ttl -> Arguments.of(shape, ttl)));
  }

  @ParameterizedTest
  @MethodSource("capturedKeys")
  void nestedRocksViewsFinishAfterLiveWinnerReplacementAndNativeOwnerClose(
      String shape, boolean ttl) throws Exception {
    CanonicalTemporalSortInteropTest.memory();
    RowType type =
        RowType.of(keyType(shape), new BigIntType(), new TimestampType(false, 3), PAYLOAD);
    int[] keys = {0};
    var selector =
        KeySelectorUtil.getRowDataSelector(
            getClass().getClassLoader(), keys, InternalTypeInfo.of(type));
    var keyType = (RowType) selector.getProducedType().toLogicalType();
    long retention = ttl ? 1000 : 0;
    var contract =
        new StockBufferedDedupStateContract(
            new RowDataSerializer(keyType), selector, keyType, type, keys, 2, retention);
    var captured = new org.apache.flink.streaming.api.operators.OperatorSnapshotFutures[2];
    var expected = new org.apache.flink.runtime.checkpoint.OperatorSubtaskState[2];
    try (var stock = harness(type, keys, retention, true);
        var source = nativeHarness(type, keys, retention, true, contract)) {
      try {
        stock.open();
        source.open();
        var owner = (AbstractNativeStatefulOperator<?>) source.getOperator();
        assertTrue(owner.directRocksDBState());
        for (int stage = 0; stage < 3; stage++) {
          var rows =
              List.of(
                  row(shape, 1L, stage + 1, 500 - stage * 100),
                  row(shape, 2L, stage + 10, 600 - stage * 100),
                  row(shape, null, stage + 20, 700 - stage * 100));
          time(stock, 1500 + stage * 100);
          feed(stock, type, rows);
          push(source, type, rows, 1500 + stage * 100);
          if (stage < 2) {
            expected[stage] = snapshot(stock);
            captured[stage] = CanonicalDistinctWindowAsyncTest.capture(source, 10 + stage);
          }
        }
        source.prepareSnapshotPreBarrier(12);
        owner.closeHandle();
        owner.handle = 0;
        for (int stage = 1; stage >= 0; stage--) {
          var saved = CanonicalDistinctWindowAsyncTest.finishOnWriter(captured[stage]);
          try (var reference = harness(type, keys, retention, true);
              var returned = harness(type, keys, retention, true)) {
            reference.initializeState(expected[stage]);
            returned.initializeState(saved);
            reference.open();
            returned.open();
            time(reference, 1800);
            time(returned, 1800);
            reference.processWatermark(new Watermark(1000));
            returned.processWatermark(new Watermark(1000));
            var expectedRows = bytes(type, reference.extractOutputValues());
            assertFalse(expectedRows.isEmpty());
            assertEquals(expectedRows, bytes(type, returned.extractOutputValues()));
          }
        }
      } finally {
        for (var future : captured) if (future != null) future.cancel();
      }
    }
  }

  private static LogicalType keyType(String shape) {
    switch (shape) {
      case "int":
        return new IntType();
      case "string":
        return new VarCharType(VarCharType.MAX_LENGTH);
      case "decimal":
        return new DecimalType(38, 2);
      case "timestamp":
        return new TimestampType(9);
      case "compact-timestamp":
        return new TimestampType(3);
      case "boolean":
        return new BooleanType();
      case "double":
        return new DoubleType();
      case "binary":
        return new VarBinaryType(VarBinaryType.MAX_LENGTH);
      case "row":
        return RowType.of(new DecimalType(19, 2), new TimestampType(9), new VarCharType(100));
      case "array":
        return new ArrayType(new TimestampType(9));
      default:
        return new BigIntType();
    }
  }

  private static Object key(String shape, Long key) {
    if (key == null) return null;
    switch (shape) {
      case "int":
        return key.intValue();
      case "string":
        return StringData.fromString("key-" + key + "-\uD800");
      case "decimal":
        return DecimalData.fromBigDecimal(
            new BigDecimal("123456789012345678901234567890.00").add(BigDecimal.valueOf(key)),
            38,
            2);
      case "timestamp":
        return TimestampData.fromEpochMillis(-1000 * key, 987654);
      case "compact-timestamp":
        return TimestampData.fromEpochMillis(-1000 * key);
      case "boolean":
        return key % 2 == 0;
      case "double":
        return key + 0.25;
      case "binary":
        return new byte[] {0, (byte) key.intValue(), -1};
      case "row":
        return GenericRowData.of(
            DecimalData.fromBigDecimal(BigDecimal.valueOf(key).setScale(2), 19, 2),
            TimestampData.fromEpochMillis(-1000 * key, 123456),
            null);
      case "array":
        return new GenericArrayData(
            new Object[] {TimestampData.fromEpochMillis(-1000 * key, 123456), null});
      default:
        return key;
    }
  }

  private static RowData row(String shape, Long key, long value, long time) {
    var row =
        GenericRowData.of(
            key(shape, key),
            value,
            TimestampData.fromEpochMillis(time),
            GenericRowData.of(
                StringData.fromString("value-" + value + "-\uD800"),
                new GenericArrayData(
                    new Object[] {
                      DecimalData.fromBigDecimal(
                          new BigDecimal("-123456789012345678901234567890.12"), 38, 2),
                      null
                    }),
                TimestampData.fromEpochMillis(-value, 987654)));
    if (!shape.equals("composite")) return row;
    return GenericRowData.of(
        row.getField(0),
        row.getField(1),
        row.getField(2),
        row.getField(3),
        key == null ? null : StringData.fromString("bucket-" + key % 2));
  }

  private static int[] descriptor(RowType type, int[] keys) throws Exception {
    var method =
        tech.streamfusion.planner.FlinkKeyGroupUtils.class.getDeclaredMethod(
            "stockKeyDescriptor", RowType.class, int[].class);
    method.setAccessible(true);
    return (int[]) method.invoke(null, type, keys);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      nativeHarness(
          RowType type,
          int[] keys,
          long ttl,
          boolean rocks,
          StockBufferedDedupStateContract contract)
          throws Exception {
    var operator = new CanonicalBuffered(type, keys, descriptor(type, keys), ttl);
    operator.setStockStateContract(contract);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>(
            operator, batch -> 0, org.apache.flink.api.common.typeinfo.Types.INT, 128, 1, 0);
    var config = new Configuration();
    if (rocks)
      config.setString(
          "state.backend.type",
          tech.streamfusion.state.RocksDBNativeStateBackendFactory.class.getName());
    tech.streamfusion.state.CanonicalStateBackend.install(
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
            .getExecutionEnvironment(),
        config,
        harness.getStreamConfig().getOperatorID().toHexString(),
        operator.getClass().getSimpleName(),
        contract,
        CanonicalBufferedDedupTypedTest.class.getClassLoader());
    harness.setStateBackend(
        new tech.streamfusion.state.CanonicalStateBackendFactory()
            .createFromConfig(config, CanonicalBufferedDedupTypedTest.class.getClassLoader()));
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
      RowType type, int[] keys, long ttl, boolean stock) throws Exception {
    OneInputStreamOperator<RowData, RowData> operator =
        stock
            ? new KeyedProcessOperator<>(
                new RowTimeDeduplicateKeepFirstRowFunction(InternalTypeInfo.of(type), ttl, 2))
            : new Projection();
    var selector =
        KeySelectorUtil.getRowDataSelector(
            CanonicalBufferedDedupTypedTest.class.getClassLoader(),
            keys,
            InternalTypeInfo.of(type));
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            operator, selector, selector.getProducedType(), 128, 1, 0);
    harness.setStateBackend(new HashMapStateBackend());
    harness.setup(new RowDataSerializer(type));
    return harness;
  }

  private static void time(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h, long now)
      throws Exception {
    h.setProcessingTime(now);
    h.setStateTtlProcessingTime(now);
  }

  private static void feed(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h,
      RowType type,
      List<RowData> rows)
      throws Exception {
    h.getOutput().clear();
    var serializer = new RowDataSerializer(type);
    for (var row : rows) h.processElement(new StreamRecord<>(serializer.toBinaryRow(row).copy()));
  }

  private static List<Long> ids(List<RowData> rows) {
    return rows.stream()
        .map(row -> row.getLong(1))
        .sorted()
        .collect(java.util.stream.Collectors.toList());
  }

  private static List<String> bytes(RowType type, List<RowData> rows) {
    var serializer = new RowDataSerializer(type);
    return rows.stream()
        .map(
            row -> {
              var binary = serializer.toBinaryRow(row);
              return java.util.Base64.getEncoder()
                  .encodeToString(
                      org.apache.flink.table.data.binary.BinarySegmentUtils.copyToBytes(
                          binary.getSegments(), binary.getOffset(), binary.getSizeInBytes()));
            })
        .sorted()
        .collect(java.util.stream.Collectors.toList());
  }

  private static void push(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RowType type,
      List<RowData> rows,
      long now)
      throws Exception {
    harness.setProcessingTime(now);
    harness.setStateTtlProcessingTime(now);
    harness.processElement(
        new StreamRecord<>(
            new ArrowBatch(RowDataArrowConverter.write(rows, type, NativeAllocator.SHARED))));
  }

  private static List<RowData> flush(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RowType type,
      long watermark)
      throws Exception {
    harness.processWatermark(new Watermark(watermark));
    var result = new ArrayList<RowData>();
    var serializer = new RowDataSerializer(type);
    while (!harness.getOutput().isEmpty()) {
      var event = harness.getOutput().poll();
      if (event instanceof StreamRecord) {
        try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          var reader = ArrowConversion.createArrowReader(root, type);
          for (int i = 0; i < root.getRowCount(); i++) result.add(serializer.copy(reader.read(i)));
        }
      }
    }
    return result;
  }

  private static final class CanonicalBuffered extends NativeColumnarDeduplicateOperator {
    private boolean canonical;

    CanonicalBuffered(RowType type, int[] keys, int[] descriptor, long ttl) {
      super(keys, descriptor, 2, type, ttl, 128);
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
        throw new AssertionError("Canonical buffered export serialized raw native state");
      return super.snapshotRawPartitions();
    }

    @Override
    protected String[] checkpointRocksDBHandle(String directory) {
      if (canonical && !directory.isEmpty())
        throw new AssertionError("Canonical buffered export created a native checkpoint");
      return super.checkpointRocksDBHandle(directory);
    }

    @Override
    protected byte[][] snapshotCanonicalPartitions() {
      if (canonical)
        throw new AssertionError("Canonical buffered export serialized a native snapshot");
      return super.snapshotCanonicalPartitions();
    }
  }

  private static final class Projection extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    @Override
    public void processElement(StreamRecord<RowData> record) {}
  }
}
