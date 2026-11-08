package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.Native;
import tech.streamfusion.arrow.ArrowConversion;
import tech.streamfusion.planner.FlinkKeyGroupUtils;
import tech.streamfusion.state.FlinkRocksDBOptions;
import tech.streamfusion.state.StockChangelogNormalizeStateContract;

class CanonicalNormalizerRowKeyTest {
  @TempDir Path directory;

  static Stream<Arguments> cases() {
    return Stream.of(false, true)
        .flatMap(
            nested ->
                Stream.of(RowKind.values())
                    .flatMap(
                        kind ->
                            Stream.of(false, true)
                                .flatMap(
                                    stockRocks ->
                                        Stream.of(false, true)
                                            .flatMap(
                                                nativeRocks ->
                                                    Stream.of(false, true)
                                                        .flatMap(
                                                            first ->
                                                                Stream.of(false, true)
                                                                    .flatMap(
                                                                        binaryInput ->
                                                                            Stream.of(false, true)
                                                                                .flatMap(
                                                                                    padding ->
                                                                                        Stream.of(
                                                                                                false,
                                                                                                true)
                                                                                            .map(
                                                                                                checkpoint ->
                                                                                                    Arguments
                                                                                                        .of(
                                                                                                            nested,
                                                                                                            kind,
                                                                                                            stockRocks,
                                                                                                            nativeRocks,
                                                                                                            first,
                                                                                                            binaryInput,
                                                                                                            padding,
                                                                                                            checkpoint)))))))));
  }

  @ParameterizedTest(
      name =
          "nested={0}, child-kind={1}, stock-rocks={2}, native-rocks={3}, native-first={4},"
              + " binary-input={5}, padding={6}, checkpoint={7}")
  @MethodSource("cases")
  void actualRowKeysPreserveCompleteStateAndContinuedChangelog(
      boolean nested,
      RowKind childKind,
      boolean stockRocks,
      boolean nativeRocks,
      boolean nativeFirst,
      boolean binaryInput,
      boolean padding,
      boolean checkpoint)
      throws Exception {
    var keyValue =
        nested
            ? RowType.of(
                new BigIntType(),
                RowType.of(new IntType(), new VarCharType(100)),
                new TimestampType(9),
                new DecimalType(38, 2))
            : RowType.of(new BigIntType(), new VarCharType(100));
    var input = RowType.of(keyValue, new BigIntType());
    var selector =
        KeySelectorUtil.getRowDataSelector(
            getClass().getClassLoader(), new int[] {0}, InternalTypeInfo.of(input));
    var keyType = selector.getProducedType().toRowType();
    var contract =
        new StockChangelogNormalizeStateContract(
            selector.getProducedType().toSerializer(), keyType, input, false);
    var prefix =
        List.of(
            row(keyValue, nested, childKind, padding, 1, 10L, RowKind.INSERT),
            row(keyValue, nested, childKind, padding, 1, 15L, RowKind.UPDATE_AFTER),
            row(keyValue, nested, childKind, padding, 2, 20L, RowKind.INSERT),
            row(keyValue, nested, childKind, padding, 3, 30L, RowKind.INSERT));
    var suffix =
        List.of(
            row(keyValue, nested, childKind, padding, 1, 15L, RowKind.UPDATE_AFTER),
            row(keyValue, nested, childKind, padding, 2, null, RowKind.DELETE),
            row(keyValue, nested, childKind, padding, 3, 35L, RowKind.UPDATE_AFTER),
            row(keyValue, nested, childKind, padding, 4, 40L, RowKind.INSERT));
    var future =
        List.of(
            row(keyValue, nested, childKind, padding, 1, null, RowKind.DELETE),
            row(keyValue, nested, childKind, padding, 4, 45L, RowKind.UPDATE_AFTER),
            row(keyValue, nested, childKind, padding, 2, 22L, RowKind.INSERT));
    if (binaryInput) {
      prefix = binaryRows(input, prefix);
      suffix = binaryRows(input, suffix);
      future = binaryRows(input, future);
    }
    var descriptorMethod =
        FlinkKeyGroupUtils.class.getDeclaredMethod(
            "stockKeyDescriptor", RowType.class, int[].class);
    descriptorMethod.setAccessible(true);
    var descriptor = (int[]) descriptorMethod.invoke(null, input, new int[] {0});
    long handle = create(nativeRocks, descriptor, new byte[0][]);
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try (var reference = stock(input, selector, stockRocks);
        var view = stock(input, selector, stockRocks)) {
      reference.open();
      var expectedPrefix = feed(reference, input, prefix);
      view.initializeState(snapshot(reference));
      view.open();
      var backend =
          (CheckpointableKeyedStateBackend<RowData>)
              view.getOperator().<RowData>getKeyedStateBackend();
      var expectedState = CanonicalUnboundedOverScalarTest.raw(reference);
      if (nativeFirst) assertEquals(expectedPrefix, push(handle, nativeRocks, input, prefix));
      else contract.importState(backend, handle, nativeRocks);
      if (checkpoint) {
        byte[][] partitions =
            nativeRocks
                ? Native.snapshotRocksDBChangelogNormalizerPartitions(handle)
                : Native.snapshotChangelogNormalizerPartitions(handle, 128, descriptor);
        assertTrue(partitions.length > 0);
        for (int index = 0; index < partitions.length; index++)
          partitions[index] =
              java.util.Arrays.copyOfRange(partitions[index], 4, partitions[index].length);
        close(handle, nativeRocks);
        handle = 0;
        handle = create(nativeRocks, descriptor, partitions);
      }
      contract.exportState(backend, handle, nativeRocks);
      assertEquals(expectedState, CanonicalUnboundedOverScalarTest.raw(view));
      assertEquals(feed(reference, input, suffix), push(handle, nativeRocks, input, suffix));
      contract.exportState(backend, handle, nativeRocks);
      assertEquals(
          CanonicalUnboundedOverScalarTest.raw(reference),
          CanonicalUnboundedOverScalarTest.raw(view));
      try (var returned = stock(input, selector, stockRocks)) {
        returned.initializeState(snapshot(view));
        returned.open();
        var expected = feed(reference, input, future);
        assertEquals(expected, feed(returned, input, future));
        assertEquals(expected, push(handle, nativeRocks, input, future));
      }
    } finally {
      if (handle != 0) close(handle, nativeRocks);
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private long create(boolean rocks, int[] descriptor, byte[][] restored) {
    if (rocks)
      return Native.createRocksDBChangelogNormalizer(
          new int[] {0},
          descriptor,
          true,
          false,
          0,
          0,
          1 << 26,
          directory.resolve("native-" + java.util.UUID.randomUUID()).toString(),
          128,
          FlinkRocksDBOptions.from(new Configuration()).json(),
          0,
          new String[0],
          new String[0],
          0,
          127,
          false,
          restored);
    return restored.length == 0
        ? Native.createChangelogNormalizer(new int[] {0}, descriptor, true, false, 0, 1 << 26)
        : Native.restoreChangelogNormalizerPartitions(
            new int[] {0}, descriptor, true, false, 0, 0, restored, 1 << 26);
  }

  private static void close(long handle, boolean rocks) {
    if (rocks) Native.closeRocksDBChangelogNormalizer(handle);
    else Native.closeChangelogNormalizer(handle);
  }

  private static RowData row(
      RowType keyType,
      boolean nested,
      RowKind kind,
      boolean padding,
      long id,
      Long value,
      RowKind change) {
    RowData key;
    if (nested) {
      var child =
          GenericRowData.of(
              id == 3 ? null : (int) id, id % 2 == 0 ? null : StringData.fromString("雪" + id));
      child.setRowKind(kind);
      key =
          GenericRowData.of(
              id == 3 ? null : id,
              id % 2 == 0 ? null : child,
              id % 2 == 0 ? null : TimestampData.fromEpochMillis(-1234L - id, 456789),
              id % 2 == 0
                  ? null
                  : DecimalData.fromBigDecimal(
                      new BigDecimal("123456789012345678901234567890123456.78"), 38, 2));
    } else
      key =
          GenericRowData.of(
              id == 3 ? null : id, id % 2 == 0 ? null : StringData.fromString("雪" + id));
    key.setRowKind(kind);
    var binaryKey = new RowDataSerializer(keyType).toBinaryRow(key).copy();
    if (padding) {
      byte[] raw =
          org.apache.flink.table.data.binary.BinarySegmentUtils.copyToBytes(
              binaryKey.getSegments(), binaryKey.getOffset(), binaryKey.getSizeInBytes());
      raw = java.util.Arrays.copyOf(raw, raw.length + 8);
      java.util.Arrays.fill(raw, raw.length - 8, raw.length, (byte) 0x55);
      binaryKey.pointTo(org.apache.flink.core.memory.MemorySegmentFactory.wrap(raw), 0, raw.length);
    }
    return GenericRowData.ofKind(change, binaryKey, value);
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(
      RowType input, RowDataKeySelector selector, boolean rocks) throws Exception {
    var function =
        tech.streamfusion.compat.CanonicalTestCompat.keepLast(
            InternalTypeInfo.of(input),
            0,
            true,
            true,
            false,
            new EqualiserCodeGenerator(input, CanonicalNormalizerRowKeyTest.class.getClassLoader())
                .generateRecordEqualiser("CanonicalNormalizerRowKeyEqualiser"));
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
            new KeyedProcessOperator<>(function), selector, selector.getProducedType(), 128, 1, 0);
    harness.setStateBackend(
        rocks
            ? tech.streamfusion.compat.CanonicalTestCompat.rocksBackend()
            : new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
    harness.setup(new RowDataSerializer(input));
    return harness;
  }

  private static OperatorSubtaskState snapshot(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h) throws Exception {
    h.prepareSnapshotPreBarrier(1);
    return h.snapshotWithLocalState(
            1,
            0,
            org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
        .getJobManagerOwnedState();
  }

  private static List<String> feed(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> h,
      RowType type,
      List<RowData> input)
      throws Exception {
    h.getOutput().clear();
    var serializer = new RowDataSerializer(type);
    for (var row : input) h.processElement(new StreamRecord<>(serializer.copy(row)));
    return bytes(type, h.extractOutputValues());
  }

  private static List<RowData> binaryRows(RowType type, List<RowData> rows) {
    var serializer = new RowDataSerializer(type);
    return rows.stream()
        .map(row -> (RowData) serializer.toBinaryRow(row).copy())
        .collect(java.util.stream.Collectors.toList());
  }

  private static List<String> bytes(RowType type, List<RowData> rows) {
    var result = new ArrayList<String>();
    var serializer = new RowDataSerializer(type);
    for (var row : rows) {
      var binary = serializer.toBinaryRow(row);
      result.add(
          java.util.Base64.getEncoder()
              .encodeToString(
                  org.apache.flink.table.data.binary.BinarySegmentUtils.copyToBytes(
                      binary.getSegments(), binary.getOffset(), binary.getSizeInBytes())));
    }
    return result;
  }

  private static org.apache.arrow.vector.VectorSchemaRoot arrowRows(
      List<RowData> rows, RowType type) {
    var root =
        org.apache.arrow.vector.VectorSchemaRoot.create(
            ArrowConversion.toBinaryRowKeySchema(type), NativeAllocator.SHARED);
    var kinds = new TinyIntVector(RowDataArrowConverter.ROW_KIND_COLUMN, NativeAllocator.SHARED);
    try {
      var writer = ArrowConversion.createRowDataArrowWriter(root, type);
      for (var row : rows) writer.write(row);
      writer.finish();
      kinds.allocateNew(rows.size());
      for (int index = 0; index < rows.size(); index++)
        kinds.setSafe(index, rows.get(index).getRowKind().toByteValue());
      kinds.setValueCount(rows.size());
      var fields = new ArrayList<>(root.getSchema().getFields());
      fields.add(kinds.getField());
      var vectors = new ArrayList<>(root.getFieldVectors());
      vectors.add(kinds);
      return new org.apache.arrow.vector.VectorSchemaRoot(fields, vectors, rows.size());
    } catch (RuntimeException | Error failure) {
      root.close();
      kinds.close();
      throw failure;
    }
  }

  private static List<String> push(long handle, boolean rocks, RowType type, List<RowData> rows) {
    try (var root = arrowRows(rows, type);
        var array = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var schema = ArrowSchema.allocateNew(NativeAllocator.SHARED);
        var output = ArrowArray.allocateNew(NativeAllocator.SHARED);
        var outputSchema = ArrowSchema.allocateNew(NativeAllocator.SHARED)) {
      Data.exportVectorSchemaRoot(
          NativeAllocator.SHARED, root, NativeAllocator.DICTIONARIES, array, schema);
      if (rocks)
        Native.pushRocksDBChangelogNormalizer(
            handle,
            array.memoryAddress(),
            schema.memoryAddress(),
            0,
            output.memoryAddress(),
            outputSchema.memoryAddress());
      else
        Native.pushChangelogNormalizer(
            handle,
            array.memoryAddress(),
            schema.memoryAddress(),
            0,
            output.memoryAddress(),
            outputSchema.memoryAddress());
      try (var result =
          Data.importVectorSchemaRoot(
              NativeAllocator.SHARED, output, outputSchema, NativeAllocator.DICTIONARIES)) {
        var reader = ArrowConversion.createArrowReader(result, type);
        var serializer = new RowDataSerializer(type);
        var kinds = (TinyIntVector) result.getVector(RowDataArrowConverter.ROW_KIND_COLUMN);
        List<RowData> values = new ArrayList<>();
        for (int index = 0; index < result.getRowCount(); index++) {
          var row = serializer.copy(reader.read(index));
          row.setRowKind(RowKind.fromByteValue(kinds.get(index)));
          values.add(row);
        }
        return bytes(type, values);
      }
    }
  }
}
