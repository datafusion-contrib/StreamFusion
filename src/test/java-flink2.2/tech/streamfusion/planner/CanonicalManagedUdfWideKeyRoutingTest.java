package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.IntStream;
import org.apache.arrow.c.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.table.data.binary.*;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import tech.streamfusion.Native;
import tech.streamfusion.operator.RowDataArrowConverter;

class CanonicalManagedUdfWideKeyRoutingTest {
  @ParameterizedTest
  @EnumSource(ManagedUdfKeyShape.class)
  void nativeExchangeUsesActualStockProjectionBytesAndHashes(ManagedUdfKeyShape shape)
      throws Exception {
    verify(shape.keys, shape.routingRows());
  }

  static void verify(
      org.apache.flink.table.types.logical.RowType type,
      java.util.List<org.apache.flink.table.data.RowData> rows)
      throws Exception {
    verify(type, rows, false);
  }

  static void verify(
      org.apache.flink.table.types.logical.RowType type,
      java.util.List<org.apache.flink.table.data.RowData> rows,
      boolean binaryRows)
      throws Exception {
    int[] keys = IntStream.range(0, type.getFieldCount()).toArray();
    var selector =
        KeySelectorUtil.getRowDataSelector(
            CanonicalManagedUdfWideKeyRoutingTest.class.getClassLoader(),
            keys,
            InternalTypeInfo.of(type));
    try (var allocator = new RootAllocator();
        var root = batch(rows, type, allocator, binaryRows);
        var dictionaries = new CDataDictionaryProvider();
        var array = ArrowArray.allocateNew(allocator);
        var schema = ArrowSchema.allocateNew(allocator)) {
      int[] descriptor = FlinkKeyGroupUtils.stockKeyDescriptor(type, keys);
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      byte[][] bytes =
          Native.flinkBinaryRows(
              array.memoryAddress(),
              schema.memoryAddress(),
              keys,
              descriptor,
              IntStream.range(0, rows.size()).toArray());
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      int[] hashes =
          Native.flinkBinaryRowHashes(
              array.memoryAddress(), schema.memoryAddress(), keys, descriptor);
      var distinct = new java.util.HashSet<BinaryRowData>();
      for (int i = 0; i < rows.size(); i++) {
        var expected = ((BinaryRowData) selector.getKey(rows.get(i))).copy();
        distinct.add(expected);
        if (binaryRows) {
          var reconstructed =
              tech.streamfusion.arrow.ArrowConversion.createArrowReader(root, type, true).read(i);
          assertEquals(expected, selector.getKey(reconstructed), "actual managed backend key " + i);
        }
        assertArrayEquals(
            BinarySegmentUtils.copyToBytes(
                expected.getSegments(), expected.getOffset(), expected.getSizeInBytes()),
            bytes[i],
            "key " + i);
        assertEquals(expected.hashCode(), hashes[i], "hash " + i);
        for (int parallelism : new int[] {1, 3, 2})
          assertEquals(
              KeyGroupRangeAssignment.assignKeyToParallelOperator(expected, 128, parallelism),
              KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                  128, parallelism, org.apache.flink.util.MathUtils.murmurHash(hashes[i]) % 128));
      }
      assertEquals(65, distinct.size(), "fractional, nested and nullable keys must stay distinct");
    }
  }

  private static org.apache.arrow.vector.VectorSchemaRoot batch(
      java.util.List<org.apache.flink.table.data.RowData> rows,
      org.apache.flink.table.types.logical.RowType type,
      org.apache.arrow.memory.BufferAllocator allocator,
      boolean binaryRows) {
    if (!binaryRows) return RowDataArrowConverter.write(rows, type, allocator);
    var root =
        org.apache.arrow.vector.VectorSchemaRoot.create(
            RowDataToArrowExecNode.binaryRowKeySchema(type), allocator);
    try {
      var writer = tech.streamfusion.arrow.ArrowConversion.createRowDataArrowWriter(root, type);
      for (var row : rows) writer.write(row);
      writer.finish();
      return root;
    } catch (RuntimeException | Error failure) {
      root.close();
      throw failure;
    }
  }
}
