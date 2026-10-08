package tech.streamfusion.fluss;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.*;
import org.apache.fluss.bucketing.FlussBucketingFunction;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.memory.MemorySegmentOutputView;
import org.apache.fluss.record.*;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.RowDataArrowConverter;

class FlussArrowWriteTest {
  private static final org.apache.fluss.types.RowType TYPE =
      org.apache.fluss.types.RowType.of(
          new org.apache.fluss.types.DataType[] {
            DataTypes.BIGINT(),
            DataTypes.BOOLEAN(),
            DataTypes.TINYINT(),
            DataTypes.SMALLINT(),
            DataTypes.INT(),
            DataTypes.BIGINT(),
            DataTypes.FLOAT(),
            DataTypes.DOUBLE(),
            DataTypes.STRING(),
            DataTypes.BYTES(),
            DataTypes.DECIMAL(18, 3),
            DataTypes.DECIMAL(38, 4),
            DataTypes.DATE(),
            DataTypes.TIME(3),
            DataTypes.TIMESTAMP(3),
            DataTypes.TIMESTAMP(9),
            DataTypes.TIMESTAMP_LTZ(9)
          },
          new String[] {
            "id", "bool", "tiny", "small", "int", "long", "float", "double", "str", "bytes",
            "dec18", "dec38", "date", "time", "ts3", "ts9", "ltz9"
          });

  private static GenericRowData row(int i) {
    return GenericRowData.of(
        (long) i,
        i % 2 == 0,
        (byte) (i - 128),
        (short) (i * 129 - 32768),
        i == 0 ? Integer.MIN_VALUE : i * 129,
        i == 0 ? Long.MIN_VALUE : (long) i * 173,
        i == 0 ? Float.intBitsToFloat(0xffc00001) : i == 1 ? -0.0f : i == 2 ? 0.0f : (float) i,
        i == 0
            ? Double.longBitsToDouble(0xfff8000000000001L)
            : i == 1 ? -0.0 : i == 2 ? 0.0 : (double) i,
        StringData.fromString(i % 3 == 0 ? "é漢🙂" : "key" + i),
        new byte[] {(byte) i, (byte) 128},
        DecimalData.fromBigDecimal(BigDecimal.valueOf((long) i - 173, 3), 18, 3),
        DecimalData.fromBigDecimal(
            new BigDecimal(
                    i == 128 ? "123456789012345678901234.5678" : "-123456789012345678901234.5678")
                .add(BigDecimal.valueOf(i)),
            38,
            4),
        i - 173,
        i * 173,
        TimestampData.fromEpochMillis((long) i - 173),
        TimestampData.fromEpochMillis((long) i - 173, i * 997),
        TimestampData.fromEpochMillis((long) i - 173, i * 997));
  }

  @Test
  void bucketGroupsMatchReleasedSdkForEveryScalarAndCompositeKey() throws Exception {
    var flinkType = FlinkConversions.toFlinkRowType(TYPE);
    var rows = new ArrayList<RowData>();
    for (int i = 0; i < 257; i++) rows.add(row(i));
    try (var allocator = new RootAllocator();
        var root = RowDataArrowConverter.write(rows, flinkType, allocator)) {
      for (int count : new int[] {1, 7, 16}) {
        for (int[] keys :
            new int[][] {
              {1},
              {2},
              {3},
              {4},
              {5},
              {6},
              {7},
              {8},
              {9},
              {10},
              {11},
              {12},
              {13},
              {14},
              {15},
              {16},
              {4, 8, 15},
              {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
            }) {
          var names =
              java.util.Arrays.stream(keys).mapToObj(i -> TYPE.getFieldNames().get(i)).toList();
          var encoder = KeyEncoder.ofBucketKeyEncoder(TYPE, names, null);
          int[] precisions =
              java.util.Arrays.stream(keys).map(i -> i == 14 ? 3 : i >= 15 ? 9 : 0).toArray();
          long handle;
          try (var array = ArrowArray.allocateNew(allocator);
              var schema = ArrowSchema.allocateNew(allocator)) {
            Data.exportVectorSchemaRoot(
                allocator, root, NativeAllocator.DICTIONARIES, array, schema);
            handle =
                NativeFluss.splitByBucket(
                    array.memoryAddress(), schema.memoryAddress(), keys, precisions, count);
          }
          boolean[] seen = new boolean[rows.size()];
          try {
            while (true) {
              try (var array = ArrowArray.allocateNew(allocator);
                  var schema = ArrowSchema.allocateNew(allocator)) {
                int bucket =
                    NativeFluss.nextBucketSlice(
                        handle, array.memoryAddress(), schema.memoryAddress());
                if (bucket < 0) break;
                try (var group =
                    Data.importVectorSchemaRoot(
                        allocator, array, schema, NativeAllocator.DICTIONARIES)) {
                  if (count == 1)
                    assertEquals(
                        root.getVector(0).getDataBuffer().memoryAddress(),
                        group.getVector(0).getDataBuffer().memoryAddress());
                  int previous = -1;
                  for (var r : RowDataArrowConverter.read(group, flinkType)) {
                    int id = (int) r.getLong(0);
                    assertFalse(seen[id]);
                    seen[id] = true;
                    assertTrue(id > previous);
                    previous = id;
                    assertEquals(
                        FlussBucketingFunction.bucketForRowKey(
                            encoder.encodeKey(new FlinkAsFlussRow(rows.get(id))), count),
                        bucket,
                        "key " + names + " row " + id);
                  }
                }
              }
            }
          } finally {
            NativeFluss.closeBucketSplit(handle);
          }
          for (boolean value : seen) assertTrue(value);
        }
      }
    }
  }

  @Test
  void statisticsMatchSdkBytesIncludingNullsNaNsSignedZeroAndFractionalTimestamps()
      throws Exception {
    var rows = new ArrayList<RowData>();
    for (int i = 0; i < 257; i++) rows.add(row(i));
    var empty = new GenericRowData(TYPE.getFieldCount());
    rows.add(empty);
    try (var allocator = new RootAllocator();
        var root =
            RowDataArrowConverter.write(rows, FlinkConversions.toFlinkRowType(TYPE), allocator)) {
      for (int[] mapping :
          new int[][] {
            {8, 7, 4, 15, 10, 9}, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
          }) {
        var collector = new LogRecordBatchStatisticsCollector(TYPE, mapping);
        for (var row : rows) collector.processRow(new FlinkAsFlussRow(row));
        var expected = new MemorySegmentOutputView(256);
        collector.writeStatistics(expected);
        byte[] actual = new FlussArrowStatistics(TYPE, mapping).collect(root);
        assertArrayEquals(expected.getCopyOfBuffer(), actual);
        try (var slice = root.slice(137, 89)) {
          var slicedCollector = new LogRecordBatchStatisticsCollector(TYPE, mapping);
          for (var row : rows.subList(137, 226))
            slicedCollector.processRow(new FlinkAsFlussRow(row));
          var slicedExpected = new MemorySegmentOutputView(256);
          slicedCollector.writeStatistics(slicedExpected);
          assertArrayEquals(
              slicedExpected.getCopyOfBuffer(),
              new FlussArrowStatistics(TYPE, mapping).collect(slice));
        }
        try (var wire =
            FlussArrowSchema.forAppend(root, FlussArrowSchema.wireSchema(TYPE), allocator)) {
          byte[] encoded =
              FlussArrowLogBatch.encode(
                  wire,
                  1,
                  17,
                  0,
                  org.apache.arrow.vector.compression.NoCompressionCodec.INSTANCE,
                  actual);
          var batch = MemoryLogRecords.pointToBytes(encoded).batches().iterator().next();
          batch.ensureValid();
          assertEquals(LogRecordBatchFormat.LOG_MAGIC_VALUE_V1, batch.magic());
          try (var decoded =
              FlussArrowLogBatch.decode(
                  java.nio.ByteBuffer.wrap(encoded), wire.getSchema(), allocator)) {
            assertEquals(rows.size(), decoded.root().getRowCount());
          }
        }
      }
    }
  }

  @Test
  void fixedWidthCharacterAndBinaryKeysMatchSdk() throws Exception {
    var type = org.apache.fluss.types.RowType.of(DataTypes.CHAR(8), DataTypes.BINARY(2));
    var rows =
        List.<RowData>of(
            GenericRowData.of(StringData.fromString("a       "), new byte[] {(byte) 128, 0}),
            GenericRowData.of(StringData.fromString("é漢🙂"), new byte[] {0, (byte) 255}));
    var encoder = KeyEncoder.ofBucketKeyEncoder(type, type.getFieldNames(), null);
    try (var allocator = new RootAllocator();
        var root =
            RowDataArrowConverter.write(rows, FlinkConversions.toFlinkRowType(type), allocator)) {
      for (int i = 0; i < rows.size(); i++) {
        try (var one = root.slice(i, 1);
            var array = ArrowArray.allocateNew(allocator);
            var schema = ArrowSchema.allocateNew(allocator)) {
          Data.exportVectorSchemaRoot(allocator, one, NativeAllocator.DICTIONARIES, array, schema);
          long handle =
              NativeFluss.splitByBucket(
                  array.memoryAddress(),
                  schema.memoryAddress(),
                  new int[] {0, 1},
                  new int[] {0, 0},
                  7);
          try (var out = ArrowArray.allocateNew(allocator);
              var outSchema = ArrowSchema.allocateNew(allocator)) {
            try {
              int bucket =
                  NativeFluss.nextBucketSlice(
                      handle, out.memoryAddress(), outSchema.memoryAddress());
              try (var group =
                  Data.importVectorSchemaRoot(
                      allocator, out, outSchema, NativeAllocator.DICTIONARIES)) {
                assertEquals(1, group.getRowCount());
                assertEquals(
                    FlussBucketingFunction.bucketForRowKey(
                        encoder.encodeKey(new FlinkAsFlussRow(rows.get(i))), 7),
                    bucket);
              }
            } finally {
              NativeFluss.closeBucketSplit(handle);
            }
          }
        }
      }
    }
  }

  @Test
  void complexStatisticsOnlyCollectNullCountsAndDoNotMistakeRowsForTimestamps() throws Exception {
    var nested =
        org.apache.fluss.types.RowType.of(
            new org.apache.fluss.types.DataType[] {DataTypes.BIGINT(), DataTypes.INT()},
            new String[] {"millis", "nano_of_milli"});
    var type =
        org.apache.fluss.types.RowType.of(
            nested,
            DataTypes.ARRAY(DataTypes.INT()),
            DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
    var rows =
        List.<RowData>of(
            GenericRowData.of(
                GenericRowData.of(1L, 0),
                new GenericArrayData(new Integer[] {1, null, 3}),
                new GenericMapData(java.util.Map.of(StringData.fromString("a"), 1))),
            new GenericRowData(3));
    try (var allocator = new RootAllocator();
        var root =
            RowDataArrowConverter.write(rows, FlinkConversions.toFlinkRowType(type), allocator)) {
      var collector = new LogRecordBatchStatisticsCollector(type, new int[] {0, 1, 2});
      for (var row : rows) collector.processRow(new FlinkAsFlussRow(row));
      var expected = new MemorySegmentOutputView(256);
      collector.writeStatistics(expected);
      assertArrayEquals(
          expected.getCopyOfBuffer(),
          new FlussArrowStatistics(type, new int[] {0, 1, 2}).collect(root));
    }
  }

  @Test
  void allNullStatisticsAndNullKeyFailuresReleaseBuffers() throws Exception {
    var type = org.apache.fluss.types.RowType.of(DataTypes.BIGINT());
    try (var allocator = new RootAllocator();
        var root =
            RowDataArrowConverter.write(
                List.of(new GenericRowData(1)), FlinkConversions.toFlinkRowType(type), allocator)) {
      var collector = new LogRecordBatchStatisticsCollector(type, new int[] {0});
      collector.processRow(org.apache.fluss.row.GenericRow.of((Object) null));
      var expected = new MemorySegmentOutputView(128);
      collector.writeStatistics(expected);
      assertArrayEquals(
          expected.getCopyOfBuffer(), new FlussArrowStatistics(type, new int[] {0}).collect(root));
      try (var array = ArrowArray.allocateNew(allocator);
          var schema = ArrowSchema.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, root, NativeAllocator.DICTIONARIES, array, schema);
        assertThrows(
            RuntimeException.class,
            () ->
                NativeFluss.splitByBucket(
                    array.memoryAddress(),
                    schema.memoryAddress(),
                    new int[] {0},
                    new int[] {0},
                    7));
      }
    }
  }
}
