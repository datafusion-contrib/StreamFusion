package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.compat.ListCollectors;

class ArrowBucketRouterTest {
  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {
            new VarCharType(VarCharType.MAX_LENGTH),
            new BigIntType(),
            new TimestampType(3),
            new IntType()
          },
          new String[] {"dt", "k", "ts", "v"});
  private static final RowType PARTITION_TYPE = RowType.of(new VarCharType(VarCharType.MAX_LENGTH));
  private static final RowType BUCKET_KEY_TYPE =
      RowType.of(new BigIntType(), new TimestampType(3));
  private static final int[] PARTITION_COLUMNS = {0};
  private static final int[] BUCKET_COLUMNS = {1, 2};
  private static final int[] BUCKET_PRECISIONS = {-1, 3};

  private static RowData row(String dt, Long k, long millis, int v) {
    GenericRowData row = new GenericRowData(4);
    row.setField(0, dt == null ? null : StringData.fromString(dt));
    row.setField(1, k);
    row.setField(2, TimestampData.fromEpochMillis(millis));
    row.setField(3, v);
    return row;
  }

  private static List<RowData> rows(int n) {
    List<RowData> rows = new ArrayList<>();
    String[] partitions = {"2026-01-01", "2026-01-02", null, "a-much-longer-partition-value"};
    for (int i = 0; i < n; i++) {
      rows.add(row(partitions[i % 4], i % 7 == 0 ? null : (long) (i % 11), 1_000L * (i % 5), i));
    }
    return rows;
  }

  private static byte[] flinkBinaryRow(RowType type, Object... fields) {
    GenericRowData row = new GenericRowData(fields.length);
    for (int i = 0; i < fields.length; i++) {
      row.setField(i, fields[i]);
    }
    BinaryRowData binary = new RowDataSerializer(type).toBinaryRow(row);
    byte[] bytes = new byte[binary.getSizeInBytes()];
    binary.getSegments()[0].get(binary.getOffset(), bytes, 0, bytes.length);
    return bytes;
  }

  private static int flinkBucket(RowData row, int numBuckets) {
    GenericRowData key = new GenericRowData(2);
    key.setField(0, row.isNullAt(1) ? null : row.getLong(1));
    key.setField(1, row.getTimestamp(2, 3));
    return Math.abs(new RowDataSerializer(BUCKET_KEY_TYPE).toBinaryRow(key).hashCode() % numBuckets);
  }

  private static List<BucketedArrowBatch> route(
      List<RowData> rows, int numBuckets, BufferAllocator allocator) throws Exception {
    return route(rows, numBuckets, allocator, false);
  }

  private static List<BucketedArrowBatch> route(
      List<RowData> rows, int numBuckets, BufferAllocator allocator, boolean withRowKind)
      throws Exception {
    return route(rows, numBuckets, allocator, withRowKind, false);
  }

  @SuppressWarnings("unchecked")
  private static List<BucketedArrowBatch> route(
      List<RowData> rows,
      int numBuckets,
      BufferAllocator allocator,
      boolean withRowKind,
      boolean keepRowKinds)
      throws Exception {
    try (OneInputStreamOperatorTestHarness<ArrowBatch, BucketedArrowBatch> harness =
        new OneInputStreamOperatorTestHarness<>(
            new ArrowBucketRouter(
                PARTITION_COLUMNS,
                new int[] {-1},
                BUCKET_COLUMNS,
                BUCKET_PRECISIONS,
                numBuckets,
                keepRowKinds),
            new ArrowBatchSerializer())) {
      harness.setup(new BucketedArrowBatchSerializer());
      harness.open();
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(RowDataArrowConverter.write(rows, SCHEMA, allocator, withRowKind))));
      List<BucketedArrowBatch> out = new ArrayList<>();
      for (Object record : harness.getOutput()) {
        if (record instanceof StreamRecord<?>) {
          out.add(((StreamRecord<BucketedArrowBatch>) record).getValue());
        }
      }
      return out;
    }
  }

  @Test
  void routesEveryRowToPaimonsPartitionAndDefaultBucket() throws Exception {
    int numBuckets = 5;
    List<RowData> rows = rows(300);
    try (BufferAllocator allocator = new RootAllocator()) {
      int total = 0;
      Set<String> destinations = new HashSet<>();
      for (BucketedArrowBatch batch : route(rows, numBuckets, allocator)) {
        assertTrue(batch.bucket() >= 0 && batch.bucket() < numBuckets, "bucket in range");
        assertTrue(
            destinations.add(batch.bucket() + "@" + java.util.Arrays.toString(batch.partition())),
            "one batch per destination");
        int previous = -1;
        try (VectorSchemaRoot sub = batch.root()) {
          for (RowData row : RowDataArrowConverter.read(sub, SCHEMA)) {
            StringData dt = row.isNullAt(0) ? null : row.getString(0);
            assertArrayEquals(
                flinkBinaryRow(PARTITION_TYPE, dt),
                batch.partition(),
                "partition BinaryRow of row " + row.getInt(3));
            assertEquals(
                flinkBucket(row, numBuckets), batch.bucket(), "bucket of row " + row.getInt(3));
            assertTrue(row.getInt(3) > previous, "arrival order kept within a destination");
            previous = row.getInt(3);
            total++;
          }
        }
      }
      assertEquals(rows.size(), total, "all rows preserved");
    }
  }

  @Test
  void partitionBytesReadBackAsABinaryRow() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      for (BucketedArrowBatch batch : route(rows(40), 3, allocator)) {
        BinaryRowData partition = new BinaryRowData(1);
        byte[] bytes = batch.partition();
        partition.pointTo(MemorySegmentFactory.wrap(bytes), 0, bytes.length);
        try (VectorSchemaRoot sub = batch.root()) {
          RowData first = RowDataArrowConverter.read(sub, SCHEMA).get(0);
          if (first.isNullAt(0)) {
            assertTrue(partition.isNullAt(0));
          } else {
            assertEquals(first.getString(0), partition.getString(0));
          }
        }
      }
    }
  }

  @Test
  void dropsTheRowKindColumnAChangelogOperatorLeavesOnInsertOnlyOutput() throws Exception {
    List<RowData> rows = rows(40);
    try (BufferAllocator allocator = new RootAllocator()) {
      int total = 0;
      for (BucketedArrowBatch batch : route(rows, 3, allocator, true)) {
        try (VectorSchemaRoot sub = batch.root()) {
          assertEquals(
              SCHEMA.getFieldNames(),
              sub.getSchema().getFields().stream()
                  .map(f -> f.getName())
                  .collect(ListCollectors.toList()));
          for (RowData row : RowDataArrowConverter.read(sub, SCHEMA)) {
            assertEquals(flinkBucket(row, 3), batch.bucket());
            total++;
          }
        }
      }
      assertEquals(rows.size(), total);
    }
  }

  @Test
  void keepsTheRowKindColumnForAPrimaryKeyTable() throws Exception {
    List<RowData> rows = rows(40);
    try (BufferAllocator allocator = new RootAllocator()) {
      int total = 0;
      for (BucketedArrowBatch batch : route(rows, 3, allocator, true, true)) {
        try (VectorSchemaRoot sub = batch.root()) {
          assertEquals(
              RowDataArrowConverter.ROW_KIND_COLUMN,
              sub.getSchema().getFields().get(SCHEMA.getFieldCount()).getName());
          for (RowData row : RowDataArrowConverter.read(sub, SCHEMA)) {
            assertEquals(flinkBucket(row, 3), batch.bucket());
            total++;
          }
        }
      }
      assertEquals(rows.size(), total);
    }
  }

  @Test
  void bucketUnawareTablesSplitByPartitionOnly() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      List<BucketedArrowBatch> routed = route(rows(40), -1, allocator);
      assertEquals(4, routed.size(), "one batch per partition value, including NULL");
      for (BucketedArrowBatch batch : routed) {
        assertEquals(0, batch.bucket());
        batch.root().close();
      }
    }
  }

  @Test
  void serializerRoundTripsPartitionBucketAndRows() throws Exception {
    BucketedArrowBatchSerializer serializer = new BucketedArrowBatchSerializer();
    try (BufferAllocator allocator = new RootAllocator()) {
      List<RowData> rows = rows(3);
      VectorSchemaRoot root = RowDataArrowConverter.write(rows, SCHEMA, allocator);
      byte[] partition = flinkBinaryRow(PARTITION_TYPE, StringData.fromString("2026-01-01"));

      DataOutputSerializer out = new DataOutputSerializer(256);
      serializer.serialize(new BucketedArrowBatch(root, partition, 4), out);
      DataOutputSerializer copied = new DataOutputSerializer(256);
      serializer.copy(new DataInputDeserializer(out.getCopyOfBuffer()), copied);
      BucketedArrowBatch back =
          serializer.deserialize(new DataInputDeserializer(copied.getCopyOfBuffer()));
      assertArrayEquals(partition, back.partition());
      assertEquals(4, back.bucket());
      assertEquals(3, back.rowCount());
      try (VectorSchemaRoot result = back.root()) {
        List<RowData> readBack = RowDataArrowConverter.read(result, SCHEMA);
        for (int i = 0; i < rows.size(); i++) {
          assertEquals(rows.get(i).getInt(3), readBack.get(i).getInt(3), "v row " + i);
        }
      }
      assertNull(serializer.createInstance());
    }
  }
}
