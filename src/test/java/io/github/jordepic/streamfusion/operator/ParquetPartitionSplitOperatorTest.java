package io.github.jordepic.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("streamfusion-parquet")
class ParquetPartitionSplitOperatorTest {

  private static final RowType SCHEMA =
      RowType.of(
          new LogicalType[] {new VarCharType(VarCharType.MAX_LENGTH), new IntType()},
          new String[] {"dt", "v"});

  private static RowData row(String dt, int v) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, dt == null ? null : StringData.fromString(dt));
    row.setField(1, v);
    return row;
  }

  private static ArrowBatch batch(BufferAllocator allocator, RowData... rows) {
    VectorSchemaRoot root = RowDataArrowConverter.write(List.of(rows), SCHEMA, allocator);
    return new ArrowBatch(root);
  }

  private static Map<String, List<Integer>> collectBuckets(
      OneInputStreamOperatorTestHarness<ArrowBatch, PartitionedArrowBatch> harness) {
    Map<String, List<Integer>> buckets = new TreeMap<>();
    harness
        .extractOutputValues()
        .forEach(
            batch -> {
              VectorSchemaRoot root = batch.root();
              IntVector values = (IntVector) root.getVector("v");
              List<Integer> rows = new ArrayList<>();
              for (int i = 0; i < root.getRowCount(); i++) {
                rows.add(values.get(i));
              }
              buckets.computeIfAbsent(batch.bucketId(), k -> new ArrayList<>()).addAll(rows);
              root.close();
            });
    return buckets;
  }

  @Test
  void routesGroupsByFlinkPartitionPaths() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, PartitionedArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(
                new ParquetPartitionSplitOperator(SCHEMA, List.of("dt"), "__DEFAULT_PARTITION__"),
                new ArrowBatchSerializer())) {
      harness.setup(new PartitionedArrowBatchSerializer());
      harness.open();
      harness.processElement(
          new StreamRecord<>(
              batch(
                  allocator,
                  row("b", 1),
                  row(null, 2),
                  row("a", 3),
                  row("b", 4),
                  // The slash must not split the path: Flink escapes it, and so must we (we do, by
                  // calling Flink's own escaping).
                  row("a/b", 5))));

      Map<String, List<Integer>> buckets = collectBuckets(harness);
      assertEquals(
          Map.of(
              "dt=b/", List.of(1, 4),
              "dt=__DEFAULT_PARTITION__/", List.of(2),
              "dt=a/", List.of(3),
              "dt=a%2Fb/", List.of(5)),
          buckets);
    }
  }

  @Test
  void unpartitionedBatchesPassThroughToTheRootBucket() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        OneInputStreamOperatorTestHarness<ArrowBatch, PartitionedArrowBatch> harness =
            new OneInputStreamOperatorTestHarness<>(
                new ParquetPartitionSplitOperator(SCHEMA, List.of(), "__DEFAULT_PARTITION__"),
                new ArrowBatchSerializer())) {
      harness.setup(new PartitionedArrowBatchSerializer());
      harness.open();
      harness.processElement(new StreamRecord<>(batch(allocator, row("x", 1), row("y", 2))));

      Map<String, List<Integer>> buckets = collectBuckets(harness);
      assertEquals(Map.of("", List.of(1, 2)), buckets);
    }
  }
}
