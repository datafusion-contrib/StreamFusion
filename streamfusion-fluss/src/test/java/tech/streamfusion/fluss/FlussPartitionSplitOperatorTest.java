package tech.streamfusion.fluss;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.row.RowPartitionGetter;
import org.junit.jupiter.api.Test;
import tech.streamfusion.operator.*;

class FlussPartitionSplitOperatorTest {
  private static final RowType TYPE =
      (RowType)
          DataTypes.ROW(
                  DataTypes.FIELD("day", DataTypes.STRING()),
                  DataTypes.FIELD("region", DataTypes.INT()),
                  DataTypes.FIELD("value", DataTypes.INT()))
              .getLogicalType();

  @Test
  void compositePartitionNamesAndRowOrderMatchTheReleasedClient() throws Exception {
    var rows =
        List.of(
            GenericRowData.of(StringData.fromString("a/b"), 1, 10),
            GenericRowData.of(StringData.fromString("other"), 2, 20),
            GenericRowData.of(StringData.fromString("a/b"), 1, 30),
            GenericRowData.of(StringData.fromString("a/b"), 2, 40));
    var getter =
        new RowPartitionGetter(FlinkConversions.toFlussRowType(TYPE), List.of("day", "region"));
    Map<String, List<Integer>> expected = new LinkedHashMap<>();
    for (var row : rows)
      expected
          .computeIfAbsent(
              getter.getPartition(new FlinkAsFlussRow(row)), ignored -> new ArrayList<>())
          .add(row.getInt(2));
    try (var allocator = new RootAllocator();
        var harness =
            new OneInputStreamOperatorTestHarness<ArrowBatch, PartitionedArrowBatch>(
                new FlussPartitionSplitOperator(TYPE, List.of("day", "region")),
                new ArrowBatchSerializer())) {
      harness.setup(new PartitionedArrowBatchSerializer());
      harness.open();
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(RowDataArrowConverter.write(new ArrayList<>(rows), TYPE, allocator)),
              123L));
      Map<String, List<Integer>> actual = new LinkedHashMap<>();
      for (var record : harness.extractOutputStreamRecords()) {
        assertEquals(123L, record.getTimestamp());
        try (var root = record.getValue().root()) {
          var values = (IntVector) root.getVector("value");
          List<Integer> group =
              actual.computeIfAbsent(record.getValue().bucketId(), ignored -> new ArrayList<>());
          for (int i = 0; i < root.getRowCount(); i++) group.add(values.get(i));
        }
      }
      assertEquals(expected, actual);
    }
  }

  @Test
  void nullPartitionKeysFailAndReleaseTheImportedGroup() throws Exception {
    try (var allocator = new RootAllocator();
        var harness =
            new OneInputStreamOperatorTestHarness<ArrowBatch, PartitionedArrowBatch>(
                new FlussPartitionSplitOperator(TYPE, List.of("day")),
                new ArrowBatchSerializer())) {
      harness.setup(new PartitionedArrowBatchSerializer());
      harness.open();
      var root =
          RowDataArrowConverter.write(List.of(GenericRowData.of(null, 1, 10)), TYPE, allocator);
      assertThrows(
          NullPointerException.class,
          () -> harness.processElement(new StreamRecord<>(new ArrowBatch(root))));
      assertTrue(harness.extractOutputStreamRecords().isEmpty());
    }
  }
}
