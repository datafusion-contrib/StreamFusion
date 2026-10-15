package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.TimestampString;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.*;

class GeneratedProcessingTimeOperatorTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void calcAndFilterBindLiveTimeAcrossEmptyAndChangelogBatches(boolean filter) throws Exception {
    ClassLoader loader = getClass().getClassLoader();
    FlinkExpressionFunction function;
    int returnType;
    if (filter) {
      var rex = new RexBuilder(new FlinkTypeFactory(loader, FlinkTypeSystem.INSTANCE));
      var condition =
          rex.makeCall(
              SqlStdOperatorTable.GREATER_THAN,
              GeneratedProcessingTimeTest.selection(3),
              rex.makeTimestampWithLocalTimeZoneLiteral(
                  TimestampString.fromMillisSinceEpoch(2000L), 3));
      function =
          new FlinkExpressionFunction(
              condition,
              new LogicalType[] {new LocalZonedTimestampType(3)},
              TableConfig.getDefault(),
              loader);
      returnType = NativeUdf.TYPE_BOOLEAN;
    } else {
      function = GeneratedProcessingTimeTest.scalar(3);
      returnType = NativeUdf.TYPE_TIMESTAMP_DATA;
    }
    var descriptor =
        NativeUdf.Descriptor.forFunction(
            function,
            FlinkExpressionFunction.class.getMethod("eval", Object[].class),
            new int[] {NativeUdf.TYPE_TIMESTAMP_DATA},
            returnType);
    var binding = new NativeUdf.Binding(new NativeUdf.Descriptor[] {descriptor}, new int[] {0});
    int[] kinds = {17, 0};
    int[] payload = {0, 0};
    int[] children = {1, 0};
    long[] longs = {0, returnType};
    OneInputStreamOperator<ArrowBatch, ArrowBatch> operator =
        filter
            ? new NativeFilterOperator(
                new int[] {0},
                kinds,
                payload,
                children,
                longs,
                new double[0],
                new String[0],
                binding)
            : new NativeCalcOperator(
                kinds,
                payload,
                children,
                longs,
                new double[0],
                new String[0],
                new int[] {0},
                -1,
                new String[] {"v"},
                binding);
    RowType schema = RowType.of(new LocalZonedTimestampType(3));
    List<RowData> input =
        List.of(
            GenericRowData.ofKind(RowKind.INSERT, (Object) null),
            GenericRowData.ofKind(RowKind.UPDATE_BEFORE, TimestampData.fromEpochMillis(500L)),
            GenericRowData.ofKind(RowKind.UPDATE_AFTER, (Object) null),
            GenericRowData.ofKind(RowKind.DELETE, TimestampData.fromEpochMillis(500L)));
    try (var allocator = new RootAllocator();
        var harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      for (long time : new long[] {1000L, 3000L}) {
        harness.setProcessingTime(time);
        harness.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(List.of(), schema, allocator, true))));
        harness.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(input, schema, allocator, true))));
        var output = new ArrayList<RowData>();
        for (Object record : harness.getOutput()) {
          if (record instanceof StreamRecord<?>) {
            try (VectorSchemaRoot root =
                ((ArrowBatch) ((StreamRecord<?>) record).getValue()).root()) {
              output.addAll(RowDataArrowConverter.read(root, schema));
            }
          }
        }
        harness.getOutput().clear();
        if (filter) {
          assertEquals(time == 1000L ? 0 : 2, output.size());
          if (!output.isEmpty()) {
            assertEquals(RowKind.INSERT, output.get(0).getRowKind());
            assertEquals(RowKind.UPDATE_AFTER, output.get(1).getRowKind());
            assertTrue(output.stream().allMatch(row -> row.isNullAt(0)));
          }
        } else {
          assertEquals(input.size(), output.size());
          for (int i = 0; i < input.size(); i++) {
            assertEquals(input.get(i).getRowKind(), output.get(i).getRowKind());
            assertEquals(
                TimestampData.fromEpochMillis(i % 2 == 0 ? time : 500L),
                output.get(i).getTimestamp(0, 3));
          }
        }
        assertEquals(
            0, allocator.getAllocatedMemory(), "the operator releases all input Arrow buffers");
      }
    }
  }
}
