package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rex.RexBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchSerializer;
import tech.streamfusion.operator.NativeCalcOperator;
import tech.streamfusion.operator.RowDataArrowConverter;

class ProctimePlaceholderOperatorTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void indicatorStaysNullUntilMaterializationAcrossEmptyAndChangelogBatches(boolean nullable)
      throws Exception {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    var indicatorType = types.createProctimeIndicatorType(nullable);
    var indicator = rex.makeCall(indicatorType, FlinkSqlOperatorTable.PROCTIME, List.of());
    var materializedType = new LocalZonedTimestampType(false, 3);
    var materialized =
        rex.makeCall(
            types.createFieldTypeFromLogicalType(materializedType),
            FlinkSqlOperatorTable.PROCTIME_MATERIALIZE,
            List.of(indicator));
    var encoded =
        RexExpression.encodeProjections(
            List.of(
                rex.makeInputRef(types.createFieldTypeFromLogicalType(new BigIntType()), 0),
                indicator,
                materialized),
            List.of("k", "pt", "observed"));
    assertNotNull(encoded);
    var operator =
        new NativeCalcOperator(
            encoded.kinds(),
            encoded.payload(),
            encoded.childCounts(),
            encoded.longs(),
            encoded.doubles(),
            encoded.strings(),
            encoded.projectionRoots(),
            encoded.conditionRoot(),
            encoded.outputNames(),
            encoded.udfBinding());
    var inputType = RowType.of(new BigIntType());
    var outputType =
        RowType.of(
            new BigIntType(), FlinkTypeFactory.toLogicalType(indicatorType), materializedType);
    List<RowData> input = new ArrayList<>();
    for (var kind : RowKind.values())
      input.add(GenericRowData.ofKind(kind, (long) kind.toByteValue()));
    try (var allocator = new RootAllocator();
        var harness =
            new OneInputStreamOperatorTestHarness<>(operator, new ArrowBatchSerializer())) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      for (int attempt = 0; attempt < 2; attempt++) {
        long earliest = System.currentTimeMillis();
        for (var batch : List.of(List.<RowData>of(), input))
          harness.processElement(
              new StreamRecord<>(
                  new ArrowBatch(RowDataArrowConverter.write(batch, inputType, allocator, true))));
        List<RowData> output = new ArrayList<>();
        for (var value : harness.getOutput())
          if (value instanceof StreamRecord<?>) {
            try (VectorSchemaRoot root =
                ((ArrowBatch) ((StreamRecord<?>) value).getValue()).root()) {
              output.addAll(RowDataArrowConverter.read(root, outputType));
            }
          }
        harness.getOutput().clear();
        assertEquals(input.size(), output.size());
        for (int index = 0; index < input.size(); index++) {
          assertEquals(input.get(index).getRowKind(), output.get(index).getRowKind());
          assertEquals(input.get(index).getLong(0), output.get(index).getLong(0));
          assertTrue(
              output.get(index).isNullAt(1), "An unobserved indicator must remain a physical NULL");
          assertFalse(output.get(index).isNullAt(2));
          long observed = output.get(index).getTimestamp(2, 3).getMillisecond();
          assertTrue(
              observed >= earliest && observed <= System.currentTimeMillis(),
              "Materialization must observe the actual execution clock");
        }
        assertEquals(0, allocator.getAllocatedMemory());
      }
    }
  }
}
