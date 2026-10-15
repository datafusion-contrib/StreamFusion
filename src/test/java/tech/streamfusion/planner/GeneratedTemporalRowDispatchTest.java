package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.calcite.rex.RexBuilder;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.arrow.TimestampAccessor;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.NativeUdf;

class GeneratedTemporalRowDispatchTest {
  @ParameterizedTest
  @CsvSource({"false,0", "false,3", "false,6", "false,9", "true,0", "true,3", "true,6", "true,9"})
  void slicedNullableTemporalRowsOwnTheirOutputAfterInputRelease(boolean ltz, int precision)
      throws Exception {
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    var loader = getClass().getClassLoader();
    var types = new FlinkTypeFactory(loader, FlinkTypeSystem.INSTANCE);
    LogicalType logical =
        ltz ? new LocalZonedTimestampType(precision) : new TimestampType(precision);
    var ref = new RexBuilder(types).makeInputRef(types.createFieldTypeFromLogicalType(logical), 0);
    var function =
        new FlinkExpressionFunction(
            List.of(ref, ref),
            null,
            new LogicalType[] {logical},
            RowType.of(logical, logical),
            TableConfig.getDefault(),
            loader);
    function.open(
        new FunctionContext(null) {
          @Override
          public ClassLoader getUserCodeClassLoader() {
            return loader;
          }
        });
    int id =
        NativeUdf.register(
            function,
            FlinkExpressionFunction.class.getMethod("eval", Object[].class),
            new int[] {NativeUdf.TYPE_TIMESTAMP_DATA},
            NativeUdf.TYPE_ROW);
    try {
      VectorSchemaRoot output;
      try (var input = input(10);
          var slice = input.slice(6, 3)) {
        output = GeneratedExpressionDispatchTest.invoke(id, slice);
      }
      try (output) {
        assertEquals(3, output.getRowCount());
        var row = (StructVector) output.getVector(0);
        var timestamps = new TimestampAccessor(row.getChildrenFromFields().get(0));
        assertEquals(value(6), timestamps.getTimestamp(0));
        assertEquals(true, timestamps.isNull(1));
        assertEquals(value(8), timestamps.getTimestamp(2));
      }
      try (var empty = input(0);
          var result = GeneratedExpressionDispatchTest.invoke(id, empty)) {
        assertEquals(0, result.getRowCount());
      }
    } finally {
      NativeUdf.unregister(id);
    }
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

  private static TimestampData value(int row) {
    return TimestampData.fromEpochMillis(
        row == 6 ? -62135596800000L : 253402300799999L, row * 12345);
  }

  private static VectorSchemaRoot input(int rows) {
    var vector = TimestampAccessor.field("t", true).createVector(NativeAllocator.SHARED);
    vector.allocateNew();
    var values = new TimestampAccessor(vector);
    for (int row = 0; row < rows; row++) values.set(row, row % 7 == 0 ? null : value(row));
    vector.setValueCount(rows);
    var root = VectorSchemaRoot.of(vector);
    root.setRowCount(rows);
    return root;
  }
}
