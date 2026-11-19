package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Base64;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.calcite.rex.RexBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.arrow.TimestampAccessor;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchSerializer;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.NativeCalcOperator;
import tech.streamfusion.operator.NativeUdf;
import tech.streamfusion.operator.RowDataArrowConverter;

class GeneratedTemporalRowDispatchTest {
  @Test
  void representedResultsKeepTheCallbackThatPreservesTheirSchema() throws Exception {
    var loader = getClass().getClassLoader();
    var types = new FlinkTypeFactory(loader, FlinkTypeSystem.INSTANCE);
    var function =
        FlinkExpressionGenerator.generate(
            List.of(new RexBuilder(types).makeLiteral("cached")),
            null,
            new LogicalType[] {new TimestampType(9)},
            RowType.of(new VarCharType()),
            TableConfig.getDefault(),
            loader,
            true);
    int id =
        NativeUdf.register(
            function,
            FlinkExpressionFunction.class.getMethod("eval", Object[].class),
            new int[] {NativeUdf.TYPE_TIMESTAMP_DATA},
            NativeUdf.TYPE_ROW);
    try {
      var direct = NativeUdf.class.getDeclaredMethod("canEvaluateTemporalRowCalc", int.class);
      direct.setAccessible(true);
      assertFalse((boolean) direct.invoke(null, id));
    } finally {
      NativeUdf.unregister(id);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "false,0,false", "false,3,false", "false,6,false", "false,9,false",
    "true,0,false", "true,3,false", "true,6,false", "true,9,false",
    "false,0,true", "false,3,true", "false,6,true", "false,9,true",
    "true,0,true", "true,3,true", "true,6,true", "true,9,true"
  })
  void directTemporalOutputsSurviveInputReleaseAndAlternateWithChangelogs(
      boolean ltz, int precision, boolean foreignAllocator) throws Exception {
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    for (boolean duplicate : new boolean[] {false, true}) {
      var loader = getClass().getClassLoader();
      var types = new FlinkTypeFactory(loader, FlinkTypeSystem.INSTANCE);
      LogicalType logical =
          ltz ? new LocalZonedTimestampType(precision) : new TimestampType(precision);
      var ref =
          new RexBuilder(types).makeInputRef(types.createFieldTypeFromLogicalType(logical), 0);
      var rex = new RexBuilder(types);
      var identity =
          rex.makeCall(
              ref.getType(),
              org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE,
              List.of(
                  rex.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL, ref),
                  ref,
                  ref));
      var function =
          FlinkExpressionGenerator.generate(
              duplicate ? List.of(ref, ref) : List.of(identity),
              null,
              new LogicalType[] {logical},
              duplicate ? RowType.of(logical, logical) : RowType.of(logical),
              TableConfig.getDefault(),
              loader);
      var descriptor =
          NativeUdf.Descriptor.forFunction(
              function,
              FlinkExpressionFunction.class.getMethod("eval", Object[].class),
              new int[] {NativeUdf.TYPE_TIMESTAMP_DATA},
              NativeUdf.TYPE_ROW);
      var binding =
          new NativeUdf.Binding(
              new NativeUdf.Descriptor[] {descriptor}, new int[] {0}, new int[] {0});
      var calc =
          new NativeCalcOperator(
              new int[] {38, 0},
              new int[] {0, 0},
              new int[] {1, 0},
              new long[] {0, 0},
              new double[0],
              new String[] {
                Base64.getEncoder()
                    .encodeToString(function.arrowResultSchema().serializeAsMessage())
              },
              new int[] {0},
              -1,
              function.rowResultType().getFieldNames().toArray(new String[0]),
              binding);
      var counter = NativeCalcOperator.class.getDeclaredField("directRows");
      counter.setAccessible(true);
      var aliases = NativeCalcOperator.class.getDeclaredField("aliasedValues");
      aliases.setAccessible(true);
      try (org.apache.arrow.memory.BufferAllocator allocator =
              foreignAllocator
                  ? new RootAllocator()
                  : NativeAllocator.SHARED.newChildAllocator("temporal-input", 0, Long.MAX_VALUE);
          var harness = new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(calc)) {
        harness.setup(new ArrowBatchSerializer());
        harness.open();
        long directRows = 0;
        for (int batch = 0; batch < 4; batch++) {
          boolean changelog = batch % 2 == 1;
          var rows = new java.util.ArrayList<RowData>();
          for (int row = 0; row < 4; row++) {
            int unit = (int) Math.pow(10, 9 - precision);
            var timestamp = value(row + 6).toLocalDateTime();
            rows.add(
                GenericRowData.ofKind(
                    changelog ? RowKind.values()[row] : RowKind.INSERT,
                    row == 1
                        ? null
                        : TimestampData.fromLocalDateTime(
                            timestamp.withNano(timestamp.getNano() / unit * unit))));
          }
          int keyGroup = 7 + batch;
          harness.processElement(
              new StreamRecord<>(
                  new ArrowBatch(
                      RowDataArrowConverter.write(rows, RowType.of(logical), allocator, changelog),
                      keyGroup)));
          assertEquals(1, harness.getOutput().size());
          var output = (ArrowBatch) ((StreamRecord<?>) harness.getOutput().poll()).getValue();
          assertEquals(keyGroup, output.keyGroup());
          try (var root = output.root()) {
            var actual = RowDataArrowConverter.read(root, function.rowResultType());
            assertEquals(rows.size(), actual.size());
            for (int row = 0; row < rows.size(); row++) {
              assertEquals(rows.get(row).getRowKind(), actual.get(row).getRowKind());
              for (int column = 0; column < function.rowResultType().getFieldCount(); column++) {
                assertEquals(rows.get(row).isNullAt(0), actual.get(row).isNullAt(column));
                if (row != 1)
                  assertEquals(
                      rows.get(row).getTimestamp(0, precision),
                      actual.get(row).getTimestamp(column, precision));
              }
            }
          }
          assertEquals(0, allocator.getAllocatedMemory(), "consumed input is released");
          if (!changelog
              && Boolean.parseBoolean(
                  System.getProperty("streamfusion.experimental.directTemporalCalc", "true")))
            directRows += rows.size();
          assertEquals(
              directRows, ((org.apache.flink.metrics.Counter) counter.get(calc)).getCount());
        }
        if (!duplicate
            && !foreignAllocator
            && directRows > 0
            && Boolean.parseBoolean(
                System.getProperty("streamfusion.experimental.temporalAliases", "true")))
          assertTrue(((org.apache.flink.metrics.Counter) aliases.get(calc)).getCount() > 0);
        if (foreignAllocator)
          assertEquals(0, ((org.apache.flink.metrics.Counter) aliases.get(calc)).getCount());
      }
    }
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

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
        FlinkExpressionGenerator.generate(
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
