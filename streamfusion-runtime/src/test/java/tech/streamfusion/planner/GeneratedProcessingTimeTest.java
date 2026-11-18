package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class GeneratedProcessingTimeTest {
  private static final ClassLoader LOADER = GeneratedProcessingTimeTest.class.getClassLoader();

  private static FunctionContext context() {
    return new FunctionContext(null) {
      @Override
      public ClassLoader getUserCodeClassLoader() {
        return LOADER;
      }
    };
  }

  static RexNode selection(int precision) {
    var types = new FlinkTypeFactory(LOADER, FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    var type = types.createFieldTypeFromLogicalType(new LocalZonedTimestampType(precision));
    var input = rex.makeInputRef(type, 0);
    var proctime =
        rex.makeCall(
            types.createProctimeIndicatorType(true), FlinkSqlOperatorTable.PROCTIME, List.of());
    var materialized =
        rex.makeCall(
            types.createFieldTypeFromLogicalType(new LocalZonedTimestampType(false, 3)),
            FlinkSqlOperatorTable.PROCTIME_MATERIALIZE,
            List.of(proctime));
    RexNode clock = rex.makeCast(type, materialized);
    return rex.makeCall(
        type,
        SqlStdOperatorTable.CASE,
        List.of(rex.makeCall(SqlStdOperatorTable.IS_NOT_NULL, input), input, clock));
  }

  static FlinkExpressionFunction scalar(int precision) {
    return FlinkExpressionGenerator.generate(
        selection(precision),
        new LogicalType[] {new LocalZonedTimestampType(precision)},
        TableConfig.getDefault(),
        LOADER);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 3, 6, 9})
  void scalarCaseReadsTheLiveClockAndKeepsInputPrecision(int precision) throws Exception {
    {
      var function = scalar(precision);
      var clock = new TestProcessingTimeService();
      clock.setCurrentTime(1234567891234L);
      function.processingTimeService(clock);
      function.open(context());
      assertThrows(
          IllegalStateException.class,
          () -> function.processingTimeService(new TestProcessingTimeService()));
      try {
        int nanos = precision == 9 ? 234567 : precision == 6 ? 234000 : 0;
        var timestamp = TimestampData.fromEpochMillis(1122000L, nanos);
        assertEquals(timestamp, function.eval(timestamp));
        assertEquals(
            TimestampData.fromEpochMillis(precision == 0 ? 1234567891000L : 1234567891234L),
            function.eval(new Object[] {null}));
        clock.setCurrentTime(1234567892345L);
        assertEquals(
            TimestampData.fromEpochMillis(precision == 0 ? 1234567892000L : 1234567892345L),
            function.evalColumns(new Object[][] {{null}}, 0));
      } finally {
        function.close();
      }
    }
  }

  @Test
  void wholeRowFilterAndProjectionUseTheSameLiveService() throws Exception {
    var types = new FlinkTypeFactory(LOADER, FlinkTypeSystem.INSTANCE);
    var rex = new RexBuilder(types);
    var selected = selection(3);
    var condition = rex.makeCall(SqlStdOperatorTable.IS_NOT_NULL, selected);
    var schema = RowType.of(new LocalZonedTimestampType(3));
    var function =
        FlinkExpressionGenerator.generate(
            List.of(selected),
            condition,
            new LogicalType[] {new LocalZonedTimestampType(3)},
            schema,
            TableConfig.getDefault(),
            LOADER);
    var clock = new TestProcessingTimeService();
    clock.setCurrentTime(1234L);
    function.processingTimeService(clock);
    function.open(context());
    try {
      RowData row = (RowData) function.eval(new Object[] {null});
      assertEquals(TimestampData.fromEpochMillis(1234L), row.getTimestamp(0, 3));
      clock.setCurrentTime(2345L);
      row = (RowData) function.eval(new Object[] {null});
      assertEquals(TimestampData.fromEpochMillis(2345L), row.getTimestamp(0, 3));
      var fixed = TimestampData.fromEpochMillis(-1234L);
      row = (RowData) function.eval(fixed);
      assertEquals(fixed, row.getTimestamp(0, 3));
    } finally {
      function.close();
    }
  }

  @Test
  void serializationAndReopenNeverRetainThePreviousTaskClock() throws Exception {
    var function = scalar(3);
    var first = new TestProcessingTimeService();
    first.setCurrentTime(1234L);
    function.processingTimeService(first);
    function.open(context());
    assertEquals(TimestampData.fromEpochMillis(1234L), function.eval(new Object[] {null}));
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (var output = new ObjectOutputStream(bytes)) {
      output.writeObject(function);
    }
    function.close();
    FlinkExpressionFunction restored;
    try (var input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      restored = (FlinkExpressionFunction) input.readObject();
    }
    for (var candidate : List.of(function, restored)) {
      candidate.open(context());
      try {
        var fixed = TimestampData.fromEpochMillis(10L);
        assertEquals(fixed, candidate.eval(fixed));
        assertThrows(IllegalStateException.class, () -> candidate.eval(new Object[] {null}));
      } finally {
        candidate.close();
      }
      var second = new TestProcessingTimeService();
      second.setCurrentTime(5678L);
      candidate.processingTimeService(second);
      candidate.open(context());
      try {
        assertEquals(TimestampData.fromEpochMillis(5678L), candidate.eval(new Object[] {null}));
      } finally {
        candidate.close();
      }
    }
  }
}
