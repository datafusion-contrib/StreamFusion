package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.over.ProcTimeRowsBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.ProcTimeUnboundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRangeBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRangeUnboundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRowsBoundedPrecedingFunction;
import org.apache.flink.table.runtime.operators.over.RowTimeRowsUnboundedPrecedingFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkNativeTypedOverOperatorTest {
  @BeforeAll
  static void memoryAuthority() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  static Stream<Arguments> typesAndFrames() {
    List<Arguments> cases = new ArrayList<>();
    for (String type : List.of("decimal", "fractional", "timestamp", "ltz", "string")) {
      for (int frame = 0; frame < 4; frame++) {
        for (boolean proctime : new boolean[] {false, true}) {
          if (proctime && frame == 2) continue;
          cases.add(Arguments.of(type, frame, proctime));
        }
      }
    }
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("typesAndFrames")
  void releasedOverFunctionsMatchNativeTypedNullsPeersAndEviction(
      String type, int frame, boolean proctime) throws Exception {
    boolean decimal = type.equals("decimal") || type.equals("fractional");
    boolean string = type.equals("string");
    int scale = type.equals("fractional") ? 38 : 2;
    LogicalType valueType =
        string
            ? new VarCharType(VarCharType.MAX_LENGTH)
            : decimal
                ? new DecimalType(38, scale)
                : type.equals("ltz") ? new LocalZonedTimestampType(9) : new TimestampType(9);
    var input = RowType.of(valueType, new BigIntType());
    boolean firstLast = true;
    if (!decimal
        && !string
        && org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.18")) {
      var failure =
          assertThrows(
              org.apache.flink.table.api.TableException.class,
              () -> stock(valueType, input, frame, proctime, true));
      String function = frame == 1 || frame == 2 ? "FIRST_VALUE with retract" : "FIRST_VALUE";
      assertTrue(
          failure.getMessage().contains(function + " aggregate function does not support type"),
          failure.getMessage());
      firstLast = false;
    }
    var output =
        firstLast
            ? RowType.of(valueType, new BigIntType(), valueType, valueType, valueType, valueType)
            : RowType.of(valueType, new BigIntType(), valueType, valueType);
    Object middle =
        string ? binary("😀") : decimal ? dec(250, scale) : TimestampData.fromEpochMillis(0, 2);
    Object earliest =
        string
            ? binary("\ue000")
            : decimal ? dec(-100, scale) : TimestampData.fromEpochMillis(-62135596800000L, 1);
    Object latest =
        string
            ? binary("")
            : decimal ? dec(125, scale) : TimestampData.fromEpochMillis(253402300799999L, 999999);
    Object[] values = {null, middle, earliest, earliest, null, latest};
    long[] times = {100, 200, 300, 300, 400, 500};
    int code = string ? 3 : decimal ? 5800 + scale : 1009;
    try (var allocator = new RootAllocator();
        var stock = stock(valueType, input, frame, proctime, firstLast);
        var nativeHarness = nativeHarness(input, code, frame, proctime, firstLast)) {
      stock.setup(new RowDataSerializer(output));
      stock.open();
      nativeHarness.setup(new ArrowBatchSerializer());
      nativeHarness.open();
      for (int start = 0; start < values.length; ) {
        int end = start + 1;
        while (end < values.length && times[end] == times[start]) end++;
        List<RowData> rows = new ArrayList<>();
        for (int i = start; i < end; i++) {
          var row = GenericRowData.of(values[i], times[i]);
          stock.processElement(new StreamRecord<>(row));
          rows.add(row);
        }
        nativeHarness.processElement(
            new StreamRecord<>(
                new ArrowBatch(RowDataArrowConverter.write(rows, input, allocator))));
        if (!proctime) {
          stock.processWatermark(new Watermark(times[start]));
          nativeHarness.processWatermark(new Watermark(times[start]));
        }
        assertEquals(
            drainStock(stock, decimal, string, scale),
            drainNative(nativeHarness, output, decimal, string, scale),
            "type="
                + type
                + " frame="
                + frame
                + " proctime="
                + proctime
                + " timestamp="
                + times[start]);
        start = end;
      }
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void cachedStringOrderChangesWhenOverStateIsSerialized(boolean rocks) throws Exception {
    var valueType = new VarCharType(VarCharType.MAX_LENGTH);
    var input = RowType.of(valueType, new BigIntType());
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState snapshot;
    try (var uninterrupted = stock(valueType, input, 0, true, false)) {
      if (rocks) uninterrupted.setStateBackend(stockRocksBackend());
      uninterrupted.setup(
          new RowDataSerializer(RowType.of(valueType, new BigIntType(), valueType, valueType)));
      uninterrupted.open();
      uninterrupted.processElement(
          new StreamRecord<>(GenericRowData.of(BinaryStringData.fromString("😀"), 100L)));
      drainStock(uninterrupted, false, true, 0);
      snapshot = uninterrupted.snapshot(1, 1);
      uninterrupted.processElement(
          new StreamRecord<>(GenericRowData.of(BinaryStringData.fromString("\ue000"), 200L)));
      assertEquals(
          List.of(rocks ? List.of("\ue000", "😀") : List.of("😀", "\ue000")),
          drainStock(uninterrupted, false, true, 0));
    }
    try (var recovered = stock(valueType, input, 0, true, false)) {
      if (rocks) recovered.setStateBackend(stockRocksBackend());
      recovered.setup(
          new RowDataSerializer(RowType.of(valueType, new BigIntType(), valueType, valueType)));
      recovered.initializeState(snapshot);
      recovered.open();
      recovered.processElement(
          new StreamRecord<>(GenericRowData.of(BinaryStringData.fromString("\ue000"), 200L)));
      assertEquals(List.of(List.of("\ue000", "😀")), drainStock(recovered, false, true, 0));
    }
  }

  @org.junit.jupiter.api.Test
  void cachedAndBinaryStringComparisonIsNotTransitive() {
    var emoji = BinaryStringData.fromString("😀");
    var privateUse = BinaryStringData.fromString("\ue000");
    var binaryPrivateUse = binary("\ue100");
    assertTrue(emoji.compareTo(privateUse) < 0);
    assertTrue(privateUse.compareTo(binaryPrivateUse) < 0);
    assertTrue(binaryPrivateUse.compareTo(emoji) < 0);
  }

  @org.junit.jupiter.api.Test
  void utf8BytesAndACacheFlagCannotReconstructMalformedJavaStrings() {
    var first = BinaryStringData.fromString("\ud800");
    var second = BinaryStringData.fromString("\ud801");
    assertTrue(first.compareTo(second) < 0);
    org.junit.jupiter.api.Assertions.assertArrayEquals(first.toBytes(), second.toBytes());
    var binary = BinaryStringData.fromBytes(first.toBytes());
    assertFalse(first.equals(second));
    assertTrue(first.equals(binary));
    assertTrue(binary.equals(second));
    assertEquals(
        0,
        BinaryStringData.fromBytes(first.toBytes())
            .compareTo(BinaryStringData.fromBytes(second.toBytes())));
  }

  static Stream<Arguments> cachedStringFrames() {
    var cases = new ArrayList<Arguments>();
    for (int frame = 0; frame < 4; frame++) {
      for (boolean proctime : new boolean[] {false, true}) {
        if (proctime && frame == 2) continue;
        for (boolean rocks : new boolean[] {false, true})
          cases.add(Arguments.of(frame, proctime, rocks));
      }
    }
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("cachedStringFrames")
  void mixedStringCachesHaveFrameAndRecoverySpecificOrdering(
      int frame, boolean proctime, boolean rocks) throws Exception {
    var type = new VarCharType(VarCharType.MAX_LENGTH);
    var input = RowType.of(type, new BigIntType());
    var output = RowType.of(type, new BigIntType(), type, type, type, type);
    var uninterrupted = new ArrayList<List<List<Object>>>();
    var recoveredOutput = new ArrayList<List<List<Object>>>();
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState snapshot;
    try (var harness = stock(type, input, frame, proctime, true)) {
      if (rocks) harness.setStateBackend(stockRocksBackend());
      harness.setup(new RowDataSerializer(output));
      harness.open();
      for (int step = 0; step < 2; step++)
        uninterrupted.add(stringStep(harness, frame, proctime, step));
      snapshot = harness.snapshot(1, 1);
      for (int step = 2; step < 7; step++)
        uninterrupted.add(stringStep(harness, frame, proctime, step));
    }
    try (var harness = stock(type, input, frame, proctime, true)) {
      if (rocks) harness.setStateBackend(stockRocksBackend());
      harness.setup(new RowDataSerializer(output));
      harness.initializeState(snapshot);
      harness.open();
      for (int step = 2; step < 7; step++)
        recoveredOutput.add(stringStep(harness, frame, proctime, step));
    }
    var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
    try (var resource =
        FlinkNativeTypedOverOperatorTest.class.getResourceAsStream(
            "/over-string-cache-oracle.json")) {
      var expected = mapper.readTree(java.util.Objects.requireNonNull(resource));
      assertEquals(
          expected.get(frame + ":" + proctime + ":" + rocks),
          mapper.valueToTree(List.of(uninterrupted, recoveredOutput)));
    }
  }

  static Stream<Arguments> runningCachedStringFrames() {
    return cachedStringFrames()
        .filter(arguments -> (int) arguments.get()[0] == 0 || (int) arguments.get()[0] == 3);
  }

  @ParameterizedTest
  @MethodSource("runningCachedStringFrames")
  void nativeRunningStringsCarryOriginalCachesThroughBothBoundaries(
      int frame, boolean proctime, boolean rocks) throws Exception {
    var type = new VarCharType(VarCharType.MAX_LENGTH);
    var input = RowType.of(type, new BigIntType());
    var output = RowType.of(type, new BigIntType(), type, type, type, type);
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState stockSnapshot, nativeSnapshot;
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, frame, proctime, true);
        var nativeOperator = nativeHarness(input, 3, frame, proctime, true)) {
      if (rocks) {
        stock.setStateBackend(stockRocksBackend());
        nativeOperator.setStateBackend(
            new tech.streamfusion.state.RocksDBNativeStateBackendFactory()
                .createFromConfig(new Configuration(), getClass().getClassLoader()));
      }
      stock.setup(new RowDataSerializer(output));
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.open();
      for (int step = 0; step < 2; step++) {
        var expected = stringStep(stock, frame, proctime, step);
        var row = stringRow(step);
        if (proctime) nativeOperator.setProcessingTime(row.getLong(1));
        nativeOperator.processElement(
            new StreamRecord<>(
                new ArrowBatch(
                    RowDataArrowConverter.write(List.of(row), input, allocator, false, true))));
        if (!proctime) nativeOperator.processWatermark(new Watermark(row.getLong(1)));
        assertEquals(expected, drainNative(nativeOperator, output, false, true, 0));
      }
      stockSnapshot = stock.snapshot(1, 1);
      nativeSnapshot = nativeOperator.snapshot(1, 1);
      for (int step = 2; step < 7; step++) {
        var expected = stringStep(stock, frame, proctime, step);
        var row = stringRow(step);
        if (proctime) nativeOperator.setProcessingTime(row.getLong(1));
        nativeOperator.processElement(
            new StreamRecord<>(
                new ArrowBatch(
                    RowDataArrowConverter.write(List.of(row), input, allocator, false, true))));
        if (!proctime) nativeOperator.processWatermark(new Watermark(row.getLong(1)));
        assertEquals(expected, drainNative(nativeOperator, output, false, true, 0));
      }
    }
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, frame, proctime, true);
        var nativeOperator = nativeHarness(input, 3, frame, proctime, true)) {
      if (rocks) {
        stock.setStateBackend(stockRocksBackend());
        nativeOperator.setStateBackend(
            new tech.streamfusion.state.RocksDBNativeStateBackendFactory()
                .createFromConfig(new Configuration(), getClass().getClassLoader()));
      }
      stock.setup(new RowDataSerializer(output));
      stock.initializeState(stockSnapshot);
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.initializeState(nativeSnapshot);
      nativeOperator.open();
      for (int step = 2; step < 7; step++) {
        var expected = stringStep(stock, frame, proctime, step);
        var row = stringRow(step);
        if (proctime) nativeOperator.setProcessingTime(row.getLong(1));
        nativeOperator.processElement(
            new StreamRecord<>(
                new ArrowBatch(
                    RowDataArrowConverter.write(List.of(row), input, allocator, false, true))));
        if (!proctime) nativeOperator.processWatermark(new Watermark(row.getLong(1)));
        assertEquals(expected, drainNative(nativeOperator, output, false, true, 0));
      }
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void batchedProctimeRocksStringsSerializeBetweenRowsAndRetainOriginalOutputUnits(boolean rocks)
      throws Exception {
    var type = new VarCharType(VarCharType.MAX_LENGTH);
    var input = RowType.of(type, new BigIntType());
    var output = RowType.of(type, new BigIntType(), type, type, type, type);
    var rows =
        List.<RowData>of(
            GenericRowData.of(BinaryStringData.fromString("\ud800"), 100L),
            GenericRowData.of(BinaryStringData.fromString("\ud801"), 200L),
            GenericRowData.of(BinaryStringData.fromString("😀"), 300L),
            GenericRowData.of(BinaryStringData.fromString("\ue000"), 400L));
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, 0, true, true);
        var nativeOperator = nativeHarness(input, 3, 0, true, true)) {
      if (rocks) {
        stock.setStateBackend(stockRocksBackend());
        nativeOperator.setStateBackend(
            new tech.streamfusion.state.RocksDBNativeStateBackendFactory()
                .createFromConfig(new Configuration(), getClass().getClassLoader()));
      }
      stock.setup(new RowDataSerializer(output));
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.open();
      for (var row : rows) stock.processElement(new StreamRecord<>(row));
      nativeOperator.processElement(
          new StreamRecord<>(
              new ArrowBatch(RowDataArrowConverter.write(rows, input, allocator, false, true))));
      var expected = new ArrayList<List<Object>>();
      for (var record : stock.extractOutputStreamRecords())
        expected.add(stringRepresentations(record.getValue()));
      stock.getOutput().clear();
      var actual = new ArrayList<List<Object>>();
      for (var record : nativeOperator.extractOutputStreamRecords()) {
        try (var root = record.getValue().root()) {
          for (var row : RowDataArrowConverter.read(root, output))
            actual.add(stringRepresentations(row));
        }
      }
      nativeOperator.getOutput().clear();
      assertEquals(expected, actual);
    }
  }

  @org.junit.jupiter.api.Test
  void pendingHeapStringsBecomeBinaryOnCheckpointRecovery() throws Exception {
    var type = new VarCharType(VarCharType.MAX_LENGTH);
    var input = RowType.of(type, new BigIntType());
    var output = RowType.of(type, new BigIntType(), type, type, type, type);
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState stockSnapshot, nativeSnapshot;
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, 0, false, true);
        var nativeOperator = nativeHarness(input, 3, 0, false, true)) {
      stock.setup(new RowDataSerializer(output));
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.open();
      var row = GenericRowData.of(BinaryStringData.fromString("\ud800"), 100L);
      stock.processElement(new StreamRecord<>(row));
      nativeOperator.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(List.of(row), input, allocator, false, true))));
      stockSnapshot = stock.snapshot(1, 1);
      nativeSnapshot = nativeOperator.snapshot(1, 1);
    }
    try (var stock = stock(type, input, 0, false, true);
        var nativeOperator = nativeHarness(input, 3, 0, false, true)) {
      stock.setup(new RowDataSerializer(output));
      stock.initializeState(stockSnapshot);
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.initializeState(nativeSnapshot);
      nativeOperator.open();
      stock.processWatermark(new Watermark(100));
      nativeOperator.processWatermark(new Watermark(100));
      var expected =
          stock.extractOutputStreamRecords().stream()
              .map(record -> stringRepresentations(record.getValue()))
              .collect(java.util.stream.Collectors.toList());
      stock.getOutput().clear();
      var actual = new ArrayList<List<Object>>();
      for (var record : nativeOperator.extractOutputStreamRecords()) {
        try (var root = record.getValue().root()) {
          for (var row : RowDataArrowConverter.read(root, output))
            actual.add(stringRepresentations(row));
        }
      }
      nativeOperator.getOutput().clear();
      assertEquals(expected, actual);
    }
  }

  static Stream<Arguments> boundedCachedFirstLastCases() {
    var cases = new ArrayList<Arguments>();
    for (int frame : new int[] {1, 2}) {
      for (boolean proctime : new boolean[] {false, true}) {
        if (proctime && frame == 2) continue;
        for (boolean writer : new boolean[] {false, true}) {
          for (boolean reader : new boolean[] {false, true}) {
            for (boolean malformed : new boolean[] {false, true})
              cases.add(Arguments.of(frame, proctime, writer, reader, malformed));
          }
        }
      }
    }
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("boundedCachedFirstLastCases")
  void boundedFirstLastPreserveCachesThroughEvictionAndNormalRestore(
      int frame, boolean proctime, boolean writer, boolean reader, boolean malformed)
      throws Exception {
    boundedStringRestore(frame, proctime, writer, reader, malformed, false);
  }

  @ParameterizedTest
  @MethodSource("boundedCachedFirstLastCases")
  void boundedExtremaPreserveCachesThroughEvictionAndNormalRestore(
      int frame, boolean proctime, boolean writer, boolean reader, boolean malformed)
      throws Exception {
    boundedStringRestore(frame, proctime, writer, reader, malformed, true);
  }

  private static void boundedStringRestore(
      int frame,
      boolean proctime,
      boolean writer,
      boolean reader,
      boolean malformed,
      boolean extrema)
      throws Exception {
    var type = new VarCharType(VarCharType.MAX_LENGTH);
    var input = RowType.of(type, new BigIntType());
    var output =
        extrema
            ? RowType.of(type, new BigIntType(), type, type, type, type)
            : RowType.of(type, new BigIntType(), type, type);
    var rows = boundedStringRows(malformed);
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState stockSnapshot, nativeSnapshot;
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, frame, proctime, true, !extrema);
        var nativeOperator = nativeCachedReductions(input, frame, proctime, writer, extrema)) {
      if (writer) stock.setStateBackend(stockRocksBackend());
      stock.setup(new RowDataSerializer(output));
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.open();
      for (int step = 0; step < 2; step++)
        firstLastStep(stock, nativeOperator, allocator, input, output, proctime, rows.get(step));
      stockSnapshot =
          writer == reader
              ? stock.snapshot(1, 1)
              : stock
                  .snapshotWithLocalState(
                      1,
                      1,
                      org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                          org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                  .getJobManagerOwnedState();
      nativeSnapshot =
          writer == reader
              ? nativeOperator.snapshot(1, 1)
              : nativeOperator
                  .snapshotWithLocalState(
                      1,
                      1,
                      org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                          org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                  .getJobManagerOwnedState();
      for (int step = 2; step < rows.size(); step++)
        firstLastStep(stock, nativeOperator, allocator, input, output, proctime, rows.get(step));
    }
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, frame, proctime, true, !extrema);
        var nativeOperator = nativeCachedReductions(input, frame, proctime, reader, extrema)) {
      if (reader) stock.setStateBackend(stockRocksBackend());
      stock.setup(new RowDataSerializer(output));
      stock.initializeState(stockSnapshot);
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.initializeState(nativeSnapshot);
      nativeOperator.open();
      for (int step = 2; step < rows.size(); step++)
        firstLastStep(stock, nativeOperator, allocator, input, output, proctime, rows.get(step));
    }
  }

  static Stream<Arguments> stringRangeOrderCases() {
    var cases = new ArrayList<Arguments>();
    for (boolean writer : new boolean[] {false, true}) {
      for (boolean reader : new boolean[] {false, true}) {
        for (int width : new int[] {1, 256}) cases.add(Arguments.of(writer, reader, width));
      }
    }
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("stringRangeOrderCases")
  void cachedExtremaRetractTimestampGroupsInBackendOrder(boolean writer, boolean reader, int width)
      throws Exception {
    var type = new VarCharType(VarCharType.MAX_LENGTH);
    var input = RowType.of(type, new BigIntType());
    var output = RowType.of(type, new BigIntType(), type, type);
    var rows =
        List.<RowData>of(
            GenericRowData.of(BinaryStringData.fromString("😀".repeat(width)), 100L),
            GenericRowData.of(BinaryStringData.fromString("\ue000".repeat(width)), 200L),
            GenericRowData.of(BinaryStringData.fromString("a".repeat(width)), 400L),
            GenericRowData.of(BinaryStringData.fromString("😀".repeat(width)), 1050L),
            GenericRowData.of(BinaryStringData.fromString("😀".repeat(width)), 1060L),
            GenericRowData.of(binary("\ue100".repeat(width)), 1070L),
            GenericRowData.of(null, 1500L));
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState stockSnapshot, nativeSnapshot;
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, 2, false, false, false, 1000);
        var nativeOperator = nativeRangeExtrema(input, writer)) {
      if (writer) stock.setStateBackend(stockRocksBackend());
      stock.setup(new RowDataSerializer(output));
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.open();
      for (int step = 0; step < 6; step++)
        firstLastStep(stock, nativeOperator, allocator, input, output, false, rows.get(step));
      stockSnapshot =
          writer == reader
              ? stock.snapshot(1, 1)
              : stock
                  .snapshotWithLocalState(
                      1,
                      1,
                      org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                          org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                  .getJobManagerOwnedState();
      nativeSnapshot =
          writer == reader
              ? nativeOperator.snapshot(1, 1)
              : nativeOperator
                  .snapshotWithLocalState(
                      1,
                      1,
                      org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                          org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
                  .getJobManagerOwnedState();
      firstLastStep(stock, nativeOperator, allocator, input, output, false, rows.get(6));
    }
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, 2, false, false, false, 1000);
        var nativeOperator = nativeRangeExtrema(input, reader)) {
      if (reader) stock.setStateBackend(stockRocksBackend());
      stock.setup(new RowDataSerializer(output));
      stock.initializeState(stockSnapshot);
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.initializeState(nativeSnapshot);
      nativeOperator.open();
      firstLastStep(stock, nativeOperator, allocator, input, output, false, rows.get(6));
    }
  }

  static Stream<Arguments> stringTreeHistories() {
    var cases = new ArrayList<Arguments>();
    for (boolean proctime : new boolean[] {false, true})
      for (int mode = 0; mode < 8; mode++)
        for (int seed = 0; seed < 8; seed++)
          if (mode != 7 || !proctime) cases.add(Arguments.of(proctime, mode, seed));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("stringTreeHistories")
  void largeCachedStringCollisionFramesMatchRetractionAndRestore(
      boolean proctime, int mode, int seed) throws Exception {
    var type = new VarCharType(VarCharType.MAX_LENGTH);
    var input = RowType.of(type, new BigIntType());
    var output = RowType.of(type, new BigIntType(), type, type, type, type);
    var collisionValues = new ArrayList<String>();
    for (int candidate = seed * 1000; collisionValues.size() < 24; candidate++) {
      String value = new String[] {"😀", "\ue000", "\ue100"}[candidate % 3] + candidate;
      int hash = BinaryStringData.fromString(value).hashCode();
      if (((hash ^ (hash >>> 16)) & 63) == (7 + seed)) collisionValues.add(value);
    }
    var rows = new ArrayList<RowData>();
    for (int i = 0; i < 80; i++) {
      String value = i == 0 ? "a" : i <= 24 ? collisionValues.get(i - 1) : null;
      var text =
          value == null
              ? null
              : value.startsWith("\ue100") ? binary(value) : BinaryStringData.fromString(value);
      rows.add(GenericRowData.of(text, 100L + i));
    }
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState stockSnapshot, nativeSnapshot;
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, 1, proctime, true, false, 100, 32);
        var nativeOperator = largeStringHarness(input, proctime)) {
      stock.setup(new RowDataSerializer(output));
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.open();
      int cutoff = mode >= 3 ? 25 : 40;
      for (int i = 0; i < cutoff; i++)
        firstLastStep(stock, nativeOperator, allocator, input, output, proctime, rows.get(i));
      if (mode >= 5) {
        var backend =
            (org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<?>)
                stock.getOperator().getKeyedStateBackend();
        var nativeBackend =
            (org.apache.flink.runtime.state.CheckpointableKeyedStateBackend<?>)
                nativeOperator.getOperator().getKeyedStateBackend();
        var options =
            org.apache.flink.runtime.checkpoint.CheckpointOptions
                .forCheckpointWithDefaultLocation();
        var future =
            backend.snapshot(
                1,
                1,
                new org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory(1 << 20),
                options);
        var nativeFuture =
            nativeBackend.snapshot(
                1,
                1,
                new org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory(1 << 20),
                options);
        if (mode == 6) {
          future.run();
          future.get().discardState();
          nativeFuture.run();
          nativeFuture.get().discardState();
        }
        int next = cutoff;
        if (mode == 7) {
          RowData pending = rows.get(next++);
          stock.processElement(new StreamRecord<>(pending));
          nativeOperator.processElement(
              new StreamRecord<>(
                  new ArrowBatch(
                      RowDataArrowConverter.write(
                          List.of(pending), input, allocator, false, true))));
          future.run();
          future.get().discardState();
          nativeFuture.run();
          nativeFuture.get().discardState();
          stock.processWatermark(new Watermark(pending.getLong(1)));
          nativeOperator.processWatermark(new Watermark(pending.getLong(1)));
          compareStringOutputs(stock, nativeOperator, output, pending);
        }
        try {
          for (int i = next; i < rows.size(); i++)
            firstLastStep(stock, nativeOperator, allocator, input, output, proctime, rows.get(i));
        } finally {
          if (mode == 5) {
            future.run();
            future.get().discardState();
            nativeFuture.run();
            nativeFuture.get().discardState();
          }
        }
        return;
      }
      var canonical =
          org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
              org.apache.flink.core.execution.SavepointFormatType.CANONICAL);
      stockSnapshot =
          mode == 0
              ? null
              : mode == 4
                  ? stock.snapshotWithLocalState(1, 1, canonical).getJobManagerOwnedState()
                  : stock.snapshot(1, 1);
      nativeSnapshot =
          mode == 0
              ? null
              : mode == 4
                  ? nativeOperator.snapshotWithLocalState(1, 1, canonical).getJobManagerOwnedState()
                  : nativeOperator.snapshot(1, 1);
      if (mode < 2) {
        for (int i = cutoff; i < rows.size(); i++)
          firstLastStep(stock, nativeOperator, allocator, input, output, proctime, rows.get(i));
        return;
      }
    }
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, 1, proctime, true, false, 100, 32);
        var nativeOperator = largeStringHarness(input, proctime)) {
      stock.setup(new RowDataSerializer(output));
      stock.initializeState(stockSnapshot);
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.initializeState(nativeSnapshot);
      nativeOperator.open();
      for (int i = mode >= 3 ? 25 : 40; i < rows.size(); i++)
        firstLastStep(stock, nativeOperator, allocator, input, output, proctime, rows.get(i));
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "false,false",
    "false,true",
    "true,false",
    "true,true"
  })
  void smallCachedValueMapsPreserveNormalAndCanonicalRestoreOrder(
      boolean proctime, boolean canonical) throws Exception {
    var type = new VarCharType(VarCharType.MAX_LENGTH);
    var input = RowType.of(type, new BigIntType());
    var output = RowType.of(type, new BigIntType(), type, type, type, type);
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState stockSnapshot, nativeSnapshot;
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, 1, proctime, true, false, 100, 4);
        var nativeOperator = stringHarness(input, proctime, 4)) {
      stock.setup(new RowDataSerializer(output));
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.open();
      firstLastStep(
          stock,
          nativeOperator,
          allocator,
          input,
          output,
          proctime,
          GenericRowData.of(BinaryStringData.fromString("a"), 100L));
      firstLastStep(
          stock,
          nativeOperator,
          allocator,
          input,
          output,
          proctime,
          GenericRowData.of(binary("\ue100"), 200L));
      var savepoint =
          org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
              org.apache.flink.core.execution.SavepointFormatType.CANONICAL);
      stockSnapshot =
          canonical
              ? stock.snapshotWithLocalState(1, 1, savepoint).getJobManagerOwnedState()
              : stock.snapshot(1, 1);
      nativeSnapshot =
          canonical
              ? nativeOperator.snapshotWithLocalState(1, 1, savepoint).getJobManagerOwnedState()
              : nativeOperator.snapshot(1, 1);
    }
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, 1, proctime, true, false, 100, 4);
        var nativeOperator = stringHarness(input, proctime, 4)) {
      stock.setup(new RowDataSerializer(output));
      stock.initializeState(stockSnapshot);
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.initializeState(nativeSnapshot);
      nativeOperator.open();
      for (var row :
          List.<RowData>of(
              GenericRowData.of(BinaryStringData.fromString("😀"), 300L),
              GenericRowData.of(BinaryStringData.fromString("\ue000"), 400L),
              GenericRowData.of(null, 500L),
              GenericRowData.of(null, 600L),
              GenericRowData.of(null, 700L),
              GenericRowData.of(null, 800L)))
        firstLastStep(stock, nativeOperator, allocator, input, output, proctime, row);
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      largeStringHarness(RowType input, boolean proctime) throws Exception {
    return stringHarness(input, proctime, 32);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      stringHarness(RowType input, boolean proctime, int precedingRows) throws Exception {
    var operator =
        new NativeOverAggregateOperator(
            1,
            new int[] {0, 0, 0, 0},
            new int[0],
            new int[] {3, 3, 3, 3},
            new int[] {1, 2, 5, 6},
            1,
            precedingRows,
            proctime,
            new int[0],
            0,
            input,
            128);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, ignored -> 0, Types.INT, 128, 1, 0);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      nativeRangeExtrema(RowType input, boolean rocks) throws Exception {
    var harness = nativeHarness(input, 3, 2, false, false, 1000);
    if (rocks)
      harness.setStateBackend(
          new tech.streamfusion.state.RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), FlinkNativeTypedOverOperatorTest.class.getClassLoader()));
    return harness;
  }

  private static void firstLastStep(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeOperator,
      RootAllocator allocator,
      RowType input,
      RowType output,
      boolean proctime,
      RowData row)
      throws Exception {
    if (proctime) {
      stock.setProcessingTime(row.getLong(1));
      nativeOperator.setProcessingTime(row.getLong(1));
    }
    stock.processElement(new StreamRecord<>(row));
    nativeOperator.processElement(
        new StreamRecord<>(
            new ArrowBatch(
                RowDataArrowConverter.write(List.of(row), input, allocator, false, true))));
    if (!proctime) {
      stock.processWatermark(new Watermark(row.getLong(1)));
      nativeOperator.processWatermark(new Watermark(row.getLong(1)));
    }
    compareStringOutputs(stock, nativeOperator, output, row);
  }

  private static void compareStringOutputs(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeOperator,
      RowType output,
      RowData row) {
    var expected = new ArrayList<List<Object>>();
    for (var record : stock.extractOutputStreamRecords())
      expected.add(stringResultRepresentations(record.getValue()));
    stock.getOutput().clear();
    var actual = new ArrayList<List<Object>>();
    for (var record : nativeOperator.extractOutputStreamRecords()) {
      try (var root = record.getValue().root()) {
        for (var value : RowDataArrowConverter.read(root, output))
          actual.add(stringResultRepresentations(value));
      }
    }
    nativeOperator.getOutput().clear();
    assertEquals(expected, actual, "rowtime=" + row.getLong(1));
  }

  private static List<RowData> boundedStringRows(boolean malformed) {
    if (!malformed)
      return java.util.stream.IntStream.range(0, 7)
          .mapToObj(FlinkNativeTypedOverOperatorTest::stringRow)
          .collect(java.util.stream.Collectors.toList());
    return List.of(
        GenericRowData.of(BinaryStringData.fromString("\ud800"), 100L),
        GenericRowData.of(BinaryStringData.fromString("\ud801"), 200L),
        GenericRowData.of(binary("?"), 250L),
        GenericRowData.of(BinaryStringData.fromString("\ud800"), 300L),
        GenericRowData.of(null, 350L),
        GenericRowData.of(BinaryStringData.fromString("😀".repeat(1024)), 500L),
        GenericRowData.of(BinaryStringData.fromString(""), 700L));
  }

  static Stream<Arguments> cachedFirstLastPeerCases() {
    return Stream.of(
        Arguments.of(1, false, false),
        Arguments.of(1, false, true),
        Arguments.of(1, true, false),
        Arguments.of(1, true, true),
        Arguments.of(2, false, false),
        Arguments.of(2, false, true));
  }

  @ParameterizedTest
  @MethodSource("cachedFirstLastPeerCases")
  void cachedFirstLastPeersRetainViewsAcrossSnapshot(int frame, boolean proctime, boolean rocks)
      throws Exception {
    cachedStringPeers(frame, proctime, rocks, false);
  }

  @ParameterizedTest
  @MethodSource("cachedFirstLastPeerCases")
  void cachedExtremaPeersRetainViewsAcrossSnapshot(int frame, boolean proctime, boolean rocks)
      throws Exception {
    cachedStringPeers(frame, proctime, rocks, true);
  }

  private static void cachedStringPeers(int frame, boolean proctime, boolean rocks, boolean extrema)
      throws Exception {
    var type = new VarCharType(VarCharType.MAX_LENGTH);
    var input = RowType.of(type, new BigIntType());
    var output =
        extrema
            ? RowType.of(type, new BigIntType(), type, type, type, type)
            : RowType.of(type, new BigIntType(), type, type);
    var groups =
        List.of(
            List.<RowData>of(
                GenericRowData.of(BinaryStringData.fromString("\ud800"), 100L),
                GenericRowData.of(BinaryStringData.fromString("\ud801"), 100L)),
            List.<RowData>of(
                GenericRowData.of(binary("?"), 200L),
                GenericRowData.of(BinaryStringData.fromString("\ud800"), 200L)),
            List.<RowData>of(GenericRowData.of(null, 350L)),
            List.<RowData>of(GenericRowData.of(BinaryStringData.fromString("tail"), 500L)));
    org.apache.flink.runtime.checkpoint.OperatorSubtaskState stockSnapshot, nativeSnapshot;
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, frame, proctime, true, !extrema);
        var nativeOperator = nativeCachedReductions(input, frame, proctime, rocks, extrema)) {
      if (rocks) stock.setStateBackend(stockRocksBackend());
      stock.setup(new RowDataSerializer(output));
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.open();
      firstLastGroup(stock, nativeOperator, allocator, input, output, proctime, groups.get(0));
      stockSnapshot = stock.snapshot(1, 1);
      nativeSnapshot = nativeOperator.snapshot(1, 1);
      for (int group = 1; group < groups.size(); group++)
        firstLastGroup(
            stock, nativeOperator, allocator, input, output, proctime, groups.get(group));
    }
    try (var allocator = new RootAllocator();
        var stock = stock(type, input, frame, proctime, true, !extrema);
        var nativeOperator = nativeCachedReductions(input, frame, proctime, rocks, extrema)) {
      if (rocks) stock.setStateBackend(stockRocksBackend());
      stock.setup(new RowDataSerializer(output));
      stock.initializeState(stockSnapshot);
      stock.open();
      nativeOperator.setup(new ArrowBatchSerializer());
      nativeOperator.initializeState(nativeSnapshot);
      nativeOperator.open();
      for (int group = 1; group < groups.size(); group++)
        firstLastGroup(
            stock, nativeOperator, allocator, input, output, proctime, groups.get(group));
    }
  }

  private static void firstLastGroup(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock,
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeOperator,
      RootAllocator allocator,
      RowType input,
      RowType output,
      boolean proctime,
      List<RowData> rows)
      throws Exception {
    long time = rows.get(0).getLong(1);
    if (proctime) {
      stock.setProcessingTime(time);
      nativeOperator.setProcessingTime(time);
    }
    for (var row : rows) stock.processElement(new StreamRecord<>(row));
    nativeOperator.processElement(
        new StreamRecord<>(
            new ArrowBatch(RowDataArrowConverter.write(rows, input, allocator, false, true))));
    if (!proctime) {
      stock.processWatermark(new Watermark(time));
      nativeOperator.processWatermark(new Watermark(time));
    }
    var expected =
        stock.extractOutputStreamRecords().stream()
            .map(record -> stringResultRepresentations(record.getValue()))
            .collect(java.util.stream.Collectors.toList());
    stock.getOutput().clear();
    var actual = new ArrayList<List<Object>>();
    for (var record : nativeOperator.extractOutputStreamRecords()) {
      try (var root = record.getValue().root()) {
        for (var value : RowDataArrowConverter.read(root, output))
          actual.add(stringResultRepresentations(value));
      }
    }
    nativeOperator.getOutput().clear();
    assertEquals(expected, actual, "peer time=" + time);
  }

  private static List<Object> stringResultRepresentations(RowData row) {
    var values = new ArrayList<Object>();
    for (int field = 0; field < row.getArity(); field++) {
      if (field == 1) continue;
      values.add(
          row.isNullAt(field)
              ? null
              : new StringRepresentation((BinaryStringData) row.getString(field)));
    }
    return values;
  }

  private static final class StringRepresentation {
    private final byte[] bytes;
    private final String cache;

    private StringRepresentation(BinaryStringData text) {
      bytes = text.toBytes();
      cache = text.getJavaObject();
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof StringRepresentation)) return false;
      var value = (StringRepresentation) other;
      return Arrays.equals(bytes, value.bytes) && java.util.Objects.equals(cache, value.cache);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(bytes) * 31 + java.util.Objects.hashCode(cache);
    }

    @Override
    public String toString() {
      String units =
          cache == null
              ? "binary"
              : cache
                      .chars()
                      .limit(8)
                      .mapToObj(value -> Integer.toHexString(value))
                      .collect(java.util.stream.Collectors.joining(","))
                  + ":length="
                  + cache.length();
      return "bytes=" + bytes.length + ":hash=" + Arrays.hashCode(bytes) + ":cache=" + units;
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      nativeFirstLast(RowType input, int frame, boolean proctime, boolean rocks) throws Exception {
    var operator =
        new NativeOverAggregateOperator(
            1,
            new int[] {0, 0},
            new int[0],
            new int[] {3, 3},
            new int[] {5, 6},
            frame,
            frame == 1 ? 1 : 100,
            proctime,
            new int[0],
            0,
            input,
            128);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(operator, ignored -> 0, Types.INT, 128, 1, 0);
    if (rocks)
      harness.setStateBackend(
          new tech.streamfusion.state.RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), FlinkNativeTypedOverOperatorTest.class.getClassLoader()));
    return harness;
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      nativeCachedReductions(
          RowType input, int frame, boolean proctime, boolean rocks, boolean extrema)
          throws Exception {
    if (!extrema) return nativeFirstLast(input, frame, proctime, rocks);
    var harness = nativeHarness(input, 3, frame, proctime, true);
    if (rocks)
      harness.setStateBackend(
          new tech.streamfusion.state.RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), FlinkNativeTypedOverOperatorTest.class.getClassLoader()));
    return harness;
  }

  private static List<Object> stringRepresentations(RowData row) {
    var values = new ArrayList<Object>();
    for (int field : new int[] {0, 2, 3, 4, 5}) {
      var text = (BinaryStringData) row.getString(field);
      values.add(java.util.Arrays.toString(text.toBytes()));
      values.add(text.getJavaObject());
    }
    return values;
  }

  private static RowData stringRow(int step) {
    String[] text = {"😀", "\ue000", "\ue100", "😀", null, "\ue000", "tail"};
    long[] time = {100, 200, 250, 300, 350, 500, 700};
    Object value =
        text[step] == null
            ? null
            : step == 2 || step >= 5 ? binary(text[step]) : BinaryStringData.fromString(text[step]);
    return GenericRowData.of(value, time[step]);
  }

  private static List<List<Object>> stringStep(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness,
      int frame,
      boolean proctime,
      int step)
      throws Exception {
    var row = stringRow(step);
    if (proctime) harness.setProcessingTime(row.getLong(1));
    harness.processElement(new StreamRecord<>(row));
    if (!proctime) harness.processWatermark(new Watermark(row.getLong(1)));
    return drainStock(harness, false, true, 0);
  }

  private static org.apache.flink.runtime.state.StateBackend stockRocksBackend() throws Exception {
    Class<?> backend;
    try {
      backend = Class.forName("org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend");
    } catch (ClassNotFoundException earlier) {
      backend =
          Class.forName("org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend");
    }
    return (org.apache.flink.runtime.state.StateBackend) backend.getConstructor().newInstance();
  }

  private static BinaryStringData binary(String value) {
    return BinaryStringData.fromBytes(value.getBytes(StandardCharsets.UTF_8));
  }

  private static DecimalData dec(long value, int scale) {
    return DecimalData.fromBigDecimal(BigDecimal.valueOf(value, scale), 38, scale);
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(
      LogicalType valueType, RowType input, int frame, boolean proctime, boolean firstLast)
      throws Exception {
    return stock(valueType, input, frame, proctime, firstLast, false);
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(
      LogicalType valueType,
      RowType input,
      int frame,
      boolean proctime,
      boolean firstLast,
      boolean onlyFirstLast)
      throws Exception {
    return stock(valueType, input, frame, proctime, firstLast, onlyFirstLast, 100);
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(
      LogicalType valueType,
      RowType input,
      int frame,
      boolean proctime,
      boolean firstLast,
      boolean onlyFirstLast,
      long rangeOffset)
      throws Exception {
    return stock(valueType, input, frame, proctime, firstLast, onlyFirstLast, rangeOffset, 1);
  }

  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> stock(
      LogicalType valueType,
      RowType input,
      int frame,
      boolean proctime,
      boolean firstLast,
      boolean onlyFirstLast,
      long rangeOffset,
      int precedingRows)
      throws Exception {
    var table =
        TableEnvironment.create(
            org.apache.flink.table.api.EnvironmentSettings.newInstance().inStreamingMode().build());
    var planner = (PlannerBase) ((TableEnvironmentImpl) table).getPlanner();
    var factory = planner.getTypeFactory();
    var calls = new ArrayList<AggregateCall>();
    var operators =
        new ArrayList<org.apache.calcite.sql.SqlAggFunction>(
            onlyFirstLast ? List.of() : List.of(SqlStdOperatorTable.MIN, SqlStdOperatorTable.MAX));
    if (firstLast)
      operators.addAll(
          List.of(FlinkSqlOperatorTable.FIRST_VALUE, FlinkSqlOperatorTable.LAST_VALUE));
    for (var operator : operators) {
      calls.add(
          AggregateCall.create(
              operator,
              false,
              List.of(0),
              -1,
              factory.createFieldTypeFromLogicalType(valueType),
              "a" + calls.size()));
    }
    boolean bounded = frame == 1 || frame == 2;
    boolean[] retract = new boolean[calls.size()];
    Arrays.fill(retract, bounded);
    var info =
        AggregateUtil.transformToStreamAggregateInfoList(
            factory,
            RowType.of(valueType),
            JavaScalaConversionUtil.toScala(calls),
            retract,
            bounded,
            true,
            true);
    var builder =
        planner
            .createRelBuilder()
            .values(
                factory.buildRelNodeRowType(
                    JavaScalaConversionUtil.toScala(List.of("v")),
                    JavaScalaConversionUtil.toScala(List.of(valueType))));
    var generator =
        new AggsHandlerCodeGenerator(
                new CodeGeneratorContext(
                    table.getConfig(), Thread.currentThread().getContextClassLoader()),
                builder,
                JavaScalaConversionUtil.toScala(List.of(valueType)),
                false)
            .needAccumulate();
    if (bounded) generator.needRetract();
    var generated = generator.generateAggsHandler("TypedOverContract", info);
    var stateTypes =
        Arrays.stream(info.getAccTypes()).map(t -> t.getLogicalType()).toArray(LogicalType[]::new);
    var fields = input.getChildren().toArray(new LogicalType[0]);
    KeyedProcessFunction<RowData, RowData, RowData> function;
    if (proctime && bounded)
      function =
          new ProcTimeRowsBoundedPrecedingFunction(
              0, 0, generated, stateTypes, fields, precedingRows + 1);
    else if (proctime)
      function =
          new ProcTimeUnboundedPrecedingFunction(StateTtlConfig.DISABLED, generated, stateTypes);
    else if (frame == 1)
      function =
          new RowTimeRowsBoundedPrecedingFunction(
              0, 0, generated, stateTypes, fields, precedingRows + 1, 1);
    else if (frame == 2)
      function =
          new RowTimeRangeBoundedPrecedingFunction(generated, stateTypes, fields, rangeOffset, 1);
    else function = unbounded(frame, generated, stateTypes, fields);
    var keyType = RowType.of(new BigIntType());
    var serializer = new RowDataSerializer(keyType);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new KeyedProcessOperator<>(function),
        row -> serializer.toBinaryRow(GenericRowData.of(0L)).copy(),
        InternalTypeInfo.of(keyType));
  }

  @SuppressWarnings("unchecked")
  private static KeyedProcessFunction<RowData, RowData, RowData> unbounded(
      int frame,
      org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction generated,
      LogicalType[] stateTypes,
      LogicalType[] fields)
      throws Exception {
    try {
      var type =
          Class.forName(
              "org.apache.flink.table.runtime.operators.over.RowTimeUnboundedPrecedingOverFunctionV2");
      return (KeyedProcessFunction<RowData, RowData, RowData>)
          type.getConstructor(
                  boolean.class,
                  long.class,
                  long.class,
                  org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction.class,
                  LogicalType[].class,
                  LogicalType[].class,
                  int.class)
              .newInstance(frame == 3, 0L, 0L, generated, stateTypes, fields, 1);
    } catch (ClassNotFoundException earlier) {
      return frame == 3
          ? new RowTimeRowsUnboundedPrecedingFunction(0, 0, generated, stateTypes, fields, 1)
          : new RowTimeRangeUnboundedPrecedingFunction(0, 0, generated, stateTypes, fields, 1);
    }
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      nativeHarness(RowType input, int code, int frame, boolean proctime, boolean firstLast)
          throws Exception {
    return nativeHarness(input, code, frame, proctime, firstLast, 100);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      nativeHarness(
          RowType input, int code, int frame, boolean proctime, boolean firstLast, long rangeOffset)
          throws Exception {
    var operator =
        new NativeOverAggregateOperator(
            1,
            firstLast ? new int[] {0, 0, 0, 0} : new int[] {0, 0},
            new int[0],
            firstLast ? new int[] {code, code, code, code} : new int[] {code, code},
            firstLast ? new int[] {1, 2, 5, 6} : new int[] {1, 2},
            frame,
            frame == 1 ? 1 : rangeOffset,
            proctime,
            new int[0],
            0,
            input,
            128);
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, ignored -> 0, Types.INT, 128, 1, 0);
  }

  private static List<List<Object>> drainStock(
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness,
      boolean decimal,
      boolean string,
      int scale) {
    List<List<Object>> rows = new ArrayList<>();
    Object event;
    while ((event = harness.getOutput().poll()) != null) {
      if (event instanceof StreamRecord)
        rows.add(values((RowData) ((StreamRecord<?>) event).getValue(), decimal, string, scale));
    }
    return rows;
  }

  private static List<List<Object>> drainNative(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> harness,
      RowType output,
      boolean decimal,
      boolean string,
      int scale) {
    List<List<Object>> rows = new ArrayList<>();
    Object event;
    while ((event = harness.getOutput().poll()) != null) {
      if (event instanceof StreamRecord) {
        try (var root = ((ArrowBatch) ((StreamRecord<?>) event).getValue()).root()) {
          for (var row : RowDataArrowConverter.read(root, output))
            rows.add(values(row, decimal, string, scale));
        }
      }
    }
    return rows;
  }

  private static List<Object> values(RowData row, boolean decimal, boolean string, int scale) {
    List<Object> values = new ArrayList<>();
    for (int field = 2; field < row.getArity(); field++)
      values.add(
          row.isNullAt(field)
              ? null
              : string
                  ? new String(row.getString(field).toBytes(), StandardCharsets.UTF_8)
                  : decimal
                      ? row.getDecimal(field, 38, scale).toBigDecimal()
                      : row.getTimestamp(field, 9));
    return values;
  }
}
