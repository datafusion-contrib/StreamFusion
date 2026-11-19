package tech.streamfusion.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import tech.streamfusion.compat.RuntimeCompat;
import tech.streamfusion.operator.ArrowBatch;
import tech.streamfusion.operator.ArrowBatchSerializer;
import tech.streamfusion.operator.NativeAllocator;
import tech.streamfusion.operator.NativeCalcOperator;
import tech.streamfusion.operator.RowDataArrowConverter;
import tech.streamfusion.operator.TaskOffHeapMemory;

/** Runs in a fresh worker JVM whose classpath physically excludes planner and Calcite artifacts. */
public final class GeneratedExpressionRuntimeProbe {
  private GeneratedExpressionRuntimeProbe() {}

  public static void main(String[] args) throws Exception {
    for (String missing :
        List.of(
            "org.apache.flink.table.planner.codegen.ExprCodeGenerator",
            "org.apache.flink.table.planner.codegen.CodeGeneratorContext",
            "org.apache.calcite.rex.RexNode")) {
      assertThrows(ClassNotFoundException.class, () -> Class.forName(missing));
    }
    Object restored;
    try (var input = new ObjectInputStream(Files.newInputStream(Path.of(args[1])))) {
      restored = input.readObject();
    }
    if (args[0].equals("sql")) executeSqlFactory((Configuration) restored);
    else if (args[0].equals("containers")) executeContainers((ExternalContainerConverter) restored);
    else executeFunction(args[0], (FlinkExpressionFunction) restored);
    System.out.println("ISOLATED_RUNTIME_OK " + args[0]);
  }

  private static void executeContainers(ExternalContainerConverter converter) {
    converter.open(GeneratedExpressionRuntimeProbe.class.getClassLoader());
    var key = new org.apache.flink.table.data.GenericArrayData(new Object[] {1, 2});
    var child =
        GenericRowData.ofKind(
            RowKind.DELETE, org.apache.flink.table.data.StringData.fromString("é🙂"));
    var values = new org.apache.flink.table.data.GenericArrayData(new Object[] {child, null});
    var map = new org.apache.flink.table.data.GenericMapData(java.util.Map.of(key, values));
    var input = GenericRowData.ofKind(RowKind.UPDATE_BEFORE, map);
    var external = (org.apache.flink.types.Row) converter.toExternal(input);
    assertEquals(RowKind.UPDATE_BEFORE, external.getKind());
    var result = (java.util.Map<?, ?>) external.getField(0);
    assertEquals(1, result.size());
    var entry = result.entrySet().iterator().next();
    assertArrayEquals(new int[] {1, 2}, (int[]) entry.getKey());
    var rows = (List<?>) entry.getValue();
    assertEquals(2, rows.size());
    var row = (org.apache.flink.types.Row) rows.get(0);
    assertEquals(RowKind.DELETE, row.getKind());
    assertEquals("é🙂", row.getField("n"));
    assertNull(rows.get(1));
    var owned = (RowData) converter.toInternal(external);
    ((int[]) entry.getKey())[0] = -1;
    row.setField("n", "mutated");
    assertEquals(1, owned.getMap(0).keyArray().getArray(0).getInt(0));
    assertEquals(
        RowKind.DELETE, owned.getMap(0).valueArray().getArray(0).getRow(0, 1).getRowKind());
    assertEquals(
        "é🙂", owned.getMap(0).valueArray().getArray(0).getRow(0, 1).getString(0).toString());
  }

  private static FunctionContext context() {
    return new FunctionContext(null) {
      @Override
      public ClassLoader getUserCodeClassLoader() {
        return GeneratedExpressionRuntimeProbe.class.getClassLoader();
      }
    };
  }

  private static void executeFunction(String mode, FlinkExpressionFunction function)
      throws Exception {
    var clock = new TestProcessingTimeService();
    clock.setCurrentTime(1234L);
    function.processingTimeService(clock);
    function.open(context());
    try {
      switch (mode) {
        case "scalar":
          assertEquals(42, function.eval(42));
          assertNull(function.eval(new Object[] {null}));
          break;
        case "cast":
          assertEquals(42, function.eval("42"));
          assertNull(function.eval(new Object[] {null}));
          assertThrows(NumberFormatException.class, () -> function.eval("invalid"));
          assertEquals(43, function.eval("43"));
          break;
        case "row":
          assertEquals(25, ((RowData) function.eval(4)).getInt(0));
          assertNull(function.eval(0));
          assertNull(function.eval(new Object[] {null}));
          assertEquals(20, ((RowData) function.eval(5)).getInt(0));
          break;
        case "clock":
          assertEquals(TimestampData.fromEpochMillis(1234L), function.eval(new Object[] {null}));
          clock.setCurrentTime(5678L);
          assertEquals(TimestampData.fromEpochMillis(5678L), function.eval(new Object[] {null}));
          break;
        case "time":
          assertEquals(
              RuntimeCompat.applyParsedTimePrecision(45_296_789, 0), function.eval("12:34:56.789"));
          assertEquals(
              RuntimeCompat.applyParsedTimePrecision(45_296_789, 1),
              CanonicalTemporalParser.tryTime(
                  org.apache.flink.table.data.StringData.fromString("12:34:56.789"), 1));
          assertNull(function.eval("invalid"));
          assertNull(function.eval(new Object[] {null}));
          break;
        default:
          fail("unknown fixture " + mode);
      }
    } finally {
      function.close();
    }
    if (mode.equals("clock")) {
      function.open(context());
      try {
        assertEquals(
            TimestampData.fromEpochMillis(10L), function.eval(TimestampData.fromEpochMillis(10L)));
        assertThrows(IllegalStateException.class, () -> function.eval(new Object[] {null}));
      } finally {
        function.close();
      }
    }
  }

  private static void executeSqlFactory(Configuration configuration) throws Exception {
    var taskManager = new Configuration();
    taskManager.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(64));
    TaskOffHeapMemory.initialize(taskManager);
    var factory =
        (SimpleOperatorFactory<?>)
            new StreamConfig(configuration)
                .getStreamOperatorFactory(GeneratedExpressionRuntimeProbe.class.getClassLoader());
    var operator = (NativeCalcOperator) factory.getOperator();
    long before = NativeAllocator.SHARED.getAllocatedMemory();
    try (var harness = new OneInputStreamOperatorTestHarness<ArrowBatch, ArrowBatch>(operator)) {
      harness.setup(new ArrowBatchSerializer());
      harness.open();
      assertEquals(1, Tag.opens);
      for (boolean changelog : new boolean[] {false, true}) {
        List<RowData> rows =
            List.of(
                GenericRowData.of((Object) null),
                GenericRowData.ofKind(
                    RowKind.UPDATE_AFTER, org.apache.flink.table.data.StringData.fromString("4")),
                GenericRowData.of(org.apache.flink.table.data.StringData.fromString("-2")),
                GenericRowData.of(org.apache.flink.table.data.StringData.fromString("5")));
        harness.processElement(
            new StreamRecord<>(
                new ArrowBatch(
                    RowDataArrowConverter.write(
                        rows, RowType.of(new VarCharType()), NativeAllocator.SHARED, changelog))));
        var values = new ArrayList<String>();
        var numbers = new ArrayList<Long>();
        var kinds = new ArrayList<RowKind>();
        for (Object record : harness.getOutput()) {
          if (!(record instanceof StreamRecord)) continue;
          try (var root = ((ArrowBatch) ((StreamRecord<?>) record).getValue()).root()) {
            for (RowData row :
                RowDataArrowConverter.read(root, RowType.of(new VarCharType(), new BigIntType()))) {
              values.add(row.getString(0).toString());
              numbers.add(row.getLong(1));
              kinds.add(row.getRowKind());
            }
          }
        }
        harness.getOutput().clear();
        assertEquals(List.of("tag-4", "tag-5"), values);
        assertEquals(List.of(4L, 5L), numbers);
        assertEquals(
            List.of(changelog ? RowKind.UPDATE_AFTER : RowKind.INSERT, RowKind.INSERT), kinds);
      }
      harness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(
                      List.of(), RowType.of(new VarCharType()), NativeAllocator.SHARED, false))));
      for (Object record : harness.getOutput()) {
        if (record instanceof StreamRecord) {
          try (var root = ((ArrowBatch) ((StreamRecord<?>) record).getValue()).root()) {
            assertEquals(0, root.getRowCount());
          }
        }
      }
    }
    assertEquals(1, Tag.closes);
    assertEquals(before, NativeAllocator.SHARED.getAllocatedMemory());
  }

  public static final class Tag extends ScalarFunction {
    private static int opens;
    private static int closes;
    private transient boolean opened;

    @Override
    public void open(FunctionContext context) {
      assertFalse(opened);
      opened = true;
      opens++;
    }

    public String eval(Long value) {
      assertTrue(opened, "generated references must use the function opened by the binding");
      return value == null ? null : "tag-" + value;
    }

    @Override
    public void close() {
      assertTrue(opened);
      opened = false;
      closes++;
    }
  }
}
