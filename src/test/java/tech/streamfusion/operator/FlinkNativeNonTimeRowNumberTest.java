package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.*;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.*;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.state.RocksDBNativeStateBackendFactory;

@ExtendWith(CoalescingOff.class)
class FlinkNativeNonTimeRowNumberTest {
  private static final RowType INPUT = RowType.of(new BigIntType(), new BigIntType());
  private static final RowType OUTPUT =
      RowType.of(new BigIntType(), new BigIntType(), new BigIntType());

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void signedRowIdentitiesSurviveTwoRestoresAndBackendTransitions(boolean writer, boolean reader)
      throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        tech.streamfusion.compat.FlinkTestCapabilities.NON_TIME_OVER);
    OperatorSubtaskState nativeState;
    // Released Flink loses a prior accumulator after restore. Compare the restored native
    // operator to an uninterrupted host, preserving the clock and every emitted changelog.
    try (var host = host()) {
      host.open();
      try (var nativeHarness = nativeHarness(writer)) {
        nativeHarness.open();
        compare(
            nativeHarness,
            host,
            5000,
            row(RowKind.INSERT, 1, 20),
            row(RowKind.INSERT, 1, 30),
            row(RowKind.INSERT, 2, 20));
        compare(nativeHarness, host, 5700, row(RowKind.INSERT, 1, 10));
        nativeState = snapshot(nativeHarness, writer != reader);
      }
      try (var nativeHarness = nativeHarness(reader)) {
        nativeHarness.initializeState(nativeState);
        nativeHarness.open();
        compare(nativeHarness, host, 5999, row(RowKind.INSERT, 1, 25));
        compare(nativeHarness, host, 6000, row(RowKind.INSERT, 2, 30));
        compare(
            nativeHarness,
            host,
            6998,
            row(RowKind.UPDATE_BEFORE, 1, 10),
            row(RowKind.UPDATE_AFTER, 1, 15));
        nativeState = snapshot(nativeHarness, false);
      }
      try (var nativeHarness = nativeHarness(reader)) {
        nativeHarness.initializeState(nativeState);
        nativeHarness.open();
        compare(
            nativeHarness,
            host,
            7998,
            row(RowKind.INSERT, 1, 40),
            row(RowKind.DELETE, 2, 30),
            row(RowKind.INSERT, 2, 50));
      }
    }
  }

  private static OperatorSubtaskState snapshot(
      org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness<?> harness,
      boolean canonical)
      throws Exception {
    return canonical
        ? harness
            .snapshotWithLocalState(
                1,
                1,
                org.apache.flink.runtime.checkpoint.SavepointType.savepoint(
                    org.apache.flink.core.execution.SavepointFormatType.CANONICAL))
            .getJobManagerOwnedState()
        : harness.snapshot(1, 1);
  }

  private static RowData row(RowKind kind, long key, long value) {
    var row = GenericRowData.of(key, value);
    row.setRowKind(kind);
    return row;
  }

  private static List<Object> values(RowData row) {
    var result = new ArrayList<Object>();
    result.add(row.getRowKind());
    for (int i = 0; i < row.getArity(); i++) result.add(row.isNullAt(i) ? null : row.getLong(i));
    return result;
  }

  private static void compare(
      KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch> nativeHarness,
      KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> host,
      long now,
      RowData... rows)
      throws Exception {
    nativeHarness.setProcessingTime(now);
    host.setProcessingTime(now);
    var serializer = new RowDataSerializer(INPUT);
    for (var row : rows) host.processElement(new StreamRecord<>(serializer.copy(row)));
    var expected = new ArrayList<List<Object>>();
    for (var row : host.extractOutputValues()) expected.add(values(row));
    host.getOutput().clear();
    var actual = new ArrayList<List<Object>>();
    try (var allocator = new RootAllocator()) {
      nativeHarness.processElement(
          new StreamRecord<>(
              new ArrowBatch(
                  RowDataArrowConverter.write(Arrays.asList(rows), INPUT, allocator, true))));
      for (var record : nativeHarness.extractOutputStreamRecords()) {
        try (var root = record.getValue().root()) {
          for (var row : RowDataArrowConverter.read(root, OUTPUT)) actual.add(values(row));
        }
      }
      nativeHarness.getOutput().clear();
    }
    assertEquals(expected, actual, "processing time=" + now);
  }

  private static KeyedOneInputStreamOperatorTestHarness<Integer, ArrowBatch, ArrowBatch>
      nativeHarness(boolean rocks) throws Exception {
    var operator =
        new NativeColumnarTopNOperator(
            new int[] {0},
            new int[] {-1},
            INPUT,
            new int[] {1},
            new int[] {1},
            new int[] {1},
            0,
            Long.MAX_VALUE,
            true,
            false,
            null,
            null,
            true,
            false,
            -1,
            0,
            128,
            -1,
            false,
            true);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(operator, batch -> 0, Types.INT, 128, 1, 0);
    if (rocks)
      harness.setStateBackend(
          new RocksDBNativeStateBackendFactory()
              .createFromConfig(
                  new Configuration(), FlinkNativeNonTimeRowNumberTest.class.getClassLoader()));
    harness.setup(new ArrowBatchSerializer());
    return harness;
  }

  @SuppressWarnings("unchecked")
  private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> host()
      throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var table = StreamTableEnvironment.create(env);
    table.getConfig().set("table.exec.state.ttl", "0 ms");
    var source =
        env.fromElements(Row.of(1L, 2L))
            .returns(Types.ROW_NAMED(new String[] {"k", "v"}, Types.LONG, Types.LONG));
    table.createTemporaryView(
        "src",
        table.fromChangelogStream(
            source,
            Schema.newBuilder().build(),
            org.apache.flink.table.connector.ChangelogMode.all()));
    var pending = new ArrayDeque<Transformation<?>>();
    pending.add(
        table
            .toChangelogStream(
                table.sqlQuery(
                    "SELECT k,v,ROW_NUMBER() OVER(PARTITION BY k ORDER BY v ASC NULLS FIRST) AS rn"
                        + " FROM src"))
            .getTransformation());
    while (!pending.isEmpty()) {
      var next = pending.removeFirst();
      if (next instanceof OneInputTransformation<?, ?>
          && next.getName().contains("OverAggregate")) {
        var op = (OneInputTransformation<RowData, RowData>) next;
        var harness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                (OneInputStreamOperator<RowData, RowData>)
                    ((SimpleOperatorFactory<RowData>) op.getOperatorFactory()).getOperator(),
                (KeySelector<RowData, RowData>) op.getStateKeySelector(),
                (TypeInformation<RowData>) op.getStateKeyType(),
                128,
                1,
                0);
        harness.setup(new RowDataSerializer(OUTPUT));
        return harness;
      }
      pending.addAll(next.getInputs());
    }
    throw new AssertionError("Released non-time OVER operator missing");
  }
}
