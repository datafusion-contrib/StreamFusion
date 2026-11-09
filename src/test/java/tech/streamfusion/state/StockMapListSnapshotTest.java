package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.operator.TaskOffHeapMemory;

class StockMapListSnapshotTest {
  private static final RowType TYPE =
      RowType.of(new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH));
  private final RowDataSerializer rows = new RowDataSerializer(TYPE);
  @TempDir Path directory;

  @BeforeAll
  static void memory() {
    var config = new Configuration();
    config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, MemorySize.parse("1g"));
    TaskOffHeapMemory.initialize(config);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("unchecked")
  void releasedBackendBytesKeepOwnerBucketAndPeerOrderAfterSourceChanges(boolean rocks)
      throws Exception {
    var keyType = RowType.of(new VarCharType(VarCharType.MAX_LENGTH));
    var keys = new RowDataSerializer(keyType);
    long baseline = TaskOffHeapMemory.reservedBytes();
    try (var harness =
            new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
                new StateView(), row -> row, InternalTypeInfo.of(keyType));
        var spool = new StockStateSpool(keys, directory)) {
      var config = new Configuration();
      config.set(StateBackendOptions.STATE_BACKEND, rocks ? "rocksdb" : "hashmap");
      harness.setStateBackend(
          StateBackendLoader.loadStateBackendFromConfig(config, getClass().getClassLoader(), null));
      harness.setup(keys);
      harness.open();
      var backend =
          (CheckpointableKeyedStateBackend<RowData>)
              harness.getOperator().<RowData>getKeyedStateBackend();
      var state =
          backend.getPartitionedState(
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE,
              new MapStateDescriptor<>(
                  "rows", LongSerializer.INSTANCE, new ListSerializer<>(rows)));
      var owner = keys.toBinaryRow(GenericRowData.of(StringData.fromString("owner"))).copy();
      backend.setCurrentKey(owner);
      for (long time : new long[] {100, Long.MIN_VALUE, -1, Long.MAX_VALUE})
        state.put(
            time,
            List.of(
                GenericRowData.of(time, StringData.fromString("first")),
                GenericRowData.of(time, StringData.fromString("second"))));
      var expectedOrder = new ArrayList<Long>();
      for (var entry : state.entries()) expectedOrder.add(entry.getKey());
      var saved = new StockMapListSnapshot(spool, rows, 1);
      saved.capture(backend, "rows", keys);
      var wrong = new StockMapListSnapshot(spool, keys, 4);
      var mismatch =
          assertThrows(IllegalArgumentException.class, () -> wrong.capture(backend, "rows", keys));
      assertTrue(mismatch.getMessage().contains("serializer changed"));
      state.clear();
      var actualOrder = new ArrayList<Long>();
      try (var timestamps = saved.timestamps(owner)) {
        while (timestamps.hasNext()) {
          long time = timestamps.next();
          actualOrder.add(time);
          assertEquals(2, saved.size(owner, time));
          var decoded = saved.rows(owner, time).iterator();
          assertEquals("first", decoded.next().getString(1).toString());
          var second = decoded.next();
          assertEquals(time, second.getLong(0));
          assertEquals("second", second.getString(1).toString());
          assertFalse(decoded.hasNext());
        }
      }
      assertEquals(expectedOrder, actualOrder);
    }
    assertEquals(baseline, TaskOffHeapMemory.reservedBytes());
    try (var files = java.nio.file.Files.list(directory)) {
      assertEquals(0, files.count());
    }
  }

  private static final class StateView extends AbstractStreamOperator<RowData>
      implements OneInputStreamOperator<RowData, RowData> {
    @Override
    public void processElement(StreamRecord<RowData> row) {}
  }

  private byte[] encode(List<RowData> values) throws Exception {
    var output = new DataOutputSerializer(64);
    output.writeBoolean(values == null);
    if (values != null) new ListSerializer<>(rows).serialize(values, output);
    return output.getCopyOfBuffer();
  }

  @Test
  void releasedListBytesDecodeLazilyWithoutReusingRows() throws Exception {
    int[] decoded = {0};
    var counting =
        new RowDataSerializer(TYPE) {
          @Override
          public RowData deserialize(DataInputView input) throws IOException {
            decoded[0]++;
            return super.deserialize(input);
          }
        };
    var first = GenericRowData.of(1L, StringData.fromString("first"));
    var large = GenericRowData.of(2L, StringData.fromString("x".repeat(2 * 1024 * 1024)));
    var last = GenericRowData.of(3L, null);
    var iterable = StockMapListSnapshot.decode(encode(List.of(first, large, last)), counting);
    var iterator = iterable.iterator();
    assertTrue(iterator.hasNext());
    assertEquals(0, decoded[0]);
    var retained = iterator.next();
    assertEquals(1, decoded[0]);
    assertEquals(rows.toBinaryRow(first), retained);
    assertEquals(rows.toBinaryRow(large), iterator.next());
    assertEquals(rows.toBinaryRow(last), iterator.next());
    assertEquals(3, decoded[0]);
    assertEquals(1L, retained.getLong(0));
    assertFalse(iterator.hasNext());
    assertThrows(NoSuchElementException.class, iterator::next);
    assertEquals(1L, iterable.iterator().next().getLong(0));
  }

  @Test
  void nullAndEmptyListsHaveNoRows() throws Exception {
    assertFalse(StockMapListSnapshot.decode(encode(null), rows).iterator().hasNext());
    assertFalse(StockMapListSnapshot.decode(encode(List.of()), rows).iterator().hasNext());
  }

  @Test
  void malformedLengthsTruncationAndTrailingBytesReject() throws Exception {
    assertThrows(
        UncheckedIOException.class,
        () -> StockMapListSnapshot.decode(new byte[] {0, -1, -1, -1, -1}, rows).iterator());
    assertThrows(
        UncheckedIOException.class,
        () -> StockMapListSnapshot.decode(new byte[] {1, 0}, rows).iterator());
    assertThrows(
        UncheckedIOException.class,
        () -> StockMapListSnapshot.decode(new byte[] {0}, rows).iterator());
    byte[] valid = encode(List.of(GenericRowData.of(1L, StringData.fromString("row"))));
    byte[] oversizedCount = valid.clone();
    java.nio.ByteBuffer.wrap(oversizedCount, 1, Integer.BYTES).putInt(Integer.MAX_VALUE);
    var lazy = StockMapListSnapshot.decode(oversizedCount, rows).iterator();
    assertEquals(1L, lazy.next().getLong(0));
    assertThrows(UncheckedIOException.class, lazy::next);
    var truncated =
        StockMapListSnapshot.decode(Arrays.copyOf(valid, valid.length - 1), rows).iterator();
    assertThrows(UncheckedIOException.class, truncated::next);
    var trailing =
        StockMapListSnapshot.decode(Arrays.copyOf(valid, valid.length + 1), rows).iterator();
    assertThrows(IllegalArgumentException.class, trailing::next);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StockMapListSnapshot.decode(new byte[] {0, 0, 0, 0, 0, 42}, rows).iterator().hasNext());
  }
}
