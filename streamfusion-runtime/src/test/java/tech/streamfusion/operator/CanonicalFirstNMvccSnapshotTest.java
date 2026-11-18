package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.*;
import static tech.streamfusion.operator.FirstNCanonicalTestSupport.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.Native;
import tech.streamfusion.state.StockCanonicalSnapshot;
import tech.streamfusion.state.StockSnapshotContext;

class CanonicalFirstNMvccSnapshotTest {
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(longs = {0, 10000})
  void concurrentCapturesRemainIndependentAfterUpdatesAndOperatorClose(long ttl) throws Exception {
    var prefix = new ArrayList<RowData>();
    for (long key = 2050; key >= 0; key--) prefix.add(GenericRowData.of(key, key));
    long handle = create(directory, true, false, true, ttl);
    SavepointResources<?> first = null, second = null;
    try {
      nativePush(handle, prefix, true, 1000);
      first = capture(handle, ttl);
      nativePush(handle, prefix, true, 1500);
      second = capture(handle, ttl);
      nativePush(handle, prefix, true, 2000);
    } finally {
      Native.closeFirstN(handle);
    }
    long allocated = NativeAllocator.SHARED.getAllocatedMemory();
    try {
      var later = finish(second);
      var earlier = finish(first);
      try (var stockFirst = harness(true, false, true, ttl);
          var stockSecond = harness(true, false, true, ttl)) {
        stockFirst.initializeState(earlier);
        stockSecond.initializeState(later);
        stockFirst.open();
        stockSecond.open();
        stockFirst.setStateTtlProcessingTime(2000);
        stockSecond.setStateTtlProcessingTime(2000);
        var suffix = List.<RowData>of(GenericRowData.of(10L, 9L), GenericRowData.of(2000L, 8L));
        assertEquals(List.of("10|9|2", "2000|8|2"), stockPush(stockFirst, suffix));
        assertEquals(List.of("10|9|3", "2000|8|3"), stockPush(stockSecond, suffix));
      }
    } finally {
      if (first != null) first.getSnapshotResources().release();
      if (second != null) second.getSnapshotResources().release();
    }
    assertEquals(allocated, NativeAllocator.SHARED.getAllocatedMemory());
    // A released cursor must no longer hold the database lock after the writer has closed.
    long reopened = create(directory, true, false, true, ttl);
    Native.closeFirstN(reopened);
    try (var files = Files.list(directory)) {
      assertTrue(files.noneMatch(path -> path.getFileName().toString().endsWith(".entries")));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void cancellationReleasesPinsBeforeAndDuringIteration(boolean started) throws Exception {
    long handle = create(directory, true, false, true, 0);
    SavepointResources<?> captured = null;
    try {
      nativePush(handle, List.of(GenericRowData.of(1L, 2L)), true, 0);
      captured = capture(handle, 0);
      if (started) assertTrue(captured.getSnapshotResources().createKVStateIterator().isValid());
    } finally {
      Native.closeFirstN(handle);
      if (captured != null) {
        captured.getSnapshotResources().release();
        captured.getSnapshotResources().release();
      }
    }
    long reopened = create(directory, true, false, true, 0);
    Native.closeFirstN(reopened);
  }

  private SavepointResources<?> capture(long handle, long ttl) throws Exception {
    var keys = new RowDataSerializer(keyType(false));
    return contract(false, ttl)
        .snapshot(
            new StockSnapshotContext(
                keys, 128, new org.apache.flink.runtime.state.KeyGroupRange(0, 127)),
            handle,
            true);
  }

  private static OperatorSubtaskState finish(SavepointResources<?> resources) throws Exception {
    var options =
        CheckpointOptions.forConfig(
            SavepointType.savepoint(SavepointFormatType.CANONICAL),
            CheckpointStorageLocationReference.getDefault(),
            true,
            false,
            0);
    try (var cancelables = new CloseableRegistry()) {
      var futures =
          StockCanonicalSnapshot.capture(
              () -> resources,
              null,
              1,
              0,
              new MemCheckpointStreamFactory(64 * 1024 * 1024),
              options,
              cancelables);
      return tech.streamfusion.compat.CanonicalTestCompat.finishSnapshot(futures);
    }
  }
}
