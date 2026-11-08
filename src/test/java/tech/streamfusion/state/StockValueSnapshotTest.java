package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class StockValueSnapshotTest {
  @TempDir Path directory;
  private static final RowDataSerializer KEYS = new RowDataSerializer(RowType.of(new BigIntType()));
  private static final CheckpointOptions OPTIONS =
      new CheckpointOptions(
          SavepointType.savepoint(SavepointFormatType.CANONICAL),
          CheckpointStorageLocationReference.getDefault());

  @ParameterizedTest
  @CsvSource({"128,false", "257,false", "128,true", "257,true"})
  void scatteredKeyGroupsRestoreAllValuesAndOriginalTtlAges(int maximum, boolean ttl)
      throws Exception {
    var resources =
        StockValueSnapshot.capture(
            KEYS,
            maximum,
            new KeyGroupRange(0, maximum - 1),
            directory,
            "stock-value",
            LongSerializer.INSTANCE,
            ttl,
            receiver -> {
              for (long i = 2050; i >= 0; i--)
                receiver.write(KEYS.toBinaryRow(GenericRowData.of(i)).copy(), i * 7, 500 + i);
            });
    var seen = new ArrayList<Integer>();
    try (var iterator = resources.getSnapshotResources().createKVStateIterator()) {
      while (iterator.isValid()) {
        int group = iterator.keyGroup();
        if (iterator.isNewKeyGroup()) seen.add(group);
        iterator.next();
      }
    }
    assertEquals(seen.stream().sorted().collect(java.util.stream.Collectors.toList()), seen);
    assertTrue(seen.size() > 1);
    try (var cancelables = new CloseableRegistry()) {
      var future =
          StockCanonicalSnapshot.capture(
              () -> resources,
              null,
              1,
              0,
              new MemCheckpointStreamFactory(1 << 24),
              OPTIONS,
              cancelables);
      var saved = CanonicalSnapshotRoutingTest.finish(future);
      assertEmptyDirectory();
      try (var restored =
          CanonicalSnapshotRoutingTest.harness(
              new HashMapStateBackend(), new AtomicInteger(), new AtomicInteger(), maximum)) {
        restored.initializeState(saved);
        restored.open();
        var backend = restored.getOperator().getKeyedStateBackend();
        for (long i = 0; i <= 2050; i++) {
          backend.setCurrentKey(KEYS.toBinaryRow(GenericRowData.of(i)).copy());
          if (ttl) {
            var descriptor =
                new ValueStateDescriptor<>(
                    "stock-value",
                    new TtlSerializer<>(LongSerializer.INSTANCE, LongSerializer.INSTANCE));
            TtlValue<Long> value =
                backend
                    .getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor)
                    .value();
            assertEquals(i * 7, value.getUserValue());
            assertEquals(500 + i, value.getLastAccessTimestamp());
          } else {
            assertEquals(
                i * 7,
                backend
                    .getPartitionedState(
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        new ValueStateDescriptor<>("stock-value", LongSerializer.INSTANCE))
                    .value());
          }
        }
      }
    }
  }

  @Test
  void cancellingBeforeWritingReleasesTheCapturedBytes() throws Exception {
    var resources =
        StockValueSnapshot.capture(
            KEYS,
            128,
            new KeyGroupRange(0, 127),
            directory,
            "stock-value",
            LongSerializer.INSTANCE,
            false,
            receiver -> receiver.write(CanonicalSnapshotRoutingTest.key(), 5L, 0));
    try (var cancelables = new CloseableRegistry()) {
      var futures =
          StockCanonicalSnapshot.capture(
              () -> resources,
              null,
              1,
              0,
              new MemCheckpointStreamFactory(1 << 20),
              OPTIONS,
              cancelables);
      assertFalse(files().isEmpty());
      futures.cancel();
      assertEmptyDirectory();
      assertThrows(IOException.class, resources.getSnapshotResources()::createKVStateIterator);
    }
  }

  @Test
  void failedExportAndUnownedKeysReleaseTheirStaging() throws Exception {
    var failure = new IOException("receiver failed");
    assertSame(
        failure,
        assertThrows(
            IOException.class,
            () ->
                StockValueSnapshot.capture(
                    KEYS,
                    128,
                    new KeyGroupRange(0, 127),
                    directory,
                    "stock-value",
                    LongSerializer.INSTANCE,
                    false,
                    receiver -> {
                      receiver.write(CanonicalSnapshotRoutingTest.key(), 5L, 0);
                      throw failure;
                    })));
    assertEmptyDirectory();
    int actualGroup =
        KeyGroupRangeAssignment.assignToKeyGroup(CanonicalSnapshotRoutingTest.key(), 128);
    int owned = actualGroup == 0 ? 1 : 0;
    assertThrows(
        IllegalStateException.class,
        () ->
            StockValueSnapshot.capture(
                KEYS,
                128,
                new KeyGroupRange(owned, owned),
                directory,
                "stock-value",
                LongSerializer.INSTANCE,
                false,
                receiver -> receiver.write(CanonicalSnapshotRoutingTest.key(), 5L, 0)));
    assertEmptyDirectory();
  }

  @Test
  void emptyExportKeepsItsReleasedSchemaAndReleasesItsFile() throws Exception {
    var resources =
        StockValueSnapshot.capture(
            KEYS,
            128,
            new KeyGroupRange(0, 127),
            directory,
            "stock-value",
            LongSerializer.INSTANCE,
            false,
            receiver -> {});
    assertEquals(
        "stock-value", resources.getSnapshotResources().getMetaInfoSnapshots().get(0).getName());
    try (var iterator = resources.getSnapshotResources().createKVStateIterator()) {
      assertFalse(iterator.isValid());
    }
    resources.getSnapshotResources().release();
    resources.getSnapshotResources().release();
    assertEmptyDirectory();
  }

  private List<Path> files() throws IOException {
    try (var files = Files.list(directory)) {
      return files.collect(java.util.stream.Collectors.toList());
    }
  }

  private void assertEmptyDirectory() throws IOException {
    assertEquals(List.of(), files());
  }
}
