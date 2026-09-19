package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RocksDBNativeSnapshotStrategyTest {
  @TempDir Path directory;
  private boolean failNativeSnapshot;

  @Test
  void failedNativeSnapshotDeletesItsPartialDirectory() throws Exception {
    RocksDBNativeSnapshotStrategy strategy = strategy();
    TrackingFactory storage = new TrackingFactory(directory.resolve("remote"));
    failNativeSnapshot = true;
    strategy.beforeSnapshot(CheckpointOptions.forCheckpointWithDefaultLocation(), storage);

    IOException failure = assertThrows(IOException.class, () -> strategy.syncPrepareResources(1));

    assertEquals("native snapshot failed", failure.getMessage());
    assertFalse(Files.exists(directory.resolve("links/chk-1")));
    failNativeSnapshot = false;
    IncrementalRemoteKeyedStateHandle next = snapshot(strategy, storage, 2);
    assertEquals(
        "token", RocksDBNativeSnapshotStrategy.readMetaDocument(next.getMetaDataStateHandle()));
    next.discardState();
    assertEquals(0, storage.files());
  }

  @Test
  void failedUploadDiscardsEarlierFilesAndNextCheckpointSucceeds() throws Exception {
    RocksDBNativeSnapshotStrategy strategy = strategy();
    TrackingFactory storage = new TrackingFactory(directory.resolve("remote"));
    storage.failCreation = 2;

    assertThrows(IOException.class, () -> snapshot(strategy, storage, 1));

    assertEquals(0, storage.files(), "the first uploaded SST must be discarded");
    assertFalse(Files.exists(directory.resolve("links/chk-1")));
    storage.failCreation = -1;
    IncrementalRemoteKeyedStateHandle next = snapshot(strategy, storage, 2);
    assertTrue(
        next.getSharedState().stream()
            .noneMatch(file -> file.getHandle() instanceof PlaceholderStreamStateHandle));
    next.discardState();
    assertEquals(0, storage.files());
  }

  @Test
  void abortedCheckpointCannotBecomeAReuseBase() throws Exception {
    RocksDBNativeSnapshotStrategy strategy = strategy();
    TrackingFactory storage = new TrackingFactory(directory.resolve("remote"));
    IncrementalRemoteKeyedStateHandle aborted = snapshot(strategy, storage, 1);

    strategy.notifyCheckpointAborted(1);
    aborted.discardState();
    strategy.notifyCheckpointComplete(1);

    assertEquals(0, storage.files());
    IncrementalRemoteKeyedStateHandle next = snapshot(strategy, storage, 2);
    assertTrue(
        next.getSharedState().stream()
            .noneMatch(file -> file.getHandle() instanceof PlaceholderStreamStateHandle));
    next.discardState();
    assertEquals(0, storage.files());
  }

  @Test
  void failedReuseNotificationCleansNewFilesWithoutDiscardingConfirmedState() throws Exception {
    RocksDBNativeSnapshotStrategy strategy = strategy();
    TrackingFactory confirmedStorage = new TrackingFactory(directory.resolve("confirmed"));
    IncrementalRemoteKeyedStateHandle confirmed = snapshot(strategy, confirmedStorage, 1);
    strategy.notifyCheckpointComplete(1);
    long confirmedFiles = confirmedStorage.files();
    TrackingFactory failedStorage = new TrackingFactory(directory.resolve("failed"));
    failedStorage.failReuse = true;

    IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> snapshot(strategy, failedStorage, 2));

    assertEquals("reuse notification failed", failure.getMessage());
    assertEquals(
        0,
        failedStorage.files(),
        "unpublished private state and metadata belong to the failed attempt");
    assertEquals(
        confirmedFiles,
        confirmedStorage.files(),
        "confirmed shared state belongs to the coordinator");
    IncrementalRemoteKeyedStateHandle next = snapshot(strategy, failedStorage.withoutFailure(), 3);
    assertTrue(
        next.getSharedState().stream()
            .allMatch(file -> file.getHandle() instanceof PlaceholderStreamStateHandle));
    next.discardState();
    confirmed.discardState();
    assertEquals(0, failedStorage.files());
    assertEquals(0, confirmedStorage.files());
  }

  private RocksDBNativeSnapshotStrategy strategy() {
    RocksDBNativeSnapshotStrategy strategy =
        new RocksDBNativeSnapshotStrategy(
            UUID.randomUUID(), new KeyGroupRange(0, 1), directory.resolve("links").toFile(), true);
    strategy.registerNativeState(
        new RocksDBNativeState() {
          @Override
          public String[] checkpoint(String path) throws Exception {
            Path target = Path.of(path);
            Files.createDirectories(target);
            Files.writeString(target.resolve("data.sst"), "immutable SST");
            if (failNativeSnapshot) throw new IOException("native snapshot failed");
            Files.writeString(target.resolve("manifest"), "manifest");
            return new String[] {"token", "d:data.sst", "m:manifest"};
          }

          @Override
          public byte[][] canonicalPartitions() {
            return new byte[0][];
          }

          @Override
          public String canonicalOperatorId() {
            return "test";
          }

          @Override
          public long canonicalTimerDeadline() {
            return Long.MIN_VALUE;
          }
        },
        0);
    return strategy;
  }

  private IncrementalRemoteKeyedStateHandle snapshot(
      RocksDBNativeSnapshotStrategy strategy, TrackingFactory storage, long id) throws Exception {
    CheckpointOptions options = CheckpointOptions.forCheckpointWithDefaultLocation();
    strategy.beforeSnapshot(options, storage);
    var resources = strategy.syncPrepareResources(id);
    try (CloseableRegistry registry = new CloseableRegistry()) {
      return (IncrementalRemoteKeyedStateHandle)
          strategy
              .asyncSnapshot(resources, id, id, storage, options)
              .get(registry)
              .getJobManagerOwnedSnapshot();
    } finally {
      resources.release();
    }
  }

  private static final class TrackingFactory implements CheckpointStreamFactory {
    final Path directory;
    int creations;
    int failCreation = -1;
    boolean failReuse;

    TrackingFactory(Path directory) throws IOException {
      this.directory = directory;
      Files.createDirectories(directory);
    }

    TrackingFactory withoutFailure() {
      failReuse = false;
      return this;
    }

    long files() throws IOException {
      try (var paths = Files.list(directory)) {
        return paths.count();
      }
    }

    @Override
    public CheckpointStateOutputStream createCheckpointStateOutputStream(
        CheckpointedStateScope scope) throws IOException {
      if (++creations == failCreation) throw new IOException("upload failed");
      Path path = directory.resolve(UUID.randomUUID().toString());
      OutputStream output = Files.newOutputStream(path);
      return new CheckpointStateOutputStream() {
        long position;
        boolean committed;

        @Override
        public void write(int value) throws IOException {
          output.write(value);
          position++;
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
          output.write(bytes, offset, length);
          position += length;
        }

        @Override
        public long getPos() {
          return position;
        }

        @Override
        public void flush() throws IOException {
          output.flush();
        }

        @Override
        public void sync() throws IOException {
          output.flush();
        }

        @Override
        public StreamStateHandle closeAndGetHandle() throws IOException {
          output.close();
          committed = true;
          return new FileStateHandle(new org.apache.flink.core.fs.Path(path.toUri()), position);
        }

        @Override
        public void close() throws IOException {
          output.close();
          if (!committed) Files.deleteIfExists(path);
        }
      };
    }

    @Override
    public boolean canFastDuplicate(StreamStateHandle handle, CheckpointedStateScope scope) {
      return false;
    }

    @Override
    public List<StreamStateHandle> duplicate(
        List<StreamStateHandle> handles, CheckpointedStateScope scope) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reusePreviousStateHandle(Collection<? extends StreamStateHandle> handles) {
      if (failReuse) throw new IllegalStateException("reuse notification failed");
    }
  }
}
